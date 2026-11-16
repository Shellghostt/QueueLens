package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestLifecycleHTTPIdentityAndAsyncConflictKeepAcceptanceContract(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	counters := metrics.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingest := collector.New(f.db, collector.Options{BufferSize: 10, BatchSize: 1,
		FlushEvery: 10 * time.Millisecond, Limits: protocol.DefaultLimits(), Logger: logger,
		Now: func() time.Time { return f.now }}, counters)
	ingest.Start(ctx)
	defer ingest.Stop()
	server := httptest.NewServer(api.New(api.Options{Collector: ingest, Token: lifecycleToken, Logger: logger}))
	defer server.Close()
	missing := lifecycleEvent(f.now, "", "missing-http", 1, protocol.StateStarted)
	provided := lifecycleEvent(f.now, "provided-http", "provided-http", 1, protocol.StateStarted)
	conflict := provided
	conflict.State = protocol.StateCompleted
	for _, event := range []protocol.Event{missing, missing, provided, provided, conflict} {
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/events", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+lifecycleToken)
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var ack collector.Result
		err = json.NewDecoder(response.Body).Decode(&ack)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != 202 || ack.Accepted != 1 || ack.Dropped != 0 || len(ack.Rejected) != 0 {
			t.Fatal("HTTP buffer acceptance changed", response.StatusCode, ack, err)
		}
	}
	ingest.Stop()
	if sqlCount(t, f, "SELECT count(*) FROM queue_events WHERE identity_kind='missing' AND event_id IS NULL") != 2 ||
		sqlCount(t, f, "SELECT count(*) FROM queue_events WHERE identity_kind='provided' AND event_id='provided-http'") != 1 ||
		sqlCount(t, f, "SELECT count(*) FROM event_identities") != 1 {
		t.Fatal("wire identity was invented, duplicated or overwritten")
	}
	if counters.EventsAccepted.Load() != 5 || counters.EventsPersisted.Load() != 3 ||
		counters.BatchErrors.Load() != 1 || counters.EventsLost.Load() != 1 {
		t.Fatal("async conflict was not counted without changing acceptance")
	}
}
