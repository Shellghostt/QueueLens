package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/internal/queues"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

const testToken = "0123456789abcdef"

// nullSink accepts every batch and stores nothing.
type nullSink struct{}

func (nullSink) WriteBatch(_ context.Context, events []protocol.Event) (int64, error) {
	return int64(len(events)), nil
}

// blockingSink never returns, so the collector's buffer fills up.
type blockingSink struct{}

func (blockingSink) WriteBatch(_ context.Context, _ []protocol.Event) (int64, error) {
	select {}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newTestServer(t *testing.T, sink collector.Sink, opts collector.Options) (http.Handler, *metrics.Metrics) {
	t.Helper()

	metricSet := metrics.New()
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC) }
	}
	opts.Logger = quietLogger()
	opts.Limits = protocol.DefaultLimits()
	if opts.BufferSize == 0 {
		opts.BufferSize = 100
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	if sink == nil {
		sink = nullSink{}
	}

	ingest := collector.New(sink, opts, metricSet)
	store := queues.NewStore()

	handler := New(Options{
		Collector: ingest,
		Queues:    store,
		Metrics:   metricSet,
		Token:     testToken,
		Version:   "0.1.0-test",
		Logger:    opts.Logger,
		Now:       opts.Now,
	})
	return handler, metricSet
}

func TestHealthzNeedsNoToken(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "0.1.0-test") {
		t.Fatalf("expected the version in the body, got %s", recorder.Body.String())
	}
}

func TestReadyzReportsAFailingDatabase(t *testing.T) {
	failing := New(Options{
		Collector: collector.New(nullSink{}, collector.Options{Logger: quietLogger()}, metrics.New()),
		Queues:    queues.NewStore(),
		Metrics:   metrics.New(),
		Token:     testToken,
		Ping: func(context.Context) error {
			return errors.New("database is unreachable")
		},
		Logger: quietLogger(),
	})

	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	failing.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", recorder.Code)
	}
}

func TestPrivateEndpointsRequireABearerToken(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	cases := []struct {
		name   string
		header string
	}{
		{"no header", ""},
		{"wrong scheme", "Basic " + testToken},
		{"wrong token", "Bearer wrongtokenwrongtoken"},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader("[]"))
			if test.header != "" {
				request.Header.Set("Authorization", test.header)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", recorder.Code)
			}
		})
	}
}

func TestIngestAcceptsAValidBatch(t *testing.T) {
	handler, metricSet := newTestServer(t, nil, collector.Options{})

	body := `[{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","job_id":"1","state":"enqueued"}]`
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Accepted int `json:"accepted"`
		Dropped  int `json:"dropped"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.Accepted != 1 {
		t.Fatalf("expected 1 accepted event, got %d", response.Accepted)
	}
	if metricSet.EventsAccepted.Load() != 1 {
		t.Fatalf("expected the accepted counter to be 1, got %d", metricSet.EventsAccepted.Load())
	}
}

func TestIngestAcceptsASingleEventObject(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	body := `{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","job_id":"1","state":"enqueued"}`
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestIngestRejectsMalformedEventsWithAReason(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	body := `[{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","state":"enqueued"}]`
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "job_id") {
		t.Fatalf("expected the reason to name the field, got %s", recorder.Body.String())
	}
}

func TestIngestRejectsAnEmptyBatch(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader("[]"))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestIngestRejectsABodyThatIsNotJSON(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader("not json"))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func TestIngestRejectsMoreThanOneThousandEvents(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	events := make([]map[string]any, 1001)
	for i := range events {
		events[i] = map[string]any{
			"v":      1,
			"ts":     "2026-11-13T10:00:00Z",
			"queue":  "emails",
			"job_id": "1",
			"state":  "enqueued",
		}
	}
	body, err := json.Marshal(events)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "1000") {
		t.Fatalf("expected the limit in the message, got %s", recorder.Body.String())
	}
}

func TestBufferSaturationReturns429(t *testing.T) {
	// The sink never returns, so the buffer fills up.
	handler, metricSet := newTestServer(t, blockingSink{}, collector.Options{
		BufferSize: 1,
		BatchSize:  1,
	})

	body := `[` +
		`{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","job_id":"1","state":"enqueued"},` +
		`{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","job_id":"2","state":"enqueued"},` +
		`{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","job_id":"3","state":"enqueued"}]`

	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Retry-After") == "" {
		t.Fatal("expected a Retry-After header")
	}
	if metricSet.EventsDroppedBuffer.Load() == 0 {
		t.Fatal("expected the drop counter to go up")
	}
}

func TestIngestRejectsABodyWithTwoDocuments(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	body := `{"v":1,"ts":"2026-11-13T10:00:00Z","queue":"emails","job_id":"1","state":"enqueued"} {"v":1}`
	request := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestMetricsEndpointIsPublicAndExposesTheCollectorCounters(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "queuelens_buffer_dropped_total") {
		t.Fatalf("expected the collector counters, got %s", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "go_goroutines") {
		t.Fatal("M1 exposes the collector counters only, not the Go runtime metrics")
	}
}

func TestQueuesEndpointReturnsTheCurrentState(t *testing.T) {
	handler, _ := newTestServer(t, nil, collector.Options{})

	request := httptest.NewRequest(http.MethodGet, "/v1/queues", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	var response struct {
		Queues []queues.View `json:"queues"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.Queues == nil {
		t.Fatal("expected an empty array, not null")
	}
}
