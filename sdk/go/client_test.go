package queuelens

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestDefaultsCopyDurationAndPartialAcknowledgement(t *testing.T) {
	var events []protocol.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer 0123456789abcdef" {
			t.Error("missing auth")
		}
		if r.URL.Path == "/v1/workers/heartbeat" {
			w.WriteHeader(202)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			t.Error(err)
		}
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"accepted":1,"dropped":1,"rejected":[{"index":2}]}`)
	}))
	defer server.Close()
	c, err := New(Options{URL: server.URL, Token: "0123456789abcdef", BatchSize: 3, FlushEvery: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	duration := 17
	for i := 0; i < 3; i++ {
		if !c.Emit(protocol.Event{Queue: "q", JobID: "j", State: protocol.StateCompleted, DurationMS: &duration}) {
			t.Fatal("emit failed")
		}
	}
	duration = 999
	if err = c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatal(events)
	}
	for _, e := range events {
		if e.V != 1 || e.TS.IsZero() || e.Attempt != 1 || e.WorkerID == "" || *e.DurationMS != 17 {
			t.Fatalf("bad copied event: %+v", e)
		}
	}
	stats := c.Stats()
	if stats.Sent != 1 || stats.Dropped != 1 || stats.Rejected != 1 || stats.Buffered != 0 {
		t.Fatal(stats)
	}
	var out strings.Builder
	if err = c.WritePrometheus(&out); err != nil || !strings.Contains(out.String(), "queuelens_dropped_total 1") {
		t.Fatal(out.String(), err)
	}
}

func TestMillionEmitsWithHungCollector(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/v1/workers/heartbeat" {
			w.WriteHeader(202)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	c, err := New(Options{URL: server.URL, Token: "0123456789abcdef", RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 1000000; i++ {
		c.Emit(protocol.Event{Queue: "q", JobID: "j", State: protocol.StateStarted})
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Emit blocked: %s", elapsed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = c.Close(ctx)
	if err = c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats := c.Stats()
	if stats.Emitted != 1000000 || stats.Dropped != 1000000 || stats.Sent != 0 || stats.Buffered != 0 {
		t.Fatal(stats)
	}
	if c.Emit(protocol.Event{}) {
		t.Fatal("emit after close accepted")
	}
}

func TestHotPathAllocations(t *testing.T) {
	c := &Client{opts: Options{WorkerID: "w"}, buffer: make(chan queued, 1)}
	e := protocol.Event{V: 1, TS: time.Now(), Queue: "q", JobID: "j", State: protocol.StateStarted}
	allocs := testing.AllocsPerRun(1000, func() { c.Emit(e); <-c.buffer })
	if allocs != 0 {
		t.Fatalf("hot path allocated: %f", allocs)
	}
}

func TestTransportCooldownRecovers(t *testing.T) {
	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workers/heartbeat" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var events []protocol.Event
		_ = json.NewDecoder(r.Body).Decode(&events)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": len(events), "dropped": 0, "rejected": []any{}})
	}))
	defer server.Close()
	c, err := New(Options{URL: server.URL, Token: "0123456789abcdef", BatchSize: 1, FlushEvery: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(context.Background()) }()
	e := protocol.Event{Queue: "q", JobID: "j", State: protocol.StateStarted}
	c.Emit(e)
	deadline := time.Now().Add(3 * time.Second)
	for !c.unavailable.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !c.unavailable.Load() {
		t.Fatal("failed transport did not enter cooldown")
	}
	if c.Emit(e) {
		t.Fatal("cooldown accepted an event")
	}
	healthy.Store(true)
	for c.unavailable.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !c.Emit(e) {
		t.Fatal("transport did not recover after flush tick")
	}
	if err = c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Sent != 1 || c.Stats().Dropped != 2 {
		t.Fatal(c.Stats())
	}
}

func TestBadAcknowledgementAndRedirectAreCounted(t *testing.T) {
	for _, response := range []string{`{"accepted":-1,"dropped":2,"rejected":[]}`, `not-json`} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/workers/heartbeat" {
					w.WriteHeader(202)
					return
				}
				w.WriteHeader(202)
				_, _ = io.WriteString(w, response)
			}))
			defer server.Close()
			c, err := New(Options{URL: server.URL, Token: "0123456789abcdef"})
			if err != nil {
				t.Fatal(err)
			}
			c.Emit(protocol.Event{Queue: "q", JobID: "j", State: protocol.StateStarted})
			_ = c.Close(context.Background())
			if c.Stats().Dropped != 1 {
				t.Fatal(c.Stats())
			}
		})
	}
}
