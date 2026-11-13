package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

type recordingSink struct {
	mu      sync.Mutex
	batches [][]protocol.Event
	err     error
	delay   time.Duration
}

func (s *recordingSink) WriteBatch(_ context.Context, events []protocol.Event) (int64, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	copyOf := make([]protocol.Event, len(events))
	copy(copyOf, events)
	s.batches = append(s.batches, copyOf)
	return int64(len(events)), nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, batch := range s.batches {
		total += len(batch)
	}
	return total
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newTestCollector(sink Sink, opts Options, m *metrics.Metrics) *Collector {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC) }
	}
	opts.Logger = testLogger()
	opts.Limits = protocol.DefaultLimits()
	if opts.FlushEvery == 0 {
		opts.FlushEvery = 10 * time.Millisecond
	}
	return New(sink, opts, m)
}

func TestIngestAcceptsValidEvents(t *testing.T) {
	metricSet := metrics.New()
	collector := newTestCollector(&recordingSink{}, Options{BufferSize: 10, BatchSize: 10}, metricSet)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	events := []protocol.Event{
		{V: protocol.Version, TS: now, Queue: "emails", JobID: "1", State: protocol.StateEnqueued},
		{V: protocol.Version, TS: now, Queue: "emails", JobID: "2", State: protocol.StateCompleted},
	}

	result := collector.Ingest(context.Background(), events)

	if result.Accepted != 2 {
		t.Fatalf("expected 2 accepted events, got %d", result.Accepted)
	}
	if result.Dropped != 0 {
		t.Fatalf("expected no drops, got %d", result.Dropped)
	}
	if len(result.Rejected) != 0 {
		t.Fatalf("expected no rejections, got %v", result.Rejected)
	}
	if got := metricSet.EventsAccepted.Load(); got != 2 {
		t.Fatalf("expected the accepted counter to be 2, got %d", got)
	}
}

func TestIngestRejectsMalformedEventsWithAPerEventReason(t *testing.T) {
	metricSet := metrics.New()
	collector := newTestCollector(&recordingSink{}, Options{BufferSize: 10, BatchSize: 10}, metricSet)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	events := []protocol.Event{
		{V: protocol.Version, TS: now, Queue: "emails", JobID: "1", State: protocol.StateEnqueued},
		{V: protocol.Version, TS: now, JobID: "2", State: protocol.StateEnqueued},
		{V: protocol.Version, TS: now, Queue: "emails", JobID: "3", State: protocol.State("nope")},
		{V: 2, TS: now, Queue: "emails", JobID: "4", State: protocol.StateEnqueued},
	}

	result := collector.Ingest(context.Background(), events)

	if result.Accepted != 1 {
		t.Fatalf("expected 1 accepted event, got %d", result.Accepted)
	}
	if len(result.Rejected) != 3 {
		t.Fatalf("expected 3 rejections, got %d", len(result.Rejected))
	}
	for _, rejection := range result.Rejected {
		if rejection.Reason == "" {
			t.Fatalf("rejection %d has no reason", rejection.Index)
		}
	}
	if result.Rejected[0].Index != 1 {
		t.Fatalf("expected the first rejection to be index 1, got %d", result.Rejected[0].Index)
	}
	if got := metricSet.EventsRejected.Load(); got != 3 {
		t.Fatalf("expected the rejected counter to be 3, got %d", got)
	}
}

func TestIngestCountsAnOldEventSeparately(t *testing.T) {
	metricSet := metrics.New()
	collector := newTestCollector(&recordingSink{}, Options{BufferSize: 10, BatchSize: 10}, metricSet)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	events := []protocol.Event{
		{V: protocol.Version, TS: now.Add(-30 * 24 * time.Hour), Queue: "emails", JobID: "1", State: protocol.StateEnqueued},
	}

	result := collector.Ingest(context.Background(), events)

	if result.Accepted != 0 {
		t.Fatalf("expected the event to be rejected, got %d accepted", result.Accepted)
	}
	if got := metricSet.EventsTooOld.Load(); got != 1 {
		t.Fatalf("expected the too-old counter to be 1, got %d", got)
	}
	if got := metricSet.EventsRejected.Load(); got != 1 {
		t.Fatalf("expected the rejected counter to be 1, got %d", got)
	}
}

func TestBufferSaturationDropsAndCounts(t *testing.T) {
	metricSet := metrics.New()
	// A sink that never returns keeps the writer busy so the buffer fills.
	sink := &recordingSink{delay: 2 * time.Second}
	collector := newTestCollector(sink, Options{BufferSize: 2, BatchSize: 1}, metricSet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collector.Start(ctx)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	events := make([]protocol.Event, 0, 5)
	for i := 0; i < 5; i++ {
		events = append(events, protocol.Event{
			V: protocol.Version, TS: now, Queue: "emails", JobID: string(rune('a' + i)), State: protocol.StateEnqueued,
		})
	}

	result := collector.Ingest(context.Background(), events)

	if result.Dropped == 0 {
		t.Fatal("expected the buffer to saturate and drop events")
	}
	if result.Accepted+result.Dropped != 5 {
		t.Fatalf("expected every event to be accounted for, got %d accepted and %d dropped", result.Accepted, result.Dropped)
	}
	if got := metricSet.EventsDroppedBuffer.Load(); got != uint64(result.Dropped) {
		t.Fatalf("expected the drop counter to be %d, got %d", result.Dropped, got)
	}
}

func TestTheWriterFlushesBufferedEvents(t *testing.T) {
	sink := &recordingSink{}
	metricSet := metrics.New()
	collector := newTestCollector(sink, Options{BufferSize: 100, BatchSize: 10}, metricSet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collector.Start(ctx)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 25; i++ {
		collector.Ingest(context.Background(), []protocol.Event{{
			V: protocol.Version, TS: now, Queue: "emails", JobID: string(rune('a' + i)), State: protocol.StateEnqueued,
		}})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sink.count() == 25 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if sink.count() != 25 {
		t.Fatalf("expected 25 persisted events, got %d", sink.count())
	}
	if got := metricSet.EventsPersisted.Load(); got != 25 {
		t.Fatalf("expected the persisted counter to be 25, got %d", got)
	}
}

func TestABatchFailureIsCountedAndDoesNotStopTheWriter(t *testing.T) {
	sink := &recordingSink{err: errors.New("database is down")}
	metricSet := metrics.New()
	collector := newTestCollector(sink, Options{BufferSize: 100, BatchSize: 1}, metricSet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collector.Start(ctx)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	collector.Ingest(context.Background(), []protocol.Event{{
		V: protocol.Version, TS: now, Queue: "emails", JobID: "1", State: protocol.StateEnqueued,
	}})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if metricSet.BatchErrors.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if metricSet.BatchErrors.Load() == 0 {
		t.Fatal("expected the batch error counter to go up")
	}
	if metricSet.EventsPersisted.Load() != 0 {
		t.Fatalf("expected nothing to be persisted, got %d", metricSet.EventsPersisted.Load())
	}
}

func TestGeneratedEventIDsAreFilledIn(t *testing.T) {
	metricSet := metrics.New()
	collector := newTestCollector(&recordingSink{}, Options{BufferSize: 10, BatchSize: 10}, metricSet)

	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	result := collector.Ingest(context.Background(), []protocol.Event{{
		V: protocol.Version, TS: now, Queue: "emails", JobID: "1", State: protocol.StateEnqueued,
	}})

	if result.Accepted != 1 {
		t.Fatalf("expected the event to be accepted, got %d", result.Accepted)
	}
	if collector.Buffered() != 1 {
		t.Fatalf("expected one buffered event, got %d", collector.Buffered())
	}
}

func TestStopOnACollectorThatWasNeverStarted(t *testing.T) {
	metricSet := metrics.New()
	collector := newTestCollector(&recordingSink{}, Options{BufferSize: 10, BatchSize: 10}, metricSet)

	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on a collector that was never started")
	}
}
