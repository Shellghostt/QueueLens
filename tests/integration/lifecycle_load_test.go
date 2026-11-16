package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

type measuredWriter struct {
	db         *store.Store
	latencies  []time.Duration
	firstError error
}

func (w *measuredWriter) WriteBatch(ctx context.Context, events []protocol.Event) (int64, error) {
	started := time.Now()
	count, err := w.db.WriteBatch(ctx, events)
	w.latencies = append(w.latencies, time.Since(started))
	if err != nil && w.firstError == nil {
		w.firstError = err
	}
	return count, err
}

type loadMemorySample struct {
	Seconds       float64 `json:"seconds"`
	LiveHeapBytes uint64  `json:"live_heap_bytes"`
	Goroutines    int     `json:"goroutines"`
	PendingRows   int     `json:"pending_rows"`
	LagSeconds    float64 `json:"lag_seconds"`
}

func percentileMilliseconds(values []time.Duration, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration{}, values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return float64(ordered[int(float64(len(ordered)-1)*fraction)]) / float64(time.Millisecond)
}

func TestLifecycleSustainedProjectionMemoryAndLag(t *testing.T) {
	if os.Getenv("QUEUELENS_RUN_M3_LOAD") != "1" {
		t.Skip("opt-in sustained run: make m3-load")
	}
	duration := 60 * time.Second
	if value := os.Getenv("QUEUELENS_M3_LOAD_DURATION"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 10*time.Second || parsed > 120*time.Second {
			t.Fatal("load duration must be 10s–120s")
		}
		duration = parsed
	}
	f := newLifecycleFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), duration+60*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	policy := lifecycle.DefaultPolicy()
	writer := &measuredWriter{db: f.db}
	counters := metrics.New()
	ingest := collector.New(writer, collector.Options{BufferSize: 10000, BatchSize: 500,
		FlushEvery: 250 * time.Millisecond, Limits: protocol.DefaultLimits(), Logger: logger}, counters)
	ingest.Start(ctx)
	defer ingest.Stop()
	backgroundCtx, cancelBackground := context.WithCancel(ctx)
	var background sync.WaitGroup
	background.Go(func() { f.db.RunProjection(backgroundCtx, policy, logger) })
	background.Go(func() { f.db.RunMaintenance(backgroundCtx, policy, logger) })
	defer func() { cancelBackground(); background.Wait() }()
	server := httptest.NewServer(api.New(api.Options{Collector: ingest, Token: lifecycleToken,
		Logger: logger, Lifecycle: store.NewReader(f.db, policy, lifecycleToken)}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	const batchSize = 1000
	const interval = 200 * time.Millisecond
	batches := int(duration / interval)
	total := batches * batchSize
	var samples []loadMemorySample
	var sampleError error
	stopSamples, samplesDone := make(chan struct{}), make(chan struct{})
	started := time.Now()
	go func() {
		defer close(samplesDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopSamples:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				var pending int
				var oldest *time.Time
				if err := f.conn.QueryRow(ctx, `SELECT count(*),min(received_at)
					FROM queue_events WHERE projected_at IS NULL`).Scan(&pending, &oldest); err != nil {
					sampleError = err
					return
				}
				runtime.GC()
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				sample := loadMemorySample{Seconds: time.Since(started).Seconds(),
					LiveHeapBytes: memory.HeapAlloc, Goroutines: runtime.NumGoroutine(), PendingRows: pending}
				if oldest != nil {
					sample.LagSeconds = time.Since(*oldest).Seconds()
				}
				samples = append(samples, sample)
				t.Logf("M3_LOAD_PROGRESS elapsed=%.1fs persisted=%d pending=%d lag=%.3fs live_heap=%.2fMiB goroutines=%d",
					sample.Seconds, counters.EventsPersisted.Load(), pending, sample.LagSeconds,
					float64(sample.LiveHeapBytes)/(1<<20), sample.Goroutines)
			}
		}
	}()
	stoppedSamples := false
	stopSampler := func() {
		if !stoppedSamples {
			close(stopSamples)
			<-samplesDone
			stoppedSamples = true
		}
	}
	defer stopSampler()
	latencies := make([]time.Duration, 0, batches)
	for batch := range batches {
		events := make([]protocol.Event, 0, batchSize)
		at := time.Now().UTC()
		for i := range batchSize {
			n := batch*batchSize + i
			state := []protocol.State{protocol.StateEnqueued, protocol.StateStarted, protocol.StateCompleted}[n%3]
			event := lifecycleEvent(at, fmt.Sprintf("load-event-%09d", n), fmt.Sprintf("load-job-%09d", n/3), 1, state)
			event.WorkerID = "load-worker"
			if state == protocol.StateCompleted {
				value := 12
				event.DurationMS = &value
			}
			events = append(events, event)
		}
		body, err := json.Marshal(events)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/events", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+lifecycleToken)
		request.Header.Set("Content-Type", "application/json")
		requestStarted := time.Now()
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var ack collector.Result
		err = json.NewDecoder(response.Body).Decode(&ack)
		_ = response.Body.Close()
		latencies = append(latencies, time.Since(requestStarted))
		if err != nil || response.StatusCode != 202 || ack.Accepted != batchSize || ack.Dropped != 0 || len(ack.Rejected) != 0 {
			t.Fatal("offered traffic was not fully accepted", response.StatusCode, ack, err)
		}
		wait := time.Until(started.Add(time.Duration(batch+1) * interval))
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			case <-timer.C:
			}
		}
	}
	elapsed := time.Since(started)
	stopSampler()
	if sampleError != nil {
		t.Fatal(sampleError)
	}
	ingest.Stop()
	if writer.firstError != nil {
		t.Fatal("storage failure under load", writer.firstError)
	}
	drainStarted := time.Now()
	drainCtx, cancelDrain := context.WithTimeout(ctx, 30*time.Second)
	defer cancelDrain()
	for {
		var pending int
		if err := f.conn.QueryRow(drainCtx, "SELECT count(*) FROM queue_events WHERE projected_at IS NULL").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-drainCtx.Done():
			timer.Stop()
			t.Fatal("projection did not drain within 30 seconds")
		case <-timer.C:
		}
	}
	var raw, runs int
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM queue_events").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM job_runs").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if raw != total || runs != (total+2)/3 || counters.EventsPersisted.Load() != uint64(total) ||
		counters.EventsLost.Load() != 0 || counters.BatchErrors.Load() != 0 || counters.EventsDroppedBuffer.Load() != 0 {
		t.Fatalf("load accounting events=%d raw=%d runs=%d persisted=%d lost=%d errors=%d dropped=%d",
			total, raw, runs, counters.EventsPersisted.Load(), counters.EventsLost.Load(), counters.BatchErrors.Load(), counters.EventsDroppedBuffer.Load())
	}
	var peakHeap uint64
	var peakPending, peakGoroutines int
	var peakLag float64
	for _, sample := range samples {
		peakHeap = max(peakHeap, sample.LiveHeapBytes)
		peakPending = max(peakPending, sample.PendingRows)
		peakGoroutines = max(peakGoroutines, sample.Goroutines)
		peakLag = max(peakLag, sample.LagSeconds)
	}
	p50, p99 := percentileMilliseconds(latencies, 0.5), percentileMilliseconds(latencies, 0.99)
	report := struct {
		GoVersion                                              string `json:"go_version"`
		GOMAXPROCS                                             int    `json:"gomaxprocs"`
		Events, RawRows, RunRows                               int
		DurationSeconds, EventsPerSecond, HTTPP50MS, HTTPP99MS float64
		StorageBatch500P50MS, StorageBatch500P99MS             float64
		DrainSeconds                                           float64
		PeakLiveHeapBytes                                      uint64
		PeakPendingRows, PeakGoroutines                        int
		PeakLagSeconds                                         float64
		Samples                                                []loadMemorySample `json:"samples"`
	}{runtime.Version(), runtime.GOMAXPROCS(0), total, raw, runs, elapsed.Seconds(), float64(total) / elapsed.Seconds(),
		p50, p99, percentileMilliseconds(writer.latencies, 0.5), percentileMilliseconds(writer.latencies, 0.99),
		time.Since(drainStarted).Seconds(), peakHeap, peakPending, peakGoroutines, peakLag, samples}
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("M3_LOAD_RESULT %s", body)
	if p99 >= 50 {
		t.Fatalf("unchanged 50ms ingest budget exceeded: p99=%.3fms", p99)
	}
	if peakHeap > 128<<20 {
		t.Fatalf("synthetic live-heap cap exceeded: %d bytes", peakHeap)
	}
}
