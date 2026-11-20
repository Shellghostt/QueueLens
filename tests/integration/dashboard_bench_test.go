package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/adapter"
	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/internal/config"
	"github.com/shellghostt/queuelens/internal/dashboard"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/internal/queues"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func dashboardSeedAttempt(job, attempt int, now time.Time, expired bool) lifecycle.Attempt {
	// Divide first: multiplying 100,000 by a nanosecond month overflows int64.
	at := now.Add(-30*24*time.Hour + 30*time.Minute + time.Duration(job)*((30*24*time.Hour-time.Hour)/100000))
	if expired {
		at = now.Add(-31 * 24 * time.Hour)
	}
	state := protocol.StateFailed
	if attempt == 3 && job%5 != 0 {
		state = protocol.StateCompleted
	}
	var result lifecycle.Attempt
	duration := 12
	for i, observed := range []protocol.State{protocol.StateEnqueued, protocol.StateStarted, state} {
		fact, _ := lifecycle.FromFact(lifecycle.Fact{Attempt: attempt, State: observed, TS: at.Add(time.Duration(attempt*3+i) * time.Second),
			ReceivedAt: at.Add(time.Duration(attempt*3+i) * time.Second), IdentityKind: "provided", DurationMS: &duration,
			JobName: "Synthetic retained task", WorkerID: fmt.Sprintf("worker-%03d", job%100)})
		result = lifecycle.MergeAttempt(result, fact)
	}
	return result
}

// Seed rows stream through COPY; no 300,000-element materialized slice.
type dashboardSeedSource struct {
	now               time.Time
	count, index      int
	attempts, expired bool
}

func (s *dashboardSeedSource) Next() bool {
	if s.index >= s.count {
		return false
	}
	s.index++
	return true
}
func (s *dashboardSeedSource) Err() error { return nil }
func (s *dashboardSeedSource) Values() ([]any, error) {
	n := s.index - 1
	job := n
	if s.attempts {
		job = n / 3
	}
	queue, id := fmt.Sprintf("queue-%03d", job%100), fmt.Sprintf("job-%06d", job)
	if s.expired {
		id = fmt.Sprintf("expired-%06d", job)
	}
	if s.attempts {
		attempt := dashboardSeedAttempt(job, n%3+1, s.now, s.expired)
		body, err := json.Marshal(attempt)
		return []any{queue, id, int16(attempt.Number), body, string(attempt.Selected.State), attempt.Selected.TS, attempt.Evidence.TerminalConflict}, err
	}
	run := lifecycle.Run{Queue: queue, JobID: id}
	for attempt := 1; attempt <= 3; attempt++ {
		run = lifecycle.ReplaceAttempt(run, nil, dashboardSeedAttempt(job, attempt, s.now, s.expired))
	}
	body, err := json.Marshal(run)
	return []any{queue, id, int16(3), 3, run.IncompleteAttempts, string(run.Current.Selected.State), run.Evidence.FirstReceiptAt,
		run.Evidence.LastReceiptAt, run.CompleteObserved(), body}, err
}

func seedDashboardCorpus(t *testing.T, f lifecycleFixture, ctx context.Context) {
	t.Helper()
	for _, expired := range []bool{false, true} {
		jobs := 100000
		if expired {
			jobs = 20
		}
		if _, err := f.conn.CopyFrom(ctx, pgx.Identifier{"job_runs"}, []string{"queue", "job_id", "reported_attempt", "observed_attempts",
			"incomplete_attempts", "state", "first_received_at", "last_received_at", "complete_observed", "summary"},
			&dashboardSeedSource{now: f.now, count: jobs, expired: expired}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.conn.CopyFrom(ctx, pgx.Identifier{"job_attempts"}, []string{"queue", "job_id", "attempt", "summary", "selected_state", "selected_at", "selected_conflict"},
			&dashboardSeedSource{now: f.now, count: jobs * 3, attempts: true, expired: expired}); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"job_runs", "job_attempts"} {
		if _, err := f.conn.Exec(ctx, "VACUUM (PARALLEL 0, ANALYZE) "+table); err != nil {
			t.Fatal(err)
		}
	}
	var jobs, attempts int64
	var oldest, newest time.Time
	if err := f.conn.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id NOT LIKE 'expired-%'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := f.conn.QueryRow(ctx, `SELECT count(*),min(selected_at),max(selected_at) FROM job_attempts WHERE job_id NOT LIKE 'expired-%'`).Scan(&attempts, &oldest, &newest); err != nil {
		t.Fatal(err)
	}
	if jobs != 100000 || attempts != 300000 || oldest.Before(f.now.Add(-30*24*time.Hour)) || !newest.Before(f.now) || newest.Sub(oldest) < 29*24*time.Hour {
		t.Fatalf("invalid retained corpus jobs=%d attempts=%d oldest=%s newest=%s", jobs, attempts, oldest, newest)
	}
}

type dashboardRequestSample struct {
	View             string  `json:"view"`
	Stage            string  `json:"stage"`
	Milliseconds     float64 `json:"milliseconds"`
	Status           int     `json:"status"`
	Error            string  `json:"error,omitempty"`
	MetricsAvailable *bool   `json:"metrics_available,omitempty"`
	LagKnown         bool    `json:"lag_known"`
}
type dashboardRouteResult struct {
	Requests int     `json:"requests"`
	P50      float64 `json:"p50_ms"`
	P95      float64 `json:"p95_ms"`
	Max      float64 `json:"max_ms"`
}

func nearestRank(values []float64, fraction float64) float64 {
	ordered := append([]float64{}, values...)
	sort.Float64s(ordered)
	if len(ordered) == 0 {
		return 0
	}
	return ordered[max(0, int(math.Ceil(float64(len(ordered))*fraction))-1)]
}

func TestDashboardThirtyDayQueryBudget(t *testing.T) {
	if os.Getenv("QUEUELENS_RUN_M4_BENCH") != "1" {
		t.Skip("opt-in 30-day retained-summary workload: make m4-bench")
	}
	f := newLifecycleFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	seedStart := time.Now()
	seedDashboardCorpus(t, f, ctx)
	seedSeconds := time.Since(seedStart).Seconds()
	t.Logf("M4_BENCH_SEEDED jobs=100000 attempts=300000 queues=100 expired_jobs=20 seed_seconds=%.2f", seedSeconds)
	policy := lifecycle.DefaultPolicy()
	broker := queues.NewStore()
	counts := make([]adapter.Counts, 100)
	for i := range counts {
		counts[i] = adapter.Counts{Queue: fmt.Sprintf("queue-%03d", i), Waiting: int64(i + 1), Active: int64(i % 10), Completed: 800, Failed: 200}
		if err := f.db.Heartbeat(ctx, protocol.Heartbeat{V: 1, WorkerID: fmt.Sprintf("worker-%03d", i), Pool: "benchmark"}); err != nil {
			t.Fatal(err)
		}
	}
	broker.Apply("synthetic-broker", counts, f.now)
	var plan json.RawMessage
	if err := f.conn.QueryRow(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON)
		SELECT count(*),count(*) FILTER(WHERE selected_state='completed') FROM (
		SELECT a.selected_state,a.selected_conflict FROM job_attempts a
		WHERE a.selected_state IN ('completed','failed') AND a.selected_at >= $1 AND a.selected_at < $2
		AND NOT EXISTS(SELECT 1 FROM job_runs r WHERE r.pruning_at IS NOT NULL AND r.queue=a.queue AND r.job_id=a.job_id)
		LIMIT 500001) eligible`, policy.EvaluationAt(f.now).Add(-30*24*time.Hour), policy.EvaluationAt(f.now)).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	counters := metrics.New()
	ingest := collector.New(f.db, collector.Options{BufferSize: 10000, BatchSize: 500, FlushEvery: 100 * time.Millisecond, Limits: protocol.DefaultLimits(), Logger: logger}, counters)
	ingest.Start(ctx)
	defer ingest.Stop()
	server := httptest.NewUnstartedServer(nil)
	origin := "http://" + server.Listener.Addr().String()
	reader := store.NewReader(f.db, policy, lifecycleToken)
	server.Config.Handler = api.New(api.Options{Collector: ingest, Token: lifecycleToken, Queues: broker, Metrics: counters, Logger: logger,
		Lifecycle: reader, Dashboard: reader, DashboardAccess: config.DashboardAccess{Password: "m4-benchmark-viewer-password", Origin: origin, InsecureDev: true}})
	server.Start()
	defer server.Close()
	client := server.Client()
	client.Timeout = 4 * time.Second
	client.Jar, _ = cookiejar.New(nil)
	login, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/dashboard/session", stringsReader(`{"password":"m4-benchmark-viewer-password"}`))
	if err != nil {
		t.Fatal(err)
	}
	login.Header.Set("Origin", origin)
	login.Header.Set("Content-Type", "application/json")
	ack, err := client.Do(login)
	if err != nil {
		t.Fatal(err)
	}
	_ = ack.Body.Close()
	if ack.StatusCode != 200 {
		t.Fatal("benchmark login", ack.StatusCode)
	}
	backgroundCtx, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	var accepted, projected, maintenance, deleted atomic.Int64
	failures := make(chan error, 3)
	stopIngest, ingestDone := make(chan struct{}), make(chan struct{})
	workers.Go(func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-backgroundCtx.Done():
				return
			case <-ticker.C:
				batch, err := f.db.ProjectBatch(backgroundCtx, time.Now(), policy)
				if err != nil {
					if backgroundCtx.Err() == nil {
						failures <- err
					}
					return
				}
				projected.Add(int64(batch.Processed))
			}
		}
	})
	workers.Go(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-backgroundCtx.Done():
				return
			case <-ticker.C:
				batch, err := f.db.MaintainBatch(backgroundCtx, time.Now(), policy)
				if err != nil {
					if backgroundCtx.Err() == nil {
						failures <- err
					}
					return
				}
				maintenance.Add(1)
				deleted.Add(batch.Runs)
			}
		}
	})
	workers.Go(func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer close(ingestDone)
		defer ticker.Stop()
		batch := 0
		for {
			select {
			case <-stopIngest:
				return
			case <-backgroundCtx.Done():
				return
			case <-ticker.C:
				var events []protocol.Event
				for i := 0; i < 99; i++ {
					n := batch*99 + i
					e := lifecycleEvent(time.Now().UTC(), fmt.Sprintf("m4-background-event-%08d", n), fmt.Sprintf("m4-background-job-%08d", n/3), 1,
						[]protocol.State{protocol.StateEnqueued, protocol.StateStarted, protocol.StateCompleted}[n%3])
					e.Queue = fmt.Sprintf("queue-%03d", n/3%100)
					events = append(events, e)
				}
				body, _ := json.Marshal(events)
				req, err := http.NewRequestWithContext(backgroundCtx, "POST", server.URL+"/v1/events", bytes.NewReader(body))
				if err != nil {
					failures <- err
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+lifecycleToken)
				response, err := client.Do(req)
				if err != nil {
					if backgroundCtx.Err() == nil {
						failures <- err
					}
					return
				}
				var result collector.Result
				err = json.NewDecoder(response.Body).Decode(&result)
				_ = response.Body.Close()
				if err != nil || response.StatusCode != 202 || result.Accepted != 99 || result.Dropped != 0 || len(result.Rejected) != 0 {
					failures <- fmt.Errorf("benchmark ingest acknowledgement: status=%d accepted=%d dropped=%d error=%v", response.StatusCode, result.Accepted, result.Dropped, err)
					return
				}
				accepted.Add(99)
				batch++
			}
		}
	})
	stopped := false
	finish := func() {
		if !stopped {
			close(stopIngest)
			<-ingestDone
			stop()
			workers.Wait()
			stopped = true
		}
	}
	defer finish()
	views := []struct{ view, path string }{
		{"overview", "overview?window=30d"}, {"queues", "queues"}, {"queue", "queue?queue=queue-010&window=30d"},
		{"jobs", "jobs?state=all"}, {"job", "job?queue=queue-000&job_id=job-000000"}, {"workers", "workers"},
	}
	var samples []dashboardRequestSample
	requestStart := time.Now()
	for round := 0; round < 42; round++ {
		stage := "sample"
		if round == 0 {
			stage = "first_http_read_after_plan_probe"
		}
		if round == 1 {
			stage = "warmup"
		}
		for _, view := range views {
			start := time.Now()
			sample := dashboardRequestSample{View: view.view, Stage: stage}
			req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/dashboard/api/"+view.path, nil)
			response, err := client.Do(req)
			if err != nil {
				sample.Error = err.Error()
			} else {
				sample.Status = response.StatusCode
				var result dashboard.Response
				err = json.NewDecoder(io.LimitReader(response.Body, dashboard.MaxResponseBytes+1)).Decode(&result)
				_ = response.Body.Close()
				if err != nil {
					sample.Error = err.Error()
				}
				sample.LagKnown = result.ProjectionLag.Known
				if result.Metrics != nil {
					available := result.Metrics.Available
					sample.MetricsAvailable = &available
					if !available {
						sample.Error = "metrics unavailable: " + result.Metrics.Reason
					}
				}
			}
			sample.Milliseconds = float64(time.Since(start)) / float64(time.Millisecond)
			samples = append(samples, sample)
		}
	}
	finish()
	// Flush accepted receipts and prove real projection/maintenance happened.
	ingest.Stop()
	for i := 0; i < 100; i++ {
		batch, err := f.db.ProjectBatch(ctx, time.Now(), policy)
		if err != nil {
			t.Fatal(err)
		}
		projected.Add(int64(batch.Processed))
		if batch.Processed == 0 && batch.Wrapped {
			break
		}
	}
	close(failures)
	for err := range failures {
		t.Error("concurrent work", err)
	}
	if accepted.Load() == 0 || projected.Load() != accepted.Load() || maintenance.Load() == 0 || deleted.Load() != 20 || counters.BatchErrors.Load() != 0 || counters.EventsLost.Load() != 0 {
		t.Errorf("concurrency evidence accepted=%d projected=%d maintenance=%d expired_runs=%d write_errors=%d", accepted.Load(), projected.Load(), maintenance.Load(), deleted.Load(), counters.BatchErrors.Load())
	}
	routes := map[string]dashboardRouteResult{}
	all := []float64{}
	errorsCount, unavailable, lagUnknown := 0, 0, 0
	for _, view := range views {
		values := []float64{}
		for _, sample := range samples {
			if sample.View == view.view {
				values = append(values, sample.Milliseconds)
			}
		}
		routes[view.view] = dashboardRouteResult{Requests: len(values), P50: nearestRank(values, .5), P95: nearestRank(values, .95), Max: nearestRank(values, 1)}
		if nearestRank(values, .95) >= 300 {
			t.Errorf("%s p95 %.2fms >=300ms", view.view, nearestRank(values, .95))
		}
	}
	for _, sample := range samples {
		all = append(all, sample.Milliseconds)
		if sample.Status != 200 || sample.Error != "" {
			errorsCount++
		}
		if sample.MetricsAvailable != nil && !*sample.MetricsAvailable {
			unavailable++
		}
		if !sample.LagKnown {
			lagUnknown++
		}
	}
	if errorsCount != 0 || unavailable != 0 || lagUnknown != 0 {
		t.Errorf("request errors=%d unavailable metrics=%d unknown lag=%d", errorsCount, unavailable, lagUnknown)
	}
	record := map[string]any{"go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "logical_cpus": runtime.NumCPU(),
		"seed_queues": 100, "seed_jobs": 100000, "seed_attempts": 300000, "expired_maintenance_jobs": 20, "history_days": 30, "seed_seconds": seedSeconds,
		"cache":           "VACUUM ANALYZE, one EXPLAIN ANALYZE probe; first HTTP reads, warmup and all subsequent reads included",
		"request_seconds": time.Since(requestStart).Seconds(), "requests": len(samples), "errors": errorsCount, "unavailable_metrics": unavailable, "unknown_lag": lagUnknown,
		"p50_ms": nearestRank(all, .5), "p95_ms": nearestRank(all, .95), "routes": routes, "accepted_events": accepted.Load(), "projected_events": projected.Load(),
		"maintenance_batches": maintenance.Load(), "expired_runs_removed": deleted.Load(), "write_errors": counters.BatchErrors.Load(), "global_metric_plan": plan, "samples": samples}
	body, _ := json.Marshal(record)
	t.Logf("M4_BENCH_RESULT %s", body)
	if path := os.Getenv("QUEUELENS_M4_BENCH_RECORD"); path != "" {
		pretty, _ := json.MarshalIndent(record, "", "  ")
		if err := os.WriteFile(path, append(pretty, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func stringsReader(value string) io.Reader { return bytes.NewBufferString(value) }
