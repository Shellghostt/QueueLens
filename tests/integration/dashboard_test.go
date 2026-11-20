package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/adapter"
	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/dashboard"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/queues"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func dashboardServer(f lifecycleFixture, policy lifecycle.Policy, broker *queues.Store) *httptest.Server {
	reader := store.NewReader(f.db, policy, lifecycleToken)
	return httptest.NewServer(api.New(api.Options{Token: lifecycleToken, Queues: broker,
		Now: func() time.Time { return f.now }, Lifecycle: reader, Dashboard: reader}))
}

func dashboardGET(t *testing.T, server *httptest.Server, view string, values url.Values) dashboard.Response {
	t.Helper()
	var result dashboard.Response
	if code := lifecycleGET(t, server, "/v1/dashboard/"+view+"?"+values.Encode(), lifecycleToken, &result); code != 200 {
		t.Fatalf("%s returned %d", view, code)
	}
	return result
}

func TestDashboardAPIAuthIdentifiersFiltersAndBoundCursors(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	broker := queues.NewStore()
	queue := "jobs/failed ' % 雪"
	ids := []string{"Z", "a/queue?%'雪", "workers"}
	for i, id := range ids {
		event := lifecycleEvent(f.now.Add(-time.Minute), fmt.Sprintf("m4-key-%d", i), id, 1, protocol.StateFailed)
		event.Queue = queue
		mustWrite(t, f, event)
	}
	drainProjection(t, f, f.now, policy)
	broker.Apply("test", []adapter.Counts{{Queue: queue, Waiting: 7}, {Queue: "Z", Active: 2}}, f.now)
	server := dashboardServer(f, policy, broker)
	defer server.Close()
	for _, view := range []string{"overview", "queues", "queue", "jobs", "job"} {
		if code := lifecycleGET(t, server, "/v1/dashboard/"+view, "", nil); code != 401 {
			t.Fatalf("%s unauthenticated = %d", view, code)
		}
	}
	values := url.Values{"queue": {queue}, "state": {"all"}, "limit": {"2"}}
	first := dashboardGET(t, server, "jobs", values)
	if first.Jobs == nil || len(first.Jobs.Items) != 2 || first.Jobs.Items[0].JobID != ids[0] ||
		first.Jobs.Items[1].JobID != ids[1] || first.Jobs.NextCursor == "" {
		t.Fatalf("first C-order page: %+v", first.Jobs)
	}
	values.Set("cursor", first.Jobs.NextCursor)
	last := dashboardGET(t, server, "jobs", values)
	if len(last.Jobs.Items) != 1 || last.Jobs.Items[0].JobID != ids[2] || last.Jobs.NextCursor != "" ||
		!last.EvaluatedAt.Equal(first.EvaluatedAt) {
		t.Fatalf("last page: %+v", last)
	}
	for _, change := range []struct{ view, key, value string }{
		{"queue", "window", "1h"}, {"jobs", "queue", "other"}, {"jobs", "state", "failed"},
		{"jobs", "cursor", first.Jobs.NextCursor + "x"},
	} {
		changed := url.Values{"queue": {queue}, "state": {"all"}, "limit": {"2"}, "cursor": {first.Jobs.NextCursor}}
		changed.Set(change.key, change.value)
		if code := lifecycleGET(t, server, "/v1/dashboard/"+change.view+"?"+changed.Encode(), lifecycleToken, nil); code != 400 {
			t.Fatalf("cursor binding %s/%s = %d", change.view, change.key, code)
		}
	}
	detail := dashboardGET(t, server, "job", url.Values{"queue": {queue}, "job_id": {ids[1]}})
	if detail.Job.Availability != "observed" || detail.Job.Summary.JobID != ids[1] ||
		detail.Job.Summary.Queue != queue || detail.Job.Summary.RetryExhausted != nil {
		t.Fatalf("exact identifier detail: %+v", detail.Job)
	}
	q := dashboardGET(t, server, "queue", url.Values{"queue": {queue}, "state": {"failed"}})
	if q.Broker == nil || q.Broker.Depth != 7 || len(q.Jobs.Items) != 3 {
		t.Fatalf("queue detail: %+v", q)
	}
	for _, raw := range []string{"limit=0", "limit=201", "limit=1&limit=2", "sort=sql", "state=dead", "queue=%FF", "queue=%xx",
		"queue=" + url.QueryEscape(strings.Repeat("x", 257)), "queue=+", "cursor=" + strings.Repeat("x", 4097),
		"queue=" + strings.Repeat("x", 12300)} {
		if code := lifecycleGET(t, server, "/v1/dashboard/jobs?"+raw, lifecycleToken, nil); code != 400 {
			t.Fatalf("invalid query %q = %d", raw[:min(len(raw), 80)], code)
		}
	}
	page := dashboardGET(t, server, "queues", url.Values{"limit": {"1"}})
	if len(page.Queues.Items) != 1 || page.Queues.Items[0].Queue != "Z" || page.Queues.NextCursor == "" {
		t.Fatalf("broker page: %+v", page.Queues)
	}
	if code := lifecycleGET(t, server, "/v1/dashboard/overview?cursor="+url.QueryEscape(page.Queues.NextCursor), lifecycleToken, nil); code != 400 {
		t.Fatalf("broker cursor route binding=%d", code)
	}
	overview := dashboardGET(t, server, "overview", url.Values{"limit": {"1"}, "window": {"1h"}})
	if code := lifecycleGET(t, server, "/v1/dashboard/overview?window=24h&cursor="+url.QueryEscape(overview.Queues.NextCursor), lifecycleToken, nil); code != 400 {
		t.Fatalf("metric window cursor binding=%d", code)
	}
}

func TestDashboardNanosecondWindowEdgesAndObservedStallPolicy(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	at := policy.EvaluationAt(f.now)
	for i, ts := range []time.Time{at.Add(-time.Hour - time.Nanosecond), at.Add(-time.Hour), at.Add(-time.Nanosecond), at} {
		mustWrite(t, f, lifecycleEvent(ts, fmt.Sprintf("edge-%d", i), fmt.Sprintf("edge-%d", i), 1, protocol.StateCompleted))
	}
	for _, state := range protocol.States() {
		mustWrite(t, f, lifecycleEvent(at.Add(-time.Minute), "state-"+string(state), "state-"+string(state), 1, state))
	}
	drainProjection(t, f, f.now, policy)
	// Receipt time alone drives suspicion, irrespective of supplied start clocks.
	for _, state := range []string{"enqueued", "started", "retried", "stalled", "failed", "completed"} {
		mustSQL(t, f, `UPDATE job_runs SET last_received_at=$1,summary=jsonb_set(summary,'{evidence,LastReceiptAt}',to_jsonb($1::timestamptz)) WHERE job_id=$2`, at.Add(-policy.StallAfter), "state-"+state)
	}
	server := dashboardServer(f, policy, nil)
	defer server.Close()
	m := dashboardGET(t, server, "queue", url.Values{"queue": {"m3"}, "window": {"1h"}}).Metrics
	if !m.Available || *m.Completed != 3 || *m.Failed != 1 {
		t.Fatalf("sub-microsecond [from,until) changed membership: %+v", m)
	}
	for _, state := range []string{"enqueued", "started", "retried", "stalled", "failed", "completed"} {
		page := dashboardGET(t, server, "jobs", url.Values{"queue": {"m3"}, "state": {state}})
		found := false
		for _, item := range page.Jobs.Items {
			if item.JobID != "state-"+state {
				continue
			}
			found = true
			expected := state == "enqueued" || state == "started" || state == "retried"
			if (item.Suspicion != nil) != expected || string(item.State) != state || item.RetryExhausted != nil {
				t.Fatalf("state %s fabricated outcome: %+v", state, item)
			}
		}
		if !found {
			t.Fatal("observed-state filter lost", state)
		}
	}
	mustSQL(t, f, `UPDATE job_runs SET last_received_at=$1,summary=jsonb_set(summary,'{evidence,LastReceiptAt}',to_jsonb($1::timestamptz)) WHERE job_id='state-started'`, at.Add(time.Hour))
	page := dashboardGET(t, server, "job", url.Values{"queue": {"m3"}, "job_id": {"state-started"}})
	if page.Job.Summary.Suspicion != nil {
		t.Fatal("future receipt was suspected stalled")
	}
}

func TestDashboardTimelinePagesAndPruningAvailability(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	var events []protocol.Event
	for i := 1; i <= 205; i++ {
		event := lifecycleEvent(f.now.Add(-time.Minute), fmt.Sprintf("m4-attempt-%d", i), "long-history", i, protocol.StateFailed)
		event.Error = "<img src=x onerror=alert(1)> & \"failure\""
		events = append(events, event)
	}
	duration := 42
	event := lifecycleEvent(f.now.Add(-30*time.Second), "m4-complete", "long-history", 205, protocol.StateCompleted)
	event.DurationMS = &duration
	events = append(events, event, lifecycleEvent(f.now.Add(-10*time.Second), "m4-reuse", "long-history", 205, protocol.StateEnqueued))
	mustWrite(t, f, events...)
	drainProjection(t, f, f.now, policy)
	server := dashboardServer(f, policy, nil)
	defer server.Close()
	values := url.Values{"queue": {"m3"}, "job_id": {"long-history"}, "limit": {"50"}}
	seen := 0
	for {
		page := dashboardGET(t, server, "job", values)
		if page.Job.Availability != "observed" || len(page.Job.Attempts) > 50 ||
			!page.Job.Summary.History.ConflictingTerminal || !page.Job.Summary.History.PossibleJobIDReuse ||
			page.Job.Summary.RetryExhausted != nil || page.Consistency != "live_row_keyset" {
			t.Fatalf("timeline evidence: %+v", page.Job)
		}
		for _, item := range page.Job.Attempts {
			seen++
			if item.Attempt != seen || item.RetryExhausted != nil || (seen < 205 && item.DurationMS != nil) {
				t.Fatalf("attempt %d: %+v", seen, item)
			}
			if seen == 205 && (item.DurationMS == nil || *item.DurationMS != 42 || len(item.SourceStates) != 3) {
				t.Fatalf("supplied duration/conflicts: %+v", item)
			}
		}
		if page.Job.NextCursor == "" {
			break
		}
		other := url.Values{"queue": {"m3"}, "job_id": {"other"}, "cursor": {page.Job.NextCursor}}
		if code := lifecycleGET(t, server, "/v1/dashboard/job?"+other.Encode(), lifecycleToken, nil); code != 400 {
			t.Fatalf("job-bound attempt cursor=%d", code)
		}
		values.Set("cursor", page.Job.NextCursor)
	}
	if seen != 205 {
		t.Fatalf("observed %d attempts", seen)
	}
	values.Del("cursor")
	mustSQL(t, f, `UPDATE job_runs SET pruning_at=$1 WHERE job_id='long-history'`, f.now)
	mustSQL(t, f, `DELETE FROM job_attempts WHERE job_id='long-history' AND attempt<=100`)
	pruning := dashboardGET(t, server, "job", values)
	if pruning.Job.Availability != "pruning" || pruning.Job.Summary != nil || len(pruning.Job.Attempts) != 0 {
		t.Fatalf("half-pruned history was exposed: %+v", pruning.Job)
	}
	mustSQL(t, f, `INSERT INTO pruned_jobs(queue,job_id,pruned_at) VALUES('m3','long-history',$1)`, f.now)
	mustSQL(t, f, `DELETE FROM job_attempts WHERE job_id='long-history'`)
	mustSQL(t, f, `DELETE FROM job_runs WHERE job_id='long-history'`)
	pruned := dashboardGET(t, server, "job", values)
	if pruned.Job.Availability != "pruned" || pruned.Job.PrunedAt == nil || len(pruned.Job.Attempts) != 0 {
		t.Fatalf("tombstone: %+v", pruned.Job)
	}
	values.Set("job_id", "never-observed")
	if result := dashboardGET(t, server, "job", values); result.Job.Availability != "not_observed" {
		t.Fatalf("missing history: %+v", result.Job)
	}
	mustWrite(t, f, lifecycleEvent(f.now, "m4-reconstruction", "long-history", 206, protocol.StateStarted))
	drainProjection(t, f, f.now, policy)
	values.Set("job_id", "long-history")
	if result := dashboardGET(t, server, "job", values); result.Job.Availability != "observed" || !result.Job.Summary.History.PrunedHistory {
		t.Fatalf("reconstructed evidence: %+v", result.Job)
	}
}

func TestDashboardSelectedOutcomeRatesAndUnavailableEvidence(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	at := policy.EvaluationAt(f.now)
	for i, state := range []protocol.State{protocol.StateCompleted, protocol.StateCompleted, protocol.StateFailed, protocol.StateFailed} {
		mustWrite(t, f, lifecycleEvent(at.Add(-time.Minute), fmt.Sprintf("m4-rate-%d", i), fmt.Sprintf("rate-%d", i), 1, state))
	}
	// The selected retried fact supersedes a failed fact for this attempt.
	mustWrite(t, f, lifecycleEvent(at.Add(-time.Minute), "m4-retried", "rate-3", 1, protocol.StateRetried),
		lifecycleEvent(at.Add(-time.Hour), "m4-left-bound", "left-bound", 1, protocol.StateCompleted),
		lifecycleEvent(at, "m4-right-bound", "right-bound", 1, protocol.StateCompleted))
	drainProjection(t, f, f.now, policy)
	broker := queues.NewStore()
	broker.Apply("test", []adapter.Counts{{Queue: "m3", Completed: 900, Failed: 600}}, f.now)
	server := dashboardServer(f, policy, broker)
	defer server.Close()
	values := url.Values{"queue": {"m3"}, "window": {"1h"}}
	result := dashboardGET(t, server, "queue", values)
	m := result.Metrics
	if !m.Available || m.Completed == nil || *m.Completed != 3 || *m.Failed != 1 ||
		m.FailureFraction == nil || *m.FailureFraction != .25 || m.CompletionsPerSecond == nil ||
		*m.CompletionsPerSecond != 3.0/3600 || !m.Until.Equal(at) || !m.From.Equal(at.Add(-time.Hour)) {
		t.Fatalf("selected outcome metrics: %+v", m)
	}
	broker.Apply("test", nil, f.now)
	result = dashboardGET(t, server, "queue", values)
	if result.Broker != nil || *result.Metrics.Completed != 3 {
		t.Fatal("broker removal changed retained rate")
	}
	values.Set("queue", "missing")
	zero := dashboardGET(t, server, "queue", values).Metrics
	if !zero.Available || zero.FailureFraction != nil || zero.CompletionsPerSecond == nil || *zero.CompletionsPerSecond != 0 {
		t.Fatalf("zero denominator: %+v", zero)
	}
	values.Set("queue", "m3")
	mustSQL(t, f, `UPDATE job_attempts SET selected_state=NULL,selected_at=NULL,selected_conflict=NULL WHERE job_id='rate-0'`)
	missing := dashboardGET(t, server, "queue", values).Metrics
	if missing.Available || missing.Reason != "missing_selected_cache" || missing.FailureFraction != nil || missing.Completed != nil {
		t.Fatalf("missing cache produced zeros: %+v", missing)
	}
	// A subsequent selected-summary merge refreshes all caches atomically.
	mustWrite(t, f, lifecycleEvent(at.Add(-30*time.Second), "m4-cache-repair", "rate-0", 1, protocol.StateCompleted))
	drainProjection(t, f, f.now, policy)
	if m = dashboardGET(t, server, "queue", values).Metrics; !m.Available || *m.Completed != 3 {
		t.Fatalf("projector cache refresh: %+v", m)
	}
	shortPolicy := policy
	shortPolicy.HistoryRetention = 24 * time.Hour
	shortServer := dashboardServer(f, shortPolicy, broker)
	defer shortServer.Close()
	values.Set("window", "30d")
	if m = dashboardGET(t, shortServer, "queue", values).Metrics; m.Available || m.Reason != "outside_history_coverage" {
		t.Fatalf("history window: %+v", m)
	}
	values.Set("window", "1h")
	tx, err := f.conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(context.Background(), "LOCK job_attempts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if m = dashboardGET(t, server, "queue", values).Metrics; m.Available || m.Reason != "aggregation_unavailable" || time.Since(start) > 2*time.Second {
		t.Fatalf("blocked metric deadline: %+v (%s)", m, time.Since(start))
	}
}
