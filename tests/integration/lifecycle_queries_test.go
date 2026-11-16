package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func lifecycleGET(t *testing.T, server *httptest.Server, path, token string, target any) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if target != nil && response.StatusCode == http.StatusOK {
		if err = json.NewDecoder(response.Body).Decode(target); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode
}

func TestLifecycleJobQueriesAuthPaginationLagAndHistory(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	reader := store.NewReader(f.db, policy, lifecycleToken)
	server := httptest.NewServer(api.New(api.Options{Token: lifecycleToken,
		Now: func() time.Time { return f.now }, Lifecycle: reader}))
	defer server.Close()
	for _, path := range []string{"/v1/jobs/failed", "/v1/jobs/stalled", "/v1/jobs/suspected-stalled", "/v1/workers"} {
		if status := lifecycleGET(t, server, path, "", nil); status != 401 {
			t.Fatalf("%s unauth=%d", path, status)
		}
		if status := lifecycleGET(t, server, path, "wrong-token", nil); status != 401 {
			t.Fatalf("%s wrong token=%d", path, status)
		}
	}
	for _, job := range []string{"Z", "a", "b", "é"} {
		event := lifecycleEvent(f.now, job, job, 1, protocol.StateFailed)
		event.Error = "SMTP timeout"
		mustWrite(t, f, event)
	}
	drainProjection(t, f, f.now, policy)
	mustWrite(t, f, lifecycleEvent(f.now, "pending", "pending", 1, protocol.StateStarted))
	var first lifecycle.JobPage
	if status := lifecycleGET(t, server, "/v1/jobs/failed?queue=m3&limit=2", lifecycleToken, &first); status != 200 {
		t.Fatal(status)
	}
	if len(first.Items) != 2 || first.Items[0].JobID != "Z" || first.Items[1].JobID != "a" ||
		first.NextCursor == "" || first.Consistency != "live_row_keyset" || !first.ProjectionLag.Known ||
		first.ProjectionLag.PendingRows == nil || *first.ProjectionLag.PendingRows != 1 ||
		first.ProjectionLag.LastBatchAt == nil {
		t.Fatalf("first page=%+v", first)
	}
	history := first.Items[0].History
	if history.CompleteHistory || !history.MissingEnqueued || !history.MissingStarted ||
		first.Items[0].RetryExhausted != nil || first.Items[0].DurationMS != nil || first.Items[0].StateEvidence != "observed" {
		t.Fatal(first.Items[0])
	}
	var second lifecycle.JobPage
	next := "/v1/jobs/failed?queue=m3&limit=2&cursor=" + url.QueryEscape(first.NextCursor)
	if status := lifecycleGET(t, server, next, lifecycleToken, &second); status != 200 {
		t.Fatal(status)
	}
	if len(second.Items) != 2 || second.Items[0].JobID != "b" || second.Items[1].JobID != "é" ||
		second.NextCursor != "" || !second.EvaluatedAt.Equal(first.EvaluatedAt) {
		t.Fatal(second)
	}
	for _, path := range []string{
		"/v1/jobs/stalled?queue=m3&cursor=" + url.QueryEscape(first.NextCursor),
		"/v1/jobs/failed?queue=other&cursor=" + url.QueryEscape(first.NextCursor),
		"/v1/jobs/failed?queue=m3&cursor=" + url.QueryEscape("x"+first.NextCursor),
		"/v1/jobs/failed?limit=0", "/v1/jobs/failed?limit=201", "/v1/jobs/failed?limit=-1",
		"/v1/jobs/failed?limit=bad", "/v1/jobs/failed?status=suspected_down",
		"/v1/workers?status=dead", "/v1/workers?queue=m3",
		"/v1/jobs/failed?cursor=" + strings.Repeat("x", lifecycle.MaxCursorBytes+1),
	} {
		if status := lifecycleGET(t, server, path, lifecycleToken, nil); status != 400 {
			t.Fatalf("%s status=%d", path, status)
		}
	}
	// Live membership changes are reflected after the keyset, not frozen.
	mustWrite(t, f, lifecycleEvent(f.now, "completed-b", "b", 2, protocol.StateCompleted))
	drainProjection(t, f, f.now, policy)
	if status := lifecycleGET(t, server, next, lifecycleToken, &second); status != 200 {
		t.Fatal(status)
	}
	if len(second.Items) != 1 || second.Items[0].JobID != "é" {
		t.Fatal("page claimed an MVCC snapshot")
	}
	many := make([]protocol.Event, 0, 250)
	for i := range 250 {
		many = append(many, lifecycleEvent(f.now, fmt.Sprintf("page-id-%d", i), fmt.Sprintf("page-%03d", i), 1, protocol.StateFailed))
	}
	mustWrite(t, f, many...)
	drainProjection(t, f, f.now, policy)
	var defaultPage, maxPage lifecycle.JobPage
	if lifecycleGET(t, server, "/v1/jobs/failed", lifecycleToken, &defaultPage) != 200 || len(defaultPage.Items) != 100 {
		t.Fatal("default page bound")
	}
	if lifecycleGET(t, server, "/v1/jobs/failed?limit=200", lifecycleToken, &maxPage) != 200 || len(maxPage.Items) != 200 {
		t.Fatal("maximum page bound")
	}
}

func TestLifecycleExplicitStallSuspectedStallAndFailureStayDistinct(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	at := policy.EvaluationAt(f.now)
	mustWrite(t, f, lifecycleEvent(f.now, "explicit", "explicit", 1, protocol.StateStalled),
		lifecycleEvent(f.now, "suspect", "suspect", 1, protocol.StateStarted),
		lifecycleEvent(f.now, "failed", "failed", 1, protocol.StateFailed),
		lifecycleEvent(f.now, "future", "future", 1, protocol.StateEnqueued))
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1 WHERE job_id IN ('suspect','failed')", at.Add(-policy.StallAfter))
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1 WHERE job_id='future'", at.Add(time.Hour))
	drainProjection(t, f, f.now, policy)
	reader := store.NewReader(f.db, policy, lifecycleToken)
	ctx := lifecycleContext(t)
	suspected, err := reader.QueryJobs(ctx, "suspected-stalled", lifecycle.PageQuery{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(suspected.Items) != 1 || suspected.Items[0].JobID != "suspect" ||
		suspected.Items[0].State != protocol.StateStarted || suspected.Items[0].Suspicion == nil ||
		suspected.Items[0].Suspicion.Reason != "no_job_receipt" {
		t.Fatal(suspected)
	}
	explicit, err := reader.QueryJobs(ctx, "stalled", lifecycle.PageQuery{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(explicit.Items) != 1 || explicit.Items[0].JobID != "explicit" || explicit.Items[0].Suspicion != nil {
		t.Fatal(explicit)
	}
	failed, err := reader.QueryJobs(ctx, "failed", lifecycle.PageQuery{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed.Items) != 1 || failed.Items[0].RetryExhausted != nil {
		t.Fatal("failure implied retry exhaustion")
	}
	policy.StallAfter = 0
	disabled, err := store.NewReader(f.db, policy, lifecycleToken).QueryJobs(ctx, "suspected-stalled", lifecycle.PageQuery{}, f.now)
	if err != nil || len(disabled.Items) != 0 {
		t.Fatal("zero stall threshold did not disable suspicion", err)
	}
}

func TestLifecycleWorkerReceiptBoundaryCursorAndRecovery(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	last := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	mustSQL(t, f, `INSERT INTO workers(worker_id,pool,first_seen,last_seen) VALUES
		('Z','old',$1,$1),('a','future',$2,$2),('é','old',$1,$1)`, last, last.Add(time.Hour))
	reader := store.NewReader(f.db, policy, lifecycleToken)
	ctx := lifecycleContext(t)
	before, err := reader.QueryWorkers(ctx, lifecycle.PageQuery{Status: "suspected_down"}, last.Add(60*time.Second))
	if err != nil || len(before.Items) != 0 {
		t.Fatal("threshold was evaluated before next UTC tick", err)
	}
	first, err := reader.QueryWorkers(ctx, lifecycle.PageQuery{Limit: 1}, last.Add(61*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || first.Items[0].WorkerID != "Z" || first.Items[0].Status != "suspected_down" ||
		first.Items[0].Suspicion == nil || first.NextCursor == "" {
		t.Fatal(first)
	}
	second, err := reader.QueryWorkers(ctx, lifecycle.PageQuery{Limit: 1, Cursor: first.NextCursor}, last.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Items[0].WorkerID != "a" || second.Items[0].Status != "alive" ||
		second.Items[0].ReceiptAgeSeconds != 0 || !second.EvaluatedAt.Equal(first.EvaluatedAt) {
		t.Fatal(second)
	}
	if _, err = reader.QueryWorkers(ctx, lifecycle.PageQuery{Status: "suspected_down", Cursor: first.NextCursor}, last.Add(2*time.Hour)); err == nil {
		t.Fatal("cursor filter was not bound")
	}
	// The source is still the M2 server-receipt upsert, independent of client time.
	if err = f.db.Heartbeat(ctx, protocol.Heartbeat{V: 1, WorkerID: "Z", Pool: "recovered"}); err != nil {
		t.Fatal(err)
	}
	page, err := reader.QueryWorkers(ctx, lifecycle.PageQuery{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if page.Items[0].Status != "alive" || page.Items[0].Pool != "recovered" || !page.Items[0].FirstSeen.Equal(last) {
		t.Fatal(page.Items[0])
	}
}

func TestLifecycleProjectionLagCappedAndTimeoutIsUnknown(t *testing.T) {
	f := newLifecycleFixture(t, true)
	events := make([]protocol.Event, 0, 1000)
	for i := range 1000 {
		events = append(events, lifecycleEvent(f.now, fmt.Sprintf("lag-%d", i), fmt.Sprintf("lag-%d", i), 1, protocol.StateStarted))
	}
	mustWrite(t, f, events...)
	mustWrite(t, f, lifecycleEvent(f.now, "extra-lag", "extra-lag", 1, protocol.StateStarted))
	lag := f.db.ProjectionLag(lifecycleContext(t), f.now)
	if !lag.Known || !lag.SampleCapped || lag.PendingLowerBound != 1000 || lag.PendingRows != nil {
		t.Fatal(lag)
	}
	tx, err := f.conn.Begin(lifecycleContext(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(lifecycleContext(t), "LOCK TABLE projection_state IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	lag = f.db.ProjectionLag(lifecycleContext(t), f.now)
	if lag.Known || lag.PendingRows != nil || lag.Reason == "" || time.Since(started) > 2*time.Second {
		t.Fatal("blocked lag did not remain bounded and unknown", lag)
	}
}
