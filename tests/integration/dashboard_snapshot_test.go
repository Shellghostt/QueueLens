package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/dashboard"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestDashboardTimelineReadSnapshotDuringPruning(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	var events []protocol.Event
	for i := 1; i <= 100; i++ {
		events = append(events, lifecycleEvent(f.now, fmt.Sprintf("snapshot-%d", i), "snapshot-job", i, protocol.StateFailed))
	}
	mustWrite(t, f, events...)
	drainProjection(t, f, f.now, policy)
	mustSQL(t, f, `ALTER TABLE job_runs RENAME TO job_runs_snapshot_base`)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = f.conn.Exec(ctx, `SELECT pg_advisory_unlock_all()`)
		_, _ = f.conn.Exec(ctx, `DROP VIEW IF EXISTS job_runs`)
		_, _ = f.conn.Exec(ctx, `ALTER TABLE job_runs_snapshot_base RENAME TO job_runs`)
		_, _ = f.conn.Exec(ctx, `DROP FUNCTION IF EXISTS dashboard_snapshot_delay(jsonb)`)
	})
	// Delay the run read after its statement snapshot. This creates a deterministic
	// commit between the run and attempt statements, without a production test hook.
	mustSQL(t, f, `CREATE FUNCTION dashboard_snapshot_delay(body jsonb) RETURNS jsonb VOLATILE LANGUAGE plpgsql AS $$
		BEGIN
		PERFORM pg_advisory_lock(hashtextextended('m4-snapshot:'||current_schema(),0));
		PERFORM pg_advisory_unlock(hashtextextended('m4-snapshot:'||current_schema(),0));
		RETURN body;
		END $$`)
	mustSQL(t, f, `CREATE VIEW job_runs AS SELECT queue,job_id,pruning_at,dashboard_snapshot_delay(summary) AS summary FROM job_runs_snapshot_base`)
	mustSQL(t, f, `SELECT pg_advisory_lock(hashtextextended('m4-snapshot:'||current_schema(),0))`)
	server := dashboardServer(f, policy, nil)
	defer server.Close()
	path := "/v1/dashboard/job?" + url.Values{"queue": {"m3"}, "job_id": {"snapshot-job"}}.Encode()
	result := make(chan dashboard.Response, 1)
	errors := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			errors <- err
			return
		}
		req.Header.Set("Authorization", "Bearer "+lifecycleToken)
		response, err := server.Client().Do(req)
		if err != nil {
			errors <- err
			return
		}
		defer func() { _ = response.Body.Close() }()
		var page dashboard.Response
		err = json.NewDecoder(response.Body).Decode(&page)
		if err != nil || response.StatusCode != 200 {
			errors <- fmt.Errorf("snapshot read status=%d error=%v", response.StatusCode, err)
			return
		}
		result <- page
	}()
	deadline := time.Now().Add(2 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		if err := f.conn.QueryRow(lifecycleContext(t), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
			WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%FROM job_runs WHERE queue=%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("run read did not reach the snapshot barrier")
	}
	mustSQL(t, f, `UPDATE job_runs_snapshot_base SET pruning_at=$1 WHERE job_id='snapshot-job'`, f.now)
	mustSQL(t, f, `DELETE FROM job_attempts WHERE job_id='snapshot-job' AND attempt<=50`)
	mustSQL(t, f, `SELECT pg_advisory_unlock(hashtextextended('m4-snapshot:'||current_schema(),0))`)
	select {
	case err := <-errors:
		t.Fatal(err)
	case page := <-result:
		if page.Job == nil || page.Job.Availability != "observed" || len(page.Job.Attempts) != 100 {
			t.Fatalf("run/attempt snapshots differed: %+v", page.Job)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot read did not finish")
	}
	page := dashboardGET(t, server, "job", url.Values{"queue": {"m3"}, "job_id": {"snapshot-job"}})
	if page.Job.Availability != "pruning" || len(page.Job.Attempts) != 0 {
		t.Fatal("later read exposed a partially pruned timeline")
	}
}
