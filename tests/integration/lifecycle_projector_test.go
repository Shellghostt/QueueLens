package integration

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func drainProjection(t *testing.T, f lifecycleFixture, now time.Time, policy lifecycle.Policy) int {
	t.Helper()
	ctx := lifecycleContext(t)
	total := 0
	for range 200 {
		batch, err := f.db.ProjectBatch(ctx, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Processed > policy.BatchSize {
			t.Fatal("unbounded projection batch")
		}
		total += batch.Processed
		if batch.Processed == 0 && batch.Wrapped {
			return total
		}
	}
	t.Fatal("projection did not finish a bounded scan")
	return total
}

func readRun(t *testing.T, f lifecycleFixture, job string) lifecycle.Run {
	t.Helper()
	var body []byte
	if err := f.conn.QueryRow(lifecycleContext(t), "SELECT summary FROM job_runs WHERE queue='m3' AND job_id=$1", job).Scan(&body); err != nil {
		t.Fatal(err)
	}
	var run lifecycle.Run
	if err := json.Unmarshal(body, &run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestLifecycleProjectorLateCommitLowerSequenceAndRestart(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx := lifecycleContext(t)
	policy := lifecycle.DefaultPolicy()
	lateConn, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lateConn.Close(ctx) }()
	lateTx, err := lateConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lateTx.Rollback(ctx) }()
	var lowID int64
	if err = lateTx.QueryRow(ctx, `INSERT INTO queue_events(ts,queue,job_id,attempt,state)
		VALUES ($1,'m3','late-commit',1,'enqueued') RETURNING id`, f.now).Scan(&lowID); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, lifecycleEvent(f.now.Add(time.Second), "higher-sequence", "late-commit", 1, protocol.StateStarted))
	if drainProjection(t, f, f.now, policy) != 1 {
		t.Fatal("uncommitted row became visible")
	}
	if err = lateTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Recreate the Store connection to exercise its durable scan/checkpoint.
	restarted, err := store.New(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	f.db = restarted
	if drainProjection(t, f, f.now, policy) != 1 {
		t.Fatal("smaller late-committing ID was missed")
	}
	run := readRun(t, f, "late-commit")
	if run.ObservedAttempts != 1 || run.Current.Selected.State != protocol.StateStarted || run.Current.Evidence.EnqueuedAt == nil || !run.Evidence.LegacyIdentity {
		t.Fatal(run)
	}
	if drainProjection(t, f, f.now, policy) != 0 {
		t.Fatal("processed rows replayed after restart")
	}
	var pending int
	if err = f.conn.QueryRow(ctx, "SELECT count(*) FROM queue_events WHERE projected_at IS NULL").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("late ID %d still pending", lowID)
	}
}

func TestLifecycleProjectorAtomicRollbackAndBoundedRoundRobin(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx := lifecycleContext(t)
	policy := lifecycle.DefaultPolicy()
	policy.BatchSize = 2
	oldMonth := time.Date(f.now.Year(), f.now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -2, 0)
	if err := f.db.EnsurePartition(ctx, oldMonth); err != nil {
		t.Fatal(err)
	}
	for i := range 7 {
		mustWrite(t, f, lifecycleEvent(oldMonth, fmt.Sprintf("old-%d", i), fmt.Sprintf("old-%d", i), 1, protocol.StateStarted))
	}
	mustWrite(t, f, lifecycleEvent(f.now, "new", "new", 1, protocol.StateFailed))
	// A real database failure after summary writes must roll back markers too.
	_, err := f.conn.Exec(ctx, `CREATE FUNCTION reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS
		'BEGIN RAISE EXCEPTION ''synthetic checkpoint failure''; END';
		CREATE TRIGGER reject_checkpoint BEFORE UPDATE OF projected_at ON queue_events
		FOR EACH ROW EXECUTE FUNCTION reject_checkpoint()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.ProjectBatch(ctx, f.now, policy); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	var runs, marked int
	if err = f.conn.QueryRow(ctx, "SELECT count(*) FROM job_runs").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err = f.conn.QueryRow(ctx, "SELECT count(*) FROM queue_events WHERE projected_at IS NOT NULL").Scan(&marked); err != nil {
		t.Fatal(err)
	}
	if runs != 0 || marked != 0 {
		t.Fatal("failed transaction persisted partial projection")
	}
	if _, err = f.conn.Exec(ctx, "DROP TRIGGER reject_checkpoint ON queue_events; DROP FUNCTION reject_checkpoint()"); err != nil {
		t.Fatal(err)
	}
	batch, err := f.db.ProjectBatch(ctx, f.now, policy)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Processed != 2 {
		t.Fatal(batch)
	}
	// New traffic gets a turn before the old partition is fully drained.
	foundNew := false
	for range 5 {
		batch, err = f.db.ProjectBatch(ctx, f.now, policy)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Processed > 2 {
			t.Fatal("batch bound")
		}
		if batch.Processed == 1 && batch.Partition != fmt.Sprintf("queue_events_%04d_%02d", oldMonth.Year(), oldMonth.Month()) {
			foundNew = true
			break
		}
	}
	if !foundNew {
		t.Fatal("historical partition monopolized the folder")
	}
	drainProjection(t, f, f.now, policy)
	var count int
	if err = f.conn.QueryRow(ctx, "SELECT count(*) FROM job_runs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 8 {
		t.Fatalf("projection rows=%d", count)
	}
}

func TestLifecycleProjectorRetriesDrainedAndExpiredHistory(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx := lifecycleContext(t)
	policy := lifecycle.DefaultPolicy()
	events := []protocol.Event{
		lifecycleEvent(f.now, "a", "retry", 1, protocol.StateFailed),
		lifecycleEvent(f.now.Add(-time.Second), "b", "retry", 1, protocol.StateRetried),
		lifecycleEvent(f.now.Add(time.Second), "c", "retry", 3, protocol.StateStarted),
		lifecycleEvent(f.now.Add(2*time.Second), "d", "retry", 2, protocol.StateCompleted),
		lifecycleEvent(f.now, "drain", "queue-only", 1, protocol.StateDrained),
		lifecycleEvent(f.now, "expired", "expired", 1, protocol.StateStarted),
	}
	mustWrite(t, f, events...)
	if _, err := f.conn.Exec(ctx, "UPDATE queue_events SET received_at=$1 WHERE job_id='expired'", f.now.Add(-policy.HistoryRetention)); err != nil {
		t.Fatal(err)
	}
	if drainProjection(t, f, f.now, policy) != 6 {
		t.Fatal("not every raw row acknowledged")
	}
	run := readRun(t, f, "retry")
	if run.Current.Number != 3 || run.Current.Selected.State != protocol.StateStarted || run.ObservedAttempts != 3 || run.Current.Selected.DurationMS != nil {
		t.Fatal(run)
	}
	var rows, expired int
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM job_runs").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM queue_events WHERE projection_status='expired_history'").Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || expired != 1 {
		t.Fatalf("runs=%d expired=%d", rows, expired)
	}
}
