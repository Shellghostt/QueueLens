package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func checkedMaintenance(t *testing.T, f lifecycleFixture, now time.Time, policy lifecycle.Policy) store.MaintenanceBatch {
	t.Helper()
	batch, err := f.db.MaintainBatch(lifecycleContext(t), now, policy)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Visited > 8 || batch.StartedPruning > 1000 || batch.Attempts > 1000 || batch.Runs > 1000 ||
		batch.Workers > 1000 || batch.Identities > 1000 || batch.Tombstones > 1000 {
		t.Fatal("maintenance bound", batch)
	}
	return batch
}

func partitionPresent(t *testing.T, f lifecycleFixture, month time.Time) bool {
	t.Helper()
	name := fmt.Sprintf("queue_events_%04d_%02d", month.UTC().Year(), month.UTC().Month())
	return sqlCount(t, f, "SELECT CASE WHEN to_regclass($1) IS NULL THEN 0 ELSE 1 END", name) == 1
}

func TestLifecycleRetentionPreservesUnexpiredPendingAndUnrelatedTables(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	start := time.Date(f.now.Year(), f.now.Month(), 1, 0, 0, 0, 0, time.UTC)
	months := []time.Time{start.AddDate(0, -4, 0), start.AddDate(0, -3, 0), start.AddDate(0, -2, 0), start.AddDate(0, -1, 0)}
	for _, month := range months {
		if err := f.db.EnsurePartition(lifecycleContext(t), month); err != nil {
			t.Fatal(err)
		}
	}
	for i, job := range []string{"expired", "late-receipt", "pending", "coarse-month"} {
		mustWrite(t, f, lifecycleEvent(months[i], job, job, 1, protocol.StateStarted))
	}
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1", f.now.Add(-45*24*time.Hour))
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1 WHERE job_id='late-receipt'", f.now)
	mustSQL(t, f, "UPDATE queue_events SET projected_at=$1,projection_status='projected' WHERE job_id!='pending'", f.now)
	mustSQL(t, f, "UPDATE event_identities SET first_received_at=$1", f.now.Add(-45*24*time.Hour))
	mustSQL(t, f, "CREATE TABLE queue_events_1990_01(value integer); INSERT INTO queue_events_1990_01 VALUES(7)")
	t.Cleanup(func() { mustSQL(t, f, "DROP TABLE queue_events_1990_01") })
	for range 3 {
		checkedMaintenance(t, f, f.now, policy)
	}
	if partitionPresent(t, f, months[0]) {
		t.Fatal("fully expired processed partition retained")
	}
	for _, month := range months[1:] {
		if !partitionPresent(t, f, month) {
			t.Fatal("unexpired/pending/coarse partition dropped")
		}
	}
	if sqlCount(t, f, "SELECT count(*) FROM event_identities") != 3 {
		t.Fatal("ledger did not outlive raw references")
	}
	if sqlCount(t, f, "SELECT count(*) FROM queue_events_1990_01 WHERE value=7") != 1 {
		t.Fatal("unrelated table changed")
	}
	mustSQL(t, f, "UPDATE queue_events SET projected_at=$1,projection_status='expired_history' WHERE job_id='pending'", f.now)
	for range 3 {
		checkedMaintenance(t, f, f.now, policy)
	}
	if partitionPresent(t, f, months[2]) {
		t.Fatal("processed expired partition was not removed")
	}
	if sqlCount(t, f, "SELECT count(*) FROM event_identities") != 2 {
		t.Fatal("identity cleanup missed dropped raw rows")
	}
}

func TestLifecyclePartitionUTCMonthRolloverConcurrencyAndMetadataGuards(t *testing.T) {
	f := newLifecycleFixture(t, false)
	ctx := lifecycleContext(t)
	if err := f.db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// A local December timestamp is already January in UTC.
	at := time.Date(2026, 12, 31, 23, 30, 0, 0, time.FixedZone("west", -2*3600))
	var wg sync.WaitGroup
	errorsCh := make(chan error, 8)
	for range 8 {
		wg.Go(func() { errorsCh <- f.db.EnsurePartitions(ctx, at) })
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, month := range []time.Time{time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)} {
		if !partitionPresent(t, f, month) {
			t.Fatal("required rollover/late-arrival month missing", month)
		}
	}
	mustSQL(t, f, "CREATE TABLE queue_events_2001_01(value integer)")
	if err := f.db.EnsurePartition(ctx, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("unattached matching name accepted")
	}
	mustSQL(t, f, "DROP TABLE queue_events_2001_01")
	mustSQL(t, f, `CREATE TABLE queue_events_2001_01 PARTITION OF queue_events
		FOR VALUES FROM ('2001-02-01T00:00:00Z') TO ('2001-03-01T00:00:00Z')`)
	if _, err := f.db.MaintainBatch(ctx, f.now, lifecycle.DefaultPolicy()); err == nil {
		t.Fatal("mismatched actual calendar bounds accepted")
	}
	if !partitionPresent(t, f, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("malformed partition dropped")
	}
}

func TestLifecycleRetentionCompletedInactivityAndUnresolvedAgeCap(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	for _, job := range []string{"complete", "unresolved", "contradictory"} {
		mustWrite(t, f, lifecycleEvent(f.now, job+"-enqueue", job, 1, protocol.StateEnqueued),
			lifecycleEvent(f.now, job+"-start", job, 1, protocol.StateStarted))
		if job != "unresolved" {
			mustWrite(t, f, lifecycleEvent(f.now, job+"-complete", job, 1, protocol.StateCompleted))
		}
		if job == "contradictory" {
			mustWrite(t, f, lifecycleEvent(f.now, job+"-failed", job, 1, protocol.StateFailed))
		}
	}
	drainProjection(t, f, f.now, policy)
	if !readRun(t, f, "complete").CompleteObserved() || readRun(t, f, "contradictory").CompleteObserved() {
		t.Fatal("observed completeness classification")
	}
	future := f.now.Add(40 * 24 * time.Hour)
	if err := f.db.EnsurePartition(lifecycleContext(t), future); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"complete", "unresolved", "contradictory"} {
		mustWrite(t, f, lifecycleEvent(future, job+"-refresh", job, 1, protocol.StateStarted))
	}
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1 WHERE ts=$1", future)
	drainProjection(t, f, future, policy)
	checkedMaintenance(t, f, future, policy)
	if sqlCount(t, f, "SELECT count(*) FROM job_runs") != 1 || !readRun(t, f, "complete").CompleteObserved() {
		t.Fatal("receipt inactivity and absolute age cap were conflated")
	}
	if sqlCount(t, f, "SELECT count(*) FROM pruned_jobs") != 2 {
		t.Fatal("pruned history tombstones missing")
	}
	// A retained late/new fact reconstructs only a limited logical history.
	mustWrite(t, f, lifecycleEvent(future, "after-prune", "unresolved", 2, protocol.StateStarted))
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1 WHERE event_id='after-prune'", future)
	drainProjection(t, f, future, policy)
	reconstructed := readRun(t, f, "unresolved")
	if !reconstructed.HistoryPruned || reconstructed.ObservedAttempts != 1 || reconstructed.Current.Number != 2 {
		t.Fatal(reconstructed)
	}
}

func TestLifecycleBoundedLargeAttemptPruningRestartAndPendingFacts(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx := lifecycleContext(t)
	policy := lifecycle.DefaultPolicy()
	events := make([]protocol.Event, 0, 1000)
	for i := 1; i <= 1201; i++ {
		events = append(events, lifecycleEvent(f.now, fmt.Sprintf("big-%d", i), "big", i, protocol.StateStarted))
		if len(events) == 1000 {
			mustWrite(t, f, events...)
			events = events[:0]
		}
	}
	mustWrite(t, f, events...)
	drainProjection(t, f, f.now, policy)
	if readRun(t, f, "big").ObservedAttempts != 1201 {
		t.Fatal("distinct attempt count")
	}
	future := f.now.Add(31 * 24 * time.Hour)
	if err := f.db.EnsurePartition(ctx, future); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, lifecycleEvent(future, "big-pending", "big", 1202, protocol.StateStarted))
	mustSQL(t, f, "UPDATE queue_events SET received_at=$1 WHERE event_id='big-pending'", future)
	first := checkedMaintenance(t, f, future, policy)
	if first.Attempts != 1000 || first.Runs != 0 || !first.More {
		t.Fatal("large pruning was not bounded", first)
	}
	if drainProjection(t, f, future, policy) != 0 {
		t.Fatal("pending fact folded into partly deleted history")
	}
	lag := f.db.ProjectionLag(ctx, future)
	if !lag.Known || lag.PruningRunsLowerBound != 1 || lag.PendingLowerBound != 1 {
		t.Fatal(lag)
	}
	restarted, err := store.New(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	f.db = restarted
	second := checkedMaintenance(t, f, future, policy)
	if second.Attempts != 201 || second.Runs != 1 {
		t.Fatal("pruning restart did not complete", second)
	}
	if sqlCount(t, f, "SELECT count(*) FROM pruned_jobs WHERE job_id='big'") != 1 {
		t.Fatal("final deletion lacked tombstone")
	}
	if drainProjection(t, f, future, policy) != 1 {
		t.Fatal("pending fact was not recovered after pruning")
	}
	run := readRun(t, f, "big")
	if run.ObservedAttempts != 1 || run.Current.Number != 1202 || !run.HistoryPruned {
		t.Fatal("partial history was resurrected as complete", run)
	}
}

func TestLifecycleWorkerAndTombstoneCleanupIsBounded(t *testing.T) {
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	old := f.now.Add(-31 * 24 * time.Hour)
	mustSQL(t, f, `INSERT INTO workers(worker_id,first_seen,last_seen)
		SELECT 'expired-worker-'||g,$1,$1 FROM generate_series(1,1201) g`, old)
	mustSQL(t, f, `INSERT INTO pruned_jobs(queue,job_id,pruned_at)
		SELECT 'm3','expired-key-'||g,$1 FROM generate_series(1,1201) g`, old)
	// An unexpired heartbeat must survive cleanup regardless of old first_seen.
	mustSQL(t, f, `INSERT INTO workers(worker_id,first_seen,last_seen) VALUES('alive',$1,$2)`, old, f.now)
	first := checkedMaintenance(t, f, f.now, policy)
	if first.Workers != 1000 || first.Tombstones != 1000 || !first.More {
		t.Fatal(first)
	}
	second := checkedMaintenance(t, f, f.now, policy)
	if second.Workers != 201 || second.Tombstones != 201 {
		t.Fatal(second)
	}
	if sqlCount(t, f, "SELECT count(*) FROM workers") != 1 {
		t.Fatal("unexpired worker pruned")
	}
}
