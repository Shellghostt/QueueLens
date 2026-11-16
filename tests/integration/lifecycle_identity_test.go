package integration

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/migrations"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestLifecycleMigrationPreservesM2AndOldCOPY(t *testing.T) {
	f := newLifecycleFixture(t, false)
	ctx := lifecycleContext(t)
	for _, name := range []string{"0001_queue_events.sql", "0002_workers.sql"} {
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.conn.Exec(ctx, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.db.EnsurePartition(ctx, f.now); err != nil {
		t.Fatal(err)
	}
	columns := []string{"ts", "queue", "job_id", "job_name", "attempt", "state", "worker_id", "duration_ms", "error"}
	row := []any{f.now, "legacy", "before", "", int16(1), "started", nil, nil, nil}
	if _, err := f.conn.CopyFrom(ctx, pgx.Identifier{"queue_events"}, columns, pgx.CopyFromRows([][]any{row})); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	row[2] = "old-writer-after"
	if _, err := f.conn.CopyFrom(ctx, pgx.Identifier{"queue_events"}, columns, pgx.CopyFromRows([][]any{row})); err != nil {
		t.Fatal(err)
	}
	var count, legacy, versions int
	if err := f.conn.QueryRow(ctx, `SELECT count(*),count(*) FILTER (
		WHERE identity_kind='legacy' AND event_id IS NULL AND event_key IS NULL
			AND projected_at IS NULL) FROM queue_events`).Scan(&count, &legacy); err != nil {
		t.Fatal(err)
	}
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if count != 2 || legacy != 2 || versions != 3 {
		t.Fatalf("rows=%d legacy=%d migrations=%d", count, legacy, versions)
	}
	var indexes int
	if err := f.conn.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema()
		AND indexname IN ('queue_events_pending','queue_events_identity_refs','job_runs_state_page','workers_page')`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 4 {
		t.Fatalf("missing M3 query indexes: %d", indexes)
	}
}

func TestLifecycleIdentityScopeDuplicatesAndAtomicConflicts(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx := lifecycleContext(t)
	event := lifecycleEvent(f.now, "same-id", "job", 1, protocol.StateStarted)
	if count := mustWrite(t, f, event, event); count != 1 {
		t.Fatalf("batch dedup wrote %d", count)
	}
	equivalent := event
	equivalent.TS = event.TS.In(time.FixedZone("other", 5*3600))
	if count := mustWrite(t, f, equivalent); count != 0 {
		t.Fatalf("UTC-normalized duplicate wrote %d", count)
	}
	other := event
	other.Queue = "other-queue"
	if count := mustWrite(t, f, other); count != 1 {
		t.Fatal("identity scope crossed queues")
	}
	missing := event
	missing.ID, missing.JobID = "", "missing"
	if count := mustWrite(t, f, missing, missing); count != 2 {
		t.Fatal("omitted IDs were deduplicated")
	}
	conflict := event
	conflict.TS = conflict.TS.Add(time.Nanosecond)
	newEvent := lifecycleEvent(f.now, "must-rollback", "rollback", 1, protocol.StateEnqueued)
	if _, err := f.db.WriteBatch(ctx, []protocol.Event{newEvent, conflict}); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("conflict=%v", err)
	}
	conflict.State = protocol.StateCompleted
	if _, err := f.db.WriteBatch(ctx, []protocol.Event{event, conflict}); !errors.Is(err, store.ErrIdentityConflict) {
		t.Fatalf("in-batch conflict=%v", err)
	}
	var raw, ledger int
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM queue_events").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM event_identities").Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if raw != 4 || ledger != 2 {
		t.Fatalf("atomic rollback failed: raw=%d ledger=%d", raw, ledger)
	}
	longID := lifecycleEvent(f.now, strings.Repeat("x", 100000), "long-id", 1, protocol.StateStarted)
	if count := mustWrite(t, f, longID, longID); count != 1 {
		t.Fatal("large optional ID failed")
	}
	if count := mustWrite(t, f, longID); count != 0 {
		t.Fatal("large optional ID was not deduplicated")
	}
}

func TestLifecycleConcurrentMigrationsAndIdentityClaims(t *testing.T) {
	f := newLifecycleFixture(t, false)
	ctx := lifecycleContext(t)
	var wg sync.WaitGroup
	errorsCh := make(chan error, 8)
	for range 8 {
		wg.Go(func() { errorsCh <- f.db.Migrate(ctx) })
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := f.db.EnsurePartitions(ctx, f.now); err != nil {
		t.Fatal(err)
	}
	event := lifecycleEvent(f.now, "concurrent", "job", 1, protocol.StateStarted)
	errorsCh = make(chan error, 8)
	counts := make(chan int64, 8)
	for range 8 {
		wg.Go(func() {
			count, err := f.db.WriteBatch(ctx, []protocol.Event{event})
			errorsCh <- err
			counts <- count
		})
	}
	wg.Wait()
	close(errorsCh)
	close(counts)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var total int64
	for count := range counts {
		total += count
	}
	if total != 1 {
		t.Fatalf("concurrent retransmission wrote %d rows", total)
	}
}
