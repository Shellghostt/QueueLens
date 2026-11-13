package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func databaseURL(t *testing.T) string {
	t.Helper()
	value := os.Getenv("QUEUELENS_TEST_DATABASE_URL")
	if value == "" {
		t.Skip("QUEUELENS_TEST_DATABASE_URL is not set; skipping the PostgreSQL integration test")
	}
	return value
}

func newStore(t *testing.T) *store.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := store.New(ctx, databaseURL(t))
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func TestMigrationsAreAppliedAndIdempotent(t *testing.T) {
	db := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("first migration run: %v", err)
	}
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("second migration run must be a no-op, got: %v", err)
	}
}

func TestEventsAreWrittenAndCounted(t *testing.T) {
	db := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	now := time.Now().UTC()
	if err := db.EnsurePartition(ctx, now); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	before, err := db.CountEvents(ctx)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}

	duration := 120
	events := []protocol.Event{
		{
			V: protocol.Version, TS: now, Queue: "integration", JobID: "job-1",
			JobName: "send-welcome", Attempt: 1, State: protocol.StateEnqueued,
		},
		{
			V: protocol.Version, TS: now, Queue: "integration", JobID: "job-1",
			JobName: "send-welcome", Attempt: 1, State: protocol.StateCompleted,
			WorkerID: "worker-1", DurationMS: &duration,
		},
		{
			V: protocol.Version, TS: now, Queue: "integration", JobID: "job-2",
			JobName: "send-invoice", Attempt: 2, State: protocol.StateFailed,
			WorkerID: "worker-2", Error: "SMTP timeout",
		},
	}

	written, err := db.WriteBatch(ctx, events)
	if err != nil {
		t.Fatalf("write batch: %v", err)
	}
	if written != 3 {
		t.Fatalf("expected 3 rows written, got %d", written)
	}

	after, err := db.CountEvents(ctx)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	if after-before != 3 {
		t.Fatalf("expected the stored count to grow by 3, it grew by %d", after-before)
	}
}

func TestTheTableIsPartitionedByTime(t *testing.T) {
	db := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	now := time.Now().UTC()
	if err := db.EnsurePartition(ctx, now); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	connection, err := pgx.Connect(ctx, databaseURL(t))
	if err != nil {
		t.Fatalf("connect for inspection: %v", err)
	}
	defer connection.Close(ctx)

	var strategy string
	err = connection.QueryRow(ctx, `
		SELECT partstrat::text FROM pg_partitioned_table
		WHERE partrelid = 'queue_events'::regclass
	`).Scan(&strategy)
	if err != nil {
		t.Fatalf("read the partition strategy: %v", err)
	}
	if strategy != "r" {
		t.Fatalf("expected the range strategy, got %q", strategy)
	}

	var partitions int
	err = connection.QueryRow(ctx, `
		SELECT count(*) FROM pg_inherits
		WHERE inhparent = 'queue_events'::regclass
	`).Scan(&partitions)
	if err != nil {
		t.Fatalf("count partitions: %v", err)
	}
	if partitions < 1 {
		t.Fatal("expected at least one partition")
	}
}

func TestTheStorageRejectsAnUnknownState(t *testing.T) {
	db := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	now := time.Now().UTC()
	if err := db.EnsurePartition(ctx, now); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	connection, err := pgx.Connect(ctx, databaseURL(t))
	if err != nil {
		t.Fatalf("connect for inspection: %v", err)
	}
	defer connection.Close(ctx)

	// The table has a CHECK constraint on the state. The collector validates
	// first, so this only proves the database does not accept nonsense either.
	_, err = connection.Exec(ctx, `
		INSERT INTO queue_events (ts, queue, job_id, state)
		VALUES ($1, 'integration', 'job-invalid', 'exploded')
	`, now)
	if err == nil {
		t.Fatal("expected the database to reject an unknown state")
	}
}
