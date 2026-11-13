package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

// BenchmarkWriteBatch1000 measures the storage side of the ingest budget from
// PLAN section 9: "Collector ingest < 50 ms per 1,000-event batch".
//
// It needs a real PostgreSQL server:
//
//	QUEUELENS_TEST_DATABASE_URL=postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens go test ./internal/store -bench WriteBatch1000 -benchtime 20x
func BenchmarkWriteBatch1000(b *testing.B) {
	dsn := os.Getenv("QUEUELENS_TEST_DATABASE_URL")
	if dsn == "" {
		b.Skip("QUEUELENS_TEST_DATABASE_URL is not set; skipping the storage benchmark")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, err := New(ctx, dsn)
	if err != nil {
		b.Fatalf("connect to postgres: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	now := time.Now().UTC()
	if err := db.EnsurePartition(ctx, now); err != nil {
		b.Fatalf("ensure partition: %v", err)
	}

	events := make([]protocol.Event, 0, 1000)
	for i := 0; i < 1000; i++ {
		events = append(events, protocol.Event{
			V:       protocol.Version,
			TS:      now,
			Queue:   "benchmark",
			JobID:   fmt.Sprintf("job-%d", i),
			JobName: "benchmark",
			Attempt: 1,
			State:   protocol.StateEnqueued,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.WriteBatch(ctx, events); err != nil {
			b.Fatalf("write batch: %v", err)
		}
	}
}
