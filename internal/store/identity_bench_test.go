package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

// Every iteration uses fresh producer IDs, preventing a fast duplicate no-op
// from being mistaken for the measured 1,000-row durable write.
func BenchmarkWriteBatch1000ProvidedIdentity(b *testing.B) {
	dsn := os.Getenv("QUEUELENS_TEST_DATABASE_URL")
	if dsn == "" {
		b.Skip("QUEUELENS_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := New(ctx, dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	at := time.Now().UTC()
	if err = db.EnsurePartition(ctx, at); err != nil {
		b.Fatal(err)
	}
	events := make([]protocol.Event, 1000)
	for i := range events {
		events[i] = protocol.Event{V: 1, TS: at, Queue: "identity-benchmark",
			JobID: fmt.Sprintf("job-%d", i), Attempt: 1, State: protocol.StateStarted}
	}
	b.ResetTimer()
	for iteration := range b.N {
		b.StopTimer()
		for i := range events {
			events[i].ID = fmt.Sprintf("provided-%d-%d-%d", at.UnixNano(), iteration, i)
		}
		b.StartTimer()
		count, err := db.WriteBatch(ctx, events)
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("benchmark wrote %d rows; duplicate no-op invalidates measurement", count)
		}
	}
}
