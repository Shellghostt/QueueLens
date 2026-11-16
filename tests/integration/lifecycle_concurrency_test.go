package integration

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestLifecycleDatabaseFoldShufflesDuplicatesBatchesAndRestarts(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	events := []protocol.Event{
		lifecycleEvent(at, "property-enqueue", "property", 1, protocol.StateEnqueued),
		lifecycleEvent(at.Add(time.Second), "", "property", 1, protocol.StateStarted),
		lifecycleEvent(at.Add(2*time.Second), "property-failed", "property", 1, protocol.StateFailed),
		lifecycleEvent(at.Add(2*time.Second), "property-retried", "property", 1, protocol.StateRetried),
		lifecycleEvent(at.Add(3*time.Second), "property-lower", "property", 3, protocol.StateCompleted),
		lifecycleEvent(at.Add(4*time.Second), "property-start", "property", 5, protocol.StateStarted),
		lifecycleEvent(at.Add(4*time.Second+100*time.Nanosecond), "property-tie-1", "property", 5, protocol.StateCompleted),
		lifecycleEvent(at.Add(4*time.Second+200*time.Nanosecond), "property-tie-2", "property", 5, protocol.StateCompleted),
		lifecycleEvent(at.Add(10*time.Second), "property-reuse", "property", 1, protocol.StateEnqueued),
		lifecycleEvent(at, "property-drained", "queue-only", 1, protocol.StateDrained),
	}
	duration := 44
	events[7].DurationMS = &duration
	var expected []byte
	for trial, batchSize := range []int{1, 3, 1000} {
		t.Run(fmt.Sprintf("batch-%d", batchSize), func(t *testing.T) {
			f := newLifecycleFixture(t, true)
			ctx := lifecycleContext(t)
			if err := f.db.EnsurePartition(ctx, at); err != nil {
				t.Fatal(err)
			}
			shuffled := append(append([]protocol.Event{}, events...), events...)
			random := rand.New(rand.NewSource(int64(trial + 17)))
			random.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			for start := 0; start < len(shuffled); start += 4 {
				end := min(start+4, len(shuffled))
				mustWrite(t, f, shuffled[start:end]...)
			}
			mustSQL(t, f, `INSERT INTO queue_events(ts,queue,job_id,attempt,state)
				VALUES($1,'m3','property',1,'enqueued')`, at.Add(-time.Second))
			mustSQL(t, f, "UPDATE queue_events SET received_at=$1", at)
			policy := lifecycle.DefaultPolicy()
			policy.BatchSize = batchSize
			for range 3 {
				if _, err := f.db.ProjectBatch(ctx, at.Add(time.Minute), policy); err != nil {
					t.Fatal(err)
				}
			}
			restarted, err := store.New(ctx, f.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			f.db = restarted
			drainProjection(t, f, at.Add(time.Minute), policy)
			run := readRun(t, f, "property")
			if run.ObservedAttempts != 3 || run.Current.Number != 5 || run.Current.Selected.State != protocol.StateCompleted ||
				!run.Current.Selected.TS.Equal(events[7].TS) || run.Current.Selected.DurationMS == nil ||
				*run.Current.Selected.DurationMS != 44 || !run.Evidence.PossibleReuse || !run.Evidence.LegacyIdentity ||
				!run.Evidence.MissingIdentity {
				t.Fatal(run)
			}
			if sqlCount(t, f, "SELECT count(*) FROM job_runs") != 1 {
				t.Fatal("drained fabricated a job")
			}
			actual, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			if trial == 0 {
				expected = actual
			} else if string(expected) != string(actual) {
				t.Fatalf("database fold changed across batches/restarts\nexpected=%s\nactual=%s", expected, actual)
			}
		})
	}
}

func TestLifecycleConcurrentIngestProjectionAndMaintenance(t *testing.T) {
	f := newLifecycleFixture(t, true)
	ctx := lifecycleContext(t)
	policy := lifecycle.DefaultPolicy()
	policy.BatchSize = 100
	writerDone, projectorDone := make(chan struct{}), make(chan struct{})
	errorsCh := make(chan error, 3)
	var processed atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(writerDone)
		for batch := range 25 {
			events := make([]protocol.Event, 0, 44)
			for j := range 40 {
				i := batch*40 + j
				state := protocol.StateStarted
				if i%3 == 0 {
					state = protocol.StateCompleted
				}
				event := lifecycleEvent(f.now, fmt.Sprintf("concurrent-id-%04d", i),
					fmt.Sprintf("concurrent-job-%02d", i%20), 1+(i/20)%2, state)
				events = append(events, event)
				if i%10 == 0 {
					events = append(events, event)
				}
			}
			if _, err := f.db.WriteBatch(ctx, events); err != nil {
				errorsCh <- err
				return
			}
		}
	})
	wg.Go(func() {
		defer close(projectorDone)
		for ctx.Err() == nil {
			batch, err := f.db.ProjectBatch(ctx, time.Now().UTC(), policy)
			if err != nil {
				errorsCh <- err
				return
			}
			processed.Add(int64(batch.Processed))
			select {
			case <-writerDone:
				if batch.Processed == 0 && batch.Wrapped {
					lag := f.db.ProjectionLag(ctx, time.Now().UTC())
					if lag.Known && lag.PendingRows != nil && *lag.PendingRows == 0 {
						return
					}
				}
			default:
			}
		}
		errorsCh <- ctx.Err()
	})
	wg.Go(func() {
		for ctx.Err() == nil {
			if _, err := f.db.MaintainBatch(ctx, time.Now().UTC(), policy); err != nil {
				errorsCh <- err
				return
			}
			select {
			case <-projectorDone:
				return
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if processed.Load() != 1000 || sqlCount(t, f, "SELECT count(*) FROM queue_events") != 1000 ||
		sqlCount(t, f, "SELECT count(*) FROM queue_events WHERE projected_at IS NULL") != 0 ||
		sqlCount(t, f, "SELECT count(*) FROM job_runs WHERE observed_attempts=2") != 20 {
		t.Fatalf("concurrent batches missed or duplicated evidence: processed=%d", processed.Load())
	}
}
