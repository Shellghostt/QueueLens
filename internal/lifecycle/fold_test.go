package lifecycle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

func fact(at time.Time, attempt int, state protocol.State, metadata string) Fact {
	event := protocol.Event{V: 1, ID: metadata, TS: at, Queue: "q", JobID: "job", Attempt: attempt, State: state}
	body, _ := json.Marshal(event)
	hash := sha256.Sum256(body)
	return Fact{Queue: "q", JobID: "job", Attempt: attempt, State: state, TS: at,
		ReceivedAt: at.Add(time.Hour), IdentityKind: "provided", Fingerprint: hex.EncodeToString(hash[:]), Error: metadata}
}

func foldFacts(facts []Fact) Run {
	attempts := make(map[int]Attempt)
	run := Run{Queue: "q", JobID: "job"}
	for _, f := range facts {
		next, ok := FromFact(f)
		if !ok {
			continue
		}
		var previous *Attempt
		if old, exists := attempts[next.Number]; exists {
			previous = &old
			next = MergeAttempt(old, next)
		}
		attempts[next.Number] = next
		run = ReplaceAttempt(run, previous, next)
	}
	return run
}

func TestFoldMultisetIsIndependentOfShuffleDuplicatesAndBatchBoundaries(t *testing.T) {
	at := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	facts := []Fact{
		fact(at, 1, protocol.StateEnqueued, "a"), fact(at.Add(time.Second), 1, protocol.StateStarted, "b"),
		fact(at.Add(2*time.Second), 1, protocol.StateFailed, "c"), fact(at.Add(2*time.Second), 1, protocol.StateRetried, "d"),
		fact(at.Add(3*time.Second), 3, protocol.StateStarted, "e"),
		fact(at.Add(4*time.Second), 3, protocol.StateCompleted, "f"),
		fact(at.Add(4*time.Second), 3, protocol.StateCompleted, "tie"),
		fact(at.Add(5*time.Second), 2, protocol.StateCompleted, "late-low"),
		fact(at, 1, protocol.StateDrained, "queue-only"),
	}
	expected := foldFacts(facts)
	if expected.ObservedAttempts != 3 || expected.Current.Number != 3 || expected.Current.Selected.State != protocol.StateCompleted {
		t.Fatal(expected)
	}
	for seed := int64(0); seed < 250; seed++ {
		random := rand.New(rand.NewSource(seed))
		shuffled := append(append([]Fact{}, facts...), facts...)
		random.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		actual := foldFacts(shuffled)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("seed %d changed projection\nexpected=%+v\nactual=%+v", seed, expected, actual)
		}
		// Serialize/restore every intermediate summary as a crash/restart boundary.
		attempts := make(map[int]Attempt)
		run := Run{Queue: "q", JobID: "job"}
		for _, f := range shuffled {
			next, ok := FromFact(f)
			if !ok {
				continue
			}
			var old *Attempt
			if previous, exists := attempts[next.Number]; exists {
				old = &previous
				next = MergeAttempt(previous, next)
			}
			attempts[next.Number] = next
			run = ReplaceAttempt(run, old, next)
			body, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(body, &run); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(run, expected) {
			t.Fatalf("restart seed %d changed projection", seed)
		}
	}
}

func TestFoldPrecedenceAttemptsUnknownTimingAndReuse(t *testing.T) {
	at := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	run := foldFacts([]Fact{fact(at, 1, protocol.StateFailed, "failed"), fact(at.Add(-time.Hour), 1, protocol.StateRetried, "retry")})
	if run.Current.Selected.State != protocol.StateRetried {
		t.Fatal("timestamp overrode retry precedence")
	}
	run = foldFacts([]Fact{fact(at, 1, protocol.StateCompleted, "lower"), fact(at.Add(-time.Hour), 32000, protocol.StateStarted, "higher")})
	if run.Current.Number != 32000 || run.Current.Selected.State != protocol.StateStarted || run.ObservedAttempts != 2 {
		t.Fatal(run)
	}
	if run.Current.Evidence.EnqueuedAt != nil || run.Current.Selected.DurationMS != nil {
		t.Fatal("invented missing timing")
	}
	run = foldFacts([]Fact{fact(at, 0, protocol.StateCompleted, "complete"), fact(at, 1, protocol.StateFailed, "conflict"), fact(at.Add(time.Second), 1, protocol.StateEnqueued, "reuse")})
	if run.ObservedAttempts != 1 || run.Current.Selected.State != protocol.StateCompleted ||
		!run.Evidence.TerminalConflict || !run.Evidence.PossibleReuse || run.CompleteObserved() {
		t.Fatal(run)
	}
	duration := 12
	completed := fact(at, 1, protocol.StateCompleted, "duration")
	completed.DurationMS = &duration
	run = foldFacts([]Fact{fact(at.Add(-time.Hour), 1, protocol.StateEnqueued, "enqueue"),
		fact(at.Add(-time.Minute), 1, protocol.StateStarted, "start"), completed})
	if run.Current.Selected.DurationMS == nil || *run.Current.Selected.DurationMS != 12 || !run.CompleteObserved() {
		t.Fatal(run)
	}
	legacy := completed
	legacy.IdentityKind = "legacy"
	if foldFacts([]Fact{legacy}).CompleteObserved() {
		t.Fatal("legacy evidence claimed complete history")
	}
	if drained, ok := FromFact(fact(at, 1, protocol.StateDrained, "drain")); ok || drained.Number != 0 {
		t.Fatal("drained became a job")
	}
}

func TestReceiptSuspicionUsesThresholdAndLogicalTick(t *testing.T) {
	policy := DefaultPolicy()
	last := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	if SuspectedDown(last, policy.EvaluationAt(last.Add(60*time.Second))) {
		t.Fatal("classified before next tick")
	}
	if !SuspectedDown(last, policy.EvaluationAt(last.Add(61*time.Second))) {
		t.Fatal("missed first eligible tick")
	}
	if SuspectedDown(last.Add(time.Hour), policy.EvaluationAt(last)) {
		t.Fatal("future receipt was down")
	}
	if policy.SuspectedStall(last, last.Add(policy.StallAfter-time.Nanosecond)) {
		t.Fatal("stall before threshold")
	}
	if !policy.SuspectedStall(last, last.Add(policy.StallAfter)) {
		t.Fatal("stall boundary")
	}
	policy.StallAfter = 0
	if policy.SuspectedStall(last, last.Add(24*time.Hour)) {
		t.Fatal("disabled stall policy fired")
	}
}
