package lifecycle

import (
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

// Fact excludes unbounded protocol IDs. Fingerprint retains their tie evidence.
type Fact struct {
	Queue, JobID, JobName, WorkerID  string
	Attempt                          int
	State                            protocol.State
	TS, ReceivedAt                   time.Time
	DurationMS                       *int
	Error, Fingerprint, IdentityKind string
}

// Observation is the representative chosen by the ADR's total tie order.
type Observation struct {
	State       protocol.State `json:"state"`
	TS          time.Time      `json:"ts"`
	Fingerprint string         `json:"fingerprint"`
	WorkerID    string         `json:"worker_id,omitempty"`
	JobName     string         `json:"job_name,omitempty"`
	DurationMS  *int           `json:"duration_ms"`
	Error       string         `json:"error,omitempty"`
}

// Evidence is a fixed-size union/min/max summary, never a list of raw events.
type Evidence struct {
	States                           uint8 `json:"states"`
	FirstEventAt, LastEventAt        time.Time
	FirstReceiptAt, LastReceiptAt    time.Time
	EnqueuedAt, StartedAt            *time.Time
	LastEnqueuedAt, FirstCompletedAt *time.Time
	LegacyIdentity, MissingIdentity  bool
	TerminalConflict, PossibleReuse  bool
}

type Attempt struct {
	Number   int         `json:"attempt"`
	Selected Observation `json:"selected"`
	Evidence Evidence    `json:"evidence"`
}

type Run struct {
	Queue              string   `json:"queue"`
	JobID              string   `json:"job_id"`
	Current            Attempt  `json:"current"`
	ObservedAttempts   int      `json:"observed_attempts"`
	IncompleteAttempts int      `json:"incomplete_attempts"`
	Evidence           Evidence `json:"evidence"`
	HistoryPruned      bool     `json:"history_pruned"`
}

func rank(state protocol.State) uint8 {
	switch state {
	case protocol.StateEnqueued:
		return 1
	case protocol.StateStarted:
		return 2
	case protocol.StateStalled:
		return 3
	case protocol.StateFailed:
		return 4
	case protocol.StateRetried:
		return 5
	case protocol.StateCompleted:
		return 6
	default:
		return 0
	}
}

func stateBit(state protocol.State) uint8 {
	if r := rank(state); r > 0 {
		return 1 << (r - 1)
	}
	return 0
}

func (e Evidence) Has(state protocol.State) bool { return e.States&stateBit(state) != 0 }

func (e Evidence) SourceStates() []protocol.State {
	states := make([]protocol.State, 0, 6)
	for _, state := range protocol.States() {
		if e.Has(state) {
			states = append(states, state)
		}
	}
	return states
}

// FromFact returns false for queue-level drained telemetry.
func FromFact(f Fact) (Attempt, bool) {
	if rank(f.State) == 0 {
		return Attempt{}, false
	}
	if f.Attempt == 0 {
		f.Attempt = 1
	}
	f.TS, f.ReceivedAt = f.TS.UTC(), f.ReceivedAt.UTC()
	e := Evidence{States: stateBit(f.State), FirstEventAt: f.TS, LastEventAt: f.TS,
		FirstReceiptAt: f.ReceivedAt, LastReceiptAt: f.ReceivedAt,
		LegacyIdentity: f.IdentityKind == "legacy", MissingIdentity: f.IdentityKind == "missing"}
	switch f.State {
	case protocol.StateEnqueued:
		e.EnqueuedAt, e.LastEnqueuedAt = &f.TS, &f.TS
	case protocol.StateStarted:
		e.StartedAt = &f.TS
	case protocol.StateCompleted:
		e.FirstCompletedAt = &f.TS
	}
	var duration *int
	if (f.State == protocol.StateCompleted || f.State == protocol.StateFailed) && f.DurationMS != nil {
		value := *f.DurationMS
		duration = &value
	}
	return Attempt{Number: f.Attempt, Evidence: e, Selected: Observation{
		State: f.State, TS: f.TS, Fingerprint: f.Fingerprint, WorkerID: f.WorkerID,
		JobName: f.JobName, DurationMS: duration, Error: f.Error}}, true
}

func laterRepresentative(a, b Observation) Observation {
	ar, br := rank(a.State), rank(b.State)
	if br > ar || (br == ar && (b.TS.After(a.TS) ||
		(b.TS.Equal(a.TS) && b.Fingerprint > a.Fingerprint))) {
		return b
	}
	return a
}

// MergeAttempt is associative, commutative and idempotent for one attempt key.
func MergeAttempt(a, b Attempt) Attempt {
	if a.Number == 0 {
		return b
	}
	if b.Number == 0 {
		return a
	}
	a.Selected = laterRepresentative(a.Selected, b.Selected)
	a.Evidence = mergeEvidence(a.Evidence, b.Evidence)
	a.Evidence.TerminalConflict = a.Evidence.TerminalConflict ||
		(a.Evidence.Has(protocol.StateCompleted) && a.Evidence.Has(protocol.StateFailed))
	return a
}

func minTime(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}
func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
func minOptional(a, b *time.Time) *time.Time {
	if a == nil || (b != nil && b.Before(*a)) {
		return b
	}
	return a
}
func maxOptional(a, b *time.Time) *time.Time {
	if a == nil || (b != nil && b.After(*a)) {
		return b
	}
	return a
}

func mergeEvidence(a, b Evidence) Evidence {
	a.States |= b.States
	a.FirstEventAt, a.LastEventAt = minTime(a.FirstEventAt, b.FirstEventAt), maxTime(a.LastEventAt, b.LastEventAt)
	a.FirstReceiptAt, a.LastReceiptAt = minTime(a.FirstReceiptAt, b.FirstReceiptAt), maxTime(a.LastReceiptAt, b.LastReceiptAt)
	a.EnqueuedAt, a.StartedAt = minOptional(a.EnqueuedAt, b.EnqueuedAt), minOptional(a.StartedAt, b.StartedAt)
	a.LastEnqueuedAt, a.FirstCompletedAt = maxOptional(a.LastEnqueuedAt, b.LastEnqueuedAt), minOptional(a.FirstCompletedAt, b.FirstCompletedAt)
	a.LegacyIdentity, a.MissingIdentity = a.LegacyIdentity || b.LegacyIdentity, a.MissingIdentity || b.MissingIdentity
	a.TerminalConflict = a.TerminalConflict || b.TerminalConflict
	a.PossibleReuse = a.PossibleReuse || b.PossibleReuse ||
		(a.LastEnqueuedAt != nil && a.FirstCompletedAt != nil && a.LastEnqueuedAt.After(*a.FirstCompletedAt))
	return a
}

// StructurallyComplete is observed evidence quality, not guaranteed telemetry delivery.
func (a Attempt) StructurallyComplete() bool {
	e := a.Evidence
	return e.EnqueuedAt != nil && e.StartedAt != nil &&
		(e.Has(protocol.StateCompleted) || e.Has(protocol.StateFailed) || e.Has(protocol.StateRetried)) &&
		!e.LegacyIdentity && !e.MissingIdentity && !e.TerminalConflict && !e.PossibleReuse
}

// ReplaceAttempt updates one compact run after an atomic attempt-summary merge.
// previous is nil only when this distinct attempt did not already exist.
func ReplaceAttempt(run Run, previous *Attempt, next Attempt) Run {
	if previous == nil {
		run.ObservedAttempts++
		if !next.StructurallyComplete() {
			run.IncompleteAttempts++
		}
	} else {
		if !previous.StructurallyComplete() {
			run.IncompleteAttempts--
		}
		if !next.StructurallyComplete() {
			run.IncompleteAttempts++
		}
	}
	if next.Number > run.Current.Number {
		run.Current = next
	} else if next.Number == run.Current.Number {
		run.Current = MergeAttempt(run.Current, next)
	}
	run.Evidence = mergeEvidence(run.Evidence, next.Evidence)
	return run
}

func (r Run) CompleteObserved() bool {
	e := r.Evidence
	return r.Current.Selected.State == protocol.StateCompleted && r.IncompleteAttempts == 0 &&
		!r.HistoryPruned && !e.TerminalConflict && !e.PossibleReuse &&
		!e.LegacyIdentity && !e.MissingIdentity
}
