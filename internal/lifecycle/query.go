package lifecycle

import (
	"context"
	"errors"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

var ErrInvalidQuery = errors.New("invalid lifecycle query or cursor")

const PageLimit = 200
const DefaultPageLimit = 100
const MaxCursorBytes = 4096

type PageQuery struct {
	Queue, Status, Cursor string
	Limit                 int
}

type Reader interface {
	QueryJobs(context.Context, string, PageQuery, time.Time) (JobPage, error)
	QueryWorkers(context.Context, PageQuery, time.Time) (WorkerPage, error)
}

type ProjectionLag struct {
	Known                 bool       `json:"known"`
	PendingRows           *int       `json:"pending_rows"`
	PendingLowerBound     int        `json:"pending_rows_lower_bound"`
	SampleLimit           int        `json:"sample_limit"`
	SampleCapped          bool       `json:"sample_capped"`
	OldestPendingReceipt  *time.Time `json:"oldest_pending_receipt"`
	LagSeconds            *float64   `json:"lag_seconds"`
	PruningRunsLowerBound int        `json:"pruning_runs_lower_bound"`
	PruningSampleCapped   bool       `json:"pruning_sample_capped"`
	LastBatchAt           *time.Time `json:"last_batch_at"`
	LastScanAt            *time.Time `json:"last_scan_at"`
	Reason                string     `json:"reason,omitempty"`
}

type PageEvidence struct {
	EvaluatedAt   time.Time     `json:"evaluated_at"`
	HistoryFrom   time.Time     `json:"history_from"`
	ProjectionLag ProjectionLag `json:"projection_lag"`
	Consistency   string        `json:"consistency"`
	Limitations   []string      `json:"limitations"`
}

type HistoryEvidence struct {
	CompleteHistory     bool `json:"complete_history"`
	MissingEnqueued     bool `json:"missing_enqueued"`
	MissingStarted      bool `json:"missing_started"`
	MissingTerminal     bool `json:"missing_terminal"`
	LegacyIdentity      bool `json:"legacy_identity"`
	MissingIdentity     bool `json:"missing_identity"`
	PrunedHistory       bool `json:"pruned_history"`
	ConflictingTerminal bool `json:"conflicting_terminal"`
	PossibleJobIDReuse  bool `json:"possible_job_id_reuse_or_clock_disagreement"`
	IncompleteAttempts  int  `json:"incomplete_attempts"`
}

type Suspicion struct {
	Reason           string  `json:"reason"`
	ThresholdSeconds float64 `json:"threshold_seconds"`
}

type JobItem struct {
	Queue            string           `json:"queue"`
	JobID            string           `json:"job_id"`
	JobName          string           `json:"job_name,omitempty"`
	WorkerID         string           `json:"worker_id,omitempty"`
	Attempt          int              `json:"attempt"`
	ObservedAttempts int              `json:"observed_attempts"`
	State            protocol.State   `json:"state"`
	StateEvidence    string           `json:"state_evidence"`
	StateAt          time.Time        `json:"state_at"`
	SourceStates     []protocol.State `json:"source_states"`
	EnqueuedAt       *time.Time       `json:"enqueued_at"`
	StartedAt        *time.Time       `json:"started_at"`
	DurationMS       *int             `json:"duration_ms"`
	DurationKind     string           `json:"duration_kind"`
	Error            string           `json:"error,omitempty"`
	FirstEventAt     time.Time        `json:"first_event_at"`
	LastEventAt      time.Time        `json:"last_event_at"`
	FirstReceiptAt   time.Time        `json:"first_receipt_at"`
	LastReceiptAt    time.Time        `json:"last_receipt_at"`
	RetryExhausted   *bool            `json:"retry_exhausted"`
	History          HistoryEvidence  `json:"history"`
	Suspicion        *Suspicion       `json:"suspicion"`
}

type JobPage struct {
	PageEvidence
	Items      []JobItem `json:"items"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

type WorkerItem struct {
	WorkerID          string     `json:"worker_id"`
	Pool              string     `json:"pool"`
	FirstSeen         time.Time  `json:"first_seen"`
	LastSeen          time.Time  `json:"last_seen"`
	ReceiptAgeSeconds float64    `json:"receipt_age_seconds"`
	Status            string     `json:"status"`
	Suspicion         *Suspicion `json:"suspicion"`
}

type WorkerPage struct {
	PageEvidence
	Items      []WorkerItem `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

func Item(run Run, suspected bool, policy Policy) JobItem {
	current, e := run.Current, run.Evidence
	selected := current.Selected
	item := JobItem{Queue: run.Queue, JobID: run.JobID, JobName: selected.JobName, WorkerID: selected.WorkerID,
		Attempt: current.Number, ObservedAttempts: run.ObservedAttempts, State: selected.State,
		StateEvidence: "observed", StateAt: selected.TS, SourceStates: current.Evidence.SourceStates(),
		EnqueuedAt: current.Evidence.EnqueuedAt, StartedAt: current.Evidence.StartedAt,
		DurationMS: selected.DurationMS, DurationKind: "unknown", Error: selected.Error,
		FirstEventAt: e.FirstEventAt, LastEventAt: e.LastEventAt,
		FirstReceiptAt: e.FirstReceiptAt, LastReceiptAt: e.LastReceiptAt,
		History: HistoryEvidence{MissingEnqueued: current.Evidence.EnqueuedAt == nil,
			MissingStarted:  current.Evidence.StartedAt == nil,
			MissingTerminal: selected.State != protocol.StateCompleted && selected.State != protocol.StateFailed,
			LegacyIdentity:  e.LegacyIdentity, MissingIdentity: e.MissingIdentity,
			PrunedHistory: run.HistoryPruned, ConflictingTerminal: e.TerminalConflict,
			PossibleJobIDReuse: e.PossibleReuse, IncompleteAttempts: run.IncompleteAttempts}}
	if selected.DurationMS != nil {
		item.DurationKind = "producer_reported_attempt"
	}
	if suspected {
		item.Suspicion = &Suspicion{Reason: "no_job_receipt", ThresholdSeconds: policy.StallAfter.Seconds()}
	}
	return item
}
