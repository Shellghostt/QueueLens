// Package lifecycle folds observed facts without inventing missing outcomes.
package lifecycle

import (
	"errors"
	"time"
)

const MaxBatchSize = 1000
const WorkerDownAfter = 60 * time.Second

// Policy uses receipt time for history, retention and telemetry suspicion.
type Policy struct {
	RawRetention, HistoryRetention time.Duration
	StallAfter, EvaluationEvery    time.Duration
	MaintenanceEvery, PollEvery    time.Duration
	BatchSize                      int
}

func DefaultPolicy() Policy {
	return Policy{RawRetention: 30 * 24 * time.Hour, HistoryRetention: 30 * 24 * time.Hour,
		StallAfter: 15 * time.Minute, EvaluationEvery: 5 * time.Second,
		MaintenanceEvery: 10 * time.Minute, PollEvery: 250 * time.Millisecond, BatchSize: MaxBatchSize}
}

func (p Policy) Validate() error {
	if p.RawRetention <= 0 || p.HistoryRetention <= 0 ||
		p.MaintenanceEvery <= 0 || p.PollEvery <= 0 || p.StallAfter < 0 ||
		p.EvaluationEvery < time.Second || p.EvaluationEvery > time.Minute ||
		p.BatchSize < 1 || p.BatchSize > MaxBatchSize {
		return errors.New("invalid lifecycle policy: positive retention/cadences, nonnegative stall threshold, evaluation 1s–1m and batch 1–1000 required")
	}
	return nil
}

func (p Policy) EvaluationAt(now time.Time) time.Time {
	return now.UTC().Truncate(p.EvaluationEvery)
}

func SuspectedDown(lastReceipt, evaluatedAt time.Time) bool {
	return !lastReceipt.After(evaluatedAt.Add(-WorkerDownAfter))
}

func (p Policy) SuspectedStall(lastReceipt, evaluatedAt time.Time) bool {
	return p.StallAfter > 0 && !lastReceipt.After(evaluatedAt.Add(-p.StallAfter))
}
