package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
)

func waitPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RunProjection drains bounded batches, yielding after a full empty cycle or
// at most 32 empty partitions. The persisted cursor continues the next cycle.
func (s *Store) RunProjection(ctx context.Context, policy lifecycle.Policy, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := policy.Validate(); err != nil {
		logger.Error("projection policy", "error", err)
		return
	}
	empty := 0
	for ctx.Err() == nil {
		result, err := s.ProjectBatch(ctx, time.Now().UTC(), policy)
		if err != nil {
			if ctx.Err() == nil {
				logger.Error("project lifecycle batch", "error", err)
			}
			if !waitPoll(ctx, policy.PollEvery) {
				return
			}
			continue
		}
		if result.Processed > 0 {
			empty = 0
			continue
		}
		empty++
		if result.Wrapped || empty >= 32 {
			empty = 0
			if !waitPoll(ctx, policy.PollEvery) {
				return
			}
		}
	}
}

// RunMaintenance resumes durable pruning phases and fills required month gaps.
func (s *Store) RunMaintenance(ctx context.Context, policy lifecycle.Policy, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := policy.Validate(); err != nil {
		logger.Error("maintenance policy", "error", err)
		return
	}
	for ctx.Err() == nil {
		if err := s.EnsurePartitions(ctx, time.Now().UTC()); err != nil {
			if ctx.Err() == nil {
				logger.Error("maintain partition horizon", "error", err)
			}
		} else {
			for ctx.Err() == nil {
				result, err := s.MaintainBatch(ctx, time.Now().UTC(), policy)
				if err != nil {
					if ctx.Err() == nil {
						logger.Error("retain lifecycle history", "error", err)
					}
					break
				}
				if !result.More {
					break
				}
			}
		}
		if !waitPoll(ctx, policy.MaintenanceEvery) {
			return
		}
	}
}
