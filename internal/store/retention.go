package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/lifecycle"
)

const maintenanceRows = 1000
const maintenancePartitions = 8

type MaintenanceBatch struct {
	Visited, Dropped                                                int
	StartedPruning, Attempts, Runs, Workers, Identities, Tombstones int64
	More                                                            bool
}

// MaintainBatch bounds every deletion category and rechecks locked partitions.
func (s *Store) MaintainBatch(ctx context.Context, now time.Time, policy lifecycle.Policy) (MaintenanceBatch, error) {
	result := MaintenanceBatch{}
	if err := policy.Validate(); err != nil {
		return result, err
	}
	ctx, cancel, tx, err := s.begin(ctx, 15*time.Second)
	if err != nil {
		return result, err
	}
	defer cancel()
	defer rollback(tx)
	// Consistent order: folding/pruning, identity claims, then partition DDL.
	for _, lane := range []string{"folder", "identity-retention", "partitions"} {
		if err = advisory(ctx, tx, lane, false); err != nil {
			return result, err
		}
	}
	now = now.UTC()
	rawFrom, historyFrom := now.Add(-policy.RawRetention), now.Add(-policy.HistoryRetention)
	if err = maintainPartitions(ctx, tx, rawFrom, &result); err != nil {
		return result, err
	}
	if err = pruneRuns(ctx, tx, now, historyFrom, &result); err != nil {
		return result, err
	}
	deletes := []struct {
		query  string
		cutoff time.Time
		count  *int64
	}{
		{`WITH expired AS (SELECT worker_id FROM workers WHERE last_seen<=$1
			LIMIT 1000 FOR UPDATE)
			DELETE FROM workers w USING expired e WHERE w.worker_id=e.worker_id AND w.last_seen<=$1`, historyFrom, &result.Workers},
		{`WITH expired AS (SELECT queue,event_key FROM event_identities d
			WHERE first_received_at<=$1 AND NOT EXISTS
				(SELECT 1 FROM queue_events r WHERE r.queue=d.queue AND r.event_key=d.event_key)
			ORDER BY first_received_at LIMIT 1000 FOR UPDATE)
			DELETE FROM event_identities d USING expired e WHERE d.queue=e.queue AND d.event_key=e.event_key`, rawFrom, &result.Identities},
		{`WITH expired AS (SELECT queue,job_id FROM pruned_jobs WHERE pruned_at<=$1
			ORDER BY pruned_at LIMIT 1000 FOR UPDATE)
			DELETE FROM pruned_jobs p USING expired e WHERE p.queue=e.queue AND p.job_id=e.job_id`, historyFrom, &result.Tombstones},
	}
	for _, category := range deletes {
		deleted, deleteErr := tx.Exec(ctx, category.query, category.cutoff)
		if deleteErr != nil {
			return result, deleteErr
		}
		*category.count = deleted.RowsAffected()
	}
	result.More = result.More || result.StartedPruning == maintenanceRows || result.Attempts == maintenanceRows ||
		result.Runs == maintenanceRows || result.Workers == maintenanceRows ||
		result.Identities == maintenanceRows || result.Tombstones == maintenanceRows
	if err = tx.Commit(ctx); err != nil {
		return MaintenanceBatch{}, err
	}
	return result, nil
}

func maintainPartitions(ctx context.Context, tx pgx.Tx, rawFrom time.Time, result *MaintenanceBatch) error {
	var after string
	if err := tx.QueryRow(ctx, "SELECT maintenance_partition FROM projection_state WHERE singleton FOR UPDATE").Scan(&after); err != nil {
		return err
	}
	seen := make(map[string]bool, maintenancePartitions)
	wrapped := false
	for result.Visited < maintenancePartitions {
		p, wrap, err := nextPartition(ctx, tx, after)
		if err != nil {
			return err
		}
		wrapped = wrapped || wrap
		if p.Name == "" || seen[p.Name] {
			break
		}
		seen[p.Name] = true
		after = p.Name
		result.Visited++
		if p.End.After(rawFrom) {
			continue
		}
		name := pgx.Identifier{p.Schema, p.Name}.Sanitize()
		if _, err = tx.Exec(ctx, "LOCK TABLE "+name+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			return err
		}
		checked, err := inspectPartition(ctx, tx, p.Schema, p.Name)
		if err != nil {
			return err
		}
		if checked.End.After(rawFrom) {
			return errors.New("locked partition bounds changed")
		}
		var pending, unexpired bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+name+" WHERE projected_at IS NULL),"+
			"EXISTS (SELECT 1 FROM "+name+" WHERE received_at>$1)", rawFrom).Scan(&pending, &unexpired); err != nil {
			return err
		}
		if pending || unexpired {
			continue
		}
		if _, err = tx.Exec(ctx, "DROP TABLE "+name); err != nil {
			return err
		}
		result.Dropped++
	}
	result.More = result.Visited == maintenancePartitions && !wrapped
	_, err := tx.Exec(ctx, "UPDATE projection_state SET maintenance_partition=$1 WHERE singleton", after)
	return err
}

func pruneRuns(ctx context.Context, tx pgx.Tx, now, historyFrom time.Time, result *MaintenanceBatch) error {
	// Work on one bounded cohort; durable marks survive partial attempt cleanup.
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE ql_prune_jobs (queue text,job_id text) ON COMMIT DROP;
		INSERT INTO ql_prune_jobs SELECT queue,job_id FROM job_runs
			WHERE pruning_at IS NOT NULL LIMIT 1000 FOR UPDATE`); err != nil {
		return err
	}
	var selected int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM ql_prune_jobs").Scan(&selected); err != nil {
		return err
	}
	if selected == 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO ql_prune_jobs SELECT queue,job_id FROM job_runs
			WHERE (complete_observed AND last_received_at<=$1)
				OR (NOT complete_observed AND first_received_at<=$1)
			ORDER BY first_received_at LIMIT 1000 FOR UPDATE`, historyFrom); err != nil {
			return err
		}
		marked, err := tx.Exec(ctx, `UPDATE job_runs r SET pruning_at=$1 FROM ql_prune_jobs p
			WHERE r.queue=p.queue AND r.job_id=p.job_id`, now)
		if err != nil {
			return err
		}
		result.StartedPruning = marked.RowsAffected()
	}
	deleted, err := tx.Exec(ctx, `WITH expired AS (SELECT a.queue,a.job_id,a.attempt FROM job_attempts a
		JOIN ql_prune_jobs p ON p.queue=a.queue AND p.job_id=a.job_id LIMIT 1000 FOR UPDATE OF a)
		DELETE FROM job_attempts a USING expired e WHERE a.queue=e.queue AND a.job_id=e.job_id AND a.attempt=e.attempt`)
	if err != nil {
		return err
	}
	result.Attempts = deleted.RowsAffected()
	finished, err := tx.Exec(ctx, `WITH removed AS (
		DELETE FROM job_runs r USING ql_prune_jobs p WHERE r.queue=p.queue AND r.job_id=p.job_id
			AND NOT EXISTS (SELECT 1 FROM job_attempts a WHERE a.queue=r.queue AND a.job_id=r.job_id)
		RETURNING r.queue,r.job_id)
		INSERT INTO pruned_jobs (queue,job_id,pruned_at) SELECT queue,job_id,$1 FROM removed
		ON CONFLICT (queue,job_id) DO UPDATE SET pruned_at=EXCLUDED.pruned_at`, now)
	if err != nil {
		return err
	}
	result.Runs = finished.RowsAffected()
	return nil
}
