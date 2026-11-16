package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

type jobKey struct {
	Queue string `json:"queue"`
	JobID string `json:"job_id"`
}
type attemptKey struct {
	jobKey
	Attempt int `json:"attempt"`
}
type rawFact struct {
	RowTS time.Time
	ID    int64
	Fact  lifecycle.Fact
}

type ProjectionBatch struct {
	Partition          string
	Processed, Expired int
	Wrapped            bool
}

// ProjectBatch acknowledges exact (ts,id) rows together with their summaries.
// The scan cursor only chooses which partition is revisited next.
func (s *Store) ProjectBatch(ctx context.Context, now time.Time, policy lifecycle.Policy) (ProjectionBatch, error) {
	result := ProjectionBatch{}
	if err := policy.Validate(); err != nil {
		return result, err
	}
	now = now.UTC()
	ctx, cancel, tx, err := s.begin(ctx, 15*time.Second)
	if err != nil {
		return result, err
	}
	defer cancel()
	defer rollback(tx)
	if err = advisory(ctx, tx, "folder", false); err != nil {
		return result, err
	}
	var after string
	if err = tx.QueryRow(ctx, "SELECT last_partition FROM projection_state WHERE singleton FOR UPDATE").Scan(&after); err != nil {
		return result, err
	}
	p, wrapped, err := nextPartition(ctx, tx, after)
	if err != nil {
		return result, err
	}
	result.Partition, result.Wrapped = p.Name, wrapped
	if p.Name != "" {
		cutoff := now.Add(-policy.HistoryRetention)
		raw, readErr := readRawFacts(ctx, tx, p, policy.BatchSize, cutoff)
		if readErr != nil {
			return result, readErr
		}
		result.Processed = len(raw)
		groups := make(map[attemptKey]lifecycle.Attempt, len(raw))
		for _, row := range raw {
			if !row.Fact.ReceivedAt.After(cutoff) {
				result.Expired++
				continue
			}
			next, ok := lifecycle.FromFact(row.Fact)
			if !ok {
				continue
			}
			key := attemptKey{jobKey: jobKey{Queue: row.Fact.Queue, JobID: row.Fact.JobID}, Attempt: next.Number}
			groups[key] = lifecycle.MergeAttempt(groups[key], next)
		}
		if err = mergeProjections(ctx, tx, groups, cutoff); err != nil {
			return result, err
		}
		if len(raw) > 0 {
			timestamps := make([]time.Time, len(raw))
			ids := make([]int64, len(raw))
			for i, row := range raw {
				timestamps[i], ids[i] = row.RowTS, row.ID
			}
			statement := "UPDATE " + pgx.Identifier{p.Schema, p.Name}.Sanitize() + ` r
				SET projected_at=$1,projection_status=CASE WHEN received_at<=$2
					THEN 'expired_history' ELSE 'projected' END
				FROM unnest($3::timestamptz[],$4::bigint[]) AS chosen(ts,id)
				WHERE r.ts=chosen.ts AND r.id=chosen.id`
			marked, markErr := tx.Exec(ctx, statement, now, cutoff, timestamps, ids)
			if markErr != nil {
				return result, markErr
			}
			if marked.RowsAffected() != int64(len(raw)) {
				return result, errors.New("processing checkpoint row count changed")
			}
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE projection_state SET last_partition=$1,last_scan_at=$2,
		last_batch_at=CASE WHEN $3::integer>0 THEN $2 ELSE last_batch_at END WHERE singleton`,
		p.Name, now, result.Processed); err != nil {
		return result, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ProjectionBatch{}, err
	}
	return result, nil
}

func readRawFacts(ctx context.Context, tx pgx.Tx, p partition, limit int, historyFrom time.Time) ([]rawFact, error) {
	statement := `SELECT ts,id,queue,job_id,left(job_name,256),attempt,state,
		COALESCE(left(worker_id,128),''),duration_ms,COALESCE(left(error,1024),''),
		received_at,identity_kind,COALESCE(encode(content_fingerprint,'hex'),''),source_ts
		FROM ` + pgx.Identifier{p.Schema, p.Name}.Sanitize() +
		` r WHERE projected_at IS NULL AND (received_at<=$2 OR state='drained' OR NOT EXISTS
			(SELECT 1 FROM job_runs j WHERE j.queue=r.queue AND j.job_id=r.job_id AND j.pruning_at IS NOT NULL))
			ORDER BY received_at,ts,id LIMIT $1 FOR UPDATE`
	rows, err := tx.Query(ctx, statement, limit, historyFrom)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	raw := make([]rawFact, 0, limit)
	for rows.Next() {
		var row rawFact
		var source *string
		f := &row.Fact
		if err = rows.Scan(&row.RowTS, &row.ID, &f.Queue, &f.JobID, &f.JobName, &f.Attempt, &f.State,
			&f.WorkerID, &f.DurationMS, &f.Error, &f.ReceivedAt, &f.IdentityKind, &f.Fingerprint, &source); err != nil {
			return nil, err
		}
		f.TS = row.RowTS
		if source != nil {
			if f.TS, err = time.Parse(time.RFC3339Nano, *source); err != nil {
				return nil, fmt.Errorf("read source timestamp: %w", err)
			}
		}
		if f.Fingerprint == "" {
			event := protocol.Event{V: 1, TS: f.TS, Queue: f.Queue, JobID: f.JobID, JobName: f.JobName,
				Attempt: f.Attempt, State: f.State, WorkerID: f.WorkerID, DurationMS: f.DurationMS, Error: f.Error}
			event.Normalize(protocol.DefaultLimits())
			digest, digestErr := fingerprint(event)
			if digestErr != nil {
				return nil, digestErr
			}
			f.Fingerprint = hex.EncodeToString(digest)
		}
		raw = append(raw, row)
	}
	return raw, rows.Err()
}

func mergeProjections(ctx context.Context, tx pgx.Tx, groups map[attemptKey]lifecycle.Attempt, historyFrom time.Time) error {
	if len(groups) == 0 {
		return nil
	}
	keys := make([]attemptKey, 0, len(groups))
	jobs := make(map[jobKey]struct{}, len(groups))
	for key := range groups {
		keys = append(keys, key)
		jobs[key.jobKey] = struct{}{}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.Queue != b.Queue {
			return a.Queue < b.Queue
		}
		if a.JobID != b.JobID {
			return a.JobID < b.JobID
		}
		return a.Attempt < b.Attempt
	})
	keyJSON, err := json.Marshal(keys)
	if err != nil {
		return err
	}
	oldAttempts, err := loadAttempts(ctx, tx, keyJSON)
	if err != nil {
		return err
	}
	jobKeys := make([]jobKey, 0, len(jobs))
	for key := range jobs {
		jobKeys = append(jobKeys, key)
	}
	jobJSON, err := json.Marshal(jobKeys)
	if err != nil {
		return err
	}
	runs, err := loadRuns(ctx, tx, jobJSON)
	if err != nil {
		return err
	}
	pruned, err := loadPruned(ctx, tx, jobJSON, historyFrom)
	if err != nil {
		return err
	}
	updates := make([]attemptUpdate, 0, len(keys))
	for _, key := range keys {
		next := groups[key]
		var previous *lifecycle.Attempt
		if old, exists := oldAttempts[key]; exists {
			previous = &old
			next = lifecycle.MergeAttempt(old, next)
		}
		run, exists := runs[key.jobKey]
		if !exists {
			run = lifecycle.Run{Queue: key.Queue, JobID: key.JobID, HistoryPruned: pruned[key.jobKey]}
		}
		run = lifecycle.ReplaceAttempt(run, previous, next)
		runs[key.jobKey] = run
		updates = append(updates, attemptUpdate{attemptKey: key, Summary: next})
	}
	attemptJSON, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO job_attempts (queue,job_id,attempt,summary)
		SELECT queue,job_id,attempt,summary FROM jsonb_to_recordset($1::jsonb)
		AS changed(queue text,job_id text,attempt smallint,summary jsonb)
		ON CONFLICT (queue,job_id,attempt) DO UPDATE SET summary=EXCLUDED.summary`, attemptJSON); err != nil {
		return err
	}
	runUpdates := make([]runUpdate, 0, len(runs))
	for _, run := range runs {
		runUpdates = append(runUpdates, runUpdate{
			jobKey: jobKey{Queue: run.Queue, JobID: run.JobID}, ReportedAttempt: run.Current.Number,
			ObservedAttempts: run.ObservedAttempts, IncompleteAttempts: run.IncompleteAttempts,
			State: run.Current.Selected.State, FirstReceivedAt: run.Evidence.FirstReceiptAt,
			LastReceivedAt: run.Evidence.LastReceiptAt, CompleteObserved: run.CompleteObserved(), Summary: run})
	}
	runJSON, err := json.Marshal(runUpdates)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO job_runs (queue,job_id,reported_attempt,observed_attempts,incomplete_attempts,
		state,first_received_at,last_received_at,complete_observed,summary)
		SELECT queue,job_id,reported_attempt,observed_attempts,incomplete_attempts,
			state,first_received_at,last_received_at,complete_observed,summary
		FROM jsonb_to_recordset($1::jsonb) AS changed(queue text,job_id text,reported_attempt smallint,
			observed_attempts integer,incomplete_attempts integer,state text,first_received_at timestamptz,
			last_received_at timestamptz,complete_observed boolean,summary jsonb)
		ON CONFLICT (queue,job_id) DO UPDATE SET reported_attempt=EXCLUDED.reported_attempt,
			observed_attempts=EXCLUDED.observed_attempts,incomplete_attempts=EXCLUDED.incomplete_attempts,
			state=EXCLUDED.state,first_received_at=EXCLUDED.first_received_at,last_received_at=EXCLUDED.last_received_at,
			complete_observed=EXCLUDED.complete_observed,summary=EXCLUDED.summary`, runJSON)
	return err
}

type attemptUpdate struct {
	attemptKey
	Summary lifecycle.Attempt `json:"summary"`
}
type runUpdate struct {
	jobKey
	ReportedAttempt    int            `json:"reported_attempt"`
	ObservedAttempts   int            `json:"observed_attempts"`
	IncompleteAttempts int            `json:"incomplete_attempts"`
	State              protocol.State `json:"state"`
	FirstReceivedAt    time.Time      `json:"first_received_at"`
	LastReceivedAt     time.Time      `json:"last_received_at"`
	CompleteObserved   bool           `json:"complete_observed"`
	Summary            lifecycle.Run  `json:"summary"`
}

func loadAttempts(ctx context.Context, tx pgx.Tx, keys []byte) (map[attemptKey]lifecycle.Attempt, error) {
	rows, err := tx.Query(ctx, `SELECT a.queue,a.job_id,a.attempt,a.summary FROM job_attempts a
		JOIN jsonb_to_recordset($1::jsonb) AS wanted(queue text,job_id text,attempt smallint)
		ON a.queue=wanted.queue AND a.job_id=wanted.job_id AND a.attempt=wanted.attempt FOR UPDATE OF a`, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[attemptKey]lifecycle.Attempt)
	for rows.Next() {
		var key attemptKey
		var body []byte
		if err = rows.Scan(&key.Queue, &key.JobID, &key.Attempt, &body); err != nil {
			return nil, err
		}
		var summary lifecycle.Attempt
		if err = json.Unmarshal(body, &summary); err != nil {
			return nil, err
		}
		result[key] = summary
	}
	return result, rows.Err()
}

func loadRuns(ctx context.Context, tx pgx.Tx, keys []byte) (map[jobKey]lifecycle.Run, error) {
	rows, err := tx.Query(ctx, `SELECT r.queue,r.job_id,r.summary FROM job_runs r
		JOIN jsonb_to_recordset($1::jsonb) AS wanted(queue text,job_id text)
		ON r.queue=wanted.queue AND r.job_id=wanted.job_id FOR UPDATE OF r`, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[jobKey]lifecycle.Run)
	for rows.Next() {
		var key jobKey
		var body []byte
		if err = rows.Scan(&key.Queue, &key.JobID, &body); err != nil {
			return nil, err
		}
		var summary lifecycle.Run
		if err = json.Unmarshal(body, &summary); err != nil {
			return nil, err
		}
		result[key] = summary
	}
	return result, rows.Err()
}

func loadPruned(ctx context.Context, tx pgx.Tx, keys []byte, historyFrom time.Time) (map[jobKey]bool, error) {
	rows, err := tx.Query(ctx, `SELECT p.queue,p.job_id FROM pruned_jobs p
		JOIN jsonb_to_recordset($1::jsonb) AS wanted(queue text,job_id text)
		ON p.queue=wanted.queue AND p.job_id=wanted.job_id WHERE p.pruned_at>$2`, keys, historyFrom)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[jobKey]bool)
	for rows.Next() {
		var key jobKey
		if err = rows.Scan(&key.Queue, &key.JobID); err != nil {
			return nil, err
		}
		result[key] = true
	}
	return result, rows.Err()
}
