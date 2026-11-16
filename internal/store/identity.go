package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

// ErrIdentityConflict excludes IDs and event content from the error.
var ErrIdentityConflict = errors.New("protocol event identity conflicts with retained content")

func fingerprint(event protocol.Event) ([]byte, error) {
	event.TS = event.TS.UTC()
	body, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	return digest[:], nil
}

type claimedIdentity struct {
	id, fingerprint string
}

// WriteBatch commits COPY staging, identity claims and raw evidence together.
// Its count excludes successful duplicate no-ops.
func (s *Store) WriteBatch(ctx context.Context, events []protocol.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	rows := make([][]any, 0, len(events))
	seen := make(map[string]claimedIdentity)
	for ordinal, event := range events {
		event.Normalize(protocol.DefaultLimits())
		digest, err := fingerprint(event)
		if err != nil {
			return 0, fmt.Errorf("fingerprint event: %w", err)
		}
		var eventID, key any
		kind := "missing"
		if event.ID != "" {
			eventID, kind = event.ID, "provided"
			sum := sha256.Sum256([]byte(event.ID))
			key = sum[:]
			scope := event.Queue + "\x00" + string(sum[:])
			previous, exists := seen[scope]
			if exists && (previous.id != event.ID || previous.fingerprint != string(digest)) {
				return 0, ErrIdentityConflict
			}
			seen[scope] = claimedIdentity{id: event.ID, fingerprint: string(digest)}
		}
		var duration any
		if event.DurationMS != nil {
			duration = *event.DurationMS
		}
		rows = append(rows, []any{ordinal, event.TS.UTC().Truncate(time.Microsecond),
			event.Queue, event.JobID, event.JobName, int16(event.Attempt), string(event.State),
			nullableString(event.WorkerID), duration, nullableString(event.Error),
			eventID, key, digest, event.TS.UTC().Format(time.RFC3339Nano), kind})
	}
	ctx, cancel, tx, err := s.begin(ctx, 15*time.Second)
	if err != nil {
		return 0, err
	}
	defer cancel()
	defer rollback(tx)
	if err = advisory(ctx, tx, "identity-retention", true); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `CREATE TEMP TABLE ql_event_batch (
		ordinal integer, ts timestamptz, queue text, job_id text, job_name text,
		attempt smallint, state text, worker_id text, duration_ms integer, error text,
		event_id text, event_key bytea, content_fingerprint bytea, source_ts text,
		identity_kind text) ON COMMIT DROP;
		CREATE TEMP TABLE ql_identity_claims (queue text, event_key bytea) ON COMMIT DROP`); err != nil {
		return 0, err
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"ql_event_batch"},
		[]string{"ordinal", "ts", "queue", "job_id", "job_name", "attempt", "state",
			"worker_id", "duration_ms", "error", "event_id", "event_key",
			"content_fingerprint", "source_ts", "identity_kind"}, pgx.CopyFromRows(rows)); err != nil {
		return 0, fmt.Errorf("stage events: %w", err)
	}
	// Stable claim order avoids deadlocks on overlapping identities.
	if _, err = tx.Exec(ctx, `WITH claims AS (
		INSERT INTO event_identities (queue,event_key,event_id,content_fingerprint)
		SELECT DISTINCT ON (queue COLLATE "C",event_key)
			queue,event_key,event_id,content_fingerprint FROM ql_event_batch
		WHERE event_id IS NOT NULL ORDER BY queue COLLATE "C",event_key,ordinal
		ON CONFLICT (queue,event_key) DO NOTHING RETURNING queue,event_key)
		INSERT INTO ql_identity_claims SELECT queue,event_key FROM claims`); err != nil {
		return 0, fmt.Errorf("claim event identities: %w", err)
	}
	var conflict bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM ql_event_batch b JOIN event_identities d
		ON d.queue=b.queue AND d.event_key=b.event_key
		WHERE b.event_id IS DISTINCT FROM d.event_id
			OR b.content_fingerprint IS DISTINCT FROM d.content_fingerprint)`).Scan(&conflict); err != nil {
		return 0, err
	}
	if conflict {
		return 0, ErrIdentityConflict
	}
	result, err := tx.Exec(ctx, `WITH ordered AS (
		SELECT *, row_number() OVER (PARTITION BY queue,event_key ORDER BY ordinal) AS occurrence
		FROM ql_event_batch)
		INSERT INTO queue_events (ts,queue,job_id,job_name,attempt,state,worker_id,duration_ms,error,
			event_id,event_key,content_fingerprint,source_ts,identity_kind,received_at)
		SELECT ts,queue,job_id,job_name,attempt,state,worker_id,duration_ms,error,
			event_id,event_key,content_fingerprint,source_ts,identity_kind,statement_timestamp()
		FROM ordered b WHERE event_id IS NULL OR
			(occurrence=1 AND EXISTS (SELECT 1 FROM ql_identity_claims c
				WHERE c.queue=b.queue AND c.event_key=b.event_key))`)
	if err != nil {
		return 0, fmt.Errorf("insert raw evidence: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
