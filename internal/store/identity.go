package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	queue           string
	key             []byte
	id, fingerprint string
}

// WriteBatch commits identity claims and raw evidence together.
// Its count excludes successful duplicate no-ops.
func (s *Store) WriteBatch(ctx context.Context, events []protocol.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	rows := make([][]any, 0, len(events))
	seen := make(map[string]claimedIdentity)
	for _, event := range events {
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
			seen[scope] = claimedIdentity{queue: event.Queue, key: sum[:],
				id: event.ID, fingerprint: string(digest)}
		}
		var duration any
		if event.DurationMS != nil {
			duration = *event.DurationMS
		}
		rows = append(rows, []any{event.TS.UTC().Truncate(time.Microsecond),
			event.Queue, event.JobID, event.JobName, int16(event.Attempt), string(event.State),
			nullableString(event.WorkerID), duration, nullableString(event.Error),
			eventID, key, digest, event.TS.UTC().Format(time.RFC3339Nano), kind, nil})
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
	if len(seen) > 0 {
		if err = advisory(ctx, tx, "identity-claims", false); err != nil {
			return 0, err
		}
	}
	// Missing-only batches require no identity verification or claims lane.
	newKeys := make(map[string]bool)
	var receipt time.Time
	if len(seen) > 0 {
		newKeys, receipt, err = copyIdentityClaims(ctx, tx, seen)
		if err != nil {
			return 0, err
		}
	} else {
		if err = tx.QueryRow(ctx, "SELECT statement_timestamp()").Scan(&receipt); err != nil {
			return 0, err
		}
	}
	selected := make([][]any, 0, len(rows))
	for _, row := range rows {
		if row[9] != nil {
			scope := row[1].(string) + "\x00" + string(row[10].([]byte))
			if !newKeys[scope] {
				continue
			}
			delete(newKeys, scope)
		}
		row[14] = receipt
		selected = append(selected, row)
	}
	var count int64
	if len(selected) > 0 {
		count, err = tx.CopyFrom(ctx, pgx.Identifier{"queue_events"},
			[]string{"ts", "queue", "job_id", "job_name", "attempt", "state", "worker_id", "duration_ms", "error",
				"event_id", "event_key", "content_fingerprint", "source_ts", "identity_kind", "received_at"},
			pgx.CopyFromRows(selected))
		if err != nil {
			return 0, fmt.Errorf("copy raw evidence: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

func copyIdentityClaims(ctx context.Context, tx pgx.Tx, incoming map[string]claimedIdentity) (map[string]bool, time.Time, error) {
	// In-batch conflicts were already checked. Keep the four input arrays aligned
	// and bounded to distinct incoming keys; raw payloads are copied only once.
	scopes := make([]string, 0, len(incoming))
	for scope := range incoming {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	queues := make([]string, 0, len(scopes))
	keys := make([][]byte, 0, len(scopes))
	ids := make([]string, 0, len(scopes))
	fingerprints := make([][]byte, 0, len(scopes))
	for _, scope := range scopes {
		value := incoming[scope]
		queues = append(queues, value.queue)
		keys = append(keys, value.key)
		ids = append(ids, value.id)
		fingerprints = append(fingerprints, []byte(value.fingerprint))
	}
	// The limit keeps each lookup on its exact key rather than hashing all history.
	rows, err := tx.Query(ctx, `SELECT b.queue,b.event_key,d.event_key IS NOT NULL,
		d.event_key IS NOT NULL AND (b.event_id IS DISTINCT FROM d.event_id
			OR b.content_fingerprint IS DISTINCT FROM d.content_fingerprint),
		statement_timestamp()
		FROM unnest($1::text[],$2::bytea[],$3::text[],$4::bytea[])
			AS b(queue,event_key,event_id,content_fingerprint)
		LEFT JOIN LATERAL (SELECT event_key,event_id,content_fingerprint FROM event_identities
			WHERE queue=b.queue AND event_key=b.event_key LIMIT 1) d ON true
		ORDER BY b.queue COLLATE "C",b.event_key`, queues, keys, ids, fingerprints)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()
	ledger := make([][]any, 0, len(incoming))
	newKeys := make(map[string]bool, len(incoming))
	var batchReceipt time.Time
	checked := 0
	for rows.Next() {
		var queue string
		var key []byte
		var exists, conflict bool
		var receipt time.Time
		if err = rows.Scan(&queue, &key, &exists, &conflict, &receipt); err != nil {
			return nil, time.Time{}, err
		}
		batchReceipt = receipt
		checked++
		if conflict {
			return nil, time.Time{}, ErrIdentityConflict
		}
		if exists {
			continue
		}
		value, found := incoming[queue+"\x00"+string(key)]
		if !found {
			return nil, time.Time{}, errors.New("verified identity key does not match incoming evidence")
		}
		key = append([]byte{}, key...)
		ledger = append(ledger, []any{queue, key, value.id, []byte(value.fingerprint), receipt})
		newKeys[queue+"\x00"+string(key)] = true
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, time.Time{}, err
	}
	if checked != len(incoming) {
		return nil, time.Time{}, errors.New("verified identity count changed")
	}
	if len(ledger) == 0 {
		return newKeys, batchReceipt, nil
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"event_identities"},
		[]string{"queue", "event_key", "event_id", "content_fingerprint", "first_received_at"},
		pgx.CopyFromRows(ledger)); err != nil {
		return nil, time.Time{}, fmt.Errorf("copy event identities: %w", err)
	}
	return newKeys, batchReceipt, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
