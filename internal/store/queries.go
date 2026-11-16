package store

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/lifecycle"
)

// QueryReader signs cursors with a key derived from the existing admin token.
type QueryReader struct {
	store  *Store
	policy lifecycle.Policy
	key    [32]byte
}

func NewReader(s *Store, policy lifecycle.Policy, adminToken string) *QueryReader {
	return &QueryReader{store: s, policy: policy, key: sha256.Sum256([]byte("queuelens-cursor-v1\x00" + adminToken))}
}

type pageCursor struct {
	Version     int       `json:"v"`
	Route       string    `json:"route"`
	Queue       string    `json:"queue"`
	Status      string    `json:"status"`
	EvaluatedAt time.Time `json:"evaluated_at"`
	LastQueue   string    `json:"last_queue"`
	LastID      string    `json:"last_id"`
}

func (r *QueryReader) prepare(route string, query lifecycle.PageQuery, now time.Time) (pageCursor, int, error) {
	invalid := lifecycle.ErrInvalidQuery
	if r.policy.Validate() != nil || query.Limit < 0 || query.Limit > lifecycle.PageLimit ||
		len(query.Queue) > 256 || !utf8.ValidString(query.Queue) ||
		(query.Queue != "" && strings.TrimSpace(query.Queue) == "") ||
		len(query.Cursor) > lifecycle.MaxCursorBytes {
		return pageCursor{}, 0, invalid
	}
	limit := query.Limit
	if limit == 0 {
		limit = lifecycle.DefaultPageLimit
	}
	cursor := pageCursor{Version: 1, Route: route, Queue: query.Queue, Status: query.Status, EvaluatedAt: r.policy.EvaluationAt(now)}
	if query.Cursor == "" {
		return cursor, limit, nil
	}
	bodyPart, signaturePart, ok := strings.Cut(query.Cursor, ".")
	if !ok {
		return pageCursor{}, 0, invalid
	}
	body, err := base64.RawURLEncoding.DecodeString(bodyPart)
	if err != nil {
		return pageCursor{}, 0, invalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(signaturePart)
	if err != nil {
		return pageCursor{}, 0, invalid
	}
	mac := hmac.New(sha256.New, r.key[:])
	_, _ = mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return pageCursor{}, 0, invalid
	}
	if err = json.Unmarshal(body, &cursor); err != nil {
		return pageCursor{}, 0, invalid
	}
	if cursor.Version != 1 || cursor.Route != route || cursor.Queue != query.Queue || cursor.Status != query.Status ||
		cursor.EvaluatedAt.IsZero() || cursor.EvaluatedAt.After(r.policy.EvaluationAt(now)) ||
		len(cursor.LastQueue) > 256 || len(cursor.LastID) > 256 || cursor.LastID == "" {
		return pageCursor{}, 0, invalid
	}
	return cursor, limit, nil
}

func (r *QueryReader) encode(cursor pageCursor) (string, error) {
	body, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, r.key[:])
	_, _ = mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (r *QueryReader) evidence(ctx context.Context, at, now time.Time) lifecycle.PageEvidence {
	return lifecycle.PageEvidence{EvaluatedAt: at, HistoryFrom: at.Add(-r.policy.HistoryRetention),
		ProjectionLag: r.store.ProjectionLag(ctx, now),
		Consistency:   "live_row_keyset",
		Limitations: []string{
			"Best-effort telemetry cannot prove complete history or retry exhaustion.",
			"Concurrent ingest, replay and retention can change membership between pages.",
			"Producer clocks do not establish execution duration; only supplied duration is reported.",
			"Pruning runs are temporarily omitted; reconstructed history is limited while its tombstone is retained.",
		}}
}

func (r *QueryReader) QueryJobs(ctx context.Context, view string, query lifecycle.PageQuery, now time.Time) (lifecycle.JobPage, error) {
	page := lifecycle.JobPage{Items: []lifecycle.JobItem{}}
	if view != "failed" && view != "stalled" && view != "suspected-stalled" || query.Status != "" {
		return page, lifecycle.ErrInvalidQuery
	}
	cursor, limit, err := r.prepare("/v1/jobs/"+view, query, now)
	if err != nil {
		return page, err
	}
	queryCtx, cancel, tx, err := r.store.begin(ctx, 3*time.Second, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer cancel()
	defer rollback(tx)
	predicate := "state=$1"
	var selection any = view
	if view == "suspected-stalled" {
		predicate = "state IN ('enqueued','started','retried') AND last_received_at<=$1"
		selection = cursor.EvaluatedAt.Add(-r.policy.StallAfter)
		if r.policy.StallAfter == 0 {
			predicate = "false AND $1::timestamptz IS NOT NULL"
		}
	}
	statement := `SELECT summary FROM job_runs WHERE pruning_at IS NULL AND ` + predicate +
		` AND ($2::text='' OR queue=$2) AND (($3::text='' AND $4::text='') OR
			(queue COLLATE "C",job_id COLLATE "C") > ($3 COLLATE "C",$4 COLLATE "C"))
			ORDER BY queue COLLATE "C",job_id COLLATE "C" LIMIT $5`
	rows, err := tx.Query(queryCtx, statement, selection, query.Queue, cursor.LastQueue, cursor.LastID, limit+1)
	if err != nil {
		return page, err
	}
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			rows.Close()
			return page, err
		}
		var run lifecycle.Run
		if err = json.Unmarshal(body, &run); err != nil {
			rows.Close()
			return page, err
		}
		page.Items = append(page.Items, lifecycle.Item(run, view == "suspected-stalled", r.policy))
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return page, err
	}
	if err = tx.Commit(queryCtx); err != nil {
		return page, err
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		last := page.Items[limit-1]
		cursor.LastQueue, cursor.LastID = last.Queue, last.JobID
		if page.NextCursor, err = r.encode(cursor); err != nil {
			return page, err
		}
	}
	page.PageEvidence = r.evidence(ctx, cursor.EvaluatedAt, now)
	if view == "suspected-stalled" && r.policy.StallAfter == 0 {
		page.Limitations = append(page.Limitations, "Suspected-stall policy is disabled.")
	}
	return page, nil
}

func (r *QueryReader) QueryWorkers(ctx context.Context, query lifecycle.PageQuery, now time.Time) (lifecycle.WorkerPage, error) {
	page := lifecycle.WorkerPage{Items: []lifecycle.WorkerItem{}}
	if query.Status == "" {
		query.Status = "all"
	}
	if query.Queue != "" || (query.Status != "all" && query.Status != "suspected_down") {
		return page, lifecycle.ErrInvalidQuery
	}
	cursor, limit, err := r.prepare("/v1/workers", query, now)
	if err != nil {
		return page, err
	}
	queryCtx, cancel, tx, err := r.store.begin(ctx, 3*time.Second, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer cancel()
	defer rollback(tx)
	rows, err := tx.Query(queryCtx, `SELECT worker_id,pool,first_seen,last_seen FROM workers
		WHERE worker_id COLLATE "C" > $1 COLLATE "C"
			AND ($2::text='all' OR last_seen<=$3)
		ORDER BY worker_id COLLATE "C" LIMIT $4`, cursor.LastID, query.Status,
		cursor.EvaluatedAt.Add(-lifecycle.WorkerDownAfter), limit+1)
	if err != nil {
		return page, err
	}
	for rows.Next() {
		var item lifecycle.WorkerItem
		if err = rows.Scan(&item.WorkerID, &item.Pool, &item.FirstSeen, &item.LastSeen); err != nil {
			rows.Close()
			return page, err
		}
		item.ReceiptAgeSeconds = cursor.EvaluatedAt.Sub(item.LastSeen).Seconds()
		if item.ReceiptAgeSeconds < 0 {
			item.ReceiptAgeSeconds = 0
		}
		item.Status = "alive"
		if lifecycle.SuspectedDown(item.LastSeen, cursor.EvaluatedAt) {
			item.Status = "suspected_down"
			item.Suspicion = &lifecycle.Suspicion{Reason: "no_worker_receipt", ThresholdSeconds: lifecycle.WorkerDownAfter.Seconds()}
		}
		page.Items = append(page.Items, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return page, err
	}
	if err = tx.Commit(queryCtx); err != nil {
		return page, err
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		cursor.LastID = page.Items[limit-1].WorkerID
		if page.NextCursor, err = r.encode(cursor); err != nil {
			return page, err
		}
	}
	page.PageEvidence = r.evidence(ctx, cursor.EvaluatedAt, now)
	return page, nil
}

// ProjectionLag reads at most 1,000 pending receipts and 1,000 pruning keys.
// A capped sample is a lower bound. A failed bounded query stays unknown.
func (s *Store) ProjectionLag(ctx context.Context, now time.Time) lifecycle.ProjectionLag {
	unknown := lifecycle.ProjectionLag{SampleLimit: 1000, Reason: "bounded lag query unavailable"}
	ctx, cancel, tx, err := s.begin(ctx, 500*time.Millisecond, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return unknown
	}
	defer cancel()
	defer rollback(tx)
	lag := lifecycle.ProjectionLag{SampleLimit: 1000}
	rows, err := tx.Query(ctx, `SELECT received_at FROM queue_events WHERE projected_at IS NULL
		ORDER BY received_at,ts,id LIMIT 1000`)
	if err != nil {
		return unknown
	}
	for rows.Next() {
		var receipt time.Time
		if err = rows.Scan(&receipt); err != nil {
			rows.Close()
			return unknown
		}
		if lag.OldestPendingReceipt == nil {
			at := receipt
			lag.OldestPendingReceipt = &at
		}
		lag.PendingLowerBound++
	}
	rows.Close()
	if rows.Err() != nil {
		return unknown
	}
	lag.SampleCapped = lag.PendingLowerBound == lag.SampleLimit
	if !lag.SampleCapped {
		count := lag.PendingLowerBound
		lag.PendingRows = &count
	}
	seconds := 0.0
	if lag.OldestPendingReceipt != nil {
		seconds = now.Sub(*lag.OldestPendingReceipt).Seconds()
		if seconds < 0 {
			seconds = 0
		}
	}
	lag.LagSeconds = &seconds
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM job_runs
		WHERE pruning_at IS NOT NULL LIMIT 1000) AS bounded`).Scan(&lag.PruningRunsLowerBound); err != nil {
		return unknown
	}
	lag.PruningSampleCapped = lag.PruningRunsLowerBound == lag.SampleLimit
	if err = tx.QueryRow(ctx, "SELECT last_batch_at,last_scan_at FROM projection_state WHERE singleton").
		Scan(&lag.LastBatchAt, &lag.LastScanAt); err != nil {
		return unknown
	}
	if err = tx.Commit(ctx); err != nil {
		return unknown
	}
	lag.Known = true
	return lag
}
