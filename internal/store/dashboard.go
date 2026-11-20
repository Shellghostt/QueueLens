package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/dashboard"
	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/queues"
)

func (r *QueryReader) QueryDashboard(ctx context.Context, view string, query dashboard.Query, broker *queues.Store, now time.Time) (dashboard.Response, error) {
	result := dashboard.Response{}
	query, err := query.Normalize(view)
	if err != nil {
		return result, err
	}
	cursor, limit, err := r.prepare("/v1/dashboard/"+view, lifecycle.PageQuery{
		Queue: query.Queue, Status: query.Binding(), Limit: query.Limit, Cursor: query.Cursor}, now)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if broker == nil {
		broker = queues.NewStore()
	}
	switch view {
	case "overview", "queues":
		items, adapterErrors, pageErr := broker.Page(cursor.LastID, limit)
		if pageErr != nil {
			return result, pageErr
		}
		result.Queues = &dashboard.QueuePage{Items: items, AdapterErrors: adapterErrors, Source: "last_broker_poll"}
		if len(items) > limit {
			result.Queues.Items = items[:limit]
			cursor.LastID = items[limit-1].Queue
			result.Queues.NextCursor, err = r.encode(cursor)
		}
	case "queue", "jobs":
		result.Jobs, err = r.dashboardJobs(ctx, query, cursor, limit)
		if view == "queue" {
			result.Broker = broker.Get(query.Queue)
		}
	case "job":
		result.Job, err = r.dashboardJob(ctx, query, cursor, limit)
	}
	if err != nil {
		return dashboard.Response{}, err
	}
	if view == "overview" || view == "queue" {
		result.Metrics = r.dashboardMetrics(ctx, query, cursor.EvaluatedAt)
	}
	result.PageEvidence = r.evidence(ctx, cursor.EvaluatedAt, now)
	return result, nil
}

func (r *QueryReader) dashboardJobs(ctx context.Context, query dashboard.Query, cursor pageCursor, limit int) (*dashboard.JobPage, error) {
	page := &dashboard.JobPage{Items: []lifecycle.JobItem{}}
	ctx, cancel, tx, err := r.store.begin(ctx, 3*time.Second, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer rollback(tx)
	rows, err := tx.Query(ctx, `SELECT CASE WHEN octet_length(summary::text)<=16384 THEN summary END
		FROM job_runs WHERE pruning_at IS NULL AND ($1::text='' OR queue=$1) AND ($2::text='all' OR state=$2)
		AND (($3::text='' AND $4::text='') OR
			(queue COLLATE "C",job_id COLLATE "C") > ($3 COLLATE "C",$4 COLLATE "C"))
		ORDER BY queue COLLATE "C",job_id COLLATE "C" LIMIT $5`, query.Queue, query.State, cursor.LastQueue, cursor.LastID, limit+1)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var body []byte
		var run lifecycle.Run
		if err = rows.Scan(&body); err == nil {
			err = json.Unmarshal(body, &run)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		page.Items = append(page.Items, r.dashboardItem(run, cursor.EvaluatedAt))
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		last := page.Items[limit-1]
		cursor.LastQueue, cursor.LastID = last.Queue, last.JobID
		page.NextCursor, err = r.encode(cursor)
	}
	return page, err
}

func (r *QueryReader) dashboardJob(ctx context.Context, query dashboard.Query, cursor pageCursor, limit int) (*dashboard.JobDetail, error) {
	job := &dashboard.JobDetail{Queue: query.Queue, JobID: query.JobID, Availability: "not_observed", Attempts: []lifecycle.JobItem{}}
	after := 0
	if cursor.LastID != "" {
		var err error
		after, err = strconv.Atoi(cursor.LastID)
		if err != nil || after < 1 || after > 32767 {
			return nil, lifecycle.ErrInvalidQuery
		}
	}
	// One snapshot prevents a committed pruning fence from being paired with a
	// different, partially deleted attempt page. There is no snapshot across pages.
	ctx, cancel, tx, err := r.store.begin(ctx, 3*time.Second, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer rollback(tx)
	var body []byte
	var pruning *time.Time
	err = tx.QueryRow(ctx, `SELECT CASE WHEN octet_length(summary::text)<=16384 THEN summary END,pruning_at
		FROM job_runs WHERE queue=$1 AND job_id=$2`, query.Queue, query.JobID).Scan(&body, &pruning)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT pruned_at FROM pruned_jobs WHERE queue=$1 AND job_id=$2 AND pruned_at>$3`,
			query.Queue, query.JobID, cursor.EvaluatedAt.Add(-r.policy.HistoryRetention)).Scan(&job.PrunedAt)
		if err == nil {
			job.Availability = "pruned"
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		return job, tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}
	if pruning != nil {
		job.Availability = "pruning"
		return job, tx.Commit(ctx)
	}
	var run lifecycle.Run
	if err = json.Unmarshal(body, &run); err != nil {
		return nil, err
	}
	item := r.dashboardItem(run, cursor.EvaluatedAt)
	job.Summary, job.Availability = &item, "observed"
	rows, err := tx.Query(ctx, `SELECT CASE WHEN octet_length(summary::text)<=16384 THEN summary END
		FROM job_attempts WHERE queue=$1 AND job_id=$2 AND attempt>$3 ORDER BY attempt LIMIT $4`,
		query.Queue, query.JobID, after, limit+1)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var attempt lifecycle.Attempt
		if err = rows.Scan(&body); err == nil {
			err = json.Unmarshal(body, &attempt)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		incomplete := 0
		if !attempt.StructurallyComplete() {
			incomplete = 1
		}
		job.Attempts = append(job.Attempts, lifecycle.Item(lifecycle.Run{Queue: run.Queue, JobID: run.JobID,
			Current: attempt, ObservedAttempts: run.ObservedAttempts, IncompleteAttempts: incomplete,
			Evidence: attempt.Evidence, HistoryPruned: run.HistoryPruned}, false, r.policy))
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(job.Attempts) > limit {
		job.Attempts = job.Attempts[:limit]
		cursor.LastID = strconv.Itoa(job.Attempts[limit-1].Attempt)
		job.NextCursor, err = r.encode(cursor)
	}
	return job, err
}

func (r *QueryReader) dashboardItem(run lifecycle.Run, at time.Time) lifecycle.JobItem {
	state := run.Current.Selected.State
	unresolved := state == "enqueued" || state == "started" || state == "retried"
	suspected := unresolved && r.policy.StallAfter > 0 && !run.Evidence.LastReceiptAt.After(at.Add(-r.policy.StallAfter))
	return lifecycle.Item(run, suspected, r.policy)
}

func (r *QueryReader) dashboardMetrics(ctx context.Context, query dashboard.Query, at time.Time) *dashboard.Metrics {
	window, _ := dashboard.WindowDuration(query.Window)
	m := &dashboard.Metrics{Window: query.Window, From: at.Add(-window), Until: at,
		Source: "retained_selected_attempt_outcomes", Reason: "aggregation_unavailable",
		Limitations: []string{
			"Selected completed attempts per window second; failed / (failed + completed), not unique production jobs.",
			"Superseded retry failures, lost telemetry, retention, late facts and producer clock disagreement can change these rates.",
			"Conflicting selected outcomes remain counted and are disclosed; broker counts are independent retained set sizes.",
		}}
	if window > r.policy.HistoryRetention {
		m.Reason = "outside_history_coverage"
		return m
	}
	ctx, cancel, tx, err := r.store.begin(ctx, dashboard.MetricTimeout, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return m
	}
	defer cancel()
	defer rollback(tx)
	// The context bounds the complete aggregation including pool wait and both
	// queries. Use the full 150ms statement budget rather than begin's short default.
	if _, err = tx.Exec(ctx, `SELECT set_config('statement_timeout','150ms',true)`); err != nil {
		return m
	}
	filter := ""
	args := []any{m.From, m.Until}
	if query.Queue != "" {
		filter = " AND a.queue=$3"
		args = append(args, query.Queue)
	}
	pruning := ` AND NOT EXISTS (SELECT 1 FROM job_runs r WHERE r.pruning_at IS NOT NULL AND r.queue=a.queue AND r.job_id=a.job_id)`
	var missing bool
	missingArgs := []any{}
	missingFilter := ""
	if query.Queue != "" {
		missingFilter = " AND a.queue=$1"
		missingArgs = append(missingArgs, query.Queue)
	}
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_attempts a
		WHERE (a.selected_state IS NULL OR a.selected_at IS NULL OR a.selected_conflict IS NULL)`+missingFilter+pruning+` LIMIT 1)`, missingArgs...).Scan(&missing); err != nil {
		return m
	}
	if missing {
		m.Reason = "missing_selected_cache"
		return m
	}
	var eligible, completed, failed, conflicting int64
	err = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE selected_state='completed'),count(*) FILTER(WHERE selected_state='failed'),
		count(*) FILTER(WHERE selected_conflict) FROM (
		SELECT a.selected_state,a.selected_conflict FROM job_attempts a
		WHERE a.selected_state IN ('completed','failed') AND a.selected_at >= $1 AND a.selected_at < $2`+filter+pruning+`
		LIMIT 500001) eligible`, args...).Scan(&eligible, &completed, &failed, &conflicting)
	if err != nil {
		return m
	}
	if eligible > dashboard.MetricRowLimit {
		m.Reason = "terminal_work_limit"
		return m
	}
	if err = tx.Commit(ctx); err != nil {
		return m
	}
	m.Available, m.Reason = true, ""
	m.Completed, m.Failed, m.Conflicting = &completed, &failed, &conflicting
	throughput := float64(completed) / window.Seconds()
	m.CompletionsPerSecond = &throughput
	if eligible != 0 {
		fraction := float64(failed) / float64(eligible)
		m.FailureFraction = &fraction
	}
	return m
}
