-- Derived selected-observation caches; no new lifecycle/folding semantics.
ALTER TABLE job_attempts
    ADD COLUMN selected_state text,
    ADD COLUMN selected_at timestamptz,
    ADD COLUMN selected_conflict boolean;
UPDATE job_attempts SET
    selected_state = summary->'selected'->>'state',
    -- Floor sub-microsecond digits before PostgreSQL can round across a UTC
    -- evaluation boundary. The selected JSON timestamp remains unchanged.
    selected_at = regexp_replace(summary->'selected'->>'ts', '(?<=[.][0-9]{6})[0-9]+', '')::timestamptz,
    selected_conflict = (summary->'evidence'->>'TerminalConflict')::boolean;
ALTER TABLE job_attempts ADD CONSTRAINT job_attempts_selected_cache CHECK (
    (selected_state IS NULL AND selected_at IS NULL AND selected_conflict IS NULL)
    OR (selected_state IN ('enqueued','started','completed','failed','retried','stalled')
        AND selected_state IS NOT NULL AND selected_at IS NOT NULL AND selected_conflict IS NOT NULL)
);
CREATE INDEX job_attempts_selected_time ON job_attempts (selected_at)
    INCLUDE (queue,job_id,selected_state,selected_conflict)
    WHERE selected_state IN ('completed','failed');
CREATE INDEX job_attempts_queue_selected_time ON job_attempts (queue,selected_at)
    INCLUDE (job_id,selected_state,selected_conflict)
    WHERE selected_state IN ('completed','failed');
CREATE INDEX job_attempts_missing_cache ON job_attempts (queue,job_id)
    WHERE selected_state IS NULL OR selected_at IS NULL OR selected_conflict IS NULL;
CREATE INDEX job_runs_dashboard_page ON job_runs (queue COLLATE "C",job_id COLLATE "C")
    WHERE pruning_at IS NULL;
