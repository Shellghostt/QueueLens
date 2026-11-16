-- Forward-only M2 upgrade: omitted IDs stay unknown, including old COPY rows.
ALTER TABLE queue_events
    ADD COLUMN event_id text,
    ADD COLUMN event_key bytea,
    ADD COLUMN content_fingerprint bytea,
    ADD COLUMN source_ts text,
    ADD COLUMN identity_kind text NOT NULL DEFAULT 'legacy',
    ADD COLUMN projected_at timestamptz,
    ADD COLUMN projection_status text;

ALTER TABLE queue_events
    ADD CONSTRAINT queue_events_identity CHECK (
        (identity_kind = 'provided' AND event_id IS NOT NULL
         AND octet_length(event_key) = 32 AND octet_length(content_fingerprint) = 32
         AND event_key IS NOT NULL AND content_fingerprint IS NOT NULL)
        OR (identity_kind IN ('legacy','missing') AND event_id IS NULL AND event_key IS NULL)
    ),
    ADD CONSTRAINT queue_events_projection CHECK (
        (projected_at IS NULL AND projection_status IS NULL)
        OR (projected_at IS NOT NULL AND projection_status IN ('projected','expired_history')
            AND projection_status IS NOT NULL)
    ),
    ADD CONSTRAINT queue_events_source_ts CHECK (source_ts IS NULL OR length(source_ts) <= 40);
CREATE INDEX queue_events_pending ON queue_events (received_at, ts, id)
    WHERE projected_at IS NULL;
CREATE INDEX queue_events_identity_refs ON queue_events (queue, event_key)
    WHERE event_key IS NOT NULL;
CREATE INDEX queue_events_receipts ON queue_events (received_at);

-- Original IDs are TOAST-able, while the btree key always stays small.
CREATE TABLE event_identities (
    queue text NOT NULL,
    event_key bytea NOT NULL CHECK (octet_length(event_key) = 32),
    event_id text NOT NULL,
    content_fingerprint bytea NOT NULL CHECK (octet_length(content_fingerprint) = 32),
    first_received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (queue, event_key)
);
CREATE INDEX event_identities_expiry ON event_identities (first_received_at);

CREATE TABLE job_runs (
    queue text NOT NULL,
    job_id text NOT NULL,
    reported_attempt smallint NOT NULL,
    observed_attempts integer NOT NULL CHECK (observed_attempts > 0),
    incomplete_attempts integer NOT NULL CHECK (
        incomplete_attempts >= 0 AND incomplete_attempts <= observed_attempts),
    state text NOT NULL CHECK (state IN
        ('enqueued','started','completed','failed','retried','stalled')),
    first_received_at timestamptz NOT NULL,
    last_received_at timestamptz NOT NULL,
    complete_observed boolean NOT NULL,
    pruning_at timestamptz,
    summary jsonb NOT NULL,
    PRIMARY KEY (queue, job_id)
);
CREATE INDEX job_runs_state_page ON job_runs (state, queue COLLATE "C", job_id COLLATE "C");
CREATE INDEX job_runs_suspected_page ON job_runs (queue COLLATE "C", job_id COLLATE "C")
    WHERE state IN ('enqueued','started','retried');
CREATE INDEX job_runs_completed_expiry ON job_runs (last_received_at) WHERE complete_observed;
CREATE INDEX job_runs_incomplete_expiry ON job_runs (first_received_at) WHERE NOT complete_observed;
CREATE INDEX job_runs_pruning ON job_runs (queue,job_id) WHERE pruning_at IS NOT NULL;
CREATE TABLE job_attempts (
    queue text NOT NULL,
    job_id text NOT NULL,
    attempt smallint NOT NULL,
    summary jsonb NOT NULL,
    PRIMARY KEY (queue, job_id, attempt)
);
CREATE TABLE pruned_jobs (
    queue text NOT NULL,
    job_id text NOT NULL,
    pruned_at timestamptz NOT NULL,
    PRIMARY KEY (queue, job_id)
);
CREATE INDEX pruned_jobs_expiry ON pruned_jobs (pruned_at);
CREATE TABLE projection_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    last_partition text NOT NULL DEFAULT '',
    maintenance_partition text NOT NULL DEFAULT '',
    last_batch_at timestamptz,
    last_scan_at timestamptz
);
INSERT INTO projection_state (singleton) VALUES (true);
CREATE INDEX workers_page ON workers (worker_id COLLATE "C");
CREATE INDEX workers_expiry ON workers (last_seen);
