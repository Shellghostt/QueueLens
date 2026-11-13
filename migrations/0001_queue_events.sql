-- QueueLens raw event storage.
--
-- The table is partitioned by time so that retention is a DROP TABLE rather
-- than a DELETE, and so that queries over recent data touch few partitions.
--
-- Only the tables M1 needs are created here. job_runs, workers,
-- metrics_hourly, alert_rules and alerts_fired belong to M3 and M5 and get
-- their own migrations when they are built.

CREATE TABLE IF NOT EXISTS queue_events (
    id           bigserial,
    ts           timestamptz NOT NULL,
    queue        text NOT NULL,
    job_id       text NOT NULL,
    job_name     text NOT NULL DEFAULT '',
    attempt      smallint NOT NULL DEFAULT 1,
    state        text NOT NULL CHECK (state IN
                 ('enqueued','started','completed','failed','retried','stalled','drained')),
    worker_id    text,
    duration_ms  integer,
    error        text,
    received_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ts, id)
) PARTITION BY RANGE (ts);
