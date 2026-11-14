-- M2 worker heartbeat persistence. Down/stall classification remains M3.
CREATE TABLE IF NOT EXISTS workers (
    worker_id text PRIMARY KEY,
    pool text NOT NULL DEFAULT '',
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now()
);
