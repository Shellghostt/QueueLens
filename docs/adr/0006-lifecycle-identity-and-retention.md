# ADR 0006 — Lifecycle identity, processing and retention

- Status: accepted
- Date: 2026-11-16
- Implements: QL-015A, QL-015–QL-018A and `docs/phases/M3.md`
- Preserves: protocol v1, ADR 0004 and ADR 0005

## Context

M2 stores lifecycle facts but omits the optional protocol event ID. PostgreSQL
allocates sequence values before commit, and event timestamps come from clients.
Neither is a safe increasing processing checkpoint. The protocol also lacks a
run generation, job heartbeat, expected duration and retry-exhaustion evidence.

## Decisions

### Identity and writer compatibility

Migration 0003 adds nullable `event_id`, a fixed-size identity key and content
fingerprint, `identity_kind`, and a nullable processing marker to raw events.
Existing rows remain intact with `identity_kind=legacy` and no fabricated ID.
The old nine-column COPY writer remains usable; its rows also have unknown
identity. The new writer records `provided` or `missing` identity. Protocol v1
continues to accept omitted IDs and normalize attempt zero as before.

An unpartitioned identity ledger deduplicates exact `(queue,event_id)` identities
across months. Its key uses SHA-256 of the ID, with the original ID retained and
compared, so an unbounded optional ID cannot overflow a PostgreSQL btree key.
Content fingerprints cover the normalized protocol event, including its UTC
timestamp and supplied duration/error metadata. Different content or timestamps
under one identity are a conflict, including within one batch; the entire storage
batch rolls back. Identical duplicates are successful no-ops. Different queues
have independent scopes. Missing IDs are not historically deduplicated.

COPY into a transaction-local staging table preserves batching. Claiming ledger
identities, checking conflicts and inserting raw events commit atomically. The
collector's existing HTTP acceptance remains buffer acceptance, not durability;
an asynchronous conflicting batch is a counted writer failure/drop, not a new
HTTP acknowledgement contract. Migration checks and recording occur under a
transaction advisory lock, preserving file order and per-file transactions.

### Folding and job-ID reuse

`(queue,job_id,attempt)` identifies a retained attempt summary. `(queue,job_id)`
identifies its logical projection. The largest reported attempt wins; the
observed-attempt count counts distinct retained summaries, not starts or gaps in
the counter. Lower attempts never replace higher-attempt state.

Same-attempt state precedence is `completed > retried > failed > stalled >
started > enqueued`. A retry on the same attempt supersedes an observed failure;
a higher attempt supersedes earlier-attempt state. Completed/failed contradiction
is retained as an evidence flag. A representative event is chosen by this
precedence, then greatest event timestamp, then lexical content fingerprint.
Min/max timestamps, receipt timestamps and evidence flags merge associatively,
commutatively and idempotently. Identical starts never increment attempt counts.
`drained` is queue-level evidence and receives a processing marker without
creating a job run.

The protocol cannot distinguish reused job IDs from retries or clock disagreement.
There is no implicit counter reset or invented generation. Producers should use
unique job IDs throughout retained history. An enqueue timestamp after retained
completion evidence is flagged as possible reuse/clock disagreement; all facts
still fold under the same logical key. This does not prove reuse. A projection
recreated after pruning is marked history-limited while its tombstone is retained.

Enqueued/started timing remains unknown when its source event is missing.
Duration is only producer-reported `duration_ms` from the selected completed or
failed event, interpreted as an attempt duration. No cross-host timestamp
subtraction estimates execution time. Failure never proves retries exhausted.
Responses always state that complete history cannot be proven by best-effort
telemetry, and identify missing/contradictory/legacy/pruned evidence separately.

### Bounded, restart-safe processing

Each raw row's `(ts,id)` and nullable `projected_at` are its durable processing
identity/checkpoint. A folder transaction claims at most 1,000 pending rows from
one checked monthly partition, merges attempt/run summaries and marks those rows
in the same transaction. A crash before commit changes neither; a crash after
commit requires no replay. Partial indexes identify pending rows. A database
advisory lock serializes folder transactions across collectors.

A persisted round-robin partition cursor bounds historical reads and prevents
an old partition from monopolizing new traffic. It is a scan position, never an
acknowledgement: every partition is revisited, so a smaller sequence value that
commits later is still pending and will be processed. Batches do not load event
ID strings or unbounded event lists into projection memory. There is one compact
summary per distinct attempt, bounded by the protocol's 0–32767 counter range.
Historical events outside the configured history receipt window are explicitly
marked `expired_history`, not reconstructed into supposedly complete runs.

The folder drains work and waits one poll interval after an empty scan cycle;
empty scans yield after at most 32 partitions. Each batch has finite statement,
lock and context deadlines. API lag describes committed pending raw rows and
includes the last batch/scan time. Unknown or timed-out lag is reported as unknown;
neither sequence position nor HTTP acceptance proves the projection is caught up.

### Query and suspicion contracts

Authenticated GET routes are:

- `/v1/jobs/failed`: highest-attempt state is observed `failed`.
- `/v1/jobs/stalled`: highest-attempt state is observed `stalled`.
- `/v1/jobs/suspected-stalled`: unresolved enqueued/started/retried state with no
  job event receipt for `QUEUELENS_STALL_AFTER`, default 15 minutes; zero disables it.
- `/v1/workers`: receipt-based worker evidence, optionally `status=suspected_down`.

Job routes accept optional `queue`, `limit` (1–200; default 100) and an opaque
cursor. Worker routes accept `limit`, `cursor` and `status=all|suspected_down`.
Ordering is queue/job ID for jobs and worker ID for workers, using PostgreSQL C
collation. Cursors bind the route/filter and evaluation time. This is live-row
keyset pagination, not an MVCC snapshot across HTTP requests: concurrent changes
or replay can affect membership. There is no unbounded count or timeline query.

Pages contain `items`, `next_cursor`, `evaluated_at`, `history_from`, projection
lag evidence and a consistency/limitations description. Job items contain the
reported attempt, observed-attempt count/state, supplied timing/duration/error,
identity/history limitations and a nullable `retry_exhausted` (always unknown in
v1). Suspected stalls add the threshold and `no_job_receipt` reason. Worker items
contain first/last receipt, pool, receipt age, `alive|suspected_down` and suspicion
evidence; they never assert that a job failed or a process actually died.

Queries evaluate liveness on UTC-aligned logical ticks, default five seconds
(`QUEUELENS_EVALUATION_INTERVAL`, 1 second–1 minute). A worker is suspected down
when the tick is at least 60 seconds after its last server receipt; detection is
visible on the next query at/after that tick, bounded by 60 seconds plus one
evaluation interval when queried. No background notification is promised. Future
receipts cannot classify a worker down. This bounded on-demand evaluation avoids
an unbounded scan/update of every worker each tick. Explicit stall projection is
independent of this suspicion policy; backlog/lag can delay it and is exposed.

### Retention and safe partition maintenance

Raw/history defaults are 30 days, configurable independently with the canonical
`QUEUELENS_*` prefix and existing `QUELENS_*` aliases. UTC cutoffs are computed
from server time. Raw partitions are dropped only when their event-time upper
bound is at/before the cutoff, every receipt is expired, and every row is processed.
Late receipts therefore retain their partition for at least the receipt window;
coarse month retention can add almost another month. Future source timestamps
remain subject to the unchanged one-hour ingest limit. Backlog extends retention.

Completed projections with structurally complete observed evidence expire after
the history window without a receipt. Unresolved, contradictory, reused, legacy
or otherwise incomplete projections also have an absolute history-window age
cap from their first receipt. This bounds missing-outcome growth; very long jobs
can lose prior projected history. Raw evidence follows its separate policy.
Attempt summaries are deleted with their run. Worker receipts expire after the
history window without a heartbeat. Pruned-key tombstones last one history window
and mark reconstruction as limited; no fake protocol generation is produced.

Ledger entries outlive all raw rows referencing them and at least one raw receipt
window from first acceptance. Cleanup is bounded and serialized against new
identity claims. Maintenance visits at most eight partitions and deletes at most
1,000 projection/worker/ledger/tombstone rows per transaction, continuing bounded
batches when necessary. The default maintenance cadence is ten minutes.

Startup/maintenance create the current and next two UTC monthly partitions, plus
the month needed for the protocol's seven-day late-arrival window when different.
Downtime recovery creates these bounded required gaps, not every unused past month.
Partition names, parent, namespace and actual calendar bounds are checked before
DDL. Drop takes a finite exclusive lock, rechecks metadata and uses no `CASCADE`;
unrelated tables are never dropped. A shared writer/exclusive-maintenance lock
protects identity retention; the folder lock protects processing/pruning decisions.

## Consequences and evidence

M3 does not add a dashboard, general timeline API, rollups, alerts or disk spool.
SDK contracts and overhead gates remain unchanged. Local tests use disposable
schemas/databases and a fresh Docker project. Migration, folding, commit/restart,
auth/pagination, liveness, retention/concurrency and sustained memory/lag evidence
will be recorded in `docs/testing/m3-lifecycle.md` after the actual checks.
