# ADR 0002 - M1 implementation decisions

- Status: accepted
- Date: 2026-11-13
- Deciders: Aditya

## Context

M1 delivers the first usable thing QueueLens can do: read queue state from a broker, accept
lifecycle events over HTTP and store them. The decisions below were made while building it, are
expensive to change later, and are not fully determined by PLAN sections 7 to 10.

## Decisions

1. **`QUEUELENS_*` is the canonical environment prefix, and `QUELENS_*` is accepted as an alias.**
   The plan writes the variables as `QUELENS_*`. A tool that ignores its own specification is a bug,
   so both spellings work: the canonical name wins when both are set. Every variable has an alias,
   including the ones the plan does not mention (`QUEUELENS_BUFFER_SIZE`, `QUEUELENS_BATCH_SIZE`,
   `QUEUELENS_FLUSH_INTERVAL`).
2. **The protocol lives in `pkg/protocol` and has no dependencies.** An adapter written in another
   language implements the same contract from the documentation, so the Go package must stay
   importable without dragging in a database driver.
3. **The collector drops the newest event when the buffer is full, and answers 429.** It never blocks
   a producer: the caller gets a load signal and the loss is counted in
   `queuelens_buffer_dropped_total`. The acceptance criterion "no event is silently lost without a
   counter" is the reason this is a counter and not a log line.
4. **A rejected event is rejected alone.** One malformed event in a batch does not fail the batch.
   The response carries one entry per rejected event with the index and the reason, because a
   producer needs to know which event to fix. `accepted`, `dropped` and `rejected` are the three
   numbers a client acts on.
5. **Events older than seven days are rejected and counted separately.** The plan calls this a clock
   skew guard. It gets its own counter (`queuelens_events_too_old_total`) because a producer with a
   broken clock is a different problem from a producer with a broken payload.
6. **The error string is redacted on a best-effort basis.** The collector removes common credential
   shapes (`token=`, `password:`, `Bearer <token>`, URLs with inline credentials) and caps the string
   at 1,024 bytes. The plan's threat model says this is a safety net, not a guarantee, and the code
   says the same thing: the redaction is a list of regular expressions, not a parser.
7. **Event identifiers are ULIDs.** Events are ordered by time, not by insertion, so the identifier
   has to be sortable. The implementation is 60 lines in `pkg/protocol` rather than a dependency,
   because the protocol package must not force a dependency on every adapter that imports it.
8. **The BullMQ adapter reads Redis keys directly, with `SCAN` and never `KEYS`.** `KEYS` blocks the
   server; `SCAN` does not. The key layout (`bull:<queue>:wait` and `:active` as lists,
   `bull:<queue>:completed`, `:failed`, `:delayed` and `:prioritized` as sorted sets,
   `bull:<queue>:meta` as the marker that a queue exists) was verified against BullMQ 6.7.6 by an
   integration test that seeds real queues with the real library and compares the adapter's counts
   with what BullMQ itself reports.
9. **An adapter failure is isolated.** The poller counts the error, keeps the last known state,
   marks it stale and carries on. A broken Redis must not look like an empty queue, and it must not
   take the collector down.
10. **The queue state is in memory; the event history is in PostgreSQL.** The queue list is a live
    view of the broker and is rebuilt from the next poll. Only events are durable in M1.
11. **The event table is range-partitioned by `ts`, and the partition bounds are literals.** PostgreSQL
    does not accept bind parameters in partition bounds, so the bounds are written into the DDL. They
    are computed from a UTC calendar month, not from user input.
12. **Only the table M1 needs is created.** `job_runs`, `workers`, `metrics_hourly`, `alert_rules` and
    `alerts_fired` are in the plan's data model but arrive with M3 and M5, each with its own
    migration. A migration that creates an unused table is a claim that the feature exists.
13. **The load generator paces itself against a fixed deadline.** Sleeping for a fixed duration after
    every request drifts, and the measured rate would always be below the target. The generator
    advances its deadline by exactly one interval per batch, so the offered rate is what it says it
    is, and a collector that cannot keep up shows up as a gap rather than as a quiet slowdown.
14. **The load generator reports the failures it saw.** A run that sent nothing says why in
    `last_error` instead of only showing zero.
15. **`/metrics` exposes the collector's own counters only.** The Go runtime and process metrics
    arrive with the full exporter in M5. A half-configured exporter that omits some series is worse
    than an honest smaller set.
16. **The bearer token must be at least 16 characters.** The configuration refuses to start without
    it. The comparison in the API is constant time.
17. **`/readyz` checks the database, not the broker.** A broken Redis must not take the collector out
    of rotation: the API keeps serving the last known queue state, marked stale.
18. **The Go floor is 1.23 and the dependencies are pinned to versions that support it.** The latest
    chi, pgx and go-redis releases require Go 1.25 or newer. The plan says Go 1.23, so the
    dependencies were pinned to the newest versions that still build with it rather than the go
    directive being raised silently.

## Consequences

- The collector accepts 1,500,000 events in five minutes at 4,999.99 events/s with no drops and no
  failed writes on a four-processor collector. The measurement, the hardware and the method are in
  `docs/benchmarks/m1-ingest-5000eps.md`.
- The BullMQ adapter's numbers are checked against BullMQ itself, not against a hand-written Redis
  fixture, so a change in BullMQ's layout fails the test rather than silently returning zero.
- A future contributor can change the environment variable names, the protocol or the storage layout
  only by writing a new ADR.
- M1 has no SDK, no analytics, no dashboard and no alert rules. The API surface is four paths plus
  the ingest endpoint, and the OpenAPI document lists exactly those.
