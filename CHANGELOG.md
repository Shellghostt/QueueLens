# Changelog

All notable changes to QueueLens are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned

- M2: the Go and TypeScript instrumentation SDKs, the worker heartbeat and the shared protocol
  conformance test.

## [0.1.0] - 2026-11-13

### Added

- Protocol v1 in `pkg/protocol`: the event schema, per-event validation, best-effort redaction of
  credentials in error strings and sortable ULID identifiers. The package has no dependency on the
  rest of the code, because adapters in other languages implement the same contract.
- The adapter interface (`internal/adapter`) and the BullMQ adapter
  (`internal/adapter/bullmq`), which reads BullMQ's Redis keys directly with `SCAN` rather than
  `KEYS`. The key layout is verified against BullMQ 6.7.6 by an integration test that seeds real
  queues with the real library and compares the counts with what BullMQ reports.
- The collector ingest API: `POST /v1/events` takes a single event or an array of up to 1,000
  events, validates each event on its own and answers with the reason an event was rejected.
- A bounded in-memory buffer. When it is full the newest events are dropped, the answer is 429 with
  `Retry-After: 1`, and `queuelens_buffer_dropped_total` goes up. Nothing is lost without a counter.
- PostgreSQL storage: a range-partitioned `queue_events` table, migrations applied at startup, and
  batch writes with `COPY`.
- The query API: `GET /v1/queues` reports the counts the adapters found. When an adapter fails, the
  last known state stays available and is marked `stale` with the error that was seen.
- Operational endpoints: `GET /healthz`, `GET /readyz` and `GET /metrics`. The metrics endpoint
  exposes the collector's own counters in the Prometheus text format.
- The load generator (`queuelens loadgen`), which paces itself against a fixed deadline and reports
  the failures it saw.
- A committed benchmark report: 1,500,000 events accepted at 4,999.99 events/s over five minutes on
  a collector pinned to four processors, with a p99 of 2.50 ms and no drops. See
  [docs/benchmarks/m1-ingest-5000eps.md](docs/benchmarks/m1-ingest-5000eps.md).
- ADR 0002, recording the M1 decisions, including the `QUEUELENS_*`/`QUELENS_*` environment prefix.
- `api/openapi.yaml` now describes the five endpoints that exist.

### Known limitations

- Nothing is analysed. There is no lifecycle reconstruction, no stall detection, no rollup, no
  dashboard and no alert rule, so the tool cannot yet tell anyone that a queue is in trouble.
- The instrumentation SDK does not exist yet; it arrives with M2. The ingest endpoint is ready for
  it, but nothing in this release emits events from inside a job.
- The data retention variables from the plan are not implemented. Nothing deletes old events yet.
- Only the `queue_events` table exists. The `job_runs`, `workers`, `metrics_hourly`, `alert_rules`
  and `alerts_fired` tables arrive with M3 and M5.
- The collector creates the partition for the current month and the next one at startup. Keeping two
  months ahead automatically is M3 work.
- A failed batch write loses the events in that batch. There is no disk queue; that is M3 work.
- The CI workflow has never executed: the repository has no remote yet.
- The race detector could not be run locally: the only C compiler on that machine is MinGW GCC
  6.3.0, which cannot build a 64-bit race-enabled binary. CI runs `go test -race` on ubuntu-latest.
- The benchmark is a single-host measurement with the database on the same machine. It is not a
  cloud result and does not establish a maximum throughput.

## [0.0.0] - 2026-11-13

- Initial skeleton. No functionality.
