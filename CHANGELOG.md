# Changelog

All notable changes to QueueLens are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- M2 Go and TypeScript SDKs with bounded non-blocking metadata buffers, background batches,
  generated worker identity, 15 second heartbeats, deadline-bounded Close and worker drop metrics.
- Authenticated `POST /v1/workers/heartbeat`, migration 0002 and receipt-time worker upserts.
- Shared protocol v1 conformance fixtures, both SDKs tested through the real collector/PostgreSQL,
  million-job dead-collector gates and an overhead report. SDKs remain unpublished.

- The BullMQ fixture is checked with MemGuard, a static analyser for JavaScript memory and resource
  problems, pinned as a development dependency. `make memory-check` runs it locally and CI runs it
  on every pull request. It reads source; it does not observe runtime behaviour, and a clean run is
  not evidence that the fixture is free of memory problems.
- The fixture requires Node 22 or newer, because MemGuard does. Its `package.json` says so and CI
  uses Node 22.
- The repository tracks CodeGraph's index ignore rules and documents it as an optional local tool
  for code navigation. Nothing in the build or in CI depends on it.

### Security

- The dependency scan was reporting 50 vulnerabilities that affect this code. The Go standard
  library advisories have no fix in the 1.23 line, and `golang.org/x/text` v0.22.0, chi v5.2.1 and
  pgx v5.7.4 were affected as well. The scan is now clean.

### Changed

- The repository is published at https://github.com/Shellghostt/QueueLens and the CI workflow has
  executed. The first run on 2026-11-13 passed the build, test and integration jobs on all three
  operating systems, including `go test -race`; the lint and vulnerability jobs failed on that run
  and are fixed by the entries above.
- The Go floor moves from 1.23 to 1.26. `go.mod` declares `go 1.26.0` and `toolchain go1.26.11`, so
  CI installs the patched toolchain and not the exact patch named by the `go` directive. ADR 0003
  supersedes decision 18 of ADR 0002.
- `github.com/go-chi/chi/v5` v5.2.1 → v5.5.0, `github.com/jackc/pgx/v5` v5.7.4 → v5.13.0,
  `golang.org/x/text` v0.22.0 → v0.42.0 and `golang.org/x/sync` v0.11.0 → v0.23.0.
- The builder image moves to `golang:1.26-alpine`.
- The linter is golangci-lint v2.15.0, pinned by version, run by `golangci/golangci-lint-action@v9`.
  The five findings it produced were fixed in the code rather than excluded.

### Fixed

- Go SDK outage recovery requires a successful authenticated heartbeat probe before reopening the
  event buffer. Failed collectors no longer trigger repeated batch encoding on every flush tick;
  dropped-event accounting and deadline-bounded shutdown are preserved (ADR 0005).
- Collector cancellation closes enqueue safely before draining, preventing send/close races.
- Storage failures now count accepted events lost after acknowledgement.
- Ingest refuses trailing JSON documents; duration values must fit PostgreSQL integer storage;
  truncated error strings preserve UTF-8.

- The collector test that waits for the writer to flush now waits for the persisted counter rather
  than for the sink. The writer records a batch in the sink and bumps the counter afterwards, so the
  old test could read the counter before the last batch was accounted for and fail intermittently.
- Five writes and closes whose error return was discarded now say so explicitly with `_, _ =`.

### Planned

- M3: lifecycle folding, stalled/dead-worker classification and retention.

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
- The CI workflow had not executed when this tag was created: the repository had no remote yet. It has
  since run on GitHub; see [Unreleased].
- The race detector could not be run locally: the only C compiler on that machine is MinGW GCC
  6.3.0, which cannot build a 64-bit race-enabled binary. CI runs `go test -race` on ubuntu-latest.
- The benchmark is a single-host measurement with the database on the same machine. It is not a
  cloud result and does not establish a maximum throughput.

## [0.0.0] - 2026-11-13

- Initial skeleton. No functionality.
