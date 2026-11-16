# QueueLens

> A self-hosted observability platform for distributed background job systems.

![status](https://img.shields.io/badge/status-M2_instrumentation-orange)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
![Go](https://img.shields.io/badge/go-1.26-00ADD8?logo=go&logoColor=white)

**Status: M2 complete and merged into main.** QueueLens reads queue state from a broker,
accepts job lifecycle events from Go and TypeScript workers and stores events and heartbeats in
PostgreSQL. **It does not analyse anything yet**: there is no lifecycle
reconstruction, no stall detection, no dashboard and no alert rule. What exists is the part that
everything else will be built on, and it is tested against a real PostgreSQL and a real Redis.

## Why this exists

Background jobs fail quietly. A queue drains, retries pile up, a worker goes stale, and the first
signal is a customer complaint. Existing APM tools treat jobs as traces, not as work. QueueLens models the
job lifecycle directly: enqueued, started, retried, failed, stalled, completed.

## What it does today

| Capability | Milestone | Status |
|---|---|---|
| Read BullMQ queue counts (waiting, active, completed, failed, delayed, paused, prioritised) | M1 | **Done** |
| Accept job lifecycle events over HTTP with per-event validation | M1 | **Done** |
| Store raw events in a time-partitioned PostgreSQL table | M1 | **Done** |
| Survive a broken broker without losing the last known state | M1 | **Done** |
| Report collector counters in the Prometheus text format | M1 | **Done** |
| Go and TypeScript instrumentation SDKs | M2 | **Done**, unpublished |
| Authenticated worker heartbeat and receipt-time storage | M2 | **Done** |
| Worker-down classification | M3 | Not started |
| Job lifecycle reconstruction and failed/stalled detection | M3 | Not started |
| Historical performance analytics | M4 | Not started |
| Configurable alerting | M5 | Not started |
| Additional queue adapters (SQS, Celery, Sidekiq) | M6 | Not started |

## Architecture

```mermaid
flowchart LR
    APP[Application] --> ADP[Queue adapter]
    ADP --> Q[(Redis)]
    ADP --> COL[Event collector]
    COL --> PG[(PostgreSQL)]
    PG --> ANA[Analytics engine]
    ANA --> DASH[Dashboard]
    ANA --> ALERT[Alert manager]
    WK[Worker processes] --> ADP
```

Full reasoning, data model, API surface and failure behaviour are in [docs/PLAN.md](docs/PLAN.md).
The shape of what exists today is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Engineering challenge

Tracking distributed job execution accurately without adding meaningful overhead to the
application being monitored. Instrumentation that blocks a job is worse than no instrumentation.

## Quickstart

Requires Go 1.26+, PostgreSQL 16 and Redis 7.2.

```bash
make build
./bin/queuelens version
```

Local infrastructure:

```bash
make db-up      # docker compose: postgres on 55433, redis on 56379
```

Run the collector:

```bash
export QUEUELENS_POSTGRES_DSN="postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens"
export QUEUELENS_REDIS_URL="redis://127.0.0.1:56379/0"
export QUEUELENS_ADMIN_TOKEN="change-me-at-least-16-characters"

./bin/queuelens serve
```

The collector applies its migrations, creates the partition for the current month and the next one,
and then serves:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/metrics
curl -H "Authorization: Bearer $QUEUELENS_ADMIN_TOKEN" http://127.0.0.1:8080/v1/queues
```

Real output from a local run against two BullMQ queues:

```json
{
  "generated_at": "2026-11-13T04:46:59.2578659Z",
  "queues": [
    {
      "queue": "emails",
      "waiting": 3,
      "active": 1,
      "completed": 0,
      "failed": 0,
      "delayed": 1,
      "paused": 0,
      "prioritized": 0,
      "depth": 4,
      "updated_at": "2026-11-13T04:46:59.2227696Z",
      "stale": false
    },
    {
      "queue": "reports",
      "waiting": 0,
      "active": 0,
      "completed": 1,
      "failed": 1,
      "delayed": 0,
      "paused": 0,
      "prioritized": 0,
      "depth": 0,
      "updated_at": "2026-11-13T04:46:59.2227696Z",
      "stale": false
    }
  ]
}
```

Send an event:

```bash
curl -X POST http://127.0.0.1:8080/v1/events \
  -H "Authorization: Bearer $QUEUELENS_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"v":1,"ts":"2026-11-13T10:00:00.123Z","queue":"emails","job_id":"8891","job_name":"send-welcome","state":"failed","duration_ms":340,"error":"SMTP timeout"}]'
```

```json
{"accepted":1,"dropped":0,"rejected":[]}
```

A malformed event is rejected on its own, with the reason, and the rest of the batch is still
accepted:

```json
{
  "accepted": 0,
  "dropped": 0,
  "rejected": [
    {"index": 0, "id": "", "reason": "job_id: is required"}
  ]
}
```

The API is described in [api/openapi.yaml](api/openapi.yaml). Only the paths that exist are in it.

### Environment variables

`QUEUELENS_*` is canonical; the `QUELENS_*` spelling from the plan is accepted as an alias.

| Variable | Default | Notes |
|---|---|---|
| `QUEUELENS_HTTP_ADDR` | `:8080` | API and collector |
| `QUEUELENS_POSTGRES_DSN` | required | PostgreSQL connection string |
| `QUEUELENS_REDIS_URL` | required for the BullMQ adapter | Redis URL |
| `QUEUELENS_ADMIN_TOKEN` | required | Bearer token, at least 16 characters |
| `QUEUELENS_ADAPTERS` | `bullmq` | Comma-separated; an empty list disables the adapters |
| `QUEUELENS_POLL_INTERVAL` | `5s` | Adapter poll cadence |
| `QUEUELENS_BUFFER_SIZE` | `10000` | Events that may wait for storage |
| `QUEUELENS_BATCH_SIZE` | `500` | Events per database round trip, maximum 1,000 |
| `QUEUELENS_FLUSH_INTERVAL` | `250ms` | Maximum time an event waits in the buffer |
| `QUEUELENS_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

The plan's `QUELENS_RETENTION_RAW_DAYS` and `QUELENS_RETENTION_ROLLUP_DAYS` are **not implemented**:
retention is M3 work and there are no rollup tables yet.

## Measured performance

The M2 dead-collector workload, latency and allocation measurements are documented in
[docs/benchmarks/m2-sdk-overhead.md](docs/benchmarks/m2-sdk-overhead.md). SDK integration and
validation evidence is in [docs/testing/m1-m2-validation.md](docs/testing/m1-m2-validation.md).
Usage and shutdown examples are in the [Go SDK](sdk/go/README.md) and
[TypeScript SDK](sdk/typescript/README.md) guides. Each worker exposes `queuelens_dropped_total`;
the collector cannot count telemetry that never reaches it. Stop producers before draining an SDK.

Every number below comes from a run in this repository. The hardware, the method and what the run
does not prove are in [docs/benchmarks/m1-ingest-5000eps.md](docs/benchmarks/m1-ingest-5000eps.md).

| Measure | Measured | Budget |
|---|---|---|
| Sustained ingest | 4,999.99 events/s for 5 minutes (1,500,000 events) | 5,000 events/s |
| p50 request latency | 1.17 ms | — |
| p99 request latency | 2.50 ms | — |
| 1,000-event batch, end to end | 3.18 ms p50, 5.41 ms p99 | < 50 ms |
| Events dropped | 0 | 0 |

The collector was pinned to four logical processors of an AMD Ryzen 5 5600H. The load generator and
the database shared the same host, so this is a local measurement, not a cloud one.

## Repository layout

| Path | Purpose |
|---|---|
| `cmd/queuelens` | Server entry point: `serve`, `loadgen`, `version` |
| `pkg/protocol` | Versioned event schema, validation, redaction, sortable identifiers |
| `sdk/go`, `sdk/typescript` | Best-effort asynchronous worker instrumentation and heartbeats |
| `internal/adapter` | The interface every queue adapter implements |
| `internal/adapter/bullmq` | The BullMQ adapter, reading Redis directly |
| `internal/collector` | Ingest pipeline, bounded buffer, backpressure |
| `internal/store` | PostgreSQL persistence, migrations, partitioning |
| `internal/poller` | Drives the adapters and isolates their failures |
| `internal/queues` | The last known state of every queue |
| `internal/api` | The HTTP API |
| `internal/loadgen` | The load generator |
| `internal/metrics` | The collector counters |
| `migrations` | The SQL the collector applies at startup |
| `internal/analytics` | Aggregations, latency percentiles, stall detection (M3) |
| `internal/alert` | Alert rules and notification delivery (M5) |
| `web` | React dashboard (M4) |
| `tests/integration` | Tests against a real PostgreSQL, a real Redis and real BullMQ |
| `docs/benchmarks` | The committed measurement reports |

## Roadmap

M3 is **Not started**. Read the [M3 requirements and acceptance gates](docs/phases/M3.md)
before implementation and the [development-tool audit](docs/DEVELOPMENT-TOOLS.md) before debugging.

| Milestone | Deliverable | Status |
|---|---|---|
| M1 | Adapter interface, Redis adapter and event collection | **Done** (v0.1.0) |
| M2 | Worker instrumentation and heartbeat storage | **Done (merged)** |
| M3 | Job lifecycle tracking, failed and stalled detection | Not started |
| M4 | Dashboard with queue state and job timelines | Not started |
| M5 | Metrics, alerting and retry analysis | Not started |
| M6 | Historical analytics, horizontal scaling, extra adapters | Not started |

This roadmap mirrors [docs/PLAN.md](docs/PLAN.md) section 12. If the two disagree, the plan is correct and
the README is a bug.

## Testing

```bash
make test         # unit tests, no infrastructure needed
make sdk-test     # install/build/test TypeScript and test Go SDK conformance
make db-up        # postgres and redis
make fixture      # npm install for the BullMQ fixture
make integration  # against the real servers
```

The integration suite skips itself when the servers are not there, and says which environment
variable is missing. The BullMQ test compares the adapter's counts against what BullMQ itself
reports, so it fails if BullMQ's key layout ever changes.

## Project standards

- Conventional Commits, Semantic Versioning, `main` is always green.
- Every release is produced by CI, never by a laptop.
- Status claims must match reality. Unbuilt features are marked "Not started", not "coming soon".
- Decisions are recorded as ADRs in [docs/adr/](docs/adr/).

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/PLAN.md](docs/PLAN.md) first. Issues labelled
`good first issue` are scoped for a first pull request.

## Using an AI assistant on this repository

Read [docs/PLAN.md](docs/PLAN.md) end to end, including the machine context block at the top. It states
what exists, what does not, and which claims are forbidden. Do not describe planned behaviour as
implemented behaviour.

## License

Apache License 2.0 - see [LICENSE](LICENSE).
