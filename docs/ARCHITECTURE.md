# QueueLens Architecture

**Status: M2 implemented, unreleased.** The collector, BullMQ adapter, raw event storage, Go and
TypeScript SDKs, worker heartbeat storage and load generator exist. Both SDKs are tested through
HTTP against PostgreSQL, and queue collection against real Redis/BullMQ. The analytics
engine, dashboard and alert manager do not exist yet. The authoritative
specification is [PLAN.md](PLAN.md), and section 0 there is the current truth.

## Context

A self-hosted observability platform for distributed background job systems.

Background jobs fail quietly. A queue drains, retries pile up, a worker goes stale, and the first
signal is a customer complaint. Existing APM tools treat jobs as traces, not as work. QueueLens models the
job lifecycle directly: enqueued, started, retried, failed, stalled, completed.

## Shape

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

## What exists today

```mermaid
flowchart LR
    BULL[(Redis / BullMQ)] --> ADP[bullmq adapter]
    ADP --> POL[poller]
    POL --> QV[(queue state in memory)]
    SDK[Go / TypeScript SDK] --> COL[collector]
    SDK --> HB[authenticated heartbeat API]
    HB --> WK[(workers)]
    COL --> BUF[bounded buffer]
    BUF --> PG[(PostgreSQL, range partitioned)]
    QV --> API[HTTP API]
    COL --> API
    PG -.->|M3| ANA
```

| Package | Responsibility |
|---|---|
| `pkg/protocol` | The wire contract: event schema, validation, redaction, sortable identifiers. No dependency on the rest of the code, because adapters in other languages implement the same contract. |
| `internal/adapter` | The interface every queue adapter implements. An adapter reports counts; it never blocks the collector and never applies business logic. |
| `internal/adapter/bullmq` | Reads BullMQ's Redis keys directly, with `SCAN` rather than `KEYS`. The key layout was verified against BullMQ 6.7.6 by an integration test that seeds a real queue with the real library. |
| `internal/poller` | Drives the adapters on a fixed interval. A failing adapter is isolated: the error is counted, the last known state is kept and marked stale, the poller keeps running. |
| `internal/queues` | The last known state of every queue, in memory. It is a live view of the broker, not history. |
| `internal/collector` | Validate, redact, buffer, batch. No business logic, no unbounded buffering: a full buffer drops the newest events, counts them and answers 429. |
| `internal/store` | Writes batches to PostgreSQL with `COPY` into a range-partitioned table. |
| `sdk/go`, `sdk/typescript` | Bounded asynchronous metadata transport, batching, heartbeats and worker drop counters. |
| `internal/api` | The HTTP endpoints: ingest, queue list, health, readiness, metrics. |
| `internal/loadgen` | The load generator, so that throughput claims come from a measurement. |
| `internal/metrics` | The collector's counters, in the Prometheus text format. Plain atomics, no client library: instrumentation must not eat the overhead budget. |

## Data path

Two ingestion paths exist and they are not equivalent:

1. **Push (SDK)** — the application emits lifecycle events and duration metadata. Requires integration.
2. **Pull (adapter)** — QueueLens polls the broker. Works with no code change, cannot see in-job
   timing.

The SDK copies metadata on Emit and performs network work in a background goroutine or scheduled
task. Default capacity is 10,000, batch size 500, flush interval 250 ms and HTTP deadline two seconds.
Slow requests may delay flushes; Emit stays independent. Heartbeats default to 15 seconds and persist
server receipt time. Worker-down classification remains M3. See ADR 0004 for the wire and delivery contract.

## Failure behaviour

| Failure | What happens |
|---|---|
| Redis is unreachable | The adapter poll fails, the error is counted, the last known queue state stays available and is marked `stale`. The collector keeps serving. |
| PostgreSQL is unreachable | The readiness check fails, batch writes are counted as errors, the events in the failed batch are lost. There is no disk queue in M1; that is M3 work. |
| The buffer is full | The newest events are dropped, `queuelens_buffer_dropped_total` goes up, and the request that hit the limit gets 429 with `Retry-After: 1`. |
| Collector unreachable from SDK | Transport drops are counted in worker `queuelens_dropped_total`; jobs continue and failed events are not retried. Go discards queued/new events until an authenticated recovery heartbeat succeeds; flush ticks probe without reopening event encoding. |
| SDK buffer full | Newest events are dropped and counted immediately. |
| Worker shutdown | Stop producers, then Close with a deadline. Undelivered events are counted; collector acceptance is not database durability. |
| A partition for the current month is missing | The collector creates it at startup. Creating partitions further ahead is M3 work. |
| A malformed event | It is rejected on its own with a reason. The rest of the batch is still accepted. |

## Component responsibilities

See [PLAN.md](PLAN.md) section 6 for the responsibility of each box, the request and event flows, and the
failure behaviour of each stage.

## What is deliberately absent

See [PLAN.md](PLAN.md) section 4 (non-goals). If a component is not on that diagram, it is not part of the
design.
