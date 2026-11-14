# ADR 0004 - M2 SDK and heartbeat contract

- Status: accepted
- Date: 2026-11-14
- Supersedes: ADR 0002 decision 12 for the `workers` table only

## Context

M2 requires Go and TypeScript instrumentation, a worker heartbeat and measured job overhead.
The protocol and collector already exist, but there is no in-process transport or heartbeat storage.

## Decisions

1. Go uses a bounded channel and TypeScript a fixed ring. Emit copies protocol metadata and returns
   without network I/O, serialization or waiting. Defaults are 10,000 buffered events, batches of
   500, a 250 ms flush interval, a 15 second heartbeat and a two second HTTP deadline.
2. Only the collector URL and bearer token are required. Worker identity is generated per client;
   callers may supply a stable identity and pool. All seven protocol v1 states are supported.
3. Delivery is best effort with no retries. Overflow, transport failures and malformed replies are
   counted in worker `queuelens_dropped_total`. Partial acknowledgements distinguish accepted,
   dropped and rejected events. SDK sent counters mean collector acceptance, not database durability.
   Redirects are refused; request and response sizes are bounded. Go transport failures discard queued
   events and drop new traffic until the next flush tick, reducing repeated encoding/connection work.
4. Producers stop before Close. Close drains with a caller deadline, cancels outstanding requests
   on expiry and counts undelivered events. Go callers use a context; TypeScript callers use a timeout.
5. `POST /v1/workers/heartbeat` is authenticated and persists receipt time in PostgreSQL. Migration
   0002 creates `workers` now because M2 uses it. An upsert preserves `first_seen` and updates pool
   and `last_seen`. The server ignores client clocks. Worker-down detection and worker query views
   remain M3; the other deferred tables in ADR 0002 remain deferred.
6. Shared JSON fixtures exercise both protocol implementations. Integration runs both actual SDKs
   through HTTP to PostgreSQL. Dead-collector benchmarks alternate baseline and instrumented jobs
   for one million pairs; timing gates are below 2% p50 and 5% p99. Measurements are specific to
   the documented synthetic workload and host, and do not establish a universal overhead bound.
7. SDKs are available as source on the feature branch. The Node package remains private and no
   release or registry publication is part of M2 implementation.

## Consequences

Applications must not base job success on telemetry delivery. Workers expose their own drop counter
using SDK helpers; collector metrics cannot observe events that never reached the collector.
Heartbeat storage provides evidence for later health analysis without claiming that analysis exists.
