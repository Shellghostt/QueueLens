# M4 dashboard query and regression measurements — 2026-11-21

Measured locally on `feat/m4-implementation`; publication/CI are pending. These
are synthetic local workloads, with the database and client sharing a host.
Raw query samples and the actual PostgreSQL plan are in
[m4-dashboard-results.json](m4-dashboard-results.json). The sustained projection
record is [m4-lifecycle-load-results.json](m4-lifecycle-load-results.json).

## Environment and method

AMD Ryzen 5 5600H, 6 cores/12 logical processors, Windows 11 Home Single Language
10.0.26300, about 15.34 GiB visible RAM. Docker Desktop engine 29.9.2 exposes 12
processors and about 7.43 GiB RAM. Go 1.26.11 runs natively with GOMAXPROCS=4;
PostgreSQL 16.16 and Redis 7.2.17 run in fresh task-owned containers.

`make m4-bench` streams 100,000 deterministic M3 folded run summaries and 300,000
selected attempt summaries across 100 queues and a 30-day window. The oldest/newest
timestamps are asserted inside that window, with more than 29 days between them.
Twenty additional expired jobs exercise actual pruning. The seed uses no unbounded
300,000-element application slice. Sequential VACUUM ANALYZE establishes visibility
maps without exceeding Docker's default shared-memory limit. One EXPLAIN ANALYZE
probe precedes HTTP reads; this is a warm-cache measurement, not a cold-disk claim.

All 252 cookie-authenticated HTTP requests are included: 42 for each of overview,
queues, queue detail, jobs, job timeline and workers. First HTTP reads, warmup and
subsequent requests are retained in the result. Percentiles use nearest rank.
All requests have recorded status/latency/metric availability/lag evidence.
Any failed request, unavailable metric or unknown lag fails this harness.

During reads, real HTTP ingestion runs at a nominal 495 events/s, projection
commits real facts, and maintenance executes every second. Final evidence:
**2,673 accepted and projected events, five maintenance batches, 20 expired runs
removed, zero write errors**. Seed time was 13.53 s and measured reads took 5.51 s.

## Query results

| View | Requests | p50 ms | p95 ms | Maximum ms |
|---|---:|---:|---:|---:|
| Overview, 30-day global window | 42 | 66.68 | 71.20 | 86.51 |
| Broker queue page | 42 | 5.29 | 5.81 | 5.87 |
| Queue detail, 30-day window | 42 | 24.77 | 28.37 | 47.31 |
| Jobs | 42 | 12.82 | 15.73 | 17.10 |
| Job and attempt page | 42 | 9.53 | 11.19 | 11.64 |
| Workers | 42 | 8.01 | 9.46 | 9.60 |

Overall p50 **11.19 ms**, p95 **67.39 ms**. All per-view p95 values satisfy the
unchanged **<300 ms** gate. **252/252 succeeded; zero request errors, unavailable
metrics or unknown lag results.** No failed responses were filtered out.

The recorded global plan is an aggregate over a **Limit**, with an anti-join to
pruning keys and index-only scans of `job_attempts_selected_time` and
`job_runs_pruning`. It reads 300,000 eligible selected summaries, with no heap
fetches or temporary I/O in the probe. Metrics do not scan raw event partitions.
The separate existing projection-lag sampler retains its 1,000-row/500 ms bound.

Migration 0004 also creates the per-queue selected-time index, the sparse missing-cache
index and the C-order retained-job page index. Terminal aggregation is capped at
500,000 rows plus a sentinel and 150 ms. `TestDashboardMetricWorkLimit` separately
proves that 500,001 rows return explicit unavailable/null rates and 500,000 return
the correct count. A lock-blocked aggregation returns unavailable instead of zeros.

## Preserved load, storage and SDK budgets

`make m3-load` passed the unchanged 60-second HTTP/storage/projection workload:
**300,000 events, 300,000 raw rows, 100,000 runs**, no drop/loss/write errors.
Observed rate was 4,999.95 events/s; drain took 0.52 s. HTTP p50/p99 were
**5.53/26.27 ms**. Sampled live heap peaked at **5.38 MiB**, goroutines at 12 and
projection lag at 1.34 s. The asynchronous 500-row write p50/p99 were
32.87/122.95 ms. This run preserves delivery/memory invariants and does not claim
the M3 baseline's lower latency; local host scheduling and cache/index work affect
these measurements.

`make bench` passed 20 iterations of both 1,000-row writer benchmarks:
**45.11 ms** with provided identities and **19.33 ms** with legacy identities,
both below the existing **50 ms per 1,000 writes** budget.

The unchanged million-pair SDK harness ran through `make VERSION=m4-local sdk-bench`
in a local Linux validation image with verified Go 1.26.11, Node 22.22.3,
GOMAXPROCS=2 and four container CPUs:

| SDK | Added p50 | Added p99 | Allocation evidence |
|---|---:|---:|---|
| Go | 0.094% | 1.217% | Existing zero-allocation Emit regression passed |
| TypeScript | 0.226% | 1.413% | 267.18 sampled bytes per successful Emit |

Budgets remain <2% added p50, <5% added p99 and <512 bytes for TypeScript.
Both SDKs accounted for all 1,010,000 emits including warmup as drops after the
dead collector/Close, with zero sent/rejected/buffered. Go's timing method remains
Linux because the native Windows clock cannot resolve that short job workload.

## Reproduction and limits

```sh
GOTOOLCHAIN=go1.26.11 GOMAXPROCS=4 make m4-bench POSTGRES_DSN=<disposable-pg>
GOTOOLCHAIN=go1.26.11 GOMAXPROCS=4 make m3-load POSTGRES_DSN=<disposable-pg>
GOTOOLCHAIN=go1.26.11 GOMAXPROCS=4 make bench POSTGRES_DSN=<disposable-pg>
# Linux, verified Go 1.26.11/Node 22.22.3, GOMAXPROCS=2:
make VERSION=m4-local sdk-bench
```

These measurements do not establish production capacity, cold-cache behavior,
multi-instance scaling, hourly rollups or latency percentiles. M5 owns those
analytics and its additional workload. CI results will be recorded only after
publishing the feature branch.
