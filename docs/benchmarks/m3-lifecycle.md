# M3 lifecycle and storage measurements — 2026-11-16

Measured source: `728999d4f76f0842800d697131652643556cbe7b` on
`feat/m3-implementation`, based on merged M2 `9ab2ebe`.
These are local synthetic measurements, not a release or a production capacity claim.
The [validation record](../testing/m3-lifecycle.md) maps the acceptance tests and gates.

## Environment and commands

Host: AMD Ryzen 5 5600H, Windows/amd64, Docker Desktop Linux containers.
Go commands explicitly used `GOTOOLCHAIN=go1.26.11`. Storage and load used
`GOMAXPROCS=4`; this limits Go execution, not the database or the entire host to four CPUs.
The separate `m3-queuelens` project supplied PostgreSQL 16.16 and Redis 7.2.17.
Integration/load fixtures use disposable schemas; existing Docker volumes were preserved.

```powershell
$env:GOTOOLCHAIN = 'go1.26.11'
$env:GOMAXPROCS = '4'
make bench POSTGRES_DSN=postgresql://queuelens:queuelens@127.0.0.1:55444/queuelens_m3_final
make m3-load POSTGRES_DSN=postgresql://queuelens:queuelens@127.0.0.1:55444/queuelens_m3_final
```

## Standalone storage: unchanged 1,000-event comparison

`make bench` runs 20 iterations. The provided-ID benchmark generates fresh IDs
outside the timed section and requires 1,000 newly persisted raw rows each time,
so a duplicate no-op cannot masquerade as a fast write. Normalization, fingerprinting,
identity verification, both durable COPY operations and commit are timed.
Numbers below are Go benchmark means, not latency percentiles. The command's
`PASS` alone does not enforce the 50 ms comparison; the measured number must also pass.

| Writer | Provided IDs, ms/1,000 | Omitted IDs, ms/1,000 | 50 ms comparison |
|---|---:|---:|---|
| Initial speculative ledger INSERT | 81.395 | 32.238 | Failed |
| Reusable staging and indexed lookup | 76.323 | 25.386 | Failed |
| Bulk ledger COPY | 59.882 | 24.582 | Failed |
| Bulk ledger and raw COPY | 54.702 | 19.455 | Failed |
| Narrow distinct-identity staging | 57.112 | 19.521 | Failed |
| **Final bounded array verification and durable COPY** | **46.013** | **17.537** | **Passed** |

Final command output:

```text
BenchmarkWriteBatch1000ProvidedIdentity-4    20    46013345 ns/op
BenchmarkWriteBatch1000-4                    20    17536965 ns/op
PASS
```

The first conflict plan hashed/scanned 25,000 retained ledger rows (about 3.9 MiB,
13.8 ms). Indexed lateral exact-key lookups avoid that retained-history scan.
Per-row speculative ledger INSERT cost roughly 30–38 ms per batch; ledger COPY
removed that work. Raw COPY replaced the subsequent individual raw INSERT work.
Tracing the remaining failure showed separate staging setup/COPY plus a lookup
before either durable write. Narrower staging retained that extra operation.
Four aligned typed arrays now feed one bounded UNNEST lookup; new ledger and raw
rows still commit in one transaction under the same retention and claims locks.
See ADRs [0008](../adr/0008-bulk-identity-claims.md),
[0009](../adr/0009-copy-raw-evidence.md) and
[0010](../adr/0010-array-identity-verification.md).

No database durability setting, identity rule, benchmark workload or budget was relaxed.
The final mean has about 4 ms of headroom and remains sensitive to the shared host.

## Final sustained HTTP/storage/projection workload

`TestLifecycleSustainedProjectionMemoryAndLag` offered 1,000-event HTTP batches
every 200 ms for 60 seconds. Each job had enqueued, started and completed facts
with distinct producer IDs. The collector used its default 500-event storage
batches and 10,000-event buffer. The actual runtime projector and maintenance
loops ran concurrently. All acknowledgements, durable rows, projections and loss
counters were checked, followed by a bounded drain.

| Measurement | Final result |
|---|---:|
| Offered and persisted events | 300,000 / 300,000 |
| Job projections | 100,000 |
| Offered duration | 60.0004299 s |
| Measured offered rate | 4,999.964 events/s |
| HTTP p50 / p99, 1,000 events | 4.6745 / 8.7608 ms |
| Asynchronous storage p50 / p99, 500 events | 24.8032 / 54.6578 ms |
| Projection drain after writer stop | 0.342182 s |
| Sampled peak live Go heap | 4,948,784 bytes, 4.72 MiB |
| Sampled peak pending rows | 1,000 |
| Sampled peak receipt lag | 0.204104 s |
| Sampled goroutines | 11–12 |
| Buffer drops, lost events, write errors | 0 / 0 / 0 |

The unchanged HTTP p99 gate is below 50 ms. HTTP acceptance is buffer acceptance,
not database durability. The 500-event storage p99 under concurrent projection
was **54.66 ms**; this run does not establish durable writes below 50 ms at p99.
The standalone storage comparison above uses means of 1,000-event writes.
Both measurements are retained rather than conflated.

Eleven samples were taken at five-second intervals, forcing GC before reading
live heap. These samples can miss transient peaks and do not measure PostgreSQL,
Node or process RSS. The test's synthetic heap cap is 128 MiB and drain deadline
is 30 seconds. Bounded summaries and sampled stable goroutines support the
observed workload; they do not prove absence of every possible leak or a universal
projection deadline. Backlog and historical partitions can extend projection lag.

An earlier 60-second run before the final writer refinements also persisted
300,000 events and 100,000 projections: HTTP p50/p99 4.9082/9.2763 ms,
sampled live heap 5,454,520 bytes, pending peak 2,498, lag peak 0.359878 s and
drain 0.373 s. The final run above validates the final implementation.
The original five-minute M1 measurement remains M1 evidence; it was not rerun for M3.

## Unchanged SDK million-job gates

The existing `make sdk-bench` target ran in `golang:1.26.11` with verified official
Node 22.22.3 binaries and GNU Make 4.4.1, using an isolated source snapshot and
`GOMAXPROCS=2`. Windows SDK dependencies were not overwritten. The
[M2 method](m2-sdk-overhead.md), workloads, alternating sampling order and budgets
are unchanged. Neither SDK nor `pkg/protocol` source changed in M3.

| SDK | Baseline p50 | SDK p50 | Added p50 | Baseline p99 | SDK p99 | Added p99 |
|---|---:|---:|---:|---:|---:|---:|
| Go | 9,739 ns | 9,740 ns | 0.010268% | 20,059 ns | 20,069 ns | 0.049853% |
| TypeScript | 8,987 ns | 8,998 ns | 0.122399% | 22,855 ns | 23,135 ns | 1.225115% |

Both pass added p50 < 2% and p99 < 5%. TypeScript successful-enqueue heap sampling
estimated **273.7752 bytes/event**, below 512 bytes. The Go allocation regression
passed with zero allocations on Emit. Each dead-collector run accounted for
1,010,000 emitted/dropped events, zero sent/rejected/buffered; Go recorded 82
failed recovery heartbeats and TypeScript one failed heartbeat. These are outage
overhead results, not healthy SDK throughput or durable delivery measurements.
