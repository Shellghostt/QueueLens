# M2 SDK overhead - 2026-11-14

These measurements come from the committed `tests/bench` harnesses on an AMD Ryzen 5 5600H
Windows host. Go runs in Ubuntu WSL with Go 1.26.11; TypeScript runs on Node 22 on Windows.
This is a local synthetic workload, not a production application or isolated hardware result.

## Method

Both harnesses start an actual loopback collector listener and close it before measuring.
They warm up 10,000 jobs, then measure one million baseline/instrumented pairs in alternating
order. Baseline and instrumented paths perform identical work; the instrumented path also emits
one started event with default timestamp and worker identity. Percentiles are computed from the
sorted samples. Gates are added p50 below 2% and p99 below 5%.

Go uses a non-inlined 10,000-step integer workload. TypeScript hashes a 16 KiB buffer through native
SHA256 on both paths, avoiding different JIT optimization of arithmetic loops at different call sites.
The Windows Go clock could not resolve the short integer job, so the Go timing gate runs on Linux.

## Initial M2 results

| Go measurement | Baseline | With Emit | Added |
|---|---:|---:|---:|
| p50 | 9,619 ns | 9,629 ns | 0.103961% |
| p99 | 19,427 ns | 20,359 ns | 4.797447% |

The initial Go run passed both gates after adding transport cooldown: after a failed batch,
queued/new events are counted as drops until the next flush tick permits new traffic. Earlier runs
without cooldown failed p99 because the dead endpoint caused continuous serialization/connection
work. The first run also overlapped cross-compilation; the final run followed those builds and tests.
The p99 result is close to the budget and remains subject to host variance.

## Pull-request regression and Go recovery fix

The [M2 pull-request run](https://github.com/Shellghostt/QueueLens/actions/runs/43554135970) measured
25.767872% added p99 at the same code as the earlier passing push run. Three consecutive Linux/WSL
runs with Go 1.26.11 and GOMAXPROCS=2 reproduced 34.518950%, 33.868472% and 29.729457%. The old
cooldown admitted another event burst every flush tick without confirming collector recovery.

[ADR 0005](../adr/0005-go-sdk-recovery-probes.md) keeps the Go event buffer closed until an
authenticated heartbeat probe succeeds. With the same million-pair harness and GOMAXPROCS=2,
all three consecutive local runs passed:

| Run | Baseline p50 | With Emit p50 | Added p50 | Baseline p99 | With Emit p99 | Added p99 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 9,619 ns | 9,628 ns | 0.093565% | 16,993 ns | 17,114 ns | 0.712058% |
| 2 | 9,729 ns | 9,729 ns | 0% | 19,228 ns | 19,258 ns | 0.156022% |
| 3 | 9,839 ns | 9,839 ns | 0% | 20,891 ns | 20,911 ns | 0.095735% |

Each run accounted for all 1,010,000 events as drops, with zero sent/rejected/buffered. Recovery
probes produced 81, 83 and 84 heartbeat errors respectively. No benchmark workload, sampling order,
percentile calculation, budget or test retry changed. These remain local synthetic measurements.

## TypeScript results

| TypeScript measurement | Baseline | With Emit | Added |
|---|---:|---:|---:|
| p50 | 10,200 ns | 10,200 ns | Below measured timing resolution |
| p99 | 40,800 ns | 41,300 ns | 1.2255% |

TypeScript heap-profiler sampling estimated **271.6216 bytes per successful default enqueue**,
below the 512 byte budget. Sampling interval was 512 bytes, including objects collected by minor
and major GC, over 10,000 successful emits. This is an estimate, not an exact allocation count.
The Go allocation regression uses `testing.AllocsPerRun` and measures zero heap allocations on Emit.

Both million-job runs account for all 1,010,000 emitted events (including warmup) as drops after Close,
with zero events sent and zero remaining buffered. Separate tests use a hung collector and a short
Close deadline to verify cancellation. Events are best effort and jobs continue independently.

## Reproduce and limits

Run `make sdk-bench` on Linux after installing Go 1.26.11 and Node 22+, or:

```sh
cd sdk/typescript && npm ci && npm run bench
cd ../..
QUEUELENS_RUN_BENCH=1 go test -count=1 -timeout=120s -v ./tests/bench
```

The three-run Go regression check uses:

```sh
GOMAXPROCS=2 QUEUELENS_RUN_BENCH=1 go test -count=3 -timeout=120s -v ./tests/bench
```

Results depend on job duration, clock resolution, CPU contention and GC. A relative overhead
percentage for this workload does not establish the same percentage for an arbitrary short job.
Most events overflow the bounded buffer while the collector is dead; the allocation check separately
measures successful enqueues. Neither benchmark measures healthy collector throughput or network latency.
SDK sent counters mean collector buffer acceptance, not durable database writes. The M1 collector
throughput report remains separate.
