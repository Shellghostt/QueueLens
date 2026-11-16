# M3 lifecycle validation — 2026-11-16

M3 is implemented and locally verified on `feat/m3-implementation`.
Runtime/test source is `728999d4f76f0842800d697131652643556cbe7b`, based on merged M2
`9ab2ebe6d2795f6af3e03b95658bb163dfb19faf`. The subsequent readiness change is
documentation only. No QueueLens push, pull request, CI run or merge has occurred.
Main and branch protection remain unchanged.

The [current M3 contract](../phases/M3.md) and accepted ADRs
[0006](../adr/0006-lifecycle-identity-and-retention.md),
[0007](../adr/0007-bounded-projection-pruning.md),
[0008](../adr/0008-bulk-identity-claims.md),
[0009](../adr/0009-copy-raw-evidence.md) and
[0010](../adr/0010-array-identity-verification.md) govern this implementation.
Accepted ADRs were preserved; later refinements are separate records.

## Environment and gates

Windows/amd64, AMD Ryzen 5 5600H; Go 1.26.11 selected explicitly, Node 22.22.3,
npm 10.9.8, GNU Make 4.4.1, golangci-lint 2.15.0. Real services were PostgreSQL
16.16 and Redis 7.2.17 in a fresh `m3-queuelens` Docker project. Tests used the
disposable `queuelens_m3_final` database and isolated M3 schemas.

```powershell
$env:GOTOOLCHAIN = 'go1.26.11'
$env:GOMAXPROCS = '4'
$env:QUEUELENS_TEST_DATABASE_URL = 'postgresql://queuelens:queuelens@127.0.0.1:55444/queuelens_m3_final'
$env:QUEUELENS_TEST_REDIS_URL = 'redis://127.0.0.1:56388/0'
```

| Command | Observed result |
|---|---|
| `make test` with the real services above | Passed all packages; integration package 20.858 s |
| `make race` with native x64 GCC below and real services | Passed all packages; integration package 22.063 s |
| `make lint` | `go fmt ./...`, vet and golangci-lint: **0 issues** |
| `make integration POSTGRES_DSN=… REDIS_URL=…` | Passed, including its `sdk-test` prerequisite: all five TypeScript tests, Go SDK/protocol and real PostgreSQL/Redis/BullMQ; integration package 16.427 s |
| `make sdk-bench` on Linux, Go 1.26.11 / Node 22.22.3 | Both unchanged million-job gates passed; full numbers in [measurements](../benchmarks/m3-lifecycle.md) |
| `make bench POSTGRES_DSN=…` | 20 iterations: provided IDs **46.013 ms/1,000**, omitted IDs **17.537 ms/1,000**, both below the unchanged 50 ms comparison |
| `make m3-load POSTGRES_DSN=…` | Passed: 300,000 persisted events / 100,000 runs in 60 s; HTTP p99 **8.7608 ms**, sampled heap **4.72 MiB**, lag **0.204 s**, no drops/loss/write errors |
| `make memory-check`; pinned MemGuard on `sdk/typescript/src` | Each analyzed one file, zero issues; limited JavaScript/TypeScript static coverage |
| `npm audit` in the SDK and BullMQ fixture | Both: **0 vulnerabilities** |
| `govulncheck ./...` | **No vulnerabilities found** |
| OpenAPI route/ref/Go-shape checker | 10 registered operations, 47 resolved references, 8 response shapes match Go fields; not full semantic OpenAPI validation |
| `make build VERSION=m3-local` | Native build passed; the exact resulting artifact printed `queuelens m3-local` |
| `make docker VERSION=m3-local-32065e3`; `bash tests/container/smoke.sh queuelens:m3-local-32065e3` in WSL | Passed nonroot/read-only packaged startup, migrations, Redis polling, auth, identity, lifecycle routes, heartbeat and clean shutdown |

The ordinary test/race/integration commands intentionally skip the opt-in load
test; `make m3-load` ran it separately on the final source. No regular M3
integration case was skipped. SDK timing ran on Linux because the documented M2
method requires its clock resolution. No benchmark workload or budget changed.

## Native Windows race compiler

The old GCC was incapable of x64 output. The installed MSYS2 UCRT compiler now
reports GCC 16.2.0 Rev7 and `x86_64-w64-mingw32`; native race is no longer blocked.
Use these process-local settings before `make race`:

```powershell
$m3CompilerBin = 'C:\Users\Aditya\AppData\Local\Programs\MSYS2-Go-x64\msys64\ucrt64\bin'
$env:PATH = "$m3CompilerBin;$env:PATH"
$env:CC = "$m3CompilerBin\gcc.exe"
$env:CXX = "$m3CompilerBin\g++.exe"
$env:CGO_ENABLED = '1'
$env:GOTOOLCHAIN = 'go1.26.11'
& $env:CC -dumpmachine
make race
```

## Acceptance mapping

There are **19 regular M3 integration tests**, one opt-in sustained load case,
pure folding properties and lifecycle configuration tests.

| Requirement | Named evidence |
|---|---|
| Forward identity migration, M2 rows/old COPY, indexes and re-entry | `TestLifecycleMigrationPreservesM2AndOldCOPY`; `TestLifecycleConcurrentMigrationsAndIdentityClaims` |
| Scoped duplicates, large IDs, nanosecond conflicts, mixed missing IDs and atomicity | `TestLifecycleIdentityScopeDuplicatesAndAtomicConflicts`; `TestLifecycleRawCOPYFailureRollsBackLedgerAndReusableWriter`; `TestLifecycleHTTPIdentityAndAsyncConflictKeepAcceptanceContract` |
| Deterministic shuffle/duplicate/missing/tie/attempt/retry/reuse folding | `TestFoldMultisetIsIndependentOfShuffleDuplicatesAndBatchBoundaries`; `TestFoldPrecedenceAttemptsUnknownTimingAndReuse`; `TestLifecycleDatabaseFoldShufflesDuplicatesBatchesAndRestarts` |
| Late commits with lower sequence IDs, rollback, restart and bounded partition replay | `TestLifecycleProjectorLateCommitLowerSequenceAndRestart`; `TestLifecycleProjectorAtomicRollbackAndBoundedRoundRobin`; `TestLifecycleProjectorRetriesDrainedAndExpiredHistory`; `TestLifecycleConcurrentIngestProjectionAndMaintenance` |
| Distinct explicit/suspected stalls and worker receipt/tick/recovery boundaries | `TestLifecycleExplicitStallSuspectedStallAndFailureStayDistinct`; `TestLifecycleWorkerReceiptBoundaryCursorAndRecovery` |
| Auth, page bounds, stable ordering, cursor binding, unknown/capped lag and limited history | `TestLifecycleJobQueriesAuthPaginationLagAndHistory`; `TestLifecycleProjectionLagCappedAndTimeoutIsUnknown`; packaged M3 route smoke |
| Unexpired/pending raw preservation, checked metadata and UTC/month rollover | `TestLifecycleRetentionPreservesUnexpiredPendingAndUnrelatedTables`; `TestLifecyclePartitionUTCMonthRolloverConcurrencyAndMetadataGuards` |
| Completed inactivity, unresolved absolute age, large durable pruning and bounded cleanup | `TestLifecycleRetentionCompletedInactivityAndUnresolvedAgeCap`; `TestLifecycleBoundedLargeAttemptPruningRestartAndPendingFacts`; `TestLifecycleWorkerAndTombstoneCleanupIsBounded` |
| Config defaults, canonical/alias spellings, disable policy and finite limits | `TestLifecycleDefaultsAndBothEnvironmentSpellings`; `TestLifecycleRejectsOverflowAndUnboundedOrInvalidCadences` |
| M1/M2 ingest, SDK/heartbeat/recovery and real BullMQ regressions | Existing unit/race suites; `TestTheBullMQAdapterMatchesWhatBullMQReports`; `TestBothSDKsPersistEventsAndHeartbeats`; `TestWorkerHeartbeatPreservesFirstSeenAndUpdatesLastSeen`; unchanged SDK gates |
| Sustained bounded memory/lag and accurate accounting | `TestLifecycleSustainedProjectionMemoryAndLag`; fresh-ID storage benchmark; [complete measurements](../benchmarks/m3-lifecycle.md) |

Folding properties hold for the same retained facts, including assigned receipt
timestamps; the database property fixture fixes those receipts across its trials.
Arrival order cannot replace a higher attempt or turn a failure into proof of
retry exhaustion. Producer-clock subtraction never supplies a missing duration.
The old plan's unconditional two-poll projection sentence is aligned with the
updated M3/ADR 0006 bounded replay contract: backlog can delay explicit evidence,
and pages expose committed lag. It is not a universal two-poll deadline.

## Diagnosed failures and practical limits

- All five failing standalone storage measurements are retained in the benchmark
  report. SQL tracing/EXPLAIN identified the work removed by each refinement;
  unchanged runs were not repeatedly retried to obtain a green number.
- The first retention fixture tried two parameterized SQL statements in one
  pgx prepared execution (`SQLSTATE 42601`). The seed was split into separate
  statements; the cleanup test and final suites then passed.
- The first lint target recursively formatted ignored Go compiler test fixtures
  under `bin`, including deliberately invalid Go. `fmt` now uses `go fmt ./...`;
  ignored tools/caches were preserved. Later lint reported zero issues.
- Linux benchmark preparation initially stopped before tests because the base
  image lacked `xz`. The official gzip Node archive was checksum verified and
  used instead. The isolated source copy has no Git metadata, so Make printed a
  harmless `git describe` diagnostic for its unused VERSION value; both actual
  benchmark commands completed and passed.
- The documentation check initially mistook the headings `Tests`/`Testing` for
  Go test names. Its pattern was restricted to actual `Test` names followed by an
  uppercase letter; local links, referenced tests and all nine phase checkboxes
  then passed validation.
- PowerShell initially resolved an older ignored `.exe` during native version
  inspection. The newly built extensionless artifact was copied to a fresh
  scratch `.exe` and printed the expected `m3-local` version. Docker's existing
  Make target does not forward VERSION as a build argument; its tested binary
  printed `dev`. Neither artifact is a published release.
- Under final concurrent load, asynchronous **500-event storage p99 was
  54.6578 ms**. HTTP p99 was 8.7608 ms and the standalone 1,000-event storage mean
  was 46.013 ms. These are different measurements; no durable p99 < 50 ms claim
  is made. Heap/lag are five-second samples, not continuous peaks or RSS/leak proof.

The four new query routes return best-effort evidence with unknown retry
exhaustion/history. Worker suspicion is evaluated on UTC ticks when queried
(default threshold 60 s plus up to one five-second interval), without a background
notification promise. General timelines/dashboard, rollups, alerts, new adapters,
disk spooling and horizontal capacity remain outside this M3 implementation.

## Local evidence and infrastructure

Raw logs, SQL plans, source snapshot and the removed temporary writer diagnostic
are preserved under the ignored task directory
`D:/Dev Repo/.codex-temp/queuelens-m3/`. Final logs are `final-test.log`,
`final-race-windows.log`, `final-lint.log`, `final-integration-sdk-test.log`,
`final-sdk-bench-linux-gzip.log`, `storage-array.log`, `final-load-60s.log`,
the audit/MemGuard logs and `final-container-smoke.log`.
The temporary diagnostic is not repository source.

The earlier scratch database `queuelens_m3` retains a superseded development
migration shape. It was preserved, and is not the final validation database;
use `queuelens_m3_final` above or the isolated seeded-M2 fixtures to reproduce.

The packaged smoke used fresh tmpfs services and removed only its own named
containers/network. The retained development project still exposes PostgreSQL
on 55444 and Redis on 56388. To stop only that project while preserving volumes:

```powershell
docker compose -p m3-queuelens -f 'D:\Dev Repo\.codex-temp\queuelens-m3\compose.yml' down
```
