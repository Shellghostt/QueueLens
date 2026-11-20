# M4 query/dashboard validation — 2026-11-21

Status: **Implemented and locally verified on `feat/m4-implementation`; unpushed,
unmerged, CI pending.** Decisions were accepted in
[ADR 0011](../adr/0011-m4-query-and-dashboard-contract.md) at `60ec7b1` before code.
The [phase requirements](../phases/M4.md) retain the M3 protocol/folding/retention
contracts and the <300 ms query gate.

## Local gates

All commands use pinned Go 1.26.11 and Node 22.22.3. PostgreSQL/Redis integration
variables were set to fresh task-owned services; these tests did not skip for
missing infrastructure.

| Gate | Result |
|---|---|
| `make test` | Passed all packages; integration package 23.57 s |
| `make race` | Passed natively on Windows with the installed x64 UCRT GCC; integration 33.39 s |
| `make lint` | gofmt, vet and golangci-lint 2.15.0: 0 issues |
| `make integration` | Passed real PG/Redis, BullMQ fixture and both SDKs; integration 25.16 s |
| `make web-check` | Lint/types, 12 tests, production build, asset scan and npm audit passed |
| Frontend MemGuard 0.2.0 | 12 files analyzed, 0 issues; static analysis only |
| `make build` / tagged native build | Assets compiled and embedded; headless Go compatibility retained |
| Container build + `tests/container/smoke.sh` | Passed nonroot/read-only runtime, migration, broker, auth, timeline, embedded assets and shutdown |
| `make m4-bench` | Passed 30-day corpus plus exact 500,000/500,001 metric bound cases |
| `make m3-load` | Passed unchanged 300,000-event / 100,000-run / 60-second workload |
| `make bench` | Provided-ID 45.11 ms / legacy 19.33 ms per 1,000 writes; <50 ms |
| Linux `make VERSION=m4-local sdk-bench` | Both million-job overhead/allocation/drop gates passed |
| `govulncheck ./...` | No vulnerabilities found |
| OpenAPI/CI YAML and local references | 22 implemented API paths, local references resolve; CI YAML parses |

The performance dataset, plans, every request and regression measurements are
recorded in [M4 measurements](../benchmarks/m4-dashboard.md). CI was not run
locally or claimed as passing; publishing remains subject to user approval.

## Acceptance mapping

| Criterion | Concrete evidence |
|---|---|
| M1–M3 compatibility | Full Go/race/integration/SDK/conformance suites; existing lifecycle and retention cases; forward migration preserves old COPY |
| Auth, exact identifiers, bounds and cursors | `TestDashboardAPIAuthIdentifiersFiltersAndBoundCursors`; browser Unicode/slash/percent/quote navigation; wrong route/queue/state/job/window cursor rejection |
| Every retained attempt and unknown/history evidence | `TestDashboardTimelinePagesAndPruningAvailability`; real browser 100/100/5 pages for 205 attempts, supplied duration and missing/conflicting/reuse evidence |
| No half-pruned timeline | `TestDashboardTimelineReadSnapshotDuringPruning`: deterministic commit between run/attempt statements; it fails with READ COMMITTED and passes with REPEATABLE READ |
| Truthful selected-outcome rates | `TestDashboardSelectedOutcomeRatesAndUnavailableEvidence`: superseded retry failure, removed broker, zero denominator, missing cache, history coverage and blocked query |
| Supplied timestamps and stall policy | `TestDashboardNanosecondWindowEdgesAndObservedStallPolicy`: inclusive/exclusive nanosecond boundaries, six state filters, receipt thresholds/future receipts; unchanged M3 worker/lag cases |
| Forward M3 upgrade | `TestDashboardMigrationBackfillsM3WithoutChangingSummaries`: JSON unchanged, cached conflict/state/time correct, CHECK enforced, repeatable migrations, omitted cache writes accepted |
| <300 ms representative query gate | `TestDashboardThirtyDayQueryBudget`: 100 queues / 100,000 jobs / 300,000 attempts across 30 days, concurrent real ingest/project/maintenance; all 252 requests included |
| Bounded resources and indexed reads | `TestDashboardMetricWorkLimit`; `TestDashboardBrokerPageVisitAndMaterializationBounds`; recorded index-only plan; response >2 MiB returns 503 before partial success |
| Browser access and credentials | `TestBrowserSessionReadOnlyOriginLogoutExpiryAndRestart`, `TestBrowserSessionBoundsAndProductionCookie`, configuration cases, production asset sentinel scan and actual browser logout/restart |
| Client lifecycle/cache/rendering | `api.test.ts`, `use-poll.test.tsx`, `App.test.tsx`: byte/item bounds, one active read, abort/generation guards, hidden/visibility cleanup, backoff/stale data, unauthorized, fixed expiry, hostile text, empty storage |
| Actual frontend and README image | Browser checks and saved [overview](../images/m4-dashboard.jpg), [timeline](../images/m4-timeline.jpg), [mobile](../images/m4-mobile.jpg), [stale](../images/m4-stale.jpg) evidence |
| Final Windows/Ubuntu/macOS/frontend CI | Pending branch publication; nine checks are configured, no result invented |

M4 selected-time caches floor sub-microsecond digits before PostgreSQL parsing can
round across a whole-second evaluation boundary. Original selected JSON timestamps
remain unchanged. No duration is manufactured by clock subtraction.

## Real-browser checks

Used the embedded native server at `http://127.0.0.1:8084` with a separate synthetic
viewer password and explicit HTTP loopback development mode. The committed
[fixture](../../tests/dashboard/seed.mjs) populated actual BullMQ queues and
stored/projected synthetic lifecycle facts. It was rerun after integration's own
disposable Redis fixture changed broker state; retained history remains separate.

Verified sign-in and incorrect-password feedback, overview/depth/rates, queue
filters, exact `jobs/failed % 雪` navigation, job search, all 205 retained attempts
across pages, a retained tombstone, an empty observed-stall filter, worker receipt
labels and their 60-second suspicion threshold. HTML-like queue/job/error text
remained text: zero injected images/inline scripts/SVG event handlers and no dialog.

At 1366×900 and 390×844, checked desktop/narrow layouts and keyboard-operated
mobile dialog/navigation. Document width stayed within the viewport; wide tables
scroll in their own container. Source/history/lag/clock limitations remain visible.
Temporary viewport overrides were reset after checks.

Stopping only the task-owned server produced visible stale data while retaining
the last read. Restart invalidated the opaque session and automatically returned
the page to login. Logout remained unauthenticated after reload. Four-hour expiry
is covered by injected server time and the frontend timer test; this session did
not wait four wall-clock hours. Poll abort/backoff/hidden behavior and resource
cleanup have deterministic client tests rather than an unmeasured heap-leak claim.

## Diagnosed failures and corrections

- The initial benchmark fixture omitted heartbeat version and overflowed a
  nanosecond month multiplied by a job index. Fixed the fixture, added row/time-span
  assertions and reran the unchanged 30-day gate; invalid fixture runs are not evidence.
- One M3 upgrade test expected three migrations. It now expects four while still
  asserting preserved rows/legacy COPY and all original indexes.
- The 500,001-row fixture's parallel VACUUM exceeded Docker's default shared-memory
  segment. Its setup uses `PARALLEL 0`; query deadlines/row caps were unchanged.
- Overlapping npm reinstall and Go discovery caused a transient missing-directory
  error. Gates were rerun sequentially; postinstall's nested module marker also
  excludes npm packages' unrelated Go sources from normal Go discovery.
- Request deadlines now release their timers/listeners explicitly. The expiry
  teardown test also waits one jsdom animation frame before asserting zero timers.
- Lint found three numeric HTTP status arguments; replaced them with standard constants.
- A cached Debian `golang:1.26.11` image contained a zero-byte `go` executable and
  silently produced no Go test output. That run was rejected as Go evidence. The
  validation image uses a verified nonempty Go 1.26.11 toolchain from the official
  Alpine image and the full Linux SDK gate was rerun with actual test output.
- The app's background-launch command was rejected automatically. A terminal-managed
  local server completed browser checks; no permission exception was required.

## Scope and cleanup

No M1–M3 protocol/folding/identity/retention behavior or accepted ADR 0001–0010 was
changed. No M5 rollups, percentiles, alerts or new adapter was added. No main push,
merge, release, protection change or work in another repository was performed.

Fresh services: `queuelens-m4-postgres` (55442) and `queuelens-m4-redis` (56384).
Browser data uses `ql_m4_browser_20261121`; integration/benchmark schemas are
generated and removed by their fixtures. User Docker data was not reset.
The terminal-managed preview listens on 8084. Stop that task process and use
`docker stop queuelens-m4-postgres queuelens-m4-redis` to stop these services;
this does not remove their data. Diagnostics, native binaries and local validation
images are task-local/ignored, not release artifacts.
