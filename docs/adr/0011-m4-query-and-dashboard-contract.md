# ADR 0011 — M4 query bounds, dashboard evidence and access

- Status: accepted for `feat/m4-implementation`
- Proposed: 2026-11-17; accepted: 2026-11-21
- Required by: [M4 requirements](../phases/M4.md), remaining QL-019 and QL-020–QL-022
- Would extend: ADR 0006 query coverage; preserves ADRs 0006–0010 identity/folding/retention policy

## Context

The M1 queue route is a live, unpaginated broker-state list. Completed/failed broker counts are retained
set sizes, not monotonic counters. M3 supplies durable selected attempt/run summaries and bounded
failure/stall/worker evidence, but no general timeline or rates. The M4 roadmap assumes 30 days of hourly
rollups even though QL-023 creates them in M5. API and SDK share one admin token, so a naive frontend
configuration would expose an ingest credential. Best-effort history cannot establish job death/exhaustion.

## Decision

1. Keep legacy `GET /v1/queues` response behavior. Add a separately documented bounded dashboard queue
   page route or opt-in page mode, plus queue detail/jobs and paginated attempt detail. Preserve M3 page
   defaults/caps, C-order keys, signed route/filter/time-bound cursors and query deadlines. New responses
   carry the existing evidence/lag/history limitations; never expose half-pruned histories.
2. Reuse selected M3 attempt/run evidence. Expose retained attempt histories across pages with supplied
   duration/errors and unknown/pruned/reuse/contradictory states. Missing heartbeat or telemetry cannot
   invent a terminal failure, generation, retry limit or complete-history guarantee.
3. Define M4 rates as selected retained attempt outcomes over a bounded UTC window: completed attempts/
   seconds and failed/(failed+completed). Label them accordingly; selected retries and superseded facts
   are not all failure events. Null for no denominator and explicit history/clock/lag caveats are required.
   Broker counts/depth retain their independent source/update/stale semantics. Do not difference retained
   broker counts or use page lengths as a production rate.
4. Use indexed bounded summary queries or an explicitly accepted minimal M4 read model. Dashboard
   history/metric queries must not scan raw partitions. Existing capped projection-lag sampling remains
   a separate bounded evidence query with its current unknown behavior. M5 percentile/hourly rollups
   remain separate work. Bound lookback/work/response bytes and report unknown on aggregation timeout.
   The proposed maximum lookback is 30 days.
5. Keep p95 query <300 ms, measured for 30 days of retained summaries with concurrent projector/retention
   work. Declare dataset/hardware/cache/index/query-plan details and all request/error/timeout counts.
   The gate requires correct bounded responses; failures or unavailable rates make the run fail and
   cannot be silently excluded to manufacture a passing latency result.
   Proposed initial corpus: 100 queues, 100,000 jobs, 300,000 attempts. Add the historical hourly-rollup
   workload to M5 when QL-023 exists; do not claim it as M4 coverage.
6. Use authenticated same-origin browser proxy/session access that keeps the static server token out of
   compiled assets and browser persistence. Restrict browser access to read routes and define login/
   expiry/logout/development behavior. Use a bounded polling client with cancellation, stale-response
   suppression and timer cleanup, and render captured metadata strictly as text.

## Exact M4 read contract

- Add authenticated `GET /v1/dashboard/overview`, `/v1/dashboard/queues`, `/v1/dashboard/queue`,
  `/v1/dashboard/jobs` and `/v1/dashboard/job`. Existing `/v1/queues` and all M3 routes remain unchanged.
  Overview returns one bounded broker queue page plus selected-outcome metrics. Queue detail returns
  the optional current broker view, metrics and a job page. Job detail contains the retained job summary
  plus a separately paginated attempt page, so each dashboard view needs one active request.
- Queue/job identifiers are query parameters, decoded once through normal URL parsing. Names remain
  exact UTF-8 strings, including slashes, quoting, percent signs and reserved words. Limits retain 256
  UTF-8 bytes and nonblank identity requirements. State is `all` or one of the six observed protocol
  states. Ordering is fixed: C-order queue/job keys and ascending numeric attempts. No custom SQL/sort.
- Pages default to 100, cap at 200 and fetch one extra row. Reuse M3's HMAC cursor key/envelope and
  evaluation-time policy; added route/filter bindings include job identity/state/window. A cursor from
  one route/filter is invalid on another. Page size does not establish a cross-page snapshot.
- Job availability is `observed`, `pruning`, `pruned` or `not_observed`. A retained pruning marker serves
  no half-deleted attempts; a tombstone supplies history-limited evidence. Missing retained history is
  not proof the production job is absent. Run and attempt reads share a read-only REPEATABLE READ
  transaction, preserving a consistent page during concurrent pruning. All M3 history/evidence fields,
  nullable duration/retry exhaustion and bounded lag remain visible.
- Every M4 response is buffered before writing and capped at 2 MiB. Route queries allow only their
  defined single-valued parameters, with a 12 KiB raw-query cap and existing 4 KiB cursor cap. Invalid
  arguments/cursors return 400; bounded read failures return explicit 503, not empty successful data.
  A broker page visits at most 10,000 current queue views and materializes at most 201.

## Selected-outcome metric read model

- Migration 0004 adds indexed selected state/time/conflict cache columns to `job_attempts`, backfilled
  from existing M3 summary JSON. The projector updates these derived fields with the summary in the
  same transaction. No state precedence, identity, markers, retention or SDK wire semantics change.
  Nullable cache fields preserve compatibility with legacy writes; a bounded missing-cache check
  reports unavailable metrics rather than treating such rows as zero outcomes.
- Windows are allowlisted `1h` (default), `24h`, `7d`, `30d`. The interval is UTC `[from, until)`, ending
  at the cursor's M3 evaluation tick. The time field is the selected observation's supplied `ts`, not
  subtraction of worker/collector clocks. A window exceeding configured retained-history coverage is
  unavailable, not a complete-history rate.
- Completion throughput is retained selected completed attempts / window seconds. Observed failure
  fraction is selected failed / (selected completed + selected failed); an empty denominator is null.
  Superseded retry failures are not counted as all failure events. Counts are selected retained attempts,
  not unique production jobs or broker set deltas. Clock disagreement, late arrival, contradictions,
  telemetry loss and pruning can change the result; labels disclose these limitations.
- Metrics query only indexed summary cache columns, exclude pruning runs, and cap eligible terminal
  work at 500,000 rows plus one overflow sentinel. Aggregation has a 150 ms deadline; an overflow,
  missing cache or timeout yields `available: false` with null rates and an explicit reason. Other
  page queries retain three-second deadlines. The existing 500 ms capped lag sampler remains separate.
  M5 hourly rollups, percentiles and alerts are not implemented as part of this cache.
- The unchanged p95 <300 ms gate uses 100 broker queues, 100,000 retained jobs and 300,000 attempts
  distributed over 30 days, with concurrent real ingest/projection/maintenance. Record plans/indexes,
  cache state, hardware, dataset and every request/error/timeout/unavailable result. Any failed or
  unavailable metric request fails the run. Preserve relevant storage, load and unchanged SDK budgets.

## Browser access and lifecycle

- Dashboard access is opt-in through separate `QUEUELENS_DASHBOARD_PASSWORD` and
  `QUEUELENS_DASHBOARD_ORIGIN`. The existing shared admin/ingest token remains server-side and keeps its
  original wire contract. The dashboard password is at least 12/max 256 bytes and cannot equal that
  token. Canonical `QUEUELENS_*` and legacy `QUELENS_*` precedence remains unchanged.
- `POST /dashboard/session` accepts only bounded JSON password input and compares a fixed SHA-256
  digest in constant time. `GET /dashboard/session` reports authenticated bootstrap/expiry;
  `DELETE /dashboard/session` logs out and revokes the session. Login, logout and browser reads check
  the configured origin/host. Cross-origin access receives no permissive CORS headers.
- Sessions are random 256-bit opaque tokens, stored by digest in a bounded in-memory map (maximum 256),
  fixed four-hour expiry, invalidated on restart/logout. Cookies are HttpOnly, SameSite Strict, Path=/;
  production requires HTTPS/Secure and the `__Host-queuelens_session` name. Explicit
  `QUEUELENS_DASHBOARD_INSECURE_DEV=true` allows an HTTP loopback origin and a distinct dev cookie.
  Credentials are never placed in assets, URLs, storage, analytics or error logs. Login rate limiting
  is bounded to 1,024 retained clients/10-minute windows, at most 20 attempts per window.
- Session-authenticated `/dashboard/api/{overview,queues,queue,jobs,job,workers}` is a fixed read-only
  dispatch allowlist to the same query handlers. It never exposes event/heartbeat/admin mutations or
  proxies arbitrary URLs. `/v1` routes keep their existing bearer authentication.
- Build React/TypeScript/Vite/Tailwind with shadcn-style accessible primitives and Recharts inside
  `web/`, with exact dependency versions and a committed lock. Browser API requests are same-origin.
  Production `make build`/container builds compile and embed assets using the `dashboard` Go build tag;
  ordinary headless Go tests/builds remain compatible without generated assets. Vite's development
  proxy serves the same session/API paths. Missing production assets are explicitly unavailable.
- Poll each active view every five seconds, with one request in flight and cancellation/generation
  guards on route/filter/page changes. Pause while hidden; on visibility return perform one refresh.
  Network failures keep last data visibly stale and back off 5/10/20/40/60 seconds. Unmount/logout aborts
  requests and releases timers/listeners. Cache only the current bounded page (no growing page history).
  Fetch/JSON response bytes are capped at 2 MiB. Unauthorized responses clear data and return to login.
- Render captured queue/job/error metadata only as text. Provide loading/empty/error/unavailable/pruned/
  stale states, keyboard navigation and narrow layouts. Real-browser checks use seeded observed data,
  verify cookie/logout/expiry, hostile text, navigation/filter/abort behavior and capture a README
  screenshot of the implemented dashboard. No screenshot or acceptance result is claimed by this ADR.

## Alternatives considered

- Use retained broker count deltas as throughput: removal/retention makes deltas nonmonotonic and false.
- Fetch all jobs/attempts into the browser: exceeds API and client bounds and hides live-page uncertainty.
- Call no heartbeat a failed job: absence of telemetry cannot prove that a worker or job died.
- Put the shared token in the frontend bundle/local storage: exposes both read and ingest capability.
- Implement M5 rollups while scaffolding the dashboard: obscures scope/dependencies and skips their
  percentile/history correctness gates. Specify the M4 read workload separately.

## Consequences and validation

QL-019 is partially implemented at baseline and needs compatible bounded extensions before UI work.
The phase requires seeded-rate and timeline/retention/query cases, explicit benchmark evidence, browser
auth/text/poll-cleanup tests and unchanged SDK/ingest regressions. This record is accepted before
dependent implementation; prior accepted ADRs 0001–0010 remain unchanged. Actual acceptance results
will be recorded separately after their gates run.
