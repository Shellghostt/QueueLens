# ADR 0011 — M4 query bounds, dashboard evidence and access

- Status: proposed; documentation only
- Date: 2026-11-17
- Required by: [M4 requirements](../phases/M4.md), remaining QL-019 and QL-020–QL-022
- Would extend: ADR 0006 query coverage; preserves ADRs 0006–0010 identity/folding/retention policy

## Context

The M1 queue route is a live, unpaginated broker-state list. Completed/failed broker counts are retained
set sizes, not monotonic counters. M3 supplies durable selected attempt/run summaries and bounded
failure/stall/worker evidence, but no general timeline or rates. The M4 roadmap assumes 30 days of hourly
rollups even though QL-023 creates them in M5. API and SDK share one admin token, so a naive frontend
configuration would expose an ingest credential. Best-effort history cannot establish job death/exhaustion.

## Recommended design, pending acceptance

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

## Choices that must be resolved before acceptance

- Exact bounded queue-page route/mode and response version, timeline shape/order, pruning/not-found
  representation, identifier encoding and cursor bindings for the added filters/windows.
- Final metric definition and UTC time field, selected-outcome limitations, rate window limits and
  whether current summaries need query columns/indexes or a minimal dedicated M4 read projection.
  Such a projection must not change accepted folding semantics or silently become M5 analytics.
- Query/resource limits, representative performance dataset and concurrent workload. Preserve both
  the 300-ms dashboard target and existing ingestion/storage/SDK budgets.
- Browser auth/session/proxy bootstrap, read-route allowlist, cookie/origin/expiry behavior and secret
  handling. Do not change the SDK bearer-token wire contract to simplify a browser login.
- Pinned frontend dependencies/checks, static-asset serving, polling/hidden-tab/backoff/caching policy,
  accessibility and real-browser verification method.

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
auth/text/poll-cleanup tests and unchanged SDK/ingest regressions. Proposed decisions must be recorded
as accepted before dependent implementation; no accepted ADR file is modified by this preparation.
