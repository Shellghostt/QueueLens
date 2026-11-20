# QueueLens dashboard

React 19.3.1, TypeScript 6.0.3, Vite 8.3.8, Tailwind 4.3.5, shadcn-style
button primitives/Radix dialog and Recharts 3.11.0. Dependencies use exact
versions and the committed lock. TypeScript 6.0.3 satisfies the pinned lint
parser's supported range. Recorded local gates use Node **22.22.3** and Go **1.26.11**.

## Build and verify

From the repository root:

```sh
make web-check  # npm ci, lint, types, 12 tests, MemGuard, build, asset scan, audit
make build     # build assets and embed them with the dashboard Go build tag
```

Plain `go test ./...` and `go build ./...` remain headless builds. Enabled browser
access on a headless binary gives an explicit unavailable-assets response.
The npm postinstall writes an ignored nested module marker in `node_modules/`
so Go's `./...` discovery excludes npm packages' unrelated Go sources. If installing
with `--ignore-scripts`, run `npm run postinstall` before Go discovery.

## Access

Configure separate `QUEUELENS_DASHBOARD_PASSWORD` and `QUEUELENS_DASHBOARD_ORIGIN`
on the server. The password must be 12–256 bytes and differ from the shared
admin/ingest token. Production requires an HTTPS origin. Cookies are HttpOnly,
Secure, SameSite Strict and expire four hours after login. Logout and server
restart revoke sessions. The session exposes six allowlisted browser read
endpoints; ingestion and heartbeats retain bearer access.
Legacy `QUELENS_*` aliases preserve their existing precedence.

For HTTP loopback development only, set `QUEUELENS_DASHBOARD_INSECURE_DEV=true`.
Its distinct development cookie is never the production `__Host-` cookie.
Keep admin tokens and dashboard passwords out of `VITE_*` variables, compiled
assets, URLs, browser storage, logs and analytics.

## Vite development

Run a backend on `127.0.0.1:8084` with exact origin `http://127.0.0.1:5173`
and explicit insecure development enabled, then:

```sh
cd web
npm ci
npm run dev
```

Vite forwards `/dashboard/session` and `/dashboard/api/*`, preserving the original
host. Shared bearer credentials stay on the server. For packaged browser checks,
use the backend directly and configure its actual origin (for example
`http://127.0.0.1:8084`). No permissive cross-origin fallback exists.

## Read behavior

Each active view uses one combined read, polls five seconds after completion,
and caches only the current page. Route/filter/page changes, unmount, logout and
hidden tabs abort active reads and release timers/listeners. Visibility return
refreshes once. Network failures retain visibly stale data with 5/10/20/40/60-second
backoff. Unauthorized reads clear data and return to login. Requests have a
ten-second client deadline with explicit timer cleanup; bytes cap at 2 MiB and
each cached list at 200 items.

Queue/job/error metadata renders as text. Query parameters and URLSearchParams
preserve exact identifiers. Histories page independently and expose missing,
conflicting, reused, pruned and unknown evidence. Worker labels describe
collector receipts and suspicion, never proven life/death.

Rates describe selected retained attempt outcomes in UTC `[from, until)` windows
(`1h`, `24h`, `7d`, `30d`). They are not broker count deltas, unique production jobs,
all retry-failure events or complete-history guarantees. Unavailable rates stay
null; an empty failure denominator stays null. Hourly rollups, percentiles and
alerts remain M5.

See [ADR 0011](../docs/adr/0011-m4-query-and-dashboard-contract.md),
[OpenAPI](../api/openapi.yaml) and [M4 requirements](../docs/phases/M4.md).
The reproducible [browser fixture](../tests/dashboard/seed.mjs) requires explicitly
disposable loopback services and never resets a broker.
