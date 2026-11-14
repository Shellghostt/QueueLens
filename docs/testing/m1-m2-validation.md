# QueueLens M1/M2 validation - 2026-11-14

Work is on `feat/m2-implementation`. Main stays at `246b58601f8d327af953ae5e73dcd5836cad26bd`.
This implementation is unreleased and neither SDK is published to a registry.

## Executed checks

- Go 1.26.11: complete native unit suite, vet, build, golangci-lint 2.15.0 (zero issues) and
  govulncheck (no vulnerabilities).
- TypeScript: strict compilation and all five Node test groups passed; npm audit found zero
  vulnerabilities. `npm pack --dry-run` contains compiled JavaScript, declarations and README.
- All 21 shared protocol fixtures pass in Go and TypeScript, covering all seven states,
  default/optional fields, invalid versions/types/timestamps, and numeric storage limits.
- Native integration: all eight tests passed against PostgreSQL 16 and Redis 7.2, with real BullMQ
  6.7.6. Both actual SDKs sent 40 events each through the HTTP collector; all 80 rows and both worker
  heartbeats persisted. Worker upserts preserve first_seen and advance last_seen.
- Regression checks exercise concurrent collector cancellation/enqueue, cancellation drain, storage
  loss counters, malformed/trailing JSON, heartbeat auth/validation/storage failure/deadline,
  duration overflow, UTF-8 truncation, partial acknowledgements and redirect refusal.
- Million-event dead/hung collector tests verify bounded buffers, counted overflow and deadline
  cancellation. See [the overhead report](../benchmarks/m2-sdk-overhead.md) for measurements and limits.
- OpenAPI YAML and shared fixture JSON parse successfully. The heartbeat path matches server wiring.

- Linux/WSL full unit suite passed with the race detector. External-service tests were skipped in
  that run because URLs were absent. The native integration run above is the database/broker evidence.
- CLI builds passed for Linux, macOS and Windows on both amd64 and arm64.

The initial Go latency gate passed at 0.103961% added p50 and 4.797447% added p99.
The later PR run failed at 25.767872% added p99. Three local Linux runs reproduced 29.729457–34.518950%.
Go recovery now requires a successful authenticated heartbeat before reopening the event buffer
(ADR 0005). The unchanged gate then passed all three consecutive Linux runs with GOMAXPROCS=2:
0–0.093565% added p50 and 0.095735–0.712058% added p99. Exact samples are in the overhead report.

`TestFailedCollectorStaysClosedUntilHealthyProbe` failed before the fix and passed afterward. It
uses a real HTTP server, rejects 10,000 events after a failed recovery probe without another event
batch, then restores the collector and verifies successful delivery and complete event accounting.
The post-fix native full suite, Linux full race suite, vet and golangci-lint passed. External-service
tests skipped in these standalone runs because service URLs were absent; the PR CI integration job
provides the updated real PostgreSQL/Redis/BullMQ and SDK validation.

## Container image validation

After Docker recovery, the image built successfully with Go 1.26.11 and ran as UID 65532.
`bash tests/container/smoke.sh queuelens:m2-test` passed with a read-only root filesystem and all
Linux capabilities dropped. The actual packaged server applied migrations, passed health/readiness,
polled an isolated Redis fixture, rejected unauthenticated and invalid heartbeats, persisted one
completed event and a worker heartbeat in PostgreSQL 16, exposed heartbeat metrics and exited 0
on graceful shutdown. The fixture uses temporary containers and tmpfs data; existing databases
and volumes are not used. Actual BullMQ and both SDKs are covered separately by integration tests.

`.dockerignore` excludes local binaries, module caches, node_modules and environment files from the
build context. CI now builds the image and executes this same container check in addition to the
portable race, real PostgreSQL/Redis/SDK integration, overhead, lint and security jobs.

## Limits

The earlier Docker infrastructure failure is resolved and the container image check above completed.
Native integration and Linux/WSL race checks retain the scope described above; remote run status is
available in GitHub Actions for `feat/m2-implementation`. External-service tests skip when their URLs
are absent; a passing standalone unit run is not evidence of a second database integration run.

Heartbeats persist worker receipt time. Worker-down detection, lifecycle reconstruction, retention,
dashboard and alerts remain later milestones. Delivery is best effort; applications stop producers
before closing the SDK and expose worker drop counters separately from collector metrics.
