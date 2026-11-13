# Deployment and local infrastructure

QueueLens is a collector and an API. It runs as one process. There is no cluster, no message broker
of its own and no dashboard server to deploy separately.

## Local PostgreSQL and Redis

`deploy/docker-compose.yml` starts a disposable PostgreSQL 16 and Redis 7.2 for development and for
the integration tests:

```bash
make db-up      # docker compose -f deploy/docker-compose.yml up -d postgres redis
make db-down    # stops them and removes the volumes
```

Connection details: host `localhost`, port `55433` for PostgreSQL and `56379` for Redis, user
`queuelens`, password `queuelens`, database `queuelens`.

```bash
export QUEUELENS_POSTGRES_DSN="postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens"
export QUEUELENS_REDIS_URL="redis://127.0.0.1:56379/0"
```

The compose file also defines a `queuelens` service that builds the image and joins the two, for
trying the whole stack in one command:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

That service listens on `8081` and reads `QUELENS_POSTGRES_DSN` and `QUELENS_REDIS_URL` from the
compose environment. It does not set `QUEUELENS_ADMIN_TOKEN`, so it will refuse to start: the token
is required and has no default.

## Migrations

The collector applies its migrations at startup and records them in `schema_migrations`. Pass
`--skip-migrate` to `queuelens serve` when the migrations are applied by something else, for example
by a DBA during a release.

Only one table is created in M1: `queue_events`, range-partitioned by `ts`. The collector creates the
partition for the current month and the next one at startup.

## Container image

`Dockerfile` builds a minimal image that runs the collector. It is not a production deployment
reference; the compose file says the same thing at the top.

```bash
make docker
docker run --rm \
  -e QUEUELENS_POSTGRES_DSN="$QUEUELENS_POSTGRES_DSN" \
  -e QUEUELENS_REDIS_URL="$QUEUELENS_REDIS_URL" \
  -e QUEUELENS_ADMIN_TOKEN="$QUEUELENS_ADMIN_TOKEN" \
  -p 8080:8080 \
  queuelens:latest serve
```

## Secrets

- The bearer token comes from `QUEUELENS_ADMIN_TOKEN` and must be at least 16 characters. The
  collector refuses to start without it.
- The connection strings come from the environment, not from command-line flags, so they do not
  appear in a process listing or a CI log.
- The image contains no credentials.
- Error strings captured from a job are redacted on a best-effort basis before they are stored. That
  is a safety net for the common cases, not a guarantee; see the threat model in
  [docs/PLAN.md](docs/PLAN.md).

## Operational endpoints

| Endpoint | Purpose | Authentication |
|---|---|---|
| `GET /healthz` | Liveness. The process is running. | None |
| `GET /readyz` | Readiness. Returns 503 when PostgreSQL cannot be reached. | None |
| `GET /metrics` | The collector counters in the Prometheus text format. | None |
| `GET /v1/queues` | The current queue state. | Bearer token |
| `POST /v1/events` | Ingest a batch of events. | Bearer token |

`/readyz` deliberately does not check Redis: a broken broker must not take the collector out of
rotation. The last known queue state stays available and is marked `stale`.

## What is not deployed yet

There is no dashboard to host, no alert manager to configure and no SDK to install. Those arrive
with M2 to M5.
