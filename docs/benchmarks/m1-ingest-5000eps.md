# QueueLens M1 ingest benchmark

**Date:** 2026-11-13
**Milestone:** M1 (adapter interface and collection)
**Question:** can the collector sustain 5,000 events/s of ingest for five minutes?

**Answer: yes.** 1,500,000 events were accepted at 4,999.99 events/s over 300.0 seconds with no
drops, no rejections, no failed writes and no 429 responses.

Every number below was measured on the machine described in [Method](#method). Nothing here is an
estimate, and nothing is extrapolated from another system.

---

## Method

### Hardware

| Component | Value |
|---|---|
| CPU | AMD Ryzen 5 5600H, 6 cores / 12 logical processors |
| RAM | 16 GB (15.3 GiB usable) |
| OS | Windows 11 Home Single Language, build 10.0.26300 |
| Go | go1.27.4 windows/amd64 |
| PostgreSQL | 16.16, `postgres:16-alpine` in Docker |
| Redis | 7.2.17, `redis:7.2-alpine` in Docker |

Everything ran on that single host: the collector, the load generator, PostgreSQL and Redis.

### Configuration

The collector was pinned to **four logical processors** with a processor affinity mask of `0x0F`:

```
$proc.ProcessorAffinity = 0x0F
```

The load generator, PostgreSQL and Redis were not pinned and used the remaining cores. The pinning is
the closest equivalent this machine offers to "4 vCPU", and it is a stricter test in one way (four
hyperthreads share four physical cores) and a weaker one in another (the database still has the whole
machine to itself, and there is no network latency between the components).

### Collector

```
QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens
QUEUELENS_REDIS_URL=redis://127.0.0.1:56379/0
QUEUELENS_ADMIN_TOKEN=<32 characters>
QUEUELENS_HTTP_ADDR=127.0.0.1:8080
```

Defaults otherwise: buffer 10,000 events, batch 500 events, flush every 250 ms, poll every 5 s.

### Load generator

```
queuelens loadgen --url http://127.0.0.1:8080 --rate 5000 --duration 5m --workers 4 --batch 100
```

Four concurrent senders, each pacing itself against a fixed deadline so the offered rate does not
drift, each sending a 100-event JSON batch every 80 ms.

---

## Result

```json
{
  "sent": 1500000,
  "accepted": 1500000,
  "rejected": 0,
  "dropped": 0,
  "failed_requests": 4,
  "requests": 15004,
  "duration": 300000735400,
  "events_per_second": 4999.987743363378,
  "p50_request": 1165700,
  "p99_request": 2497100,
  "status_429": 0,
  "last_error": "Post \"http://127.0.0.1:8080/v1/events\": context deadline exceeded"
}
```

| Measure | Value |
|---|---|
| Events sent | 1,500,000 |
| Events accepted by the collector | 1,500,000 (100%) |
| Events rejected by validation | 0 |
| Events dropped because the buffer was full | 0 |
| Batches that failed to write | 0 |
| Responses with HTTP 429 | 0 |
| Sustained rate | 4,999.99 events/s (target 5,000) |
| p50 request latency | 1.17 ms |
| p99 request latency | 2.50 ms |
| Collector CPU time during the run | 18.6 s over 300 s wall clock (6.2% of one core) |

### The four failed requests

The four failures are the requests that were in flight when the five-minute deadline cancelled the
client's context. The client reports them instead of hiding them; they account for 0.027% of the run
and were never counted as sent or accepted.

### Collector counters after the run

```text
queuelens_ingest_requests_total 17994
queuelens_events_accepted_total 1799201
queuelens_events_rejected_total 1
queuelens_events_too_old_total 0
queuelens_events_persisted_total 1799201
queuelens_buffer_dropped_total 0
queuelens_batches_persisted_total 4420
queuelens_batch_errors_total 0
queuelens_adapter_polls_total 133
queuelens_adapter_poll_errors_total 0
```

(The counters are cumulative over the whole life of that collector process, which also served an
earlier 20-second smoke run and one manual request. The single rejection is that manual request,
whose timestamp was hours in the future.)

---

## The other half of the overhead budget

PLAN section 9 also sets a budget of **"Collector ingest < 50 ms per 1,000-event batch"**.

### End to end, over HTTP

```json
{
  "sent": 60000,
  "accepted": 60000,
  "rejected": 0,
  "dropped": 0,
  "failed_requests": 0,
  "requests": 60,
  "duration": 30000397800,
  "events_per_second": 1999.9734803516506,
  "p50_request": 3181300,
  "p99_request": 5408000,
  "status_429": 0
}
```

`queuelens loadgen --rate 2000 --duration 30s --workers 1 --batch 1000`: a 1,000-event batch takes
**3.18 ms at p50** and **5.41 ms at p99** end to end, including the HTTP round trip on loopback.

### Storage only

```
$ go test ./internal/store -bench WriteBatch1000 -benchtime 20x
goos: windows
goarch: amd64
pkg: github.com/shellghostt/queuelens/internal/store
cpu: AMD Ryzen 5 5600H with Radeon Graphics
BenchmarkWriteBatch1000-12    	      20	   7505390 ns/op
```

Writing 1,000 rows with `COPY` takes **7.51 ms** against the local PostgreSQL 16 server.

### Storage footprint

1,880,213 rows occupy **250 MB** in the `queue_events_2026_11` partition, about 139 bytes per row
including the primary key index.

---

## What this does not prove

- **Not a cloud measurement.** One host, loopback networking, a database on the same machine. A
  deployment with the database on a different host will be slower, and the collector was given four
  logical processors rather than four dedicated vCPUs.
- **The rate is the generator's target, not a discovered limit.** The generator paced itself at
  5,000 events/s and the collector kept up with a p99 of 2.5 ms and 6% of one core. The headroom
  above 5,000 events/s was not measured, so no maximum throughput is claimed here.
- **No long-running stability claim.** Five minutes is one run on one machine, not a soak test.
- **The buffer never saturated**, so the 429 path was not exercised by this benchmark. It is covered
  by the unit test `TestBufferSaturationReturns429` instead.
- **The database did not run out of room**, but the partition maintenance that keeps that true is M3
  work and does not exist yet.

## Reproducing

```bash
docker compose -f deploy/docker-compose.yml up -d postgres redis

export QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@127.0.0.1:55433/queuelens
export QUEUELENS_REDIS_URL=redis://127.0.0.1:56379/0
export QUEUELENS_ADMIN_TOKEN=loadtest-token-0123456789

go build -o bin/queuelens ./cmd/queuelens
./bin/queuelens serve

# in another shell
./bin/queuelens loadgen --url http://127.0.0.1:8080 --rate 5000 --duration 5m --workers 4 --batch 100
```
