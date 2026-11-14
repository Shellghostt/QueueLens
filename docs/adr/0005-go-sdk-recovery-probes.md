# ADR 0005 — Go SDK recovery probes

- Status: accepted
- Date: 2026-11-14
- Supersedes: ADR 0004 decision 3, only the Go transport cooldown/recovery behavior

## Context

The pull-request overhead run measured 25.767872% added p99, exceeding the unchanged 5% budget.
Three local Linux runs with GOMAXPROCS=2 reproduced 29.729457–34.518950% added p99. The old Go
cooldown reopened the event buffer every flush tick even when the collector was still unavailable.
That caused repeated event batching, JSON encoding and failed requests during an outage.

## Decision

After a failed batch or invalid acknowledgement, the Go client drops pending/new events and stays
unavailable. Each flush tick sends the existing authenticated heartbeat as a recovery probe. Only
HTTP 202 reopens event admission. Failed probes keep the buffer closed, without encoding event
batches. Probes use the existing HTTP deadline, redirect policy and heartbeat counters; the default
250 ms flush interval bounds probing to four requests per second, in addition to periodic heartbeats.

The producer still performs no I/O or waiting. Close drops any remaining queued events when the
transport is unavailable and retains its caller deadline. Failed events are never retried. A live
collector with a failing event endpoint can reopen after a successful heartbeat, but the next failed
batch closes it again; this admits at most one failed batch per successful recovery probe.

## Consequences

The configured collector must implement the M2 heartbeat endpoint and return HTTP 202 for recovery.
Recovery may take one flush interval plus the probe duration. Heartbeat counters include recovery
probes. The TypeScript SDK and protocol are unchanged. The million-job benchmark, alternating sample
order, 2% p50 limit, 5% p99 limit and accounting checks are unchanged; no failed timing sample is
discarded or retried by the test.
