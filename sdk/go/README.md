# QueueLens Go worker SDK (M2, unpublished)

Import `github.com/shellghostt/queuelens/sdk/go` from this feature branch/local checkout and
`github.com/shellghostt/queuelens/pkg/protocol` for event types. Only a collector URL and token are
required; a worker ID is generated. Optional pool/worker ID settings identify a worker across its jobs.

```go
client, err := queuelens.New(queuelens.Options{URL: "http://127.0.0.1:8081", Token: token})
if err != nil { return err }
client.Emit(protocol.Event{Queue: "emails", JobID: jobID, State: protocol.StateStarted})
// Run the application job independently of telemetry success.
client.Emit(protocol.Event{Queue: "emails", JobID: jobID, State: protocol.StateCompleted})
// Stop producers, then drain with a deadline during shutdown.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
return client.Close(ctx)
```

Emit performs a non-blocking channel send, copies duration values and defaults version/time/attempt/worker
identity. No JSON encoding, validation or HTTP occurs on that path. A background goroutine batches up to
500 events every 250 ms and sends heartbeats every 15 seconds. Requests time out after two seconds,
redirects are refused and there is no retry. Buffers hold 10,000 events; newest arrivals are dropped on
overflow. After a transport failure or invalid acknowledgement, queued/new events are counted as drops.
Each flush tick probes the authenticated heartbeat endpoint; only HTTP 202 reopens the event buffer.
Failed probes keep event encoding disabled while the collector is dead; failed events are never retried.
Recovery probes use the existing request deadline and heartbeat counters. Stop emitting before Close
for a full final drain. Emit after Close returns false.

`Stats().Sent` counts collector acceptance, not durable PostgreSQL writes. `Dropped` counts buffer,
transport and malformed-acknowledgement losses; `Rejected` counts collector validation rejections.
Partial 429 acknowledgements retain these distinctions. `WritePrometheus` exposes
`queuelens_dropped_total` in the worker process. Heartbeat success/error counters are in Stats.
Delivery is best effort; application job success must not depend on Emit's return value.
