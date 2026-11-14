# QueueLens TypeScript worker SDK (M2, unpublished)

Requires Node 22+. Build this local SDK with `npm ci && npm run build`; install the directory or
the output of `npm pack` in the worker project. It is not published to npm.

```typescript
import { Client } from "@shellghostt/queuelens";
const client = new Client({url: "http://127.0.0.1:8081", token});
client.emit({queue: "emails", job_id: job.id, state: "started"});
try {
  const result = await processJob(job);
  client.emit({queue: "emails", job_id: job.id, state: "completed"});
  return result;
} catch (error) {
  client.emit({queue: "emails", job_id: job.id, state: "failed", error: String(error)});
  throw error;
}
// At worker shutdown, after producers have stopped:
await client.close(5000);
```

All seven protocol states are available. Version, timestamp, attempt and generated worker ID default
automatically. Emit copies only protocol metadata into a bounded 10,000-entry ring, never serializes,
fetches or awaits. Full/closed buffers increment `queuelens_dropped_total`; `prometheus()` exposes it.
Background flushes occur every 250 ms, up to 500 events; heartbeats every 15 seconds. Fetch has a two-second
deadline and refuses redirects. No retry runs on the job path or in the background. Timers are unref'd;
await Close before process exit for a final drain. Close is idempotent and bounds/cancels pending requests.

Stats distinguish accepted events (`sent`, not database durability), overflow/transport losses (`dropped`),
collector rejections and heartbeat results. Partial 429 responses preserve accepted/dropped counts.
Events are best effort; the application still owns job results and thrown errors. Shared protocol cases
and an end-to-end PostgreSQL test exercise this SDK and the Go SDK against the same collector.
