# ADR 0008 — Bulk identity claims under a bounded writer lock

- Status: accepted
- Date: 2026-11-16
- Refines: ADR 0006's atomic identity writer

## Context

The first 20-iteration provided-ID storage benchmark averaged 81.4 ms per
1,000 rows. Tracing and EXPLAIN showed per-row speculative ledger INSERT work,
recreated staging definitions, and a conflict plan scanning retained identities.
Keyed conflict lookup and reusable empty staging reduced that average to 76.3 ms,
still above the existing 50 ms storage-side ingest budget.

## Decision

Provided-ID writer transactions take one schema-scoped identity-claims advisory
lock in addition to the existing shared identity-retention lock. Under that lock,
at most the incoming batch's distinct keys are checked through indexed lateral
lookups. Different original IDs/fingerprints still reject the whole transaction.
Existing identical keys remain successful no-ops.

New ledger entries and their compact staging claims use COPY, avoiding per-row
speculative INSERT overhead. All M3 writers use this lane; M2's nine-column writer
still emits legacy rows and does not claim producer IDs. Missing-only batches do
not need the claims lock. Cross-writer claim concurrency is serialized; horizontal
collector capacity remains an M6 measurement, not an M3 throughput promise.

Connection-local staging definitions are reused with ON COMMIT DELETE ROWS.
Their data, ledger claims and raw writes remain one transaction, including rollback.
Deadlines/lock waits stay finite and retention still exclusively fences all claims.
Receipts use one server statement time for a batch's new identity entries.

## Evidence

Repeat the original conflict/concurrent-writer checks and the unchanged
1,000-event storage and sustained-ingest measurements after implementation.
Retain all earlier measurements and the diagnosed cause in M3 validation.
