# ADR 0009 — COPY raw evidence after fenced identity verification

- Status: accepted
- Date: 2026-11-16
- Refines: ADR 0008's compact claim staging and final raw-write implementation

## Context

Bulk ledger COPY reduced the provided-ID benchmark to 59.9 ms per 1,000 events,
still above the unchanged 50 ms storage-side budget. The final INSERT SELECT
continued to write raw tuples individually after the identity decision was
already fenced and known.

## Decision

The bounded indexed identity lookup returns only compact keys, conflict/existence
flags and server receipt time. Its new-key set selects exactly one incoming raw
event for each new provided identity; missing identities remain separate rows.
This in-memory set replaces the extra temporary claim table from ADR 0008.

New ledger entries and selected raw rows both use COPY in the same transaction.
The reusable input staging table still supports provided-key verification.
Missing-only batches can COPY raw rows directly. Receipts come from a server
statement within the transaction; no producer clock supplies them.

The shared retention fence and serialized claims lane remain unchanged. A COPY
constraint/connection failure rolls back raw rows and ledger entries together.
Identical duplicates, conflicting identities, M2 writer compatibility, event
timestamp precision and all projection/checkpoint policies remain unchanged.

## Evidence

Run the unchanged benchmark and sustained workload again, and explicitly test a
raw COPY failure after successful new-key verification/ledger COPY, followed by
a successful write on the same pooled store.
