# ADR 0010 — Bounded array input for fenced identity verification

- Status: accepted
- Date: 2026-11-16
- Refines: ADR 0009's input staging transport

## Context

Raw COPY reduced the provided-ID write to 54.7 ms per 1,000 events, above the
unchanged 50 ms budget. Narrowing staging to distinct identity fields still
measured 57.1 ms. Tracing showed that staging COPY, its table setup and a separate
verification query retained avoidable client/server work before durable writes.

## Decision

After normalization and in-batch conflict checking, pass each distinct provided
identity as four aligned, typed PostgreSQL arrays: queue, digest key, original ID
and content fingerprint. One UNNEST query performs the existing indexed lateral
lookups and returns compact existence/conflict flags and one server statement
receipt time. Input remains bounded by the unchanged 1,000-event collector batch.
No temporary staging table is needed.

The schema-scoped retention fence and serialized claims lane remain unchanged.
New ledger entries and exactly one raw row per new identity still use COPY in the
same transaction. Missing-ID rows remain independent, identical retained IDs are
no-ops, and conflicting IDs or any COPY failure roll back the entire batch.
Receipt time, timestamp precision, M2 COPY compatibility and lifecycle policy do
not change. This decision supersedes only the input staging transport, leaving
accepted ADRs intact.

## Evidence

Repeat identity/large-ID/mixed-batch/concurrent-writer checks and the raw COPY
rollback followed by pooled-writer recovery. Run the unchanged storage and
sustained-load gates and retain every earlier failing result in M3 validation.
