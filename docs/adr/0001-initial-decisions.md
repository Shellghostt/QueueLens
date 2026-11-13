# ADR 0001 - Initial technology and scope decisions

- Status: accepted
- Date: 2026-11-13
- Deciders: Aditya

## Context

QueueLens is a greenfield open-source project. Before any code exists, the expensive mistakes to avoid are
scope creep and undocumented stack choices that a future contributor will try to reverse.

## Decisions

1. **Go backend, React frontend** - stream ingestion is a throughput problem, the UI is a rendering problem. Different tools, one repository.
2. **Adapter interface first, one adapter done properly** - a generic abstraction invented before two real implementations is always wrong. Redis (BullMQ) is the first and only adapter until M6.
3. **Instrumentation must never block a job** - emitting an event is a non-blocking write to a bounded buffer. On saturation the counter increments and the event is dropped. Blocking a job to record that it ran is a failure mode.
4. **Versioned wire protocol (v1)** - adapters in other languages do not import Go packages; the contract is documented in pkg/protocol and versioned.
5. **PostgreSQL before ClickHouse** - at the volumes this tool actually sees, Postgres with monthly partitions is sufficient. Adding an OLAP store before measuring is premature.
6. **Overhead target is a release gate** - measured in tests/bench, not asserted in a README. If the gate fails, the release does not happen.

## Consequences

- The project ships one narrow capability that actually works rather than a broad surface of stubs.
- Contributors have a reference point: changing any decision above requires a superseding ADR, not a
  silent pull request.
- Reversing a decision later is allowed but must be written down with the reason and the migration cost.

## Follow-up

- ADR 0002 is created at M1, recording the concrete schema and interface decisions made while building.
