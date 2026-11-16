# ADR 0007 — Bounded pruning of large attempt histories

- Status: accepted
- Date: 2026-11-16
- Clarifies: ADR 0006's bounded projection/attempt retention implementation

## Context

One logical run can contain 32,767 distinct reported attempts. Deleting 1,000
runs and all their summaries in one transaction would exceed the declared
1,000-row maintenance bounds. Skipping large runs would leave unresolved
history unbounded.

## Decision

Under the existing folder lock, maintenance marks at most 1,000 expired runs
with a durable nullable `pruning_at`. It deletes at most 1,000 attempt summaries
per transaction, then removes at most 1,000 marked runs whose summaries are
gone. Final run deletion and its retained-key tombstone commit together.
Each worker/identity/tombstone cleanup category is separately capped at 1,000.

Marked runs are temporarily omitted from lifecycle query pages. Unexpired
pending job facts for those keys stay unprocessed until final deletion; drained
and expired-history facts can still receive their markers. Projection lag also
reports bounded pruning evidence. This prevents a crash or concurrent folder
batch from merging into a partially deleted attempt history.

Maintenance continues bounded batches while a category is full. After a restart,
the durable mark resumes cleanup. Raw retention still preserves every pending
row. Final deletion starts the tombstone's history window, and subsequent
reconstruction is explicitly history-limited under ADR 0006. No run generation,
job outcome or source event ID is invented.

## Evidence

Integration tests must exercise a run larger than one cleanup batch, a restart
between pruning phases, concurrent pending facts, and reconstruction after final
deletion. Results are recorded in the M3 validation report after execution.
