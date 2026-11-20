# Architecture Decision Records

Each file records one decision that was expensive to make and would be expensive to reverse.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-initial-decisions.md) | Initial technology and scope decisions | accepted |
| [0002](0002-m1-implementation-decisions.md) | M1 implementation decisions | accepted, partly superseded by 0003 |
| [0003](0003-go-126-floor-and-ci-contract.md) | Go 1.26 floor, dependency upgrades and CI contract | accepted |
| [0004](0004-m2-sdk-and-heartbeat-contract.md) | M2 SDK and heartbeat contract | accepted; supersedes 0002 decision 12 for workers |
| [0005](0005-go-sdk-recovery-probes.md) | Go SDK recovery probes | accepted; supersedes 0004 decision 3 for Go recovery |
| [0006](0006-lifecycle-identity-and-retention.md) | Lifecycle identity, processing and retention | accepted |
| [0007](0007-bounded-projection-pruning.md) | Bounded pruning of large attempt histories | accepted; clarifies 0006 |
| [0008](0008-bulk-identity-claims.md) | Bulk identity claims under a bounded writer lock | accepted; refines 0006 |
| [0009](0009-copy-raw-evidence.md) | COPY raw evidence after fenced identity verification | accepted; refines 0008 |
| [0010](0010-array-identity-verification.md) | Bounded array input for fenced identity verification | accepted; refines 0009 |
| [0011](0011-m4-query-and-dashboard-contract.md) | M4 query bounds, dashboard evidence and access | accepted 2026-11-21; locally implemented M4 |

New ADRs are numbered sequentially and never edited after acceptance. A changed mind gets a new ADR
that supersedes the old one.
