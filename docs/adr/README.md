# Architecture Decision Records

Each file records one decision that was expensive to make and would be expensive to reverse.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-initial-decisions.md) | Initial technology and scope decisions | accepted |
| [0002](0002-m1-implementation-decisions.md) | M1 implementation decisions | accepted, partly superseded by 0003 |
| [0003](0003-go-126-floor-and-ci-contract.md) | Go 1.26 floor, dependency upgrades and CI contract | accepted |
| [0004](0004-m2-sdk-and-heartbeat-contract.md) | M2 SDK and heartbeat contract | accepted; supersedes 0002 decision 12 for workers |
| [0005](0005-go-sdk-recovery-probes.md) | Go SDK recovery probes | accepted; supersedes 0004 decision 3 for Go recovery |

New ADRs are numbered sequentially and never edited after acceptance. A changed mind gets a new ADR
that supersedes the old one.
