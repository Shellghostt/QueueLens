# ADR 0003 - Go 1.26 floor, dependency upgrades and CI contract

- Status: accepted
- Date: 2026-11-13
- Deciders: Aditya

## Context

The first CI run on GitHub, on 2026-11-13, failed the vulnerability scan with 50 code-affecting
vulnerabilities. They came from three places:

- The Go standard library, in `net/http`, `crypto/tls`, `crypto/x509`, `net/url`, `net/textproto`,
  `encoding/xml` and `encoding/asn1`. The database records these advisories as `introduced 0`,
  `fixed 1.25.x` or `fixed 1.26.x`: every release before the fix is affected, and there is no fix
  in the 1.23 line at all. `go.mod` declared `go 1.23.0`, so setup-go installed exactly 1.23.0.
- `github.com/go-chi/chi/v5` v5.2.1, `github.com/jackc/pgx/v5` v5.7.4 and `golang.org/x/text`
  v0.22.0, which were pinned to the newest releases that still supported Go 1.23.

ADR 0002 decision 18 chose that pinning deliberately: the plan named Go 1.23, and the newest chi,
pgx and go-redis releases required Go 1.25. The decision kept the plan's promise and accepted the
consequence. The consequence is this: a project that serves HTTP and speaks TLS cannot run on a Go
release line that receives no security fixes.

## Decisions

1. **The Go floor moves from 1.23 to 1.26.** `go.mod` declares `go 1.26.0` and
   `toolchain go1.26.11`. This supersedes decision 18 of ADR 0002. The plan named a Go version; the
   plan is updated to name the version that is still supported, because a version floor that forces
   an unpatched toolchain is not a floor worth keeping.
2. **The dependencies move with it.** `github.com/go-chi/chi/v5` goes to v5.5.0,
   `github.com/jackc/pgx/v5` to v5.13.0 and `golang.org/x/text` to v0.42.0, which brings
   `golang.org/x/sync` to v0.23.0. `github.com/redis/go-redis/v9` stays at v9.7.3: it was not
   affected, and a version that is not vulnerable is not changed for the sake of changing it.
3. **The `toolchain` directive names the exact toolchain CI installs.** `actions/setup-go` prefers it
   over the `go` directive. A `go` directive with a patch version and no `toolchain` line makes
   setup-go install that exact patch, which is how this project ended up on 1.23.0 and 1.26.0.
   `go mod tidy` preserves the `toolchain` line.
4. **The builder image moves to `golang:1.26-alpine`** so that the container and CI build with the
   same toolchain.
5. **The linter is golangci-lint v2.15.0, pinned by version, run by `golangci/golangci-lint-action@v9`.**
   The v1 line cannot be used against a Go 1.26 module: its newest release, v1.64.8, is built with
   Go 1.24 and refuses to analyse it.
6. **Unchecked error returns are fixed, not excluded.** The v2 findings are five discarded error
   returns. Each one now says so explicitly with `_, _ =`. No linter exclusion list is added, so the
   next unchecked write is still reported.

## Consequences

- The vulnerability scan is clean: `govulncheck ./...` under Go 1.26.11 reports no vulnerabilities.
- The collector still accepts the same events. The pgx upgrade is a minor-version upgrade inside
  `pgx/v5`, and the BullMQ adapter still reads Redis directly with `SCAN`.
- The benchmark in `docs/benchmarks/m1-ingest-5000eps.md` was measured with Go 1.27.4 and is
  unaffected by this change; a future measurement records the toolchain again.
- The floor is now the same as EventRelay's, and the same as the linter version, so every Go project
  in the portfolio is built and checked the same way.
