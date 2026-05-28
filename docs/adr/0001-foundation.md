# ADR-0001: Foundation — module, license, Go version, dependency policy

**Status:** Accepted (2026-05-28)

## Context

We are starting an open-source event sourcing toolkit for Go and need to make foundational, hard-to-reverse choices about identity, licensing, language version, and dependency posture before writing code.

## Decision

- **Module path:** `github.com/ianunruh/synapse`
- **License:** Apache-2.0
- **Go toolchain:** 1.26
- **Dependency policy:** the root module declares zero third-party dependencies. Optional concerns that require external libraries (specific store backends, protobuf codec, OpenTelemetry instrumentation) live as sibling packages or sibling modules under the same repository.

## Consequences

- Users on Go < 1.26 cannot import the module. This is acceptable: we want the modernization-clean style (`iter.Seq2`, `clear()` builtin, generic type aliases, `testing/synctest`) and adoption momentum builds within ~6 months.
- Apache-2.0's explicit patent grant makes the library palatable to corporate users; it matches the licensing of comparable infrastructure libraries.
- A dependency-free core means `go.mod` stays trivial, supply-chain risk is minimal, and codec/backend choice is unbounded.
- Sibling packages pay any external-dep cost only when users opt in.

## Alternatives considered

- **MIT** (no patent grant) or **BSD-3-Clause** (stdlib aesthetics) — viable but Apache-2.0 is the safer default for infrastructure.
- **Go 1.24** as a conservative baseline — rejected because we want the latest modernize lints and 1.26-specific features will accumulate.
