# ADR-0006: Optimistic concurrency — `Revision` sum type

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md), [ADR-0010 Error model](0010-error-model.md)

## Context

The event store needs a way for callers to express their expectation about the current state of a stream when appending. The expectation must be cheap to evaluate at the store boundary and expressive enough to cover "first append", "append to existing stream", "any state", and "must be at exactly version N".

## Decision

- A `Revision` struct with an unexported `kind` discriminator and a `uint64` value.
- Sentinel values: `Any`, `NoStream`, `StreamExists`. Constructor: `Exact(v uint64) Revision`.
- `Revision` is comparable, copyable, has a zero value (`Any`), and fits in two machine words.
- `EventStore.Append` returns `Exact(newHead)` on success and `*ConflictError` (wrapping `ErrConflict`, see [ADR-0010](0010-error-model.md)) on mismatch.

## Consequences

- Most expressive optimistic-concurrency model: callers say exactly what they mean.
- Zero runtime cost: tagged value, no boxing, no interface dispatch.
- Stores implement a single switch over `kind` to enforce the constraint.

## Alternatives considered

- **`uint64` with sentinel constants** (e.g. `^uint64(0)` for "any"). Rejected: sentinels are easy to misuse and ugly in logs.
- **`uint64`-only, no sentinels** — callers must `Load` first. Rejected: forces an extra round-trip for new streams.
- **Sum implemented as an interface with concrete types.** Rejected: every revision use would carry interface dispatch and possible boxing alloc.
