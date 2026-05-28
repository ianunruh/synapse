# ADR-0011: Clock injection — `Clock` interface, `SystemClock` default

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0004 Aggregate model](0004-aggregate-model.md)

## Context

The `Repository` stamps `RecordedAt` on outgoing events. Tests want deterministic timestamps; production wants the wall clock. Aggregates should not depend on infrastructure services like clocks.

## Decision

- `Clock` is an interface with one method: `NowUTC() time.Time`.
- `SystemClock` is the default implementation backed by `time.Now().UTC()`.
- `Repository` accepts a `Clock` through the `WithClock(c)` option; the default is `SystemClock`.
- Aggregates do not depend on the clock — recording stays a pure state transition. The timestamp is stamped by `Repository.Save` immediately before append.

## Consequences

- Tests can supply a virtual clock or use `testing/synctest` to make timestamps deterministic.
- Domain code stays clock-free: aggregates can be unit-tested without injecting infrastructure services.
- One tiny interface added to the API. It defines exactly one method to keep implementations trivial.

## Alternatives considered

- **Calling `time.Now()` inline.** Rejected: timestamps in tests become non-deterministic, and `testing/synctest` does not virtualize raw `time.Now()` values.
- **Passing `time.Time` into every event-recording call.** Rejected: ergonomic burden, infectious through domain code.
