# ADR-0004: Aggregate API — interface + embeddable `AggregateBase`, accumulator only

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0003 Stream identity](0003-stream-id.md), [ADR-0005 Event envelope](0005-event-envelope.md)

## Context

We needed to choose how application code expresses an aggregate: as a pure value with reducer functions (functional/decider style), or as a struct with mutating methods that record events (OO accumulator style). We also considered shipping both styles.

## Decision

- `Aggregate` is an interface: `StreamID()`, `Version()`, `Apply(Envelope) error`, `SetVersion(uint64)`, `Pending() []Envelope`, `ClearPending()`. `SetVersion` is called by the [Repository] after each `Apply` during rehydration so the aggregate's version tracks the stream head; embedders of [AggregateBase] get a correct implementation for free.
- The package ships an embeddable `AggregateBase` struct that satisfies most of the interface. Domain types embed `*AggregateBase` and write only the type-specific `Apply` method.
- New events are recorded by calling `(*AggregateBase).Record(eventType, payload, apply)`. The `apply` argument is typically the embedder's own `Apply` method; threading it explicitly avoids reflection without making `AggregateBase` aware of the concrete aggregate type. On apply error, the version is left unchanged and the envelope is not queued.
- A generic `FoldEvents[S]` helper is provided for ad-hoc projections and read-model rebuilds that want a pure reducer style. It does not flow through the `Repository`.
- **v0 supports the accumulator pattern only.** A functional `RunCommand` helper may be added in a later milestone if demand warrants it.

## Consequences

- One canonical aggregate style: less documentation surface, fewer code-review arguments, no "what if both are mixed" footgun.
- `Apply` serves double duty: rehydration from history and post-record state synchronization. Implementations must be deterministic at a given version.
- Aggregates carry a small pending-event slice (~24 bytes of header plus capacity); the cost is negligible.
- Functional users have `FoldEvents` for analytics-style usage but cannot route through `Repository.Save` without the accumulator. Adding that path later is non-breaking.

## Alternatives considered

- **Shipping both styles in v0** (accumulator + `RunCommand[C, S]`). Rejected because of doubled docs/tests, two `Repository.Save` code paths, and the risk that users mix them ambiguously.
- **Pure-reducer-only** (`Decide` + `Evolve`). Rejected as too opinionated for v0 — the accumulator style is more familiar to most Go developers, and the reducer style can be layered on later.
- **Generic `Aggregate[ID, S]`.** Rejected: the type parameters are infectious through `Repository`, `Handler`, and admin tooling.
