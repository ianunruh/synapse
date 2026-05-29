# ADR-0022: Causation / correlation / metadata threading via context

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0005 Event envelope](0005-event-envelope.md), [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md)

## Context

`Envelope` and `RawEnvelope` already carry `Causation`, `Correlation`, and `Metadata` fields (ADR-0005), but no public API helped populate them. Aggregate command methods that call `(*AggregateBase).Record` produced envelopes with these fields empty, and `Repository.Save` wrote whatever was there. Real-world tracing — wire a request ID from an HTTP middleware to every event recorded under it, preserve the causation chain across a projection-driven side effect — required users to reach into the envelope by hand, which the public API did not support.

We needed a threading mechanism that:

- Fits naturally with HTTP / gRPC middleware that already populate `context.Context`.
- Does not infect every aggregate command method with extra parameters.
- Handles the saga case where a `Projection.Project` records new events that should carry the inbound event's identifiers.
- Allows per-event overrides for the unusual cases.
- Stays backward-compatible: code that never touches the helpers gets the same behavior as before.

## Decision

Threading happens through `context.Context`:

- `es.WithCorrelation(ctx, id) context.Context`
- `es.WithCausation(ctx, id) context.Context`
- `es.WithMetadata(ctx, meta Metadata) context.Context` — merges with prior context metadata, later keys winning.

`Repository.Save` reads these values from the `ctx` it receives, then for each pending event:

- Stamps `Correlation` and `Causation` where the envelope field is empty. An explicit non-empty value on the envelope wins.
- Merges `Metadata`: the ctx map is the base; per-event `Envelope.Metadata` keys override on collision.

`projection.Runner` derives a child context for each `Project` call by default, setting `Causation = inbound.EventID`, propagating `Correlation` when non-empty, and forwarding `Metadata`. A `Project` body that calls `es.Execute` or `repo.Save` therefore writes outbound events with the right saga chain stamped automatically. Opt out with `projection.WithoutContextEnrichment()`.

## Consequences

- HTTP middleware pattern works directly: install a chain that calls `ctx = es.WithCorrelation(ctx, requestID)` and every event saved under it carries that correlation ID, with no aggregate-level changes.
- Saga projections compose: a chain of `Project → Execute → Project → Execute` preserves the original correlation and threads the immediate prior `EventID` into each step's outbound `Causation`.
- The Record API stays narrow. Forward-compatible: when a future Record variant takes per-event metadata, the "explicit wins" precedence in Save is already correct.
- One small implicit-behavior surface in the Runner. The opt-out keeps it controllable; the default reflects the most common saga shape.
- No new types and no codec changes. Existing on-disk envelopes round-trip identically.

## Alternatives considered

- **Per-call options on `Execute` / `Save`** (e.g., `es.WithSaveCorrelation(id)`). Rejected: every call site has to pass them, middleware does not flow them naturally, and `Execute`'s signature grows for a concern that ~every event in a request shares.
- **A new `RecordWith` on `AggregateBase`** that takes per-event metadata. Rejected for v0: the 95% case is "all events in this request share these IDs", which is exactly what context threading models. The per-event case can be added later (the Save precedence already supports it) without a breaking change.
- **Aggregate-level setter** (e.g., `agg.SetMetadata(...)`). Rejected: introduces hidden state on the aggregate that has to be reset between calls. Brittle.
- **Always enrich Project's ctx, no opt-out.** Rejected: read-only projections that never record events should not pay the (small) attributing cost, and some users want full control over the context their `Project` body sees.
- **Mirror the values into an interceptor-style middleware in `es`.** Rejected: `context.Context` is the stdlib answer to scoped values, and middleware is for wrapping the load-handle-save pipeline, not for injecting per-event metadata.
