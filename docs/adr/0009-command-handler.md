# ADR-0009: Command handler — generic `Handler[C, A]`, bus deferred

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0004 Aggregate model](0004-aggregate-model.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md)

## Context

Commands are typed intents addressed at an aggregate. The library could provide a fully dynamic command bus, a generic typed handler primitive, or both. We had to pick a primitive that aligns with the rest of the type-safe core and decide what to defer.

## Decision

- The core primitive is `Handler[C any, A Aggregate] func(ctx context.Context, cmd C, agg A) error`.
- A convenience `Execute[C, A](ctx, repo, id, cmd, handler) error` loads, handles, and saves in one call.
- Handlers call domain methods on the aggregate that internally record events via `(*AggregateBase).Record` ([ADR-0004](0004-aggregate-model.md)).
- A dynamic `CommandBus` (interface-based dispatch for transports like gRPC or HTTP) is **deferred** to a future subpackage. It will sit on top of the typed `Handler` rather than replacing it.

## Consequences

- Type safety at the call site: the compiler verifies command/aggregate pairing.
- No interface dispatch on the command-handling hot path.
- Transports that route untyped commands (gRPC, HTTP) will live above the core via a future bus subpackage.
- v0 surface stays small.

## Alternatives considered

- **Command interface with `AggregateID()` method as the core primitive.** Deferred to the future bus subpackage: useful for dynamic routing, but pays interface dispatch for in-process call sites that don't need it.
- **No command modeling at all in v0.** Rejected: the typed handler primitive is small and pays for itself by giving `Repository.Execute` a clear shape.
