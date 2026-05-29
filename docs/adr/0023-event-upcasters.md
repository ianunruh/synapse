# ADR-0023: Event upcasters for schema evolution

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0005 Event envelope](0005-event-envelope.md), [ADR-0007 Codec registry](0007-codec-registry.md), [ADR-0013 Snapshotting](0013-snapshotting.md)

## Context

Events in the log are immutable history. Once `order.placed.v1` is written, a release-trains-onward shop cannot rewrite it. But the domain evolves: fields get added, renamed, restructured. The aggregate code wants to know one canonical shape; the log holds every shape that ever existed.

The library needed a way to:

- Read an old event from the log, then hand a newer-version typed value to the aggregate's `Apply`.
- Keep the aggregate ignorant of historical versions — `Apply` switches on the latest types only.
- Work transparently in the projection runner, so projections also see upcasted payloads.
- Cover snapshots the same way: a snapshot saved as `counter.snapshot.v1` should be restorable as `counter.snapshot.v2`.

## Decision

Upcasting is typed and lives on the same `Registry` as codecs. The registration is generic; the runtime stored form is type-erased:

```go
es.RegisterUpcaster[OrderPlacedV1, OrderPlacedV2](reg,
    "order.placed.v1", "order.placed.v2",
    func(in OrderPlacedV1) (OrderPlacedV2, error) {
        return OrderPlacedV2{Total: in.Amount, Currency: "USD"}, nil
    })
```

`Registry.Upcast(payload, type)` walks the chain — looking up an upcaster by the current type, running it, repeating until no upcaster matches. Visited types are tracked; revisiting one returns `*UpcasterCycleError`. A safety cap (`upcastMaxHops = 32`) catches degenerate chains.

The chain runs in two places:

- **`Repository.Load`** — once on each event after codec `Unmarshal` and once on the snapshot (if any) after its codec decode. The `Envelope` handed to `Apply` carries the final upcasted `Type` and `Payload`; the snapshot value handed to `Restore` is similarly final.
- **`projection.Runner.decode`** — same point in the runner's event path, so projections see upcasted shapes uniformly.

Aggregate command methods continue to write the *current* type directly (`c.Record("order.placed.v2", OrderPlacedV2{...}, c.Apply)`). The log naturally becomes mixed v1/v2/v3 over time; the read path always converges on the latest.

User upcaster functions can return errors. The library wraps them with from-type context (`"synapse: upcast %s at v%d: %w"`) so the underlying error remains accessible via `errors.Is`/`errors.As`.

Single in, single out. Fan-out (one v1 → many v2) and fan-in (many v1 → one v2) are deliberately out of scope for now.

## Consequences

- Aggregates handle the current schema only. Adding a new event version means: register a codec for the new type, register an upcaster from the previous type, and update `Apply` to recognize the new typed payload.
- Snapshots evolve through the same mechanism without a parallel API surface. A user who already has snapshot codecs registered drops in an upcaster the same way they would for events.
- `Envelope.Type` reflects the final upcasted type, not the on-disk type. Projections that route on `Type` strings see the latest names; tooling that needs the on-disk type can read it from the store directly.
- The cycle check is per-call (visited set scoped to one `Upcast` invocation), so independent calls cannot poison each other.
- Backward compatibility: registries with no upcasters behave exactly as before. Existing examples and tests remain unchanged.
- Per-event allocation: one type assertion + at most a few hops of map lookups + the user's transform. No reflection on the hot path.

## Alternatives considered

- **Bytes-level upcasters** (`func([]byte) []byte` rewriting JSON before decode). Rejected: codec-agnostic concerns leak into a codec-specific representation. Type-level upcasting works for protobuf, msgpack, JSON, or any future codec without per-codec plumbing.
- **Per-aggregate version switch in `Apply`** (handle every historical type directly). Rejected: aggregate code accumulates dead schemas, and the version-handling logic is duplicated across every aggregate that touches the same event family.
- **Separate snapshot upcaster API**. Rejected: snapshots already share the codec registry, and the upcaster contract — `In type → Out type` — is identical. One API, one mental model.
- **Always-on cycle protection vs. opt-in**. Rejected the opt-in path: cycles are bugs, and a 32-hop cap plus visited-set is essentially free.
- **Fan-out / fan-in upcasters**. Out of scope for v0: rare in practice, complicate the contract substantially, and can be modeled outside the registry by application code that issues additional events.
