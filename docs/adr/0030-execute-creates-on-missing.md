# ADR-0030: Execute treats missing stream as fresh aggregate

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0009 Command handler](0009-command-handler.md) (refines its `Execute` semantics), [ADR-0028 CommandBus](0028-command-bus.md) (this is what unblocks create-by-command through the bus)

## Context

ADR-0009 designed `Execute[C, A]` as a load-handle-save convenience. It calls `Repository.Load`, runs the handler against the loaded aggregate, then saves. Until now, `Load` returned `*StreamNotFoundError` when neither snapshot nor events existed, and `Execute` propagated that error unchanged — so the **only** way to create a new aggregate was outside `Execute`:

```go
c := NewCounter(stream)
if err := c.Create("name"); err != nil { ... }
if err := repo.Save(ctx, c); err != nil { ... }
```

Existing examples did exactly this. The CommandBus (ADR-0028) made the gap acute: a transport hits `bus.Dispatch`, which always goes through `es.Execute`, so a "create" command could never succeed through the bus — the very first hit on a new stream returned `*StreamNotFoundError`. The HTTP demo exposed this immediately.

## Decision

`Execute` now treats `*StreamNotFoundError` from `Load` as **"start fresh"** rather than as a terminal error. When `Load` errors with `*StreamNotFoundError`, `Execute` constructs a fresh aggregate via the Repository's `newFn` (the same constructor `Load` itself uses internally) and runs the handler against it.

`Repository.Save`'s expected-revision logic already does the right thing for this path: the first pending event has `Version == 1`, so `versionBefore == 0`, so `expected == NoStream` — which `Append` honors as the create case.

Other Load errors (codec missing, snapshot decode failure, store I/O failure, …) still propagate unchanged.

```go
agg, err := r.Load(ctx, stream)
if err != nil {
    if !errors.Is(err, ErrStreamNotFound) { return err }
    agg = r.newFn(stream)
}
```

Handlers that require an existing aggregate guard explicitly:

```go
func IncrementHandler(ctx, cmd IncrementCmd, c *Counter) error {
    if c.Version() == 0 { return ErrNotCreated }
    return c.Increment(cmd.By)
}
```

`Repository.Load` is unchanged: it still returns `*StreamNotFoundError` for read paths, where "not found" is genuinely meaningful information. Only `Execute`'s pre-handler step changes.

## Consequences

- A command going through `Execute` (or `commandbus.Dispatch`, which calls `Execute`) can now create an aggregate on first dispatch, matching how DDD/CQRS frameworks conventionally model "command on aggregate."
- The existing `NewCounter(stream) + repo.Save` pattern still works — it's just no longer the only path. Examples can choose either style.
- Handlers gain a small responsibility: if they require an existing aggregate, they must guard on `agg.Version() == 0` (or equivalent state). The Counter / Order / Wallet examples all express this naturally; the demo's `Increment` handler does it in one line.
- One pre-existing test (`TestExecute_StreamNotFound_Propagates`) asserted the old behavior; it has been replaced with `TestExecute_OnMissingStream_CreatesFreshAggregate`, which exercises the new path end-to-end. No other tests, examples, or backends depend on the old behavior.
- Repository middleware is unaffected: it still wraps the same load-or-fresh + handler + save pipeline, just with a slightly wider "load" step.

## Alternatives considered

- **Keep `Execute` strict and add a separate `ExecuteCreate[C, A]` variant.** Rejected: API duplication — the bus would also need a `RegisterCreate` mirror, and users would have to know up front which commands are "creates." Most commands neither know nor care; modeling that as a static property is friction. The handler is the right place to express "must exist" preconditions.
- **Have the CommandBus do the StreamNotFound recovery itself**, by exposing `Repository.newFn` and reconstructing the load-handle-save loop. Rejected: this duplicates `Execute`'s body in the bus and means the bus's behavior diverges from direct `Execute` calls — exactly the divergence ADR-0028 set out to avoid by sitting *on top of* `Execute`.
- **Make `Load` itself return a fresh aggregate on missing streams** rather than `*StreamNotFoundError`. Rejected: read paths legitimately want to distinguish "doesn't exist" from "exists with no events," and downgrading `Load`'s signal would force every read caller to re-derive the distinction by checking `Version() == 0`. Keeping `Load`'s contract and refining `Execute`'s use of it is the smaller, more targeted change.
- **Add a per-Execute option** (e.g. `ExecuteOption(AllowMissing)`). Rejected: configuration for behavior the handler already controls — over-engineered.
