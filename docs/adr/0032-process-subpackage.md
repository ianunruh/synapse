# ADR-0032: `es/process` — process managers as a thin wrapper

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0009 Command handler](0009-command-handler.md), [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md), [ADR-0022 Causation/correlation/metadata](0022-context-causation-correlation-metadata.md), [ADR-0030 Execute creates on missing](0030-execute-creates-on-missing.md)

## Context

A process manager is a stateful workflow coordinator: it consumes events from one or more streams, maintains its own aggregate state, and emits commands to drive the next step. It's the orchestration shape of a saga (where the alternative is choreography — events chain through services with no central brain).

The synapse primitives already cover everything a process manager needs:

- **Event consumption** — `SubscribableEventStore` + `projection.Runner` (`WithTypes` narrows the subscription to relevant event types).
- **Stateful PM identity** — an aggregate + `Repository`, persisted to the same event store.
- **Atomic load-handle-save** — `es.Execute`, which since ADR-0030 also creates the aggregate on first event.
- **Command emission with causation** — `es.Execute` or `commandbus.Dispatch` inside the handler, with the saga chain auto-propagated through context (ADR-0022).
- **Per-PM serialization** — `PerAggregateLocking` middleware on the PM's repo.
- **Idempotency** — projections are already required to be idempotent (ADR-0014).

What's missing is the **glue**. Every process manager re-implements the same load-decide-save dance inside its `Projection.Project` method:

```go
func (m *MyPM) Project(ctx context.Context, env es.Envelope) error {
    id := someCorrelation(env)
    pm, err := m.repo.Load(ctx, id)
    if errors.Is(err, es.ErrStreamNotFound) { pm = m.newFn(id) }
    if err := m.handle(ctx, env, pm); err != nil { return err }
    return m.repo.Save(ctx, pm)
}
```

That's mechanical, and `es.Execute` already does the load-or-fresh + handle + save flow. So a one-line wrapper turns `Project` into:

```go
return es.Execute(ctx, repo, correlate(env), env, handle)
```

with `Handler[es.Envelope, M]` as the user's per-event logic.

## Decision

Ship a small subpackage `es/process` containing one type and one constructor.

### API

```go
package process

// Correlate maps an inbound event to the process-manager stream id
// that should consume it. Returning "" skips the event (no PM is
// loaded or mutated, no checkpoint advance is forced beyond what the
// runner does normally).
type Correlate func(env es.Envelope) es.StreamID

// Manager implements es.Projection by routing each inbound event to a
// process-manager aggregate determined by a correlation function.
type Manager struct { /* unexported */ }

// New returns a Manager wired with the repository for the
// process-manager aggregate M, the correlation function, and the
// per-event handler.
func New[M es.Aggregate](
    repo *es.Repository[M],
    correlate Correlate,
    handle es.Handler[es.Envelope, M],
) *Manager

func (m *Manager) Project(ctx context.Context, env es.Envelope) error
```

`Manager` is **non-generic** even though `New` is generic over the PM aggregate type — the generic parameters live entirely inside the closure built by `New`, mirroring the proven `typedAdapter[E]` erasure pattern in `es/codec.go` and the `commandbus.entry` closure in ADR-0028. Users get type-checked construction without generic noise at the use site.

### Wiring

The user composes `Manager` with `projection.NewRunner` exactly the same way they'd compose any other `es.Projection`:

```go
pm := process.New(transferRepo, correlateByTransferID, transferStep)
runner := projection.NewRunner("transfer-process", store, reg, pm,
    projection.WithLive(true),
    projection.WithTypes("transfer.requested", "account.debited", "account.credited"),
    projection.WithCheckpoint(checkpoints),
)
go runner.Run(ctx)
```

Inside `transferStep` (an `es.Handler[es.Envelope, *Transfer]`), the user mutates the PM aggregate via its domain methods and freely calls `es.Execute` on other repositories to dispatch commands (debit, credit, …). The PM's repo middleware (locking, retry) wraps each step; the runner's checkpoint advances on success.

### What this is, and isn't

- **It is**: packaging of an existing pattern, so users don't re-write the load-or-fresh + handle + save glue per PM.
- **It is not**: a new orchestration runtime, scheduler, or distributed coordinator. There's no scheduler; deadlines/timeouts are out of scope (revisit when a real use case shows up).
- **It is not**: a compensation engine. Compensations are domain logic — the PM's handler decides to emit an `UndoX` command exactly the same way it emits any other command.

## Consequences

- One canonical place for the load-handle-save shape, named for what it does (`process.Manager`) rather than left as boilerplate in every PM's `Project`.
- The existing causation propagation from `projection.Runner` (ADR-0022) carries cleanly through `es.Execute`'s nested calls inside the handler, so events emitted by the PM are stamped with the correct saga chain without extra wiring.
- Composes with everything: `WithTypes` narrows the subscription; `WithCheckpoint` persists progress; the PM's repo middleware applies to every step; `commandbus.Bus` can be called from the handler.
- Choreography (no central PM, services chain via events) requires nothing new — it's just one or more independent projections. `process.Manager` is for the orchestration case where the workflow has its own identity and state.
- One example under `examples/process/` demonstrates a small money-transfer saga end-to-end; that's the test of the pattern.

## Alternatives considered

- **Document the pattern, ship no code.** The wiring is genuinely small — six lines of glue per PM. Rejected because the boilerplate is identical every time and gets the causation/checkpoint composition slightly wrong unless you're careful. A single tested implementation pays for itself.
- **Generic `Manager[M]`** rather than non-generic with generic `New[M]`. Rejected: the type parameter would infect every reference to the variable (`var pm *process.Manager[*Transfer]`) for no type-safety gain over the closure-erased version. Same call site reads cleaner.
- **`Decide`-style API** returning `(events []ToRecord, commands []ToDispatch)` from a pure function. Rejected for v0: it duplicates the work `es.Execute` already does and forces the user to learn a new return shape. The current shape — handler mutates the PM aggregate, freely calls `Execute` for outbound commands — is the same shape the rest of the toolkit uses for command handlers.
- **Built-in scheduler for timeouts** (`if no event in 5min, fire Timeout`). Real new capability — needs a persistent scheduler that survives restarts. Out of scope; revisit when a concrete need shows up.
- **Distributed coordination** (multiple PM Runner instances racing). The race exists today for `projection.Runner` generally (ADR-0014 explicit gap) and a process manager inherits it. Solving distributed coordination is the same problem either way; out of scope here.
