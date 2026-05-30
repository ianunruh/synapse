# ADR-0029: CommandBus middleware + built-ins

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0028 CommandBus](0028-command-bus.md) (revises its "no bus middleware in v0" decision), [ADR-0012 Command middleware](0012-command-middleware.md)

## Context

ADR-0028 shipped the `CommandBus` and explicitly deferred bus-level middleware to keep v0 minimal, noting that all command middleware was the Repository's concern (ADR-0012). That left a real gap: per-aggregate locking and retry wrap the *aggregate operation* (`func(ctx, StreamID) error`), but they don't see the *dispatch boundary* — the transport-facing concerns like request logging, panic recovery, and per-dispatch deadlines have nowhere clean to live.

A transport-facing demo would have had to either inline these concerns in the HTTP handler (every consumer reinvents them) or wrap each `Dispatch` call by hand. Both are exactly the kind of friction ADR-0012 eliminated for repository-level concerns.

## Decision

Add bus-level middleware mirroring ADR-0012's shape exactly. Bus middleware wraps `Dispatch (ctx, name, []byte) → error`; repo middleware wraps `Operation (ctx, StreamID) → error`. **Different shapes, different boundaries** — bus middleware sits outside lookup/decode/Execute, repo middleware sits inside Execute, and they compose without ever double-wrapping each other:

```
bus.Dispatch
  └── bus middleware chain (Logging, Recover, Timeout, …)
        └── lookup name + decode payload
              └── es.Execute
                    └── repo middleware chain (Retry, PerAggregateLocking, …)
                          └── Load → Handler → Save
```

### API

```go
package commandbus

type Operation func(ctx context.Context, name string, payload []byte) error
type Middleware func(next Operation) Operation

func WithMiddleware(mws ...Middleware) Option
```

Composition follows ADR-0012: left-to-right outer-to-inner. `WithMiddleware(A, B, C)` means `A` wraps `B` wraps `C` wraps the core lookup+decode+Execute.

`New` pre-builds the chain once around a small core closure that does the lookup; `Dispatch` becomes `b.chain(ctx, name, payload)`. There is no per-Dispatch chain construction.

### Built-ins (in `es/commandbus/middleware.go`, same package)

| | Behavior |
|---|---|
| `Logging(logger *slog.Logger)` | Records each dispatch with `command` (name) and `duration` attrs. Successful dispatches log at `slog.LevelDebug` (quiet by default — chatty audit goes through a user-configured Debug handler); failed dispatches log at `slog.LevelWarn` with an `err` attr. Nil logger falls back to `slog.Default()`. |
| `Recover()` | `recover()`s panics inside handlers (or any deeper layer), returning `*PanicError{Name, Value, Stack}` which unwraps to a new sentinel `ErrPanic`. Essential for transport-facing dispatch where a single bad handler must not bring down the process. |
| `Timeout(d time.Duration)` | Wraps `ctx` with `context.WithTimeout(ctx, d)` before calling `next`. Useful when the transport doesn't impose its own deadline. |

Built-ins live in the same `commandbus` package rather than a sibling subpackage (the ADR-0012 split exists because `es` defines the middleware shape and `es/middleware` ships concrete ones; for `commandbus` both the shape *and* the v0 built-ins are small enough to share a package without bloating the surface).

### What's deliberately not built in

Metrics, tracing, authorization, idempotency, and rate limiting are policy-dependent — they bind to specific libraries (Prometheus, OTel) or stateful stores (a seen-key cache). The `Middleware` type lets users write them in five lines; we don't ship them.

## Consequences

- The transport-boundary concerns (logging, panic-recovery, deadlines) have a single canonical home that composes with everything ADR-0012 already gave the Repository. The HTTP demo (next step) can wire `commandbus.New(WithMiddleware(Logging(log), Recover(), Timeout(5*time.Second)))` and be done.
- `Bus` now stores a pre-built `Operation` chain alongside its entries map; one extra field, one closure built per `New` call.
- Repository middleware semantics are completely unchanged. The bus's chain runs strictly outside, so `PerAggregateLocking` still wraps load-handle-save (where it correctly serializes per stream), and `Recover` at the bus catches anything that escapes Execute (including the very rare panic inside `Load`/`Save` itself, not just handler panics).
- The `ErrPanic` sentinel and `*PanicError` typed wrapper extend the bus's error surface in the same brand-prefixed style as `ErrUnknownCommand` and `ErrDecode` — transports can `errors.Is(err, commandbus.ErrPanic)` to alert on / page for handler crashes specifically.

## Alternatives considered

- **Ship middleware in a sibling subpackage** (`es/commandbus/middleware`, mirroring `es/middleware`). Rejected for v0: with three small built-ins and zero third-party deps, the split would add an import without buying isolation. The ADR-0012 split exists because `es` itself is dep-free and concrete middleware concerns belong out of the core; the bus is already that "concrete concern" sibling. Revisit if the bus's built-in middleware grows or pulls in a non-stdlib dep.
- **Build the chain per `Dispatch` call** instead of once in `New`. Rejected: registration and middleware configuration are startup-time concerns; rebuilding the chain on every dispatch is pure waste.
- **A mutable `bus.Use(mw)` method.** Rejected: a `WithMiddleware` option at `New` matches the codebase pattern (ADR-0016) and keeps the bus's behavior immutable after construction — easier to reason about under concurrent dispatch.
- **Ship metrics/tracing/authz built-ins.** Rejected for v0: each is policy- or library-dependent, and the `Middleware` type makes them straightforward to author. The toolkit stays opinion-free at the boundary.
- **Recover as a default (always-on) behavior.** Rejected: users embedding the bus in their own panic-recovery layer (or who want panics to propagate during development) should opt in. The middleware list is explicit.
- **`Timeout` per-command rather than global.** Rejected for v0: a per-command timeout map would require an additional registration argument or option, and command-specific SLAs vary enough that a user-written middleware (switch on `name`) handles it without adding API surface.
