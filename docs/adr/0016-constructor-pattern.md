# ADR-0016: Constructor pattern — functional options for configurable types

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md), [ADR-0015 Observability via log/slog](0015-slog-observability.md)

## Context

Two constructible types accumulated optional configuration as the library grew:

- `es.Repository` shipped with the functional options pattern: `NewRepository(store, reg, newFn, opts...)` with `WithClock`, `WithIDGenerator`, `WithMiddleware`, `WithSnapshotStore`, `WithSnapshotPolicy`, `WithLogger`.
- `projection.Runner` originally shipped as a public-field struct: `&projection.Runner{Name: …, Store: …, Live: true, …}`.

Both styles are idiomatic in Go. Repository's choice was driven by interdependent defaults (the default `IDGen` is `idgen.UUIDv7{Now: clock.NowUTC}` — it needs the *resolved* Clock). Runner's choice was driven by its simpler config surface and the precedent of stdlib types like `http.Server` and `tls.Config`.

The inconsistency was noticed during code review. Two patterns for the same kind of API is more cognitive load than the marginal per-type fit savings justify.

## Decision

Standardize on the functional options pattern for all long-lived configurable types in the library:

```go
// Required arguments are positional in NewX:
func NewX(req1 T1, req2 T2, opts ...XOption) *X

// Optional configuration is expressed as functions:
type XOption func(*xOptions)
func WithY(y Y) XOption { return func(o *xOptions) { o.y = y } }
```

`projection.Runner` is refactored from public-field struct to `NewRunner(name, store, reg, proj, opts...)` with `WithCheckpoint`, `WithLive`, `WithStream`, `WithOnError`, `WithLogger`. The `Runner` struct itself has unexported fields; users only construct via `NewRunner` and only configure via `WithX`. Nil required arguments behave like nil arguments to `NewRepository`: they propagate to a runtime panic at first use, not a construction-time error. The caller is responsible for non-nil required args.

Boolean options use `With<Name>(bool)` rather than a marker function (`Live()`), for symmetry with non-boolean options.

### Where this rule does NOT apply

Three patterns explicitly remain as they are:

1. **One-shot config blobs passed to utility functions.** `middleware.RetryConfig` stays as a public-field struct: `middleware.Retry(middleware.RetryConfig{MaxAttempts: 5})`. The middleware constructor is a function returning a `Middleware`, not a long-lived configurable type with a method set.

2. **Pure data types describing a call.** `es.SubscriptionOptions`, `es.ReadOptions`, `es.SnapshotPolicy` stay as public-field structs / function types. They describe the *shape of a call*, not a long-lived runtime object.

3. **Single-arg factories.** `jsoncodec.For[E]()`, `memory.New()`, `idgen.UUIDv7{Now: …}` stay simple. No options needed.

The rule applies when both:

- the type has interdependent or computed defaults, OR a meaningful "required vs optional" distinction worth enforcing at construction; AND
- the type is long-lived and has methods (not a one-shot value passed into a function).

## Consequences

- One pattern for users to learn. `New<Type>(required, opts...)` + `With<Field>(value)` covers Repository, Runner, and any future configurable types.
- "Required vs optional" is encoded in the function signature. Required args are positional; optional defaults are documented per-`WithX`.
- Adding a new optional field is a `WithX` function and an entry in the private options struct — no breaking change to existing constructors.
- Post-construction mutation is impossible (unexported fields), removing a class of footguns.
- Bool options are slightly verbose: `WithLive(true)` vs `Live: true`. Acceptable cost.
- Generic type parameters compose cleanly: `NewRepository[A Aggregate]` works the same as before.

## Alternatives considered

- **Public-field structs everywhere.** Rejected: works for Runner but loses Repository's interdependent-defaults story (would need lazy initialization or getter methods, adding complexity). Public fields also allow post-construction mutation that races against running operations.
- **Keep both patterns, document the rule by type.** Rejected: hand-wavy "use options for type A, struct for type B" gives no clear guidance for new types. Inconsistency stays visible to users.
- **Use marker functions for booleans** (`Live()` instead of `WithLive(bool)`). Rejected: asymmetric with non-boolean options. The marginal readability gain is not worth the inconsistency within the options namespace.
- **Validate required args in `NewRunner`/`NewRepository`.** Rejected: matches existing `NewRepository` behavior, which doesn't validate. Caller is responsible for passing non-nil required arguments; a runtime panic on first use is a clear signal of misuse.
