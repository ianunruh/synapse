# ADR-0012: Command execution middleware

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0009 Command handler](0009-command-handler.md)

## Context

Cross-cutting concerns naturally wrap the load-handle-save pipeline that [Execute](0009-command-handler.md) drives: per-aggregate locking to serialize in-process command execution, retry on transient errors (most notably `*ConflictError`), and future additions like timeouts, structured logging, distributed tracing, and metrics. We needed a primitive that lets these concerns compose around `Execute` without each user inventing their own ad-hoc wrapper, and without infecting the entire API with generic type parameters.

The natural unit to wrap is the *operation* — one full Load → Handle → Save against a given stream — not the bare `Handler` (which doesn't include Load/Save) nor the concrete `Execute[C, A]` (which is generic and would force middleware to be generic too).

## Decision

- `Operation func(ctx context.Context, stream StreamID) error` is the type-erased form of one `Execute` call. It captures the load-handle-save pipeline at the granularity of "run this against this stream."
- `Middleware func(next Operation) Operation` wraps an Operation to add behavior before, after, or around it.
- `WithMiddleware(mws ...Middleware) RepositoryOption` registers middleware on a Repository. Successive calls append. Middleware compose left-to-right: `WithMiddleware(a, b, c)` means `a` wraps `b` wraps `c` wraps the underlying pipeline.
- `Execute` builds the Operation closure that loads, calls the handler, and saves, then applies the Repository's middleware chain around it.
- Concrete middlewares live in the sibling subpackage `github.com/ianunruh/synapse/es/middleware`. v0 ships `PerAggregateLocking()` and `Retry(RetryConfig)` plus the `IsTransient` predicate that classifies `*ConflictError` as retryable by default.
- The core types (`Operation`, `Middleware`, `chain`, `WithMiddleware`) live in `es` because the Repository's contract depends on them. Concrete middlewares are optional and import `es` to satisfy the `es.Middleware` interface.

## Consequences

- Middleware sees only `(ctx, StreamID)` — not the command or the aggregate. That is the cost of type erasure. It is sufficient for locking, retry, timeout, logging, tracing, and metrics; concerns that genuinely need the typed command should be expressed inside the handler.
- One middleware chain serves all command types on a Repository. No per-command-type configuration is needed.
- The `es` package stays focused on core types and `Execute`. Built-in middlewares (and future logging/tracing/metrics middlewares) live in `es/middleware`, mirroring the sibling-subpackage pattern used for `codec/json`, `eventstore/memory`, and `idgen` ([ADR-0002](0002-package-layout.md)).
- Locking eliminates same-process contention; retry covers conflicts from other processes or instances. They compose naturally when both are needed (`WithMiddleware(Retry(...), PerAggregateLocking())` to wait through a lock before retrying).
- Default `Retry.Retryable = IsTransient` treats only `*ConflictError` as retryable. Other transient classes (network blips on remote stores, deadlocks on SQL backends) require backend-specific predicates that backends should expose.

## Alternatives considered

- **Typed middleware `Middleware[C, A any]`.** Rejected: would force every signature wrapping commands to carry both type parameters, leaking generics through transports and admin layers, and would prevent a single middleware chain from serving multiple command types.
- **Middleware on `Handler` (wrap the handler, not the pipeline).** Rejected: wraps too narrowly — would not surround Load and Save, so locking and retry-around-conflict would not work.
- **Bake locking and retry into Execute directly with options.** Rejected: each new concern (timeout, logging, tracing) would grow `RepositoryOption` indefinitely. Middleware is the open-ended extension point.
- **Ship concrete middlewares in `es` directly.** Rejected for the same reason `codec/json` and `eventstore/memory` are siblings: the core package stays focused, optional concerns opt in via separate imports, and future middlewares (logging, tracing, metrics) have an obvious home.
