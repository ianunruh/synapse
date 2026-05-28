# ADR-0015: Observability via log/slog

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0001 Foundation](0001-foundation.md), [ADR-0013 Snapshotting](0013-snapshotting.md), [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md)

## Context

The library has two places where it deliberately swallows or downgrades errors so the caller does not have to handle them:

1. `Repository.Save` calls `trySaveSnapshot` after a successful event append. Events are committed; the snapshot is an optimization. ADR-0013 left a `// TODO: surface this error via slog once we add logging.` marker.
2. `projection.Runner` invokes `OnError` when `Projection.Project` fails; when `OnError` returns true, the event is skipped silently. Operators have no record that data may be inconsistent.

Both are reasonable to swallow, but operators need visibility. The library needs a logging primitive that doesn't conflict with the "zero third-party deps in core" constraint (ADR-0001) and that users can route however they want.

## Decision

Use `log/slog` (Go 1.21+ stdlib) as the library's logging primitive. Add a configurable `*slog.Logger` to both the `Repository` and the `projection.Runner`:

```go
// es package
func WithLogger(l *slog.Logger) RepositoryOption

// es/projection package
type Runner struct {
    // ... existing fields ...
    Logger *slog.Logger
}
```

Default is `slog.Default()` in both places. The default Go program receives the library's warnings without any configuration; users who want silence install a discard handler, and users who want structured routing wire up their own slog handler. No library-defined logger interface — `*slog.Logger` is the stdlib choice and users with custom loggers adapt via slog handlers.

### What gets logged

| Site | Level | Message | Attributes |
|---|---|---|---|
| `Repository.Save` snapshot-save failure | `Warn` | `"synapse: snapshot save failed"` | `stream`, `version`, `err` |
| `projection.Runner` OnError-skipped event | `Warn` | `"synapse: projection error, skipping event"` | `name`, `type`, `stream`, `position`, `err` |

Both use `WarnContext(ctx, …)` so request-scoped context attributes (trace IDs, request IDs) propagate through user-configured slog handlers.

### What is deliberately not logged

- Errors that are returned to the caller (Load failures, Save event-write failures, projection errors without OnError, codec-missing during Load). Double-recording would be noise.
- Successful operations. Projections process events at scale; per-event INFO/DEBUG would swamp logs.
- Per-event traces. Users who need them wrap their `Projection.Project` with their own instrumentation.

### Conventions

- Message prefix `"synapse: …"` mirrors the existing error-message convention from ADR-0002.
- Bare string attribute keys (`"stream"`, `"version"`, `"name"`, `"position"`, `"err"`). No library-defined constants for v0; promote if duplication becomes painful.
- No `slog.Group` wrapper by default. Callers who want a namespace can install a logger via `slog.Default().With(slog.Group("synapse", …))`.

## Consequences

- The snapshot-save TODO from ADR-0013 is closed: the same path now emits a Warn with full context (stream, version, err) instead of being silent.
- Operators get visibility into Runner skips without having to instrument every projection. The skip path stays best-effort (event is checkpointed past) but is auditable.
- Tests can capture log output by installing a custom `slog.Logger` wrapping a buffered handler — the pattern is straightforward and exercised in `es/logging_test.go` and `es/projection/logging_test.go`.
- The library never imports beyond stdlib for logging.
- Future log sites are easy to add — same option, same level, same key conventions.

## Alternatives considered

- **Define a library-local `Logger` interface.** Rejected: `*slog.Logger` is the stdlib choice and accepts any handler. A custom interface is one more thing for users to learn and adapt to, with no practical advantage now that `log/slog` is in stdlib.
- **Default to a silent (discard handler) logger.** Rejected: real failures would go unseen unless users explicitly opt in. Defaulting to `slog.Default()` makes warnings hard to ignore by default; users who want quiet can install a discard handler in one line.
- **Pass loggers exclusively through context.** Rejected: discoverable configuration via `WithLogger` matches the pattern set by `WithClock`/`WithIDGenerator`/`WithSnapshotStore`. Context-based logger lookup is less obvious in API docs and forces users to know the convention.
- **Log every event the Runner processes at Debug level.** Rejected for v0: noisy by default; users who want this wrap their Projection. Can revisit if a concrete need emerges.
- **Surface snapshot-save errors by returning them from Save.** Rejected in ADR-0013 and unchanged here: conflating event-commit success with snapshot-save failure forces every caller to disentangle them.
