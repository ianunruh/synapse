# ADR-0010: Error model — sentinel errors wrapped by typed errors

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0002 Package layout](0002-package-layout.md)

## Context

Callers need both classification ("is this a concurrency conflict?") and detail ("what was the expected vs actual revision?"). We had to pick an error pattern that gives both without forcing callers into one or the other.

## Decision

- Each error class has a sentinel `Err*` (e.g. `ErrConflict`, `ErrStreamNotFound`, `ErrCodecNotFound`, `ErrPayloadType`) and a typed `*Error` carrying detail (e.g. `ConflictError{Stream, Expected, Actual}`).
- The typed error implements `Unwrap() error` returning its sentinel, so `errors.Is(err, ErrConflict)` and `errors.As(err, &ce)` both work.
- Error message strings use the `"synapse: ..."` brand prefix (see [ADR-0002](0002-package-layout.md)) rather than `"es: ..."`.

## Consequences

- Callers choose classification or detail (or both) without ceremony.
- Sentinels make package-style `errors.Is` checks idiomatic; typed values make detail extraction safe and explicit.
- Doubling the names (`ErrConflict` plus `ConflictError`) is mild API surface but follows established Go conventions (e.g. `fs.ErrNotExist` plus `fs.PathError`).

## Alternatives considered

- **Sentinels only.** Rejected: callers can't inspect what conflicted without parsing the message.
- **Typed errors only.** Rejected: breaks the common `errors.Is(err, pkg.ErrConflict)` idiom that Go users reach for.
