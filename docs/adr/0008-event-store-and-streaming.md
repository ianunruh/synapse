# ADR-0008: Event store and streaming reads

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0005 Event envelope](0005-event-envelope.md), [ADR-0006 Optimistic concurrency](0006-optimistic-concurrency.md), [ADR-0007 Codec registry](0007-codec-registry.md)

## Context

The event store is the persistence boundary. We had to decide its signature, whether it deals in typed payloads or bytes, what append granularity it supports, and how it returns events to the reader.

## Decision

The codec lives **above** the store. `EventStore` only sees `RawEnvelope` with `[]byte` payloads ([ADR-0005](0005-event-envelope.md)). Repositories serialize via the `Registry` ([ADR-0007](0007-codec-registry.md)) before calling `Append` and deserialize after `Load`.

**Append signature:**

```go
Append(ctx, stream StreamID, expected Revision, events ...RawEnvelope) (Revision, error)
```

Variadic batch into a single stream. Atomic per call. Returns the new head revision on success or `*ConflictError` on optimistic-concurrency violation ([ADR-0006](0006-optimistic-concurrency.md)).

**Load signature:**

```go
Load(ctx, stream StreamID, opts ReadOptions) iter.Seq2[RawEnvelope, error]
```

Returns events in ascending version order as a Go 1.23+ iterator. The iterator yields at most one terminal `(zero, err)` and stops. `ReadOptions{From, Limit}` zero-value asks for the whole stream from the start.

**Cross-stream atomic writes are out of scope.** Sagas and process managers compose via an outbox or similar pattern at the application layer.

## Consequences

- Backends stay simple: append a slice of byte-payloads under a version check; read them back in order.
- `iter.Seq2` avoids the goroutine/channel overhead that a streaming `<-chan` would impose and gives callers `break`/`continue`/`return` cleanly.
- A multi-event command is naturally atomic per stream — which matches the standard "an aggregate's command emits N events" invariant.
- Multi-stream transactions are pushed up to application-level coordination, which keeps the `EventStore` interface implementable by a wider variety of backends.

## Alternatives considered

- **Store sees `any`, holds its own codec.** Rejected: couples backends to codec choices.
- **Single-event `Append`.** Rejected: breaks per-command atomicity for multi-event commands and adds round-trips on remote stores.
- **Transactional multi-stream `Append([]StreamAppend)`.** Rejected: most backends don't support it natively; raises the bar for what counts as a valid store.
- **Channel-based `Load`.** Rejected: goroutine + channel sync overhead per call; awkward error handling.
- **Callback-based `Load`.** Rejected: callers can't `break`/`continue` cleanly.
- **Slice-returning `Load`.** Rejected: forces full materialization, unbounded memory for long streams.
