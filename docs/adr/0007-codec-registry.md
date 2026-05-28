# ADR-0007: Codec registry — per-type, heterogeneous, with typed adapter

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0005 Event envelope](0005-event-envelope.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md)

## Context

The core must stay serialization-agnostic so users can pick JSON, protobuf, MessagePack, or anything else. Some applications need to mix formats across event types (legacy JSON events, new protobuf events). The hot path is serialize-on-save and deserialize-on-load, so the abstraction should be allocation-light and avoid reflection on every event.

## Decision

- A `Registry` maps event type names to `EventCodec` implementations. Each registered entry can use a different wire format.
- `EventCodec` is the erased interface: `ContentType()`, `Marshal(any) ([]byte, error)`, `Unmarshal([]byte) (any, error)`.
- `TypedCodec[E]` is the strongly typed counterpart: `Marshal(E)`, `Unmarshal([]byte) (E, error)`. Codec subpackages expose `For[E]() TypedCodec[E]` constructors.
- A generic top-level `Register[E](r *Registry, eventType string, c TypedCodec[E])` adapts a `TypedCodec[E]` into an `EventCodec` via a small `typedAdapter[E]` value. The single type assertion in `Marshal` is the only reflection-adjacent operation on the hot path.
- Concrete codecs (`synapse/codec/json`, future `synapse/codec/proto`) live in subpackages so the core has no third-party deps.

## Consequences

- Heterogeneous formats per registry: legitimately register JSON for one event type and protobuf for another.
- Hot path is one map lookup plus the codec's own marshal/unmarshal cost — no library-imposed reflection.
- Registration is typically done at startup; lookup uses `RWMutex` for safe concurrent access.
- Combined with the self-describing `ContentType` on the envelope ([ADR-0005](0005-event-envelope.md)), per-event-type format migration is supported.

## Alternatives considered

- **Single global codec with reflection-based dispatch.** Rejected: forces one format per registry and pays reflection cost on every event.
- **`Codec[E]` generic with one codec instance per event type, no erasure.** Rejected: the registry map becomes `map[string]any`, which is the same erasure with worse ergonomics.
- **Two-interface split (`PayloadCodec` plus `EnvelopeCodec`).** Deferred: useful for stores that want to lay events out as columns, but unnecessary surface for v0.
