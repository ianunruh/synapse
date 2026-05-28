# ADR-0005: Event envelope — `Envelope` + `RawEnvelope`, self-describing `ContentType`

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0007 Codec registry](0007-codec-registry.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md)

## Context

Events carry metadata (id, version, time, type, causation, correlation, free-form annotations) alongside a domain payload. We needed to choose where this metadata lives, whether stores see typed payloads or opaque bytes, and whether events should be self-describing about their wire format.

## Decision

Two envelope types:

- **`Envelope`** is application-facing. `Payload` is `any` (the user's domain event value). This is what aggregates `Apply` and what command handlers receive.
- **`RawEnvelope`** is storage-facing. `Payload` is `[]byte`. This is what `EventStore` implementations append and load. Stores never know about codecs or domain types.

Both envelopes carry:

- `Type` — logical event name, e.g. `"order.placed"`. The key for codec registry lookup.
- `ContentType` — IANA media type, e.g. `"application/json"`. Populated by `Repository.Save` from the codec chosen for the event type.
- `Causation`, `Correlation` — string identifiers for cross-event and cross-stream tracing.
- `Metadata` — `map[string]string` for free-form annotations.

## Consequences

- The store is a pure byte-mover with no codec dependency. Backends can be implemented and tested without knowing anything about the domain.
- Events are self-describing: admin tooling, replication, and future maintainers can identify an event's wire format without consulting the current registry configuration.
- Per-event format migration is supported: a stream may legitimately contain mixed `ContentType` events (JSON v1 plus protobuf v2), and consumers route on `ContentType`.
- Two related-but-distinct types means slight duplication in field declarations. The benefit of the explicit boundary outweighs the cost.

## Alternatives considered

- **Single `Envelope` with `Payload any` everywhere; stores hold their own codec.** Rejected: couples backends to codec choices and complicates testing.
- **`ContentType` implicit, registry-only.** Rejected: breaks tooling that decodes events without runtime access to the configured registry, and complicates per-event format migration.
- **Embedded metadata fields directly on user event structs.** Rejected: infra concerns leak into domain types and metadata schema evolution becomes a domain-model concern.
