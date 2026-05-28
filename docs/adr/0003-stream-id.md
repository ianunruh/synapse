# ADR-0003: Stream identity — `type StreamID string`, domain IDs in user code

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0004 Aggregate model](0004-aggregate-model.md)

## Context

Event stores key on a stream identifier, and aggregates own one. The question was whether the library forces a concrete identifier type, parameterizes everything on a generic ID, or punts the choice to users — and how to balance type safety, API ergonomics, hot-path performance, and storage simplicity.

## Decision

- The core defines `type StreamID string`. All storage, transport, indices, and admin tooling traffic in this single type.
- Aggregates expose a `StreamID() StreamID` method. The core does not carry a generic `[ID]` type parameter through its API surface.
- Domain-ID typing is a user concern. Users define their own newtypes (e.g. `type OrderID string`) and convert at the aggregate boundary; conversion is a zero-cost cast because both are string-shaped.

## Consequences

- Every signature in the library is free of `[ID comparable]` noise. Repositories, command handlers, and stores stay readable.
- Stores serialize identifiers trivially (a string column or key) without an `IDCodec` constraint.
- Admin tooling and replication see a single concrete identifier type; they don't need to know aggregate-specific ID types.
- Users lose compile-time pairing between aggregate types and identifier types in *library* signatures. They retain it in their own domain code by defining newtypes and only converting at the boundary.

## Alternatives considered

- **Generic `[ID comparable]` parameter on all interfaces.** Rejected: the type parameter is infectious — it appears on `EventStore`, `Repository`, `Handler`, and admin tooling — and forces an additional `IDCodec[ID]` constraint or interface to keep stores serialization-aware.
- **`[]byte` identifier.** Rejected: mutable, ugly in logs, awkward as a map key.
- **Tagged struct `{Type, Key string}`.** Rejected: heavier identifier; admin tools can derive the same separation from a naming convention without the extra field.
- **Interface `ID` with `String()` / `Bytes()`.** Rejected: every ID use becomes an interface value (two-word header, possible boxing) plus a dispatched call.
