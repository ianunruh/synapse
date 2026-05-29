# ADR-0026: Protobuf codec + shared codec contract suite

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0007 Codec registry](0007-codec-registry.md), [ADR-0018 Backend contract tests](0018-backend-contract-tests.md), [ADR-0005 Event envelope](0005-event-envelope.md)

## Context

ADR-0007 designed the codec registry around a per-type `EventCodec` with a `TypedCodec[E]` adapter and named `synapse/codec/proto` as an anticipated second codec. Until now only `codec/json` existed, so the codec abstraction had never been exercised by a second wire format — the same gap that motivated a second event-store backend (ADR-0024). A single implementation can accidentally encode assumptions of its format into the interface; a second one is what proves the abstraction holds.

`codec/json` also carried a full hand-written test file. Its tests split into two kinds: format-agnostic behavior every codec must satisfy (round-trip fidelity, stable content type, registry integration, type-mismatch rejection) and JSON-specific behavior (struct tags, `omitzero`, custom `Marshaler` hooks, byte-for-byte equivalence). Adding a second codec would have duplicated the first kind — exactly the problem ADR-0018 solved for stores.

## Decision

Two pieces:

- **`codec/codectest`** — a contract suite in the root module, the codec analogue of `eventstore/eventstoretest` (ADR-0018). It exports `RunContract[E](t, newCodec, eventType, sample, equal)`. A codec package's generic `For[E]` constructor satisfies the `Factory[E] = func() es.TypedCodec[E]` parameter directly. The caller supplies a representative `sample` and an `equal` func (E is often not comparable with `==`, and proto messages must be compared with `proto.Equal`). The suite asserts only the cross-format contract; format-specific tests stay in each codec package.

- **`codec/proto`** — an `es.TypedCodec` over `google.golang.org/protobuf`. `For[E proto.Message]()` constrains the type parameter to the pointer message type generated code produces (`*orderpb.Placed`). `Marshal` is `proto.Marshal`; `Unmarshal` allocates a fresh message with `zero.ProtoReflect().New().Interface()` (valid on the nil zero value of a pointer message) and decodes into it. Content type is `application/vnd.google.protobuf`, matching the example in the core `EventCodec` doc.

`codec/proto` is a sibling module with its own `go.mod` so the protobuf dependency stays out of the dependency-free core, per ADR-0002/ADR-0007. `codec/json` stays in the root module because it needs only the standard library.

## Consequences

- The registry abstraction is now proven against two wire formats. A single `es.Registry` can legitimately hold JSON for one event type and protobuf for another (ADR-0007's heterogeneous-format promise), which the contract exercises through `es.Register`/`Lookup`.
- `codec/json`'s test file shed its shared tests (round-trip, registry integration, type mismatch, cross-instance) in favor of one `RunContract` call, keeping only JSON-specific tests — mirroring how the store backends shrank under ADR-0018.
- Adding a future codec (MessagePack, CBOR, …) is now mechanical: implement `For[E]`, call `RunContract`, add format-specific tests.
- Protobuf payload types must be the pointer message type; this is the natural form from generated code and is documented on `For`.

## Alternatives considered

- **Generate a `.proto` and run `protoc` in tests.** Rejected: pulls a codegen toolchain into the build for no extra coverage. The protobuf runtime's well-known types (`wrapperspb`, `structpb`) are real `proto.Message`s and exercise the codec hermetically.
- **A non-generic codec contract that takes an `EventCodec`.** Rejected: it would lose the typed round-trip assertion and force every check through `any`. The generic `RunContract[E]` tests the typed surface and the erased `EventCodec` (via the registry) in one place.
- **Make `equal` optional, defaulting to `reflect.DeepEqual`.** Rejected: `reflect.DeepEqual` is wrong for proto messages (internal state fields). Requiring the caller to pass equality keeps the suite correct for every format.
- **Put the proto codec in the root module.** Rejected: it needs a third-party dependency, which ADR-0002 and the project constraints keep out of the core. Sibling module it is.
