// Package es provides event sourcing and CQRS primitives that can be
// composed into application-specific aggregates, command handlers, and
// read models. It is the core package of the synapse toolkit.
//
// The package is intentionally serialization-agnostic: stores deal only
// in opaque bytes ([RawEnvelope]), and concrete codecs are registered
// per event type through a [Registry]. Concrete codec implementations
// (encoding/json, protobuf, etc.) live in sibling packages so es itself
// remains free of third-party dependencies.
//
// Typical usage couples three pieces:
//
//   - An [Aggregate] type (usually embedding [AggregateBase]) that owns domain
//     state and reacts to events through Apply.
//   - A [Registry] populated with [EventCodec] entries for every event
//     type.
//   - An [EventStore] implementation backed by an in-memory map, a
//     database, or a remote service.
//
// A [Repository] wires those three together so application code can
// Load and Save aggregates without thinking about serialization or
// optimistic-concurrency mechanics.
package es

// StreamID is the storage-facing identity of an event stream. It is a
// plain string so that backends, indices, logs, and admin tools can move
// it around with zero cost.
//
// Domain code is encouraged to keep typed identifiers in its own
// package (for example `type OrderID string`) and convert at the
// aggregate boundary:
//
//	type OrderID string
//	func (id OrderID) Stream() es.StreamID { return es.StreamID("order-" + string(id)) }
type StreamID string

// ContentType describes the wire format of a serialized event payload.
// Values follow the IANA media type convention, e.g. "application/json"
// or "application/vnd.google.protobuf".
//
// An [EventCodec] reports the ContentType it produces, and a
// [RawEnvelope] carries it alongside the payload so that consumers can
// decode an event without consulting external registries.
type ContentType string
