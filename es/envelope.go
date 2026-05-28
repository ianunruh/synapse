package es

import "time"

// Metadata is free-form key/value context attached to an event:
// correlation identifiers, actor information, trace IDs, and so on.
//
// Values are strings by convention so they can be serialized by every
// codec without negotiation. Richer metadata that needs structured
// types belongs in the event payload itself.
type Metadata map[string]string

// Envelope is the application-facing event record. The Payload field
// holds the user's domain event value; a [Repository] serializes it
// through an [EventCodec] before passing it to an [EventStore].
//
// Identity, time, and content-type fields are stamped by the
// [Repository] at save time, so domain code never needs a clock or an
// ID generator.
type Envelope struct {
	// EventID uniquely identifies this event. The default
	// generator emits UUIDv7 values so IDs sort chronologically.
	EventID string

	// StreamID is the stream this event belongs to.
	StreamID StreamID

	// Version is the 1-based position of this event within its stream.
	Version uint64

	// GlobalPosition is the 1-based position of this event in the
	// store's global append order, across all streams. It is assigned
	// by the [EventStore] on Append and surfaces through Load and
	// [SubscribableEventStore.Subscribe]. The Repository and the
	// command-side code paths ignore it; it is meaningful primarily
	// to subscription consumers.
	GlobalPosition uint64

	// RecordedAt is the wall-clock time at which the event was appended.
	RecordedAt time.Time

	// Type is the logical event name used to look up an [EventCodec]
	// in the [Registry], e.g. "order.placed".
	Type string

	// ContentType is the wire format of Payload once serialized.
	// It is populated by the Repository at save time from the codec
	// chosen for this event Type.
	ContentType ContentType

	// Causation is the EventID of the event that directly caused this one.
	// Empty when the event has no upstream cause.
	Causation string

	// Correlation groups events that share a causal chain — typically
	// the EventID of the initiating command or external request.
	Correlation string

	// Metadata holds free-form annotations.
	Metadata Metadata

	// Payload is the typed domain event value.
	Payload any
}

// RawEnvelope is the storage-facing form of an event. Payload is opaque
// bytes; [EventStore] implementations never need to know about codecs
// or domain types.
//
// The field set mirrors [Envelope] so backends can persist a single
// flat row without consulting a schema.
type RawEnvelope struct {
	EventID        string
	StreamID       StreamID
	Version        uint64
	GlobalPosition uint64
	RecordedAt     time.Time
	Type           string
	ContentType    ContentType
	Causation      string
	Correlation    string
	Metadata       Metadata
	Payload        []byte
}
