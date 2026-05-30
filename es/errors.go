package es

import (
	"errors"
	"fmt"
)

// Sentinel errors used for classification with [errors.Is].
//
// Typed errors elsewhere in this package wrap these sentinels via
// Unwrap, so callers can write either:
//
//	if errors.Is(err, es.ErrConflict) { ... }
//
// or
//
//	var ce *es.ConflictError
//	if errors.As(err, &ce) { /* use ce.Expected, ce.Actual */ }
var (
	// ErrConflict indicates an optimistic-concurrency violation at
	// append time. Detailed information is available on
	// [*ConflictError].
	ErrConflict = errors.New("synapse: revision conflict")

	// ErrStreamNotFound indicates a load against a stream that holds
	// no events. Detailed information is available on
	// [*StreamNotFoundError].
	ErrStreamNotFound = errors.New("synapse: stream not found")

	// ErrCodecNotFound indicates no [EventCodec] was registered for
	// an event Type encountered during marshal or unmarshal.
	ErrCodecNotFound = errors.New("synapse: codec not registered for event type")

	// ErrPayloadType indicates a codec received a payload that did
	// not match the type it was registered for.
	ErrPayloadType = errors.New("synapse: payload type mismatch")

	// ErrUpcasterType indicates a registered upcaster received a
	// payload whose dynamic type did not match the In type it was
	// registered with. Detailed information is available on
	// [*UpcasterTypeError].
	ErrUpcasterType = errors.New("synapse: upcaster payload type mismatch")

	// ErrUpcasterCycle indicates the registered upcasters form a cycle
	// or exceed the per-call hop limit. Detailed information is
	// available on [*UpcasterCycleError].
	ErrUpcasterCycle = errors.New("synapse: upcaster cycle")
)

// ConflictError reports an optimistic-concurrency violation. It
// unwraps to [ErrConflict].
type ConflictError struct {
	Stream   StreamID
	Expected Revision
	Actual   Revision
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("synapse: stream %q: expected revision %s, got %s",
		e.Stream, e.Expected, e.Actual)
}

func (*ConflictError) Unwrap() error { return ErrConflict }

// StreamNotFoundError reports a load against an empty stream. It
// unwraps to [ErrStreamNotFound].
type StreamNotFoundError struct {
	Stream StreamID
}

func (e *StreamNotFoundError) Error() string {
	return fmt.Sprintf("synapse: stream %q not found", e.Stream)
}

func (*StreamNotFoundError) Unwrap() error { return ErrStreamNotFound }

// CodecNotFoundError reports a missing [EventCodec] registration. It
// unwraps to [ErrCodecNotFound].
type CodecNotFoundError struct {
	EventType string
}

func (e *CodecNotFoundError) Error() string {
	return fmt.Sprintf("synapse: no codec registered for event type %q", e.EventType)
}

func (*CodecNotFoundError) Unwrap() error { return ErrCodecNotFound }

// PayloadTypeError reports a payload whose dynamic type did not match
// the type a codec was registered for. It unwraps to [ErrPayloadType].
type PayloadTypeError struct {
	EventType string
	Expected  string
	Got       string
}

func (e *PayloadTypeError) Error() string {
	return fmt.Sprintf("synapse: codec for %q: expected payload %s, got %s",
		e.EventType, e.Expected, e.Got)
}

func (*PayloadTypeError) Unwrap() error { return ErrPayloadType }
