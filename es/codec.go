package es

import (
	"fmt"
	"reflect"
	"sync"
)

// EventCodec is the per-event-type serialization interface. The
// [Registry] holds one EventCodec per registered event Type; concrete
// implementations (encoding/json, protobuf, etc.) live in subpackages
// so the root package stays free of third-party dependencies.
//
// Implementations should be safe for concurrent use after construction.
type EventCodec interface {
	// ContentType returns the wire format produced by Marshal — for
	// example "application/json" or "application/vnd.google.protobuf".
	// The [Repository] copies this onto the [RawEnvelope] before
	// handing it to the [EventStore].
	ContentType() ContentType

	// Marshal serializes a payload to bytes. Implementations should
	// reject payloads whose dynamic type does not match what the
	// codec was registered for, returning [*PayloadTypeError].
	Marshal(payload any) ([]byte, error)

	// Unmarshal decodes a payload from bytes into a freshly allocated
	// value of the codec's registered type.
	Unmarshal(data []byte) (any, error)
}

// TypedCodec[E] is the strongly typed counterpart to [EventCodec].
// Codec subpackages typically expose constructors that return
// TypedCodec[E] so users can write:
//
//	es.Register(reg, "order.placed", json.For[OrderPlaced]())
//
// [Register] adapts a TypedCodec[E] into an [EventCodec] without
// reflection on the hot path.
type TypedCodec[E any] interface {
	ContentType() ContentType
	Marshal(E) ([]byte, error)
	Unmarshal([]byte) (E, error)
}

// Registry maps event Type strings to [EventCodec] implementations
// and, optionally, to [Upcaster] functions for schema evolution. A
// single Registry may mix codecs that use different wire formats —
// for instance, JSON for legacy events and protobuf for new ones —
// and upcasters that turn old event versions into newer ones.
//
// A Registry is safe for concurrent use. Registration is typically
// done at startup; Lookup and Upcast are called on every load.
type Registry struct {
	mu        sync.RWMutex
	codecs    map[string]EventCodec
	upcasters map[string]Upcaster
}

// NewRegistry returns an empty [Registry].
func NewRegistry() *Registry {
	return &Registry{
		codecs:    make(map[string]EventCodec),
		upcasters: make(map[string]Upcaster),
	}
}

// Register associates an [EventCodec] with an event Type. Registering
// the same Type twice replaces the previous entry.
//
// Most callers use the generic top-level [Register] function instead,
// which adapts a [TypedCodec] of the concrete event type.
func (r *Registry) Register(eventType string, c EventCodec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codecs[eventType] = c
}

// Lookup returns the [EventCodec] registered for eventType. The
// boolean is false when no codec has been registered for that type.
func (r *Registry) Lookup(eventType string) (EventCodec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.codecs[eventType]
	return c, ok
}

// Types returns the event types currently registered, in unspecified
// order. The returned slice is a fresh copy and may be retained.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.codecs))
	for t := range r.codecs {
		out = append(out, t)
	}
	return out
}

// Register adapts a strongly typed [TypedCodec] into an [EventCodec]
// and stores it in r under eventType. It is the preferred entry point
// when registering codecs from user code:
//
//	es.Register(reg, "order.placed", json.For[OrderPlaced]())
func Register[E any](r *Registry, eventType string, c TypedCodec[E]) {
	r.Register(eventType, typedAdapter[E]{eventType: eventType, inner: c})
}

// typedAdapter erases the payload type of a [TypedCodec] so it can be
// stored as an [EventCodec]. The single type assertion in Marshal is
// the only reflection-adjacent operation on the hot path. eventType is
// captured so [*PayloadTypeError] can report which registration the
// mismatch came from.
type typedAdapter[E any] struct {
	eventType string
	inner     TypedCodec[E]
}

func (a typedAdapter[E]) ContentType() ContentType {
	return a.inner.ContentType()
}

func (a typedAdapter[E]) Marshal(payload any) ([]byte, error) {
	e, ok := payload.(E)
	if !ok {
		return nil, &PayloadTypeError{
			EventType: a.eventType,
			Expected:  reflect.TypeFor[E]().String(),
			Got:       fmt.Sprintf("%T", payload),
		}
	}
	return a.inner.Marshal(e)
}

func (a typedAdapter[E]) Unmarshal(data []byte) (any, error) {
	return a.inner.Unmarshal(data)
}
