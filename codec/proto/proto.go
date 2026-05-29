// Package proto provides an [es.TypedCodec] backed by Google's protobuf
// runtime (google.golang.org/protobuf). Event payloads are generated
// protobuf messages.
//
// Typical usage:
//
//	import (
//	    "github.com/ianunruh/synapse/es"
//	    protocodec "github.com/ianunruh/synapse/codec/proto"
//	)
//
//	reg := es.NewRegistry()
//	es.Register(reg, "order.placed",  protocodec.For[*orderpb.Placed]())
//	es.Register(reg, "order.shipped", protocodec.For[*orderpb.Shipped]())
//
// The type parameter is the pointer message type (for example
// *orderpb.Placed) — the form generated code makes implement
// [proto.Message]. Because the core registry is serialization-agnostic
// (ADR-0007), a single registry may mix this codec with codec/json on a
// per-event-type basis.
//
// This package lives in its own module so the dependency on
// google.golang.org/protobuf stays out of the dependency-free core.
package proto

import (
	"github.com/ianunruh/synapse/es"
	"google.golang.org/protobuf/proto"
)

// ContentType is the IANA media type emitted by codecs from this
// package.
const ContentType es.ContentType = "application/vnd.google.protobuf"

// For returns an [es.TypedCodec] that serializes values of the protobuf
// message type E through google.golang.org/protobuf. E must be a pointer
// message type — the form generated code makes implement [proto.Message]
// — for example *orderpb.Placed. Register it with [es.Register]:
//
//	es.Register(reg, "order.placed", protocodec.For[*orderpb.Placed]())
//
// The returned codec is stateless and safe for concurrent use; the zero
// value carries no fields, so calling For is effectively free.
func For[E proto.Message]() es.TypedCodec[E] {
	return codec[E]{}
}

// codec is the zero-size adapter returned by [For].
type codec[E proto.Message] struct{}

// ContentType implements [es.TypedCodec].
func (codec[E]) ContentType() es.ContentType { return ContentType }

// Marshal implements [es.TypedCodec].
func (codec[E]) Marshal(e E) ([]byte, error) { return proto.Marshal(e) }

// Unmarshal implements [es.TypedCodec]. It allocates a fresh message of
// E's concrete type via the protobuf reflection prototype — which is
// valid even though the zero value of a pointer message type is nil —
// and decodes into it.
func (codec[E]) Unmarshal(data []byte) (E, error) {
	var zero E
	msg := zero.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(data, msg); err != nil {
		return zero, err
	}
	return msg.(E), nil
}
