// Package json provides an [es.TypedCodec] backed by the standard
// library's encoding/json package.
//
// Typical usage:
//
//	import (
//	    "github.com/ianunruh/synapse/es"
//	    jsoncodec "github.com/ianunruh/synapse/codec/json"
//	)
//
//	reg := es.NewRegistry()
//	es.Register(reg, "order.placed",  jsoncodec.For[OrderPlaced]())
//	es.Register(reg, "order.shipped", jsoncodec.For[OrderShipped]())
//
// Event types may freely implement [encoding/json.Marshaler] and
// [encoding/json.Unmarshaler]; encoding/json honors them. The Go 1.24
// `omitzero` struct tag works transparently.
package json

import (
	"encoding/json"

	"github.com/ianunruh/synapse/es"
)

// ContentType is the IANA media type emitted by codecs from this
// package.
const ContentType es.ContentType = "application/json"

// For returns an [es.TypedCodec] that serializes values of type E
// through encoding/json. Register it with [es.Register]:
//
//	es.Register(reg, "order.placed", jsoncodec.For[OrderPlaced]())
//
// The returned codec is stateless and safe for concurrent use; the
// zero value of the internal type carries no fields, so calling For
// is effectively free.
func For[E any]() es.TypedCodec[E] {
	return codec[E]{}
}

// codec is the zero-size adapter returned by [For].
type codec[E any] struct{}

// ContentType implements [es.TypedCodec].
func (codec[E]) ContentType() es.ContentType { return ContentType }

// Marshal implements [es.TypedCodec].
func (codec[E]) Marshal(e E) ([]byte, error) { return json.Marshal(e) }

// Unmarshal implements [es.TypedCodec].
func (codec[E]) Unmarshal(data []byte) (E, error) {
	var e E
	err := json.Unmarshal(data, &e)
	return e, err
}
