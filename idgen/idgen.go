// Package idgen provides event identifier generators for the synapse
// event sourcing toolkit.
//
// The default generator emits RFC 9562 UUIDv7 identifiers, which embed
// a millisecond timestamp in the leading 48 bits so identifiers sort
// chronologically and pagination through an event log can use plain
// lexicographic comparisons.
//
// Typical usage from application code:
//
//	import (
//	    "github.com/ianunruh/synapse/es"
//	    "github.com/ianunruh/synapse/idgen"
//	)
//
//	repo := es.NewRepository(store, reg, NewOrder,
//	    es.WithIDGenerator(idgen.UUIDv7{}))
//
// The zero value of [UUIDv7] uses [time.Now] as its clock source, so
// most callers can pass it without configuration. Tests that need
// deterministic timestamps inject a custom Now function.
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Generator produces unique event identifiers. The Repository invokes
// NewEventID for every pending event during Save unless the envelope
// already has an EventID set.
//
// Implementations should be safe for concurrent use.
type Generator interface {
	NewEventID() string
}

// GeneratorFunc adapts a plain func() string into a [Generator]. Useful
// for tests and one-off custom generators:
//
//	g := idgen.GeneratorFunc(func() string { return "static" })
type GeneratorFunc func() string

// NewEventID implements [Generator].
func (f GeneratorFunc) NewEventID() string { return f() }

// UUIDv7 emits RFC 9562 UUIDv7 identifiers.
//
// Layout (128 bits / 16 bytes):
//
//	bits  0-47 : unix timestamp in milliseconds (big-endian)
//	bits 48-51 : version (0b0111)
//	bits 52-63 : 12 random bits
//	bits 64-65 : variant (0b10)
//	bits 66-127: 62 random bits
//
// The zero value of UUIDv7 is usable and emits identifiers timestamped
// from [time.Now]. Tests can supply a custom clock through the Now
// field.
type UUIDv7 struct {
	// Now returns the time used for the embedded millisecond timestamp.
	// If nil, time.Now is used.
	Now func() time.Time
}

// NewEventID implements [Generator]. It is safe to call concurrently.
func (g UUIDv7) NewEventID() string {
	now := g.Now
	if now == nil {
		now = time.Now
	}

	var b [16]byte
	ts := uint64(now().UnixMilli())
	b[0] = byte(ts >> 40)
	b[1] = byte(ts >> 32)
	b[2] = byte(ts >> 24)
	b[3] = byte(ts >> 16)
	b[4] = byte(ts >> 8)
	b[5] = byte(ts)
	_, _ = rand.Read(b[6:])
	b[6] = (b[6] & 0x0F) | 0x70 // version 7
	b[8] = (b[8] & 0x3F) | 0x80 // variant 10

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
