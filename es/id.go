package es

import (
	"crypto/rand"
	"encoding/hex"
)

// IDGenerator produces unique event identifiers. The [Repository]
// invokes NewEventID for every pending event during Save (unless the
// envelope already has an EventID set).
//
// The default implementation emits RFC 9562 UUIDv7 values keyed off
// the Repository's [Clock] so identifiers sort chronologically. Tests
// that need deterministic identifiers can inject a custom generator
// through [WithIDGenerator].
type IDGenerator interface {
	NewEventID() string
}

// uuidv7Generator emits RFC 9562 UUIDv7 identifiers.
//
// Layout (16 bytes):
//
//	bits  0-47 : unix timestamp in milliseconds (big-endian)
//	bits 48-51 : version (0b0111)
//	bits 52-63 : 12 random bits
//	bits 64-65 : variant (0b10)
//	bits 66-127: 62 random bits
type uuidv7Generator struct {
	clock Clock
}

// NewEventID implements [IDGenerator].
func (g uuidv7Generator) NewEventID() string {
	var b [16]byte
	ts := uint64(g.clock.NowUTC().UnixMilli())
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
