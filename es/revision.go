package es

import "strconv"

// revisionKind discriminates a [Revision]'s case.
type revisionKind uint8

const (
	revAny revisionKind = iota
	revNoStream
	revStreamExists
	revExact
)

// Revision expresses a caller's expectation about a stream's state when
// appending events. It is a tagged value type — comparable, copyable,
// and free of allocations — so it can flow through hot paths without
// runtime cost.
//
// The zero value is [Any], meaning "no expectation"; callers can pass
// it without ceremony when concurrency control is not desired.
type Revision struct {
	kind  revisionKind
	value uint64
}

// Sentinel revisions covering the three open-ended cases.
var (
	// Any allows the append regardless of current stream state.
	Any = Revision{kind: revAny}
	// NoStream requires the stream to not yet exist.
	NoStream = Revision{kind: revNoStream}
	// StreamExists requires the stream to already contain at least one event.
	StreamExists = Revision{kind: revStreamExists}
)

// Exact requires the stream to be at exactly version v.
//
// A successful append against Exact(v) advances the stream to v + N
// where N is the number of events appended.
func Exact(v uint64) Revision { return Revision{kind: revExact, value: v} }

// IsExact reports whether r expresses an Exact(v) constraint.
func (r Revision) IsExact() bool { return r.kind == revExact }

// Value returns the exact version r requires, if any.
// The boolean is false for Any, NoStream, and StreamExists.
func (r Revision) Value() (uint64, bool) {
	if r.kind != revExact {
		return 0, false
	}
	return r.value, true
}

// String renders r in a form suitable for logs and error messages.
func (r Revision) String() string {
	switch r.kind {
	case revAny:
		return "Any"
	case revNoStream:
		return "NoStream"
	case revStreamExists:
		return "StreamExists"
	case revExact:
		return "Exact(" + strconv.FormatUint(r.value, 10) + ")"
	default:
		return "Revision(?)"
	}
}
