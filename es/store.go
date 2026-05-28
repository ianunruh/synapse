package es

import (
	"context"
	"iter"
)

// ReadOptions controls a [EventStore.Load] call. The zero value asks
// for the entire stream from the beginning.
type ReadOptions struct {
	// From is the first version to return (1-based). A value of 0
	// means "start at the beginning of the stream".
	From uint64

	// Limit caps the number of events returned. A value of 0 means
	// "no limit".
	Limit uint64
}

// EventStore is the persistence boundary for serialized events. It
// knows nothing about codecs or domain types; payloads flow through
// as opaque bytes inside [RawEnvelope].
//
// Implementations may back the store with an in-memory map, a SQL
// database, a log-oriented system, or a remote service. The contract
// is the same in every case:
//
//   - Append is atomic per call and per stream: either every
//     envelope is persisted at consecutive versions, or none are.
//   - Append enforces the caller's [Revision] expectation and
//     returns a *[ConflictError] (wrapping [ErrConflict]) on a
//     mismatch.
//   - Load yields events in ascending version order. The iterator
//     terminates cleanly when the stream is exhausted or the Limit
//     is hit, and yields a single terminal (zero, err) on failure.
type EventStore interface {
	// Append writes events to a stream atomically and returns the
	// new head revision (always Exact(v)) on success.
	Append(
		ctx context.Context,
		stream StreamID,
		expected Revision,
		events ...RawEnvelope,
	) (Revision, error)

	// Load returns events from a stream as an iterator of
	// (envelope, error) pairs. The iterator emits at most one
	// terminal error and then stops.
	Load(
		ctx context.Context,
		stream StreamID,
		opts ReadOptions,
	) iter.Seq2[RawEnvelope, error]
}
