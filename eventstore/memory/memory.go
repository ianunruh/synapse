// Package memory provides an in-memory [es.EventStore] suitable for
// tests, examples, and local development.
//
// The store is safe for concurrent use. Events are held in process
// memory only — restarting the program discards every stream.
//
// The store does not deep-copy event payloads or metadata. Callers must
// not mutate [es.RawEnvelope.Payload] or [es.RawEnvelope.Metadata]
// after Append or after receiving an envelope from the iterator
// returned by Load.
package memory

import (
	"context"
	"iter"
	"sync"

	"github.com/ianunruh/synapse/es"
)

// Store is an in-memory [es.EventStore] backed by per-stream slices
// guarded by a single [sync.RWMutex].
type Store struct {
	mu      sync.RWMutex
	streams map[es.StreamID][]es.RawEnvelope
}

// New returns an empty [Store].
func New() *Store {
	return &Store{streams: make(map[es.StreamID][]es.RawEnvelope)}
}

// Append implements [es.EventStore].
//
// It enforces expected against the current stream head and, on
// success, appends every envelope atomically. The returned
// [es.Revision] is always Exact(newHead).
//
// On expectation mismatch it returns a *[es.ConflictError] (which
// wraps [es.ErrConflict]) and the stream is left unchanged.
func (s *Store) Append(
	ctx context.Context,
	stream es.StreamID,
	expected es.Revision,
	events ...es.RawEnvelope,
) (es.Revision, error) {
	if err := ctx.Err(); err != nil {
		return es.Revision{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := uint64(len(s.streams[stream]))

	if err := checkRevision(stream, expected, current); err != nil {
		return es.Revision{}, err
	}

	if len(events) == 0 {
		return es.Exact(current), nil
	}

	s.streams[stream] = append(s.streams[stream], events...)
	return es.Exact(current + uint64(len(events))), nil
}

// Load implements [es.EventStore].
//
// The returned iterator yields a snapshot of the stream taken at the
// time Load is called. Appends that complete after Load returns do
// not appear in the iteration. If the context is canceled mid-iteration
// the iterator yields a single terminal (zero, err) and stops.
func (s *Store) Load(
	ctx context.Context,
	stream es.StreamID,
	opts es.ReadOptions,
) iter.Seq2[es.RawEnvelope, error] {
	if err := ctx.Err(); err != nil {
		return func(yield func(es.RawEnvelope, error) bool) {
			yield(es.RawEnvelope{}, err)
		}
	}

	snapshot := s.snapshot(stream, opts)

	return func(yield func(es.RawEnvelope, error) bool) {
		for _, env := range snapshot {
			if err := ctx.Err(); err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}
			if !yield(env, nil) {
				return
			}
		}
	}
}

// snapshot copies the relevant slice of a stream under the read lock,
// so the iterator can safely traverse it without further locking.
func (s *Store) snapshot(stream es.StreamID, opts es.ReadOptions) []es.RawEnvelope {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events := s.streams[stream]

	var start uint64
	if opts.From > 0 {
		start = opts.From - 1
	}
	if start >= uint64(len(events)) {
		return nil
	}
	end := uint64(len(events))
	if opts.Limit > 0 && start+opts.Limit < end {
		end = start + opts.Limit
	}
	out := make([]es.RawEnvelope, end-start)
	copy(out, events[start:end])
	return out
}

// checkRevision returns *es.ConflictError when expected disagrees
// with current; nil otherwise.
func checkRevision(stream es.StreamID, expected es.Revision, current uint64) error {
	switch {
	case expected == es.Any:
		return nil
	case expected == es.NoStream:
		if current == 0 {
			return nil
		}
	case expected == es.StreamExists:
		if current > 0 {
			return nil
		}
	default:
		if v, ok := expected.Value(); ok && v == current {
			return nil
		}
	}
	return &es.ConflictError{
		Stream:   stream,
		Expected: expected,
		Actual:   es.Exact(current),
	}
}
