// Package memory provides an in-memory [es.EventStore] (and
// [es.SubscribableEventStore]) suitable for tests, examples, and
// local development.
//
// The store is safe for concurrent use. Events are held in process
// memory only — restarting the program discards every stream.
//
// The store does not deep-copy event payloads or metadata. Callers
// must not mutate [es.RawEnvelope.Payload] or [es.RawEnvelope.Metadata]
// after Append or after receiving an envelope from the iterators
// returned by Load, Subscribe, or SubscribeStream.
//
// Subscribe and SubscribeStream support both catch-up and live tail.
// Live subscribers receive a broadcast wake-up after every successful
// Append via a close-and-replace signal channel; the implementation
// captures the channel under the read lock before snapshotting events
// to avoid missed wake-ups.
package memory

import (
	"context"
	"iter"
	"sync"

	"github.com/ianunruh/synapse/es"
)

// Store is an in-memory [es.SubscribableEventStore] backed by per-
// stream slices and a global append-ordered slice, guarded by a
// single [sync.RWMutex].
type Store struct {
	mu      sync.RWMutex
	streams map[es.StreamID][]es.RawEnvelope
	global  []es.RawEnvelope
	nextPos uint64
	notify  chan struct{}
}

// New returns an empty [Store].
func New() *Store {
	return &Store{
		streams: make(map[es.StreamID][]es.RawEnvelope),
		notify:  make(chan struct{}),
	}
}

// Append implements [es.EventStore].
//
// It enforces expected against the current stream head and, on
// success, stamps each event with a monotonic [es.RawEnvelope.GlobalPosition]
// and appends it to both the per-stream slice and the global log
// atomically. The caller's input slice is left unchanged; positions
// are stamped on internal copies.
//
// After a successful append, all live subscribers are woken via a
// close-and-replace broadcast on the store's internal signal channel.
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

	positioned := make([]es.RawEnvelope, len(events))
	for i, ev := range events {
		s.nextPos++
		ev.GlobalPosition = s.nextPos
		positioned[i] = ev
	}

	s.streams[stream] = append(s.streams[stream], positioned...)
	s.global = append(s.global, positioned...)

	close(s.notify)
	s.notify = make(chan struct{})

	return es.Exact(current + uint64(len(positioned))), nil
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

	snapshot := s.loadSnapshot(stream, opts)

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

// Subscribe implements [es.SubscribableEventStore]. The iterator
// yields events from the global log with GlobalPosition > opts.From,
// in append order. When opts.Live is true, the iterator blocks
// waiting for new events after catching up; otherwise it terminates
// when caught up.
//
// Context cancellation terminates the iterator with a single terminal
// (zero, ctx.Err()) yield.
func (s *Store) Subscribe(ctx context.Context, opts es.SubscriptionOptions) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		from := opts.From
		for {
			if err := ctx.Err(); err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}

			notify, snapshot := s.subscribeGlobalSnapshot(from)

			for _, env := range snapshot {
				if !yield(env, nil) {
					return
				}
				from = env.GlobalPosition
			}

			if !opts.Live {
				return
			}

			select {
			case <-notify:
			case <-ctx.Done():
				yield(es.RawEnvelope{}, ctx.Err())
				return
			}
		}
	}
}

// SubscribeStream implements [es.SubscribableEventStore]. The
// iterator yields events for stream with Version > opts.From, in
// append order. Same Live and ctx semantics as [Subscribe].
func (s *Store) SubscribeStream(ctx context.Context, stream es.StreamID, opts es.SubscriptionOptions) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		from := opts.From
		for {
			if err := ctx.Err(); err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}

			notify, snapshot := s.subscribeStreamSnapshot(stream, from)

			for _, env := range snapshot {
				if !yield(env, nil) {
					return
				}
				from = env.Version
			}

			if !opts.Live {
				return
			}

			select {
			case <-notify:
			case <-ctx.Done():
				yield(es.RawEnvelope{}, ctx.Err())
				return
			}
		}
	}
}

// loadSnapshot copies the relevant slice of a stream under the read
// lock for [Load].
func (s *Store) loadSnapshot(stream es.StreamID, opts es.ReadOptions) []es.RawEnvelope {
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

// subscribeGlobalSnapshot captures the notify channel and copies the
// global slice past `from` under the read lock. Returning the channel
// captured BEFORE snapshotting avoids a missed wake-up: any Append
// concurrent with this call either appears in the snapshot or has not
// yet closed this notify channel.
func (s *Store) subscribeGlobalSnapshot(from uint64) (chan struct{}, []es.RawEnvelope) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	notify := s.notify
	var snapshot []es.RawEnvelope
	if from < uint64(len(s.global)) {
		snapshot = make([]es.RawEnvelope, uint64(len(s.global))-from)
		copy(snapshot, s.global[from:])
	}
	return notify, snapshot
}

// subscribeStreamSnapshot is the per-stream counterpart of
// [subscribeGlobalSnapshot].
func (s *Store) subscribeStreamSnapshot(stream es.StreamID, from uint64) (chan struct{}, []es.RawEnvelope) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	notify := s.notify
	events := s.streams[stream]
	var snapshot []es.RawEnvelope
	if from < uint64(len(events)) {
		snapshot = make([]es.RawEnvelope, uint64(len(events))-from)
		copy(snapshot, events[from:])
	}
	return notify, snapshot
}

// checkRevision returns *es.ConflictError when expected disagrees
// with current; nil otherwise.
func checkRevision(stream es.StreamID, expected es.Revision, current uint64) error {
	switch expected {
	case es.Any:
		return nil
	case es.NoStream:
		if current == 0 {
			return nil
		}
	case es.StreamExists:
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
