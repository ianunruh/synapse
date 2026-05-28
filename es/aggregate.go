package es

import "iter"

// Aggregate is the unit of consistency in event sourcing. Concrete
// aggregate types embed [AggregateBase] to satisfy most of the
// interface; only Apply needs domain-specific logic.
//
//	type Order struct {
//	    *es.AggregateBase
//	    status string
//	    total  int
//	}
//
//	func (o *Order) Apply(env es.Envelope) {
//	    switch p := env.Payload.(type) {
//	    case OrderPlaced:
//	        o.status, o.total = "placed", p.Total
//	    case OrderShipped:
//	        o.status = "shipped"
//	    }
//	}
type Aggregate interface {
	// StreamID returns the stream this aggregate writes to and loads from.
	StreamID() StreamID

	// Version returns the number of events the aggregate has consumed.
	// Newly constructed aggregates report 0.
	Version() uint64

	// Apply mutates aggregate state in response to a single event. It
	// is invoked both during rehydration (events loaded from the
	// store) and immediately after a new event is recorded.
	//
	// Apply must not fail. Events are facts that already happened:
	// refusing to apply one during rehydration cannot unmake the past,
	// and validation of recordable events belongs in the command method
	// that calls Record, before the event is added to the pending
	// queue.
	Apply(env Envelope)

	// SetVersion advances the aggregate's loaded version. The
	// [Repository] calls SetVersion after each Apply during
	// rehydration so the aggregate's version tracks the stream's head.
	// Domain code should not call SetVersion directly; embedders of
	// [AggregateBase] get a correct implementation for free.
	SetVersion(v uint64)

	// Pending returns the events that have been recorded but not yet
	// persisted. The slice is read-only from the caller's perspective.
	Pending() []Envelope

	// ClearPending discards the recorded-but-unpersisted events. The
	// [Repository] calls this after a successful append.
	ClearPending()
}

// AggregateBase is an embeddable struct that supplies the bookkeeping
// every aggregate needs: stream identity, version tracking, and a
// pending event buffer.
//
// Embed by pointer so the embedding type can mutate state through
// (*AggregateBase).Record:
//
//	type Order struct { *es.AggregateBase; /* domain fields */ }
//
//	func NewOrder(id OrderID) *Order {
//	    return &Order{AggregateBase: es.NewAggregateBase(id.Stream())}
//	}
type AggregateBase struct {
	streamID StreamID
	version  uint64
	pending  []Envelope
}

// NewAggregateBase returns an [AggregateBase] bound to id at version 0.
func NewAggregateBase(id StreamID) *AggregateBase {
	return &AggregateBase{streamID: id}
}

// StreamID implements [Aggregate].
func (b *AggregateBase) StreamID() StreamID { return b.streamID }

// Version implements [Aggregate].
func (b *AggregateBase) Version() uint64 { return b.version }

// SetVersion implements [Aggregate].
func (b *AggregateBase) SetVersion(v uint64) { b.version = v }

// Pending implements [Aggregate].
func (b *AggregateBase) Pending() []Envelope { return b.pending }

// ClearPending implements [Aggregate].
func (b *AggregateBase) ClearPending() {
	clear(b.pending)
	b.pending = b.pending[:0]
}

// Record stages a new event on the aggregate. It composes an
// [Envelope] from the embedder's stream id and the next version
// number, invokes apply so the aggregate's in-memory state reflects
// the change, and queues the envelope for the next save.
//
// The apply callback is typically the embedder's own Apply method.
// Threading it through Record explicitly avoids the runtime cost of
// reflection while keeping AggregateBase unaware of the concrete
// aggregate type.
//
// Validation of recordable events should happen in the command method
// that calls Record, before Record is invoked.
func (b *AggregateBase) Record(eventType string, payload any, apply func(Envelope)) {
	next := b.version + 1
	env := Envelope{
		StreamID: b.streamID,
		Version:  next,
		Type:     eventType,
		Payload:  payload,
	}
	apply(env)
	b.version = next
	b.pending = append(b.pending, env)
}

// ReplayAll advances AggregateBase across events loaded from history,
// invoking apply for each one. It does not enqueue events for
// persistence; rehydration is read-only with respect to the store.
//
// The [Repository] does not use ReplayAll (it advances version through
// [Aggregate.SetVersion] so it can interleave codec lookups), but
// callers writing custom load paths can reuse it for convenience.
//
// ReplayAll's apply callback does not return an error; events are
// facts that already happened. The returned error is only ever the
// terminal error yielded by the events iterator (typically an
// [EventStore] read failure).
func (b *AggregateBase) ReplayAll(events iter.Seq2[Envelope, error], apply func(Envelope)) error {
	for env, err := range events {
		if err != nil {
			return err
		}
		apply(env)
		b.version = env.Version
	}
	return nil
}

// FoldEvents is a generic helper for callers who prefer a pure-reducer
// style over the [Aggregate]/[AggregateBase] approach: given an
// initial state and a sequence of envelopes, fold step over them and
// return the resulting state.
//
// FoldEvents is independent of the [Repository] machinery and is
// useful for projections, read-model rebuilds, and ad-hoc analysis.
// The step function may return an error (FoldEvents is not constrained
// to aggregate semantics; it is a general reducer).
func FoldEvents[S any](
	init S,
	events iter.Seq2[Envelope, error],
	step func(S, Envelope) (S, error),
) (S, error) {
	state := init
	for env, err := range events {
		if err != nil {
			return state, err
		}
		next, err := step(state, env)
		if err != nil {
			return state, err
		}
		state = next
	}
	return state, nil
}
