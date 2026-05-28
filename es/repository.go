package es

import (
	"context"
	"fmt"
)

// Repository binds an [EventStore], a codec [Registry], a [Clock], and
// an [IDGenerator] together so application code can load and save
// aggregates of type A without thinking about serialization, ID
// generation, or optimistic concurrency.
//
// A Repository is safe for concurrent use as long as its dependencies
// are. The newFn factory is invoked to construct an empty aggregate
// before rehydration.
type Repository[A Aggregate] struct {
	store EventStore
	reg   *Registry
	clock Clock
	idgen IDGenerator
	newFn func(StreamID) A
}

// repositoryOptions collects the configurable knobs threaded through
// [RepositoryOption] values.
type repositoryOptions struct {
	clock Clock
	idgen IDGenerator
}

// RepositoryOption configures a [Repository] at construction time.
type RepositoryOption func(*repositoryOptions)

// WithClock overrides the wall-clock used to stamp RecordedAt on saved
// events. The default is [SystemClock].
func WithClock(c Clock) RepositoryOption {
	return func(o *repositoryOptions) { o.clock = c }
}

// WithIDGenerator overrides the [IDGenerator] used to stamp EventID on
// saved events. The default is a UUIDv7 generator backed by the
// Repository's [Clock].
func WithIDGenerator(g IDGenerator) RepositoryOption {
	return func(o *repositoryOptions) { o.idgen = g }
}

// NewRepository constructs a [Repository] over the given store and
// codec registry. newFn returns a zero-value aggregate bound to the
// requested stream id; it is invoked by Load before replaying history.
func NewRepository[A Aggregate](
	store EventStore,
	reg *Registry,
	newFn func(StreamID) A,
	opts ...RepositoryOption,
) *Repository[A] {
	o := repositoryOptions{clock: SystemClock{}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.idgen == nil {
		o.idgen = uuidv7Generator{clock: o.clock}
	}
	return &Repository[A]{
		store: store,
		reg:   reg,
		clock: o.clock,
		idgen: o.idgen,
		newFn: newFn,
	}
}

// Load constructs a fresh aggregate via newFn and replays the stream's
// events through Apply, advancing SetVersion after each event.
//
// If the stream holds no events Load returns *[StreamNotFoundError]
// wrapping [ErrStreamNotFound]. Callers needing "load or create"
// semantics should construct an aggregate directly and call Save.
func (r *Repository[A]) Load(ctx context.Context, id StreamID) (A, error) {
	var zero A
	agg := r.newFn(id)

	var seen int
	for raw, err := range r.store.Load(ctx, id, ReadOptions{}) {
		if err != nil {
			return zero, err
		}

		c, ok := r.reg.Lookup(raw.Type)
		if !ok {
			return zero, &CodecNotFoundError{EventType: raw.Type}
		}

		payload, err := c.Unmarshal(raw.Payload)
		if err != nil {
			return zero, fmt.Errorf("synapse: unmarshal %s at v%d: %w", raw.Type, raw.Version, err)
		}

		env := Envelope{
			EventID:     raw.EventID,
			StreamID:    raw.StreamID,
			Version:     raw.Version,
			RecordedAt:  raw.RecordedAt,
			Type:        raw.Type,
			ContentType: raw.ContentType,
			Causation:   raw.Causation,
			Correlation: raw.Correlation,
			Metadata:    raw.Metadata,
			Payload:     payload,
		}

		if err := agg.Apply(env); err != nil {
			return zero, fmt.Errorf("synapse: apply %s at v%d: %w", raw.Type, raw.Version, err)
		}
		agg.SetVersion(env.Version)
		seen++
	}

	if seen == 0 {
		return zero, &StreamNotFoundError{Stream: id}
	}
	return agg, nil
}

// Save serializes the aggregate's Pending events via the codec
// registry, stamps EventID/RecordedAt/ContentType where missing, and
// appends them to the store under an expected revision matching the
// aggregate's loaded version. On success, Pending is cleared.
//
// Returns nil immediately if there are no pending events.
func (r *Repository[A]) Save(ctx context.Context, agg A) error {
	pending := agg.Pending()
	if len(pending) == 0 {
		return nil
	}

	firstVersion := pending[0].Version
	expected := NoStream
	if firstVersion > 1 {
		expected = Exact(firstVersion - 1)
	}

	now := r.clock.NowUTC()
	raws := make([]RawEnvelope, len(pending))
	for i, env := range pending {
		c, ok := r.reg.Lookup(env.Type)
		if !ok {
			return &CodecNotFoundError{EventType: env.Type}
		}
		data, err := c.Marshal(env.Payload)
		if err != nil {
			return fmt.Errorf("synapse: marshal %s at v%d: %w", env.Type, env.Version, err)
		}

		eventID := env.EventID
		if eventID == "" {
			eventID = r.idgen.NewEventID()
		}
		recordedAt := env.RecordedAt
		if recordedAt.IsZero() {
			recordedAt = now
		}

		raws[i] = RawEnvelope{
			EventID:     eventID,
			StreamID:    env.StreamID,
			Version:     env.Version,
			RecordedAt:  recordedAt,
			Type:        env.Type,
			ContentType: c.ContentType(),
			Causation:   env.Causation,
			Correlation: env.Correlation,
			Metadata:    env.Metadata,
			Payload:     data,
		}
	}

	if _, err := r.store.Append(ctx, agg.StreamID(), expected, raws...); err != nil {
		return err
	}
	agg.ClearPending()
	return nil
}
