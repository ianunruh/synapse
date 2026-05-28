package es

import (
	"context"
	"errors"
)

// Repository binds an [EventStore], a codec [Registry], and a [Clock]
// together so application code can load and save aggregates of type A
// without thinking about serialization, ID generation, or optimistic
// concurrency.
//
// A Repository is safe for concurrent use as long as its dependencies
// are. The newFn factory is invoked to construct an empty aggregate
// before rehydration.
type Repository[A Aggregate] struct {
	store EventStore
	reg   *Registry
	clock Clock
	newFn func(StreamID) A
}

// repositoryOptions collects the configurable knobs threaded through
// [RepositoryOption] values.
type repositoryOptions struct {
	clock Clock
}

// RepositoryOption configures a [Repository] at construction time.
type RepositoryOption func(*repositoryOptions)

// WithClock overrides the wall-clock used to stamp RecordedAt on saved
// events. The default is [SystemClock].
func WithClock(c Clock) RepositoryOption {
	return func(o *repositoryOptions) { o.clock = c }
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
	return &Repository[A]{
		store: store,
		reg:   reg,
		clock: o.clock,
		newFn: newFn,
	}
}

// Load constructs a fresh aggregate via newFn and replays the stream's
// events through Apply. The returned aggregate's Version reflects the
// number of events consumed.
//
// Implementation deferred to a follow-up milestone.
func (r *Repository[A]) Load(ctx context.Context, id StreamID) (A, error) {
	var zero A
	_ = ctx
	_ = id
	return zero, errors.ErrUnsupported
}

// Save serializes the aggregate's Pending events via the codec
// registry, stamps identity and time fields, and appends them to the
// stream under an [Exact] revision matching the aggregate's loaded
// version. On success, Pending is cleared.
//
// Implementation deferred to a follow-up milestone.
func (r *Repository[A]) Save(ctx context.Context, agg A) error {
	_ = ctx
	_ = agg
	return errors.ErrUnsupported
}
