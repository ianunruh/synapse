package es

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/ianunruh/synapse/idgen"
)

// Repository binds an [EventStore], a codec [Registry], a [Clock], an
// [idgen.Generator], an optional [SnapshotStore]/[SnapshotPolicy], an
// optional chain of [Middleware], and an [slog.Logger] together so
// application code can load and save aggregates of type A without
// thinking about serialization, ID generation, snapshotting, or
// optimistic concurrency.
//
// A Repository is safe for concurrent use as long as its dependencies
// are. The newFn factory is invoked to construct an empty aggregate
// before rehydration.
type Repository[A Aggregate] struct {
	store      EventStore
	reg        *Registry
	clock      Clock
	idGen      idgen.Generator
	newFn      func(StreamID) A
	middleware []Middleware
	snapStore  SnapshotStore
	snapPolicy SnapshotPolicy
	logger     *slog.Logger
}

// repositoryOptions collects the configurable knobs threaded through
// [RepositoryOption] values.
type repositoryOptions struct {
	clock      Clock
	idGen      idgen.Generator
	middleware []Middleware
	snapStore  SnapshotStore
	snapPolicy SnapshotPolicy
	logger     *slog.Logger
}

// RepositoryOption configures a [Repository] at construction time.
type RepositoryOption func(*repositoryOptions)

// WithClock overrides the wall-clock used to stamp RecordedAt on saved
// events. The default is [SystemClock].
func WithClock(c Clock) RepositoryOption {
	return func(o *repositoryOptions) { o.clock = c }
}

// WithIDGenerator overrides the [idgen.Generator] used to stamp
// EventID on saved events. The default is [idgen.UUIDv7] backed by the
// Repository's [Clock].
func WithIDGenerator(g idgen.Generator) RepositoryOption {
	return func(o *repositoryOptions) { o.idGen = g }
}

// WithMiddleware appends [Middleware] to the Repository's command
// execution chain. Subsequent calls append rather than replace, so
// constructors that compose multiple option groups behave naturally.
//
// Middleware run left-to-right as outer wrappers: the first middleware
// passed wraps the second, which wraps the third, and so on around the
// load-handle-save pipeline that [Execute] invokes.
func WithMiddleware(mws ...Middleware) RepositoryOption {
	return func(o *repositoryOptions) {
		o.middleware = append(o.middleware, mws...)
	}
}

// WithSnapshotStore wires a [SnapshotStore] into the Repository,
// enabling the snapshot-aware [Repository.Load] path and unlocking
// [Repository.SaveSnapshot]. Without a store, automatic snapshots
// never fire and manual SaveSnapshot returns an error.
func WithSnapshotStore(s SnapshotStore) RepositoryOption {
	return func(o *repositoryOptions) { o.snapStore = s }
}

// WithSnapshotPolicy installs a [SnapshotPolicy] that the Repository
// consults after each successful Save to decide whether to write a
// new snapshot. Without a policy, automatic snapshots never fire even
// if a [SnapshotStore] is configured; [Repository.SaveSnapshot]
// remains usable for manual checkpoints.
func WithSnapshotPolicy(p SnapshotPolicy) RepositoryOption {
	return func(o *repositoryOptions) { o.snapPolicy = p }
}

// WithLogger overrides the [slog.Logger] used by the Repository to
// record best-effort failures — currently, automatic snapshot save
// errors that [Save] intentionally swallows. The default is
// [slog.Default], so library warnings reach the program's default
// handler without explicit configuration. To silence, install a
// logger backed by a discard handler.
func WithLogger(l *slog.Logger) RepositoryOption {
	return func(o *repositoryOptions) { o.logger = l }
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
	if o.idGen == nil {
		o.idGen = idgen.UUIDv7{Now: o.clock.NowUTC}
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	return &Repository[A]{
		store:      store,
		reg:        reg,
		clock:      o.clock,
		idGen:      o.idGen,
		newFn:      newFn,
		middleware: slices.Clone(o.middleware),
		snapStore:  o.snapStore,
		snapPolicy: o.snapPolicy,
		logger:     o.logger,
	}
}

// Load constructs a fresh aggregate via newFn. If a [SnapshotStore] is
// configured AND the aggregate implements [Snapshotter], Load first
// tries to restore from the latest snapshot and then replays only
// events with Version > snapshot.Version; otherwise it replays the
// full event history.
//
// If neither a snapshot nor any events exist for the stream, Load
// returns *[StreamNotFoundError] wrapping [ErrStreamNotFound].
// Callers needing "load or create" semantics should construct an
// aggregate directly and call Save.
func (r *Repository[A]) Load(ctx context.Context, id StreamID) (A, error) {
	var zero A
	agg := r.newFn(id)

	fromVersion := uint64(0) // 0 in ReadOptions = from the start
	foundSnapshot := false

	if r.snapStore != nil {
		if snapper, ok := any(agg).(Snapshotter); ok {
			snap, found, err := r.snapStore.Latest(ctx, id)
			if err != nil {
				return zero, fmt.Errorf("synapse: snapshot load: %w", err)
			}
			if found {
				c, ok := r.reg.Lookup(snap.Type)
				if !ok {
					return zero, &CodecNotFoundError{EventType: snap.Type}
				}
				state, err := c.Unmarshal(snap.Payload)
				if err != nil {
					return zero, fmt.Errorf("synapse: unmarshal snapshot %s: %w", snap.Type, err)
				}
				if err := snapper.Restore(state); err != nil {
					return zero, fmt.Errorf("synapse: restore %s at v%d: %w", snap.Type, snap.Version, err)
				}
				agg.SetVersion(snap.Version)
				fromVersion = snap.Version + 1
				foundSnapshot = true
			}
		}
	}

	var seenEvents int
	for raw, err := range r.store.Load(ctx, id, ReadOptions{From: fromVersion}) {
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
			EventID:        raw.EventID,
			StreamID:       raw.StreamID,
			Version:        raw.Version,
			GlobalPosition: raw.GlobalPosition,
			RecordedAt:     raw.RecordedAt,
			Type:           raw.Type,
			ContentType:    raw.ContentType,
			Causation:      raw.Causation,
			Correlation:    raw.Correlation,
			Metadata:       raw.Metadata,
			Payload:        payload,
		}

		agg.Apply(env)
		agg.SetVersion(env.Version)
		seenEvents++
	}

	if !foundSnapshot && seenEvents == 0 {
		return zero, &StreamNotFoundError{Stream: id}
	}
	return agg, nil
}

// Save serializes the aggregate's Pending events via the codec
// registry, stamps EventID/RecordedAt/ContentType where missing, and
// appends them to the store under an expected revision matching the
// aggregate's loaded version. On success, Pending is cleared.
//
// After a successful append, if a [SnapshotStore] and [SnapshotPolicy]
// are both configured and the aggregate implements [Snapshotter], the
// policy is consulted with the version before and after the Save. If
// it returns true, a best-effort snapshot save is attempted; errors
// from that step are logged at Warn level via the Repository's
// [slog.Logger] and otherwise swallowed (events are already committed;
// the snapshot is an optimization).
//
// Returns nil immediately if there are no pending events.
func (r *Repository[A]) Save(ctx context.Context, agg A) error {
	pending := agg.Pending()
	if len(pending) == 0 {
		return nil
	}

	firstVersion := pending[0].Version
	versionBefore := firstVersion - 1
	expected := NoStream
	if versionBefore > 0 {
		expected = Exact(versionBefore)
	}

	now := r.clock.NowUTC()
	ctxCorrelation := correlationFromContext(ctx)
	ctxCausation := causationFromContext(ctx)
	ctxMetadata := metadataFromContext(ctx)
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
			eventID = r.idGen.NewEventID()
		}
		recordedAt := env.RecordedAt
		if recordedAt.IsZero() {
			recordedAt = now
		}
		correlation := env.Correlation
		if correlation == "" {
			correlation = ctxCorrelation
		}
		causation := env.Causation
		if causation == "" {
			causation = ctxCausation
		}

		raws[i] = RawEnvelope{
			EventID:     eventID,
			StreamID:    env.StreamID,
			Version:     env.Version,
			RecordedAt:  recordedAt,
			Type:        env.Type,
			ContentType: c.ContentType(),
			Causation:   causation,
			Correlation: correlation,
			Metadata:    mergeMetadata(ctxMetadata, env.Metadata),
			Payload:     data,
		}
	}

	if _, err := r.store.Append(ctx, agg.StreamID(), expected, raws...); err != nil {
		return err
	}
	versionAfter := agg.Version()
	agg.ClearPending()

	if r.snapStore != nil && r.snapPolicy != nil && r.snapPolicy(agg, versionBefore, versionAfter) {
		if err := r.trySaveSnapshot(ctx, agg); err != nil {
			r.logger.WarnContext(ctx, "synapse: snapshot save failed",
				"stream", agg.StreamID(),
				"version", agg.Version(),
				"err", err,
			)
		}
	}
	return nil
}

// SaveSnapshot persists a snapshot of the aggregate's current state
// to the configured [SnapshotStore]. It is intended for explicit
// checkpoints — migration scripts, integration tests, or
// application-driven snapshotting outside the [SnapshotPolicy].
//
// Returns an error when no [SnapshotStore] is configured or when the
// aggregate does not implement [Snapshotter].
func (r *Repository[A]) SaveSnapshot(ctx context.Context, agg A) error {
	if r.snapStore == nil {
		return fmt.Errorf("synapse: SaveSnapshot: no snapshot store configured")
	}
	snapper, ok := any(agg).(Snapshotter)
	if !ok {
		return fmt.Errorf("synapse: SaveSnapshot: aggregate %T does not implement Snapshotter", agg)
	}
	return r.writeSnapshot(ctx, agg, snapper)
}

// trySaveSnapshot is the internal entry point used by Save. It
// short-circuits silently when the aggregate is not a Snapshotter.
func (r *Repository[A]) trySaveSnapshot(ctx context.Context, agg A) error {
	snapper, ok := any(agg).(Snapshotter)
	if !ok {
		return nil
	}
	return r.writeSnapshot(ctx, agg, snapper)
}

// writeSnapshot serializes the aggregate's snapshot state and writes
// it to the snapshot store.
func (r *Repository[A]) writeSnapshot(ctx context.Context, agg A, snapper Snapshotter) error {
	state, err := snapper.Snapshot()
	if err != nil {
		return fmt.Errorf("synapse: snapshot: %w", err)
	}

	snapType := snapper.SnapshotType()
	c, ok := r.reg.Lookup(snapType)
	if !ok {
		return &CodecNotFoundError{EventType: snapType}
	}
	payload, err := c.Marshal(state)
	if err != nil {
		return fmt.Errorf("synapse: marshal snapshot %s: %w", snapType, err)
	}

	return r.snapStore.Save(ctx, RawSnapshot{
		StreamID:    agg.StreamID(),
		Version:     agg.Version(),
		Type:        snapType,
		ContentType: c.ContentType(),
		RecordedAt:  r.clock.NowUTC(),
		Payload:     payload,
	})
}
