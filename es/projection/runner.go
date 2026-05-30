// Package projection drives consumers of the event log via a [Runner]
// that subscribes to a [es.SubscribableEventStore], decodes events
// through a codec [es.Registry], invokes a [es.Projection], and
// (optionally) checkpoints progress to a [es.CheckpointStore] so
// consumers resume across restarts.
//
// Typical usage:
//
//	runner := projection.NewRunner(
//	    "order-totals",
//	    eventStore,
//	    reg,
//	    totalsProjection,
//	    projection.WithCheckpoint(checkpoints),
//	    projection.WithLive(true),
//	)
//	if err := runner.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
package projection

import (
	"context"
	"fmt"
	"iter"
	"log/slog"

	"github.com/ianunruh/synapse/es"
)

// Runner drives one [es.Projection] over events from a
// [es.SubscribableEventStore]. Construct via [NewRunner] and call
// [Runner.Run].
//
// A Runner instance is single-shot: it is run by exactly one
// goroutine at a time. Concurrent calls to Run on the same Runner are
// undefined.
type Runner struct {
	name              string
	store             es.SubscribableEventStore
	reg               *es.Registry
	projection        es.Projection
	checkpoint        es.CheckpointStore
	live              bool
	stream            es.StreamID
	types             []string
	checkpointEvery   int
	onError           func(env es.Envelope, err error) bool
	logger            *slog.Logger
	disableEnrichment bool
}

// runnerOptions collects the configurable knobs threaded through
// [RunnerOption] values.
type runnerOptions struct {
	checkpoint        es.CheckpointStore
	live              bool
	stream            es.StreamID
	types             []string
	checkpointEvery   int
	onError           func(env es.Envelope, err error) bool
	logger            *slog.Logger
	disableEnrichment bool
}

// RunnerOption configures a [Runner] at construction time.
type RunnerOption func(*runnerOptions)

// WithCheckpoint installs a [es.CheckpointStore] for persisting
// progress across restarts. Without one, every [Runner.Run] starts
// from position 0 and progress is lost on shutdown.
func WithCheckpoint(c es.CheckpointStore) RunnerOption {
	return func(o *runnerOptions) { o.checkpoint = c }
}

// WithLive controls whether the underlying subscription blocks
// waiting for new events after catching up. When false (the default),
// Run terminates cleanly when the existing event log is exhausted.
func WithLive(live bool) RunnerOption {
	return func(o *runnerOptions) { o.live = live }
}

// WithStream scopes the Runner to a single stream via
// [es.SubscribableEventStore.SubscribeStream]. Checkpoint positions
// then track [es.RawEnvelope.Version] within that stream rather than
// global position.
func WithStream(stream es.StreamID) RunnerOption {
	return func(o *runnerOptions) { o.stream = stream }
}

// WithTypes restricts the subscription to events whose type is one of
// the given names, via [es.SubscriptionOptions.Types]. Without it the
// Runner sees every event type — and returns [es.CodecNotFoundError] for
// any type with no registered codec. Use this to scope a projection to
// the event types it actually understands.
func WithTypes(types ...string) RunnerOption {
	return func(o *runnerOptions) { o.types = types }
}

// WithCheckpointEvery batches checkpoint saves: the Runner saves
// progress once every n processed events rather than after each one.
// n <= 1 (the default) saves after every event. The final position is
// always flushed when a non-live subscription drains cleanly.
//
// Batching trades durability granularity for fewer checkpoint writes: on
// an unclean stop (context cancellation, crash) up to n-1 already-
// processed events may be redelivered on the next run. Projections are
// already required to be idempotent, so redelivery is safe. Has no
// effect without [WithCheckpoint].
func WithCheckpointEvery(n int) RunnerOption {
	return func(o *runnerOptions) { o.checkpointEvery = n }
}

// WithOnError installs an error handler invoked when
// [es.Projection.Project] returns an error. Returning true tells the
// Runner to skip the event (and still checkpoint past it) and
// continue; returning false makes [Runner.Run] return the error.
//
// Without this option, Run returns on the first projection error.
// Skipped events are logged at Warn level via the Runner's logger.
func WithOnError(fn func(env es.Envelope, err error) bool) RunnerOption {
	return func(o *runnerOptions) { o.onError = fn }
}

// WithLogger overrides the [slog.Logger] used to record best-effort
// warnings — currently, events skipped via the [WithOnError] handler.
// The default is [slog.Default].
func WithLogger(l *slog.Logger) RunnerOption {
	return func(o *runnerOptions) { o.logger = l }
}

// WithoutContextEnrichment disables the Runner's default behavior of
// deriving a child context for each [es.Projection.Project] call with
// the inbound event's identifiers — EventID as causation, Correlation
// and Metadata propagated. With enrichment on, a Projection whose body
// calls [es.Execute] or [es.Repository.Save] gets the right saga
// chain stamped onto outbound events automatically. Opt out when the
// Projection records no events of its own, or when you want full
// control over the context Project sees.
func WithoutContextEnrichment() RunnerOption {
	return func(o *runnerOptions) { o.disableEnrichment = true }
}

// NewRunner constructs a [Runner] that consumes events from store,
// decoding payloads through reg and applying them via proj. name
// identifies the projection for checkpointing.
//
// Required arguments are positional; optional configuration is
// expressed via [RunnerOption] values such as [WithCheckpoint],
// [WithLive], [WithStream], [WithOnError], and [WithLogger].
func NewRunner(
	name string,
	store es.SubscribableEventStore,
	reg *es.Registry,
	proj es.Projection,
	opts ...RunnerOption,
) *Runner {
	o := runnerOptions{logger: slog.Default(), checkpointEvery: 1}
	for _, opt := range opts {
		opt(&o)
	}
	if o.checkpointEvery < 1 {
		o.checkpointEvery = 1
	}
	return &Runner{
		name:              name,
		store:             store,
		reg:               reg,
		projection:        proj,
		checkpoint:        o.checkpoint,
		live:              o.live,
		stream:            o.stream,
		types:             o.types,
		checkpointEvery:   o.checkpointEvery,
		onError:           o.onError,
		logger:            o.logger,
		disableEnrichment: o.disableEnrichment,
	}
}

// Run starts the subscription, loads the saved checkpoint (or 0),
// decodes each event via the codec registry, and applies it via the
// projection. After each successful Project (or OnError-skipped event)
// it saves the new position to the checkpoint store.
//
// Run returns:
//
//   - nil when the subscription is non-Live and the event log is
//     exhausted, or when the context is canceled.
//   - the iterator's error when the underlying subscription fails.
//   - [es.CodecNotFoundError] when an event arrives whose type has no
//     registered codec.
//   - the projection's error when [es.Projection.Project] returns one
//     and the OnError hook does not request skip.
//   - a wrapped checkpoint error when the checkpoint Save fails.
func (r *Runner) Run(ctx context.Context) error {
	from := uint64(0)
	if r.checkpoint != nil {
		pos, found, err := r.checkpoint.Load(ctx, r.name)
		if err != nil {
			return fmt.Errorf("load checkpoint %q: %w", r.name, err)
		}
		if found {
			from = pos
		}
	}

	opts := es.SubscriptionOptions{From: from, Live: r.live, Types: r.types}
	var seq iter.Seq2[es.RawEnvelope, error]
	if r.stream != "" {
		seq = r.store.SubscribeStream(ctx, r.stream, opts)
	} else {
		seq = r.store.Subscribe(ctx, opts)
	}

	var (
		pendingPos  uint64
		havePending bool
		sinceSave   int
	)
	for raw, err := range seq {
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		env, err := r.decode(raw)
		if err != nil {
			return err
		}

		projectCtx := ctx
		if !r.disableEnrichment {
			projectCtx = es.WithCausation(projectCtx, env.EventID)
			if env.Correlation != "" {
				projectCtx = es.WithCorrelation(projectCtx, env.Correlation)
			}
			if len(env.Metadata) > 0 {
				projectCtx = es.WithMetadata(projectCtx, env.Metadata)
			}
		}
		if err := r.projection.Project(projectCtx, env); err != nil {
			if r.onError == nil || !r.onError(env, err) {
				return err
			}
			r.logger.WarnContext(ctx, "synapse: projection error, skipping event",
				"name", r.name,
				"type", env.Type,
				"stream", env.StreamID,
				"position", env.GlobalPosition,
				"err", err,
			)
		}

		if r.checkpoint != nil {
			pos := raw.GlobalPosition
			if r.stream != "" {
				pos = raw.Version
			}
			pendingPos, havePending = pos, true
			sinceSave++
			if sinceSave >= r.checkpointEvery {
				if err := r.checkpoint.Save(ctx, r.name, pos); err != nil {
					return fmt.Errorf("save checkpoint %q at pos %d: %w",
						r.name, pos, err)
				}
				sinceSave, havePending = 0, false
			}
		}
	}

	// Clean drain (non-live exhaustion): flush any position not yet
	// saved by the batch. Early returns above — context cancel, iterator
	// error, projection error — intentionally skip this; on an unclean
	// stop up to checkpointEvery-1 events are redelivered next run.
	if r.checkpoint != nil && havePending {
		if err := r.checkpoint.Save(ctx, r.name, pendingPos); err != nil {
			return fmt.Errorf("synapse: save checkpoint %q at pos %d: %w",
				r.name, pendingPos, err)
		}
	}
	return nil
}

func (r *Runner) decode(raw es.RawEnvelope) (es.Envelope, error) {
	codec, ok := r.reg.Lookup(raw.Type)
	if !ok {
		return es.Envelope{}, &es.CodecNotFoundError{EventType: raw.Type}
	}
	payload, err := codec.Unmarshal(raw.Payload)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("synapse: unmarshal %s at pos=%d: %w",
			raw.Type, raw.GlobalPosition, err)
	}
	payload, finalType, err := r.reg.Upcast(payload, raw.Type)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("synapse: upcast %s at pos=%d: %w",
			raw.Type, raw.GlobalPosition, err)
	}
	return es.Envelope{
		EventID:        raw.EventID,
		StreamID:       raw.StreamID,
		Version:        raw.Version,
		GlobalPosition: raw.GlobalPosition,
		RecordedAt:     raw.RecordedAt,
		Type:           finalType,
		ContentType:    raw.ContentType,
		Causation:      raw.Causation,
		Correlation:    raw.Correlation,
		Metadata:       raw.Metadata,
		Payload:        payload,
	}, nil
}
