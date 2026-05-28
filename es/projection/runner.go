// Package projection drives consumers of the event log via a [Runner]
// that subscribes to a [es.SubscribableEventStore], decodes events
// through a codec [es.Registry], invokes a [es.Projection], and
// (optionally) checkpoints progress to a [es.CheckpointStore] so
// consumers resume across restarts.
//
// Typical usage:
//
//	runner := &projection.Runner{
//	    Name:       "order-totals",
//	    Store:      eventStore,
//	    Registry:   reg,
//	    Projection: totalsProjection,
//	    Checkpoint: checkpoints,
//	    Live:       true,
//	}
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
// [es.SubscribableEventStore]. Configure the required fields (Name,
// Store, Registry, Projection) and call [Runner.Run].
//
// A Runner instance is single-shot: it is run by exactly one
// goroutine at a time. Concurrent calls to Run on the same Runner are
// undefined.
type Runner struct {
	// Name identifies the projection for checkpointing. Required.
	Name string

	// Store is the source of events. Required.
	Store es.SubscribableEventStore

	// Registry decodes event payloads. Required.
	Registry *es.Registry

	// Projection consumes decoded events. Required.
	Projection es.Projection

	// Checkpoint persists progress across restarts. Optional; without
	// one, every [Run] starts from position 0 and progress is lost on
	// shutdown.
	Checkpoint es.CheckpointStore

	// Live, when true, makes the underlying subscription block
	// waiting for new events after catching up. When false, Run
	// terminates cleanly when the existing event log is exhausted.
	Live bool

	// Stream, when non-empty, scopes the Runner to a single stream
	// via [es.SubscribableEventStore.SubscribeStream]. Checkpoint
	// positions then track [es.RawEnvelope.Version] within that
	// stream rather than global position.
	Stream es.StreamID

	// OnError, when non-nil, is consulted when [es.Projection.Project]
	// returns an error. Returning true tells the Runner to skip the
	// event (and still checkpoint past it) and continue; returning
	// false makes [Run] return the error.
	//
	// When OnError is nil, Run returns on the first projection
	// error. Skipped events are logged at Warn level via Logger.
	OnError func(env es.Envelope, err error) bool

	// Logger records best-effort warnings — currently, events
	// skipped via OnError. Nil falls back to [slog.Default].
	Logger *slog.Logger
}

// Run starts the subscription, loads the saved checkpoint (or 0),
// decodes each event via Registry, and applies it via Projection.
// After each successful Project (or OnError-skipped event) it saves
// the new position to Checkpoint.
//
// Run returns:
//
//   - nil when the subscription is non-Live and the event log is
//     exhausted, or when the context is canceled.
//   - the iterator's error when the underlying subscription fails.
//   - [es.CodecNotFoundError] when an event arrives whose type has no
//     registered codec.
//   - the projection's error when [es.Projection.Project] returns one
//     and OnError does not request skip.
//   - a wrapped checkpoint error when the checkpoint Save fails.
func (r *Runner) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}

	from := uint64(0)
	if r.Checkpoint != nil {
		pos, found, err := r.Checkpoint.Load(ctx, r.Name)
		if err != nil {
			return fmt.Errorf("synapse/projection: load checkpoint %q: %w", r.Name, err)
		}
		if found {
			from = pos
		}
	}

	opts := es.SubscriptionOptions{From: from, Live: r.Live}
	var seq iter.Seq2[es.RawEnvelope, error]
	if r.Stream != "" {
		seq = r.Store.SubscribeStream(ctx, r.Stream, opts)
	} else {
		seq = r.Store.Subscribe(ctx, opts)
	}

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

		if err := r.Projection.Project(ctx, env); err != nil {
			if r.OnError == nil || !r.OnError(env, err) {
				return err
			}
			logger.WarnContext(ctx, "synapse: projection error, skipping event",
				"name", r.Name,
				"type", env.Type,
				"stream", env.StreamID,
				"position", env.GlobalPosition,
				"err", err,
			)
		}

		if r.Checkpoint != nil {
			pos := raw.GlobalPosition
			if r.Stream != "" {
				pos = raw.Version
			}
			if err := r.Checkpoint.Save(ctx, r.Name, pos); err != nil {
				return fmt.Errorf("synapse/projection: save checkpoint %q at pos %d: %w",
					r.Name, pos, err)
			}
		}
	}
	return nil
}

func (r *Runner) validate() error {
	var missing []string
	if r.Name == "" {
		missing = append(missing, "Name")
	}
	if r.Store == nil {
		missing = append(missing, "Store")
	}
	if r.Registry == nil {
		missing = append(missing, "Registry")
	}
	if r.Projection == nil {
		missing = append(missing, "Projection")
	}
	if len(missing) > 0 {
		return fmt.Errorf("synapse/projection: Runner missing required fields: %v", missing)
	}
	return nil
}

func (r *Runner) decode(raw es.RawEnvelope) (es.Envelope, error) {
	codec, ok := r.Registry.Lookup(raw.Type)
	if !ok {
		return es.Envelope{}, &es.CodecNotFoundError{EventType: raw.Type}
	}
	payload, err := codec.Unmarshal(raw.Payload)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("synapse/projection: unmarshal %s at pos=%d: %w",
			raw.Type, raw.GlobalPosition, err)
	}
	return es.Envelope{
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
	}, nil
}
