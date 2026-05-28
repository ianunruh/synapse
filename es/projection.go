package es

import "context"

// Projection is the consumer side of an event-sourcing read model. It
// receives decoded events from a [SubscribableEventStore] — typically
// via a projection.Runner — and applies them to derived state: a SQL
// table, an in-memory map, a search index, an outbound integration,
// etc.
//
// Implementations should be deterministic and idempotent. The Runner
// may present the same event twice on retry after a checkpoint-write
// failure, and Live subscribers reconnecting from a checkpoint may
// also re-encounter the boundary event.
type Projection interface {
	Project(ctx context.Context, env Envelope) error
}

// CheckpointStore persists per-projection progress so consumers can
// resume across restarts.
//
// The name parameter is a caller-chosen identifier — typically the
// projection's logical name. Names must be unique per concurrent
// consumer.
type CheckpointStore interface {
	// Save persists the last successfully processed position for
	// name. Implementations should treat repeated Save calls with
	// the same (name, position) as idempotent.
	Save(ctx context.Context, name string, position uint64) error

	// Load returns the last saved position for name. The bool is
	// false when no checkpoint has been saved for name; that is not
	// an error.
	Load(ctx context.Context, name string) (position uint64, found bool, err error)

	// Reset removes the checkpoint for name. Subsequent Loads return
	// (0, false, nil). Used for projection rebuilds.
	Reset(ctx context.Context, name string) error
}
