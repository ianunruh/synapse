package es

import (
	"context"
	"time"
)

// Snapshotter is the optional capability an aggregate type implements
// to support snapshotting. When an aggregate satisfies Snapshotter,
// the [Repository] uses snapshots to skip event replay up to the
// snapshot's version on Load, and (when a [SnapshotPolicy] agrees)
// writes new snapshots after a successful Save.
//
// Aggregates that do not implement Snapshotter still work with a
// Repository configured for snapshots; their Load path simply falls
// through to full event replay and their Save path skips the snapshot
// step.
type Snapshotter interface {
	// SnapshotType returns the type name registered with the codec
	// [Registry] for the snapshot's state value. By convention this
	// includes a version suffix (e.g. "counter.snapshot.v1") so
	// schema evolution is explicit.
	SnapshotType() string

	// Snapshot returns a typed view of the aggregate's domain state.
	// The returned value is passed through the codec registered for
	// [SnapshotType].
	Snapshot() (state any, err error)

	// Restore populates the aggregate from a previously taken
	// snapshot. The state argument is the value returned by the
	// codec's Unmarshal. The [Repository] sets the aggregate's
	// version (via [Aggregate.SetVersion]) after a successful
	// Restore; Restore itself should not touch version state.
	Restore(state any) error
}

// RawSnapshot is the storage-facing form of an aggregate state
// checkpoint, paralleling [RawEnvelope] for events. Payload is opaque
// bytes; [SnapshotStore] implementations do not know about codecs or
// domain types.
type RawSnapshot struct {
	StreamID    StreamID
	Version     uint64
	Type        string
	ContentType ContentType
	RecordedAt  time.Time
	Metadata    Metadata
	Payload     []byte
}

// SnapshotStore persists and retrieves aggregate state checkpoints.
// Unlike [EventStore], snapshots are not append-only: [SnapshotStore.Save]
// replaces any prior snapshot for the same stream.
type SnapshotStore interface {
	// Save persists snap, replacing any earlier snapshot for the
	// same stream.
	Save(ctx context.Context, snap RawSnapshot) error

	// Latest returns the most recent snapshot for stream. The bool
	// is false when no snapshot has been saved for stream; that is
	// not an error.
	Latest(ctx context.Context, stream StreamID) (RawSnapshot, bool, error)
}

// SnapshotPolicy decides whether the [Repository] should take a new
// snapshot after a successful Save. It is invoked with the aggregate
// at its post-Save state, the version before the Save started, and
// the version after the Save's events were applied.
//
// Returning true triggers an immediate, best-effort snapshot save;
// returning false skips it.
type SnapshotPolicy func(agg Aggregate, versionBefore, versionAfter uint64) bool

// EveryNVersions returns a [SnapshotPolicy] that fires when a Save
// advances the aggregate past a multiple of n. With n=100, snapshots
// happen after the first save that reaches v=100, 200, 300, and so
// on. Returns a policy that always returns false when n == 0.
//
// Because the policy compares versionBefore/n with versionAfter/n, it
// fires at most once per multiple of n regardless of batch size — a
// single Save that advances from v=49 to v=210 still triggers only
// one snapshot, not two.
func EveryNVersions(n uint64) SnapshotPolicy {
	return func(_ Aggregate, before, after uint64) bool {
		if n == 0 {
			return false
		}
		return before/n != after/n
	}
}
