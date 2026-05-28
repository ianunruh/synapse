// Package memory provides an in-memory [es.SnapshotStore] suitable
// for tests, examples, and local development.
//
// The store is safe for concurrent use. Snapshots are held in process
// memory only — restarting the program discards them. Save replaces
// any prior snapshot for the same stream.
//
// The store does not deep-copy snapshot payloads or metadata; callers
// must not mutate them after Save or after receiving them from Latest.
package memory

import (
	"context"
	"sync"

	"github.com/ianunruh/synapse/es"
)

// Store is an in-memory [es.SnapshotStore] backed by a map keyed on
// [es.StreamID] and guarded by a single [sync.RWMutex].
type Store struct {
	mu        sync.RWMutex
	snapshots map[es.StreamID]es.RawSnapshot
}

// New returns an empty [Store].
func New() *Store {
	return &Store{snapshots: make(map[es.StreamID]es.RawSnapshot)}
}

// Save implements [es.SnapshotStore].
func (s *Store) Save(ctx context.Context, snap es.RawSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[snap.StreamID] = snap
	return nil
}

// Latest implements [es.SnapshotStore]. Returns (zero, false, nil)
// when no snapshot has been saved for stream.
func (s *Store) Latest(ctx context.Context, stream es.StreamID) (es.RawSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return es.RawSnapshot{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.snapshots[stream]
	return snap, ok, nil
}
