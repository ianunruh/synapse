// Package memory provides an in-memory [es.CheckpointStore] suitable
// for tests, examples, and local development.
//
// The store is safe for concurrent use. Checkpoints are held in
// process memory only — restarting the program discards them. Save
// replaces any prior checkpoint for the same name.
package memory

import (
	"context"
	"sync"
)

// Store is an in-memory [es.CheckpointStore] backed by a map under a
// single [sync.RWMutex].
type Store struct {
	mu     sync.RWMutex
	points map[string]uint64
}

// New returns an empty [Store].
func New() *Store {
	return &Store{points: make(map[string]uint64)}
}

// Save implements [es.CheckpointStore].
func (s *Store) Save(ctx context.Context, name string, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.points[name] = position
	return nil
}

// Load implements [es.CheckpointStore].
func (s *Store) Load(ctx context.Context, name string) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	pos, ok := s.points[name]
	return pos, ok, nil
}

// Reset implements [es.CheckpointStore].
func (s *Store) Reset(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.points, name)
	return nil
}
