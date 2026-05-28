// Package checkpointstoretest provides a contract test suite that any
// implementation of [es.CheckpointStore] can run to verify the
// documented behavior.
//
// Usage from a backend's *_test.go:
//
//	func TestMyStore_Contract(t *testing.T) {
//	    checkpointstoretest.RunContract(t, func(t *testing.T) es.CheckpointStore {
//	        return mystore.New(t.TempDir())
//	    })
//	}
//
// The factory returns a fresh, independent store per invocation. Each
// contract subtest calls factory exactly once, so backends register
// cleanup via t.Cleanup inside the factory.
package checkpointstoretest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ianunruh/synapse/es"
)

// Factory returns a fresh [es.CheckpointStore] for each invocation.
type Factory func(t *testing.T) es.CheckpointStore

// TestName is the canonical checkpoint name used by contract tests
// that exercise a single checkpoint.
const TestName = "test-checkpoint"

// RunContract runs every [es.CheckpointStore] contract test against
// the store returned by factory.
func RunContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("Load_EmptyStore", func(t *testing.T) { testLoadEmpty(t, factory) })
	t.Run("Save_Load_RoundTrip", func(t *testing.T) { testSaveLoadRoundTrip(t, factory) })
	t.Run("Save_Overwrites", func(t *testing.T) { testSaveOverwrites(t, factory) })
	t.Run("Reset_RemovesCheckpoint", func(t *testing.T) { testResetRemoves(t, factory) })
	t.Run("Reset_NonExistentName_NoError", func(t *testing.T) { testResetNonExistent(t, factory) })
	t.Run("PerNameIsolation", func(t *testing.T) { testPerNameIsolation(t, factory) })
	t.Run("Save_Zero", func(t *testing.T) { testSaveZero(t, factory) })
	t.Run("Save_ContextCanceled", func(t *testing.T) { testSaveContextCanceled(t, factory) })
	t.Run("Load_ContextCanceled", func(t *testing.T) { testLoadContextCanceled(t, factory) })
	t.Run("Reset_ContextCanceled", func(t *testing.T) { testResetContextCanceled(t, factory) })
	t.Run("Concurrent_SaveAndLoad", func(t *testing.T) { testConcurrent(t, factory) })
}

func testLoadEmpty(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	pos, found, err := store.Load(ctx, TestName)
	if err != nil {
		t.Errorf("Load: %v", err)
	}
	if found {
		t.Errorf("found = true on empty store")
	}
	if pos != 0 {
		t.Errorf("pos = %d, want 0", pos)
	}
}

func testSaveLoadRoundTrip(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if err := store.Save(ctx, TestName, 42); err != nil {
		t.Fatalf("Save: %v", err)
	}

	pos, found, err := store.Load(ctx, TestName)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Errorf("found = false after Save")
	}
	if pos != 42 {
		t.Errorf("pos = %d, want 42", pos)
	}
}

func testSaveOverwrites(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	for _, p := range []uint64{1, 5, 10, 99} {
		if err := store.Save(ctx, TestName, p); err != nil {
			t.Fatalf("Save %d: %v", p, err)
		}
	}

	pos, _, err := store.Load(ctx, TestName)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if pos != 99 {
		t.Errorf("pos = %d, want 99 (last Save wins)", pos)
	}
}

func testResetRemoves(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if err := store.Save(ctx, TestName, 42); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Reset(ctx, TestName); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	pos, found, err := store.Load(ctx, TestName)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if found {
		t.Errorf("found = true after Reset")
	}
	if pos != 0 {
		t.Errorf("pos = %d, want 0", pos)
	}
}

func testResetNonExistent(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)
	if err := store.Reset(ctx, "never-saved"); err != nil {
		t.Errorf("Reset on missing name: %v", err)
	}
}

func testPerNameIsolation(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if err := store.Save(ctx, "a", 10); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if err := store.Save(ctx, "b", 20); err != nil {
		t.Fatalf("Save b: %v", err)
	}

	posA, _, _ := store.Load(ctx, "a")
	posB, _, _ := store.Load(ctx, "b")
	if posA != 10 || posB != 20 {
		t.Errorf("a=%d, b=%d; want 10, 20", posA, posB)
	}

	// Reset one stream, the other is unaffected.
	if err := store.Reset(ctx, "a"); err != nil {
		t.Fatalf("Reset a: %v", err)
	}
	_, foundA, _ := store.Load(ctx, "a")
	posB2, foundB, _ := store.Load(ctx, "b")
	if foundA {
		t.Errorf("found a after Reset a: want false")
	}
	if !foundB || posB2 != 20 {
		t.Errorf("b after Reset a: found=%v pos=%d, want (true, 20)", foundB, posB2)
	}
}

func testSaveZero(t *testing.T, factory Factory) {
	// Saving position 0 is distinct from no checkpoint: Load returns
	// (0, true, nil) afterwards rather than (0, false, nil).
	ctx := t.Context()
	store := factory(t)

	if err := store.Save(ctx, TestName, 0); err != nil {
		t.Fatalf("Save 0: %v", err)
	}

	pos, found, err := store.Load(ctx, TestName)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Errorf("found = false after Save(0); want true")
	}
	if pos != 0 {
		t.Errorf("pos = %d, want 0", pos)
	}
}

func testSaveContextCanceled(t *testing.T, factory Factory) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := factory(t)
	if err := store.Save(ctx, TestName, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Save: err = %v, want context.Canceled", err)
	}
}

func testLoadContextCanceled(t *testing.T, factory Factory) {
	store := factory(t)
	if err := store.Save(t.Context(), TestName, 1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, _, err := store.Load(ctx, TestName); !errors.Is(err, context.Canceled) {
		t.Errorf("Load: err = %v, want context.Canceled", err)
	}
}

func testResetContextCanceled(t *testing.T, factory Factory) {
	store := factory(t)
	if err := store.Save(t.Context(), TestName, 1); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := store.Reset(ctx, TestName); !errors.Is(err, context.Canceled) {
		t.Errorf("Reset: err = %v, want context.Canceled", err)
	}
}

func testConcurrent(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	const N = 64
	var maxSeen atomic.Uint64
	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() { _ = store.Save(ctx, TestName, uint64(i+1)) })
		wg.Go(func() {
			if pos, found, err := store.Load(ctx, TestName); err == nil && found {
				for {
					m := maxSeen.Load()
					if pos <= m || maxSeen.CompareAndSwap(m, pos) {
						break
					}
				}
			}
		})
	}
	wg.Wait()

	final, _, _ := store.Load(ctx, TestName)
	if final < 1 || final > N {
		t.Errorf("final = %d, want 1..%d", final, N)
	}
}
