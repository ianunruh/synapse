package memory_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ianunruh/synapse/checkpointstore/memory"
	"github.com/ianunruh/synapse/es"
)

func TestStoreImplementsCheckpointStore(t *testing.T) {
	var _ es.CheckpointStore = memory.New()
}

func TestLoad_EmptyStore(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	pos, found, err := store.Load(ctx, "any-name")
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

func TestSave_Load_RoundTrip(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	if err := store.Save(ctx, "p1", 42); err != nil {
		t.Fatalf("Save: %v", err)
	}

	pos, found, err := store.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Errorf("found = false")
	}
	if pos != 42 {
		t.Errorf("pos = %d, want 42", pos)
	}
}

func TestSave_Overwrites(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	for _, p := range []uint64{1, 5, 10, 99} {
		if err := store.Save(ctx, "p1", p); err != nil {
			t.Fatalf("Save %d: %v", p, err)
		}
	}

	pos, _, err := store.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if pos != 99 {
		t.Errorf("pos = %d, want 99 (last Save wins)", pos)
	}
}

func TestReset_RemovesCheckpoint(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	if err := store.Save(ctx, "p1", 42); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Reset(ctx, "p1"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	pos, found, err := store.Load(ctx, "p1")
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

func TestReset_NonExistentName_NoError(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	if err := store.Reset(ctx, "never-saved"); err != nil {
		t.Errorf("Reset on missing name: %v", err)
	}
}

func TestPerNameIsolation(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

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
}

func TestContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := memory.New()
	if err := store.Save(ctx, "p", 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Save: err = %v, want context.Canceled", err)
	}
	if _, _, err := store.Load(ctx, "p"); !errors.Is(err, context.Canceled) {
		t.Errorf("Load: err = %v, want context.Canceled", err)
	}
	if err := store.Reset(ctx, "p"); !errors.Is(err, context.Canceled) {
		t.Errorf("Reset: err = %v, want context.Canceled", err)
	}
}

func TestConcurrent_SaveAndLoad(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	const N = 256
	var wg sync.WaitGroup
	var maxSeen atomic.Uint64
	for i := range N {
		wg.Go(func() {
			_ = store.Save(ctx, "p", uint64(i+1))
		})
		wg.Go(func() {
			if pos, found, err := store.Load(ctx, "p"); err == nil && found {
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

	final, _, _ := store.Load(ctx, "p")
	if final < 1 || final > N {
		t.Errorf("final = %d, want 1..%d", final, N)
	}
}
