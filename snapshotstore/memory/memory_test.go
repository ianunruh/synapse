package memory_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

const testStream es.StreamID = "test-stream"

func makeSnapshot(stream es.StreamID, version uint64) es.RawSnapshot {
	return es.RawSnapshot{
		StreamID:    stream,
		Version:     version,
		Type:        "counter.snapshot.v1",
		ContentType: "application/json",
		RecordedAt:  time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Payload:     []byte(`{"count":42}`),
	}
}

func TestStoreImplementsSnapshotStore(t *testing.T) {
	var _ es.SnapshotStore = snapmem.New()
}

func TestLatest_EmptyStore(t *testing.T) {
	ctx := t.Context()
	store := snapmem.New()

	snap, ok, err := store.Latest(ctx, testStream)
	if err != nil {
		t.Errorf("Latest: %v", err)
	}
	if ok {
		t.Errorf("ok = true, want false on empty store")
	}
	if snap.StreamID != "" || snap.Version != 0 || snap.Type != "" || len(snap.Payload) != 0 {
		t.Errorf("snap = %+v, want zero RawSnapshot", snap)
	}
}

func TestSave_Latest_RoundTrip(t *testing.T) {
	ctx := t.Context()
	store := snapmem.New()

	in := makeSnapshot(testStream, 7)
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, ok, err := store.Latest(ctx, testStream)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false after Save")
	}
	if out.StreamID != in.StreamID || out.Version != in.Version ||
		out.Type != in.Type || out.ContentType != in.ContentType ||
		!out.RecordedAt.Equal(in.RecordedAt) ||
		!slices.Equal(out.Payload, in.Payload) {
		t.Errorf("round-trip mismatch:\nin  = %+v\nout = %+v", in, out)
	}
}

func TestSave_Overwrites(t *testing.T) {
	// Save with the same stream id replaces the prior snapshot.
	ctx := t.Context()
	store := snapmem.New()

	if err := store.Save(ctx, makeSnapshot(testStream, 1)); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	if err := store.Save(ctx, makeSnapshot(testStream, 9)); err != nil {
		t.Fatalf("Save 2: %v", err)
	}

	snap, ok, err := store.Latest(ctx, testStream)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false")
	}
	if snap.Version != 9 {
		t.Errorf("snap.Version = %d, want 9 (Save should overwrite)", snap.Version)
	}
}

func TestLatest_PerStreamIsolation(t *testing.T) {
	ctx := t.Context()
	store := snapmem.New()

	if err := store.Save(ctx, makeSnapshot("stream-a", 1)); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if err := store.Save(ctx, makeSnapshot("stream-b", 2)); err != nil {
		t.Fatalf("Save b: %v", err)
	}

	a, _, _ := store.Latest(ctx, "stream-a")
	b, _, _ := store.Latest(ctx, "stream-b")
	if a.Version != 1 || b.Version != 2 {
		t.Errorf("a.Version=%d, b.Version=%d; want 1, 2", a.Version, b.Version)
	}
}

func TestSave_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := snapmem.New()
	if err := store.Save(ctx, makeSnapshot(testStream, 1)); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestLatest_ContextCanceled(t *testing.T) {
	store := snapmem.New()
	if err := store.Save(t.Context(), makeSnapshot(testStream, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err := store.Latest(ctx, testStream)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestConcurrent_SaveAndLatest(t *testing.T) {
	// Many goroutines hammer Save and Latest on the same stream; the
	// store must not panic and must always return a consistent
	// snapshot.
	ctx := t.Context()
	store := snapmem.New()

	const N = 256
	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() { _ = store.Save(ctx, makeSnapshot(testStream, uint64(i+1))) })
		wg.Go(func() { _, _, _ = store.Latest(ctx, testStream) })
	}
	wg.Wait()

	snap, ok, err := store.Latest(ctx, testStream)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false after concurrent saves")
	}
	if snap.Version < 1 || snap.Version > N {
		t.Errorf("snap.Version = %d, want 1..%d", snap.Version, N)
	}
}
