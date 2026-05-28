// Package snapshotstoretest provides a contract test suite that any
// implementation of [es.SnapshotStore] can run to verify the
// documented behavior.
//
// Usage from a backend's *_test.go:
//
//	func TestMyStore_Contract(t *testing.T) {
//	    snapshotstoretest.RunContract(t, func(t *testing.T) es.SnapshotStore {
//	        return mystore.New(t.TempDir())
//	    })
//	}
//
// The factory returns a fresh, independent store per invocation. Each
// contract subtest calls factory exactly once, so backends register
// cleanup via t.Cleanup inside the factory.
package snapshotstoretest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
)

// Factory returns a fresh [es.SnapshotStore] for each invocation.
type Factory func(t *testing.T) es.SnapshotStore

// TestStream is the canonical stream id used by contract tests that
// exercise a single stream.
const TestStream es.StreamID = "test-stream"

// MakeSnapshot constructs a deterministic [es.RawSnapshot] for tests.
func MakeSnapshot(stream es.StreamID, version uint64) es.RawSnapshot {
	return es.RawSnapshot{
		StreamID:    stream,
		Version:     version,
		Type:        "test.snapshot.v1",
		ContentType: "application/json",
		RecordedAt:  time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Payload:     fmt.Appendf(nil, `{"version":%d}`, version),
	}
}

// RunContract runs every [es.SnapshotStore] contract test against the
// store returned by factory.
func RunContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("Latest_EmptyStore", func(t *testing.T) { testLatestEmpty(t, factory) })
	t.Run("Save_Latest_RoundTrip", func(t *testing.T) { testSaveLatestRoundTrip(t, factory) })
	t.Run("Save_Overwrites", func(t *testing.T) { testSaveOverwrites(t, factory) })
	t.Run("Latest_PerStreamIsolation", func(t *testing.T) { testPerStreamIsolation(t, factory) })
	t.Run("MetadataRoundTrip", func(t *testing.T) { testMetadataRoundTrip(t, factory) })
	t.Run("Save_ContextCanceled", func(t *testing.T) { testSaveContextCanceled(t, factory) })
	t.Run("Latest_ContextCanceled", func(t *testing.T) { testLatestContextCanceled(t, factory) })
	t.Run("Concurrent_SaveAndLatest", func(t *testing.T) { testConcurrent(t, factory) })
}

func testLatestEmpty(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	snap, ok, err := store.Latest(ctx, TestStream)
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

func testSaveLatestRoundTrip(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	in := MakeSnapshot(TestStream, 7)
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, ok, err := store.Latest(ctx, TestStream)
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

func testSaveOverwrites(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if err := store.Save(ctx, MakeSnapshot(TestStream, 1)); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	if err := store.Save(ctx, MakeSnapshot(TestStream, 9)); err != nil {
		t.Fatalf("Save 2: %v", err)
	}

	snap, ok, err := store.Latest(ctx, TestStream)
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

func testPerStreamIsolation(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if err := store.Save(ctx, MakeSnapshot("stream-a", 1)); err != nil {
		t.Fatalf("Save a: %v", err)
	}
	if err := store.Save(ctx, MakeSnapshot("stream-b", 2)); err != nil {
		t.Fatalf("Save b: %v", err)
	}

	a, _, _ := store.Latest(ctx, "stream-a")
	b, _, _ := store.Latest(ctx, "stream-b")
	if a.Version != 1 || b.Version != 2 {
		t.Errorf("a.Version=%d, b.Version=%d; want 1, 2", a.Version, b.Version)
	}
}

func testMetadataRoundTrip(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	in := MakeSnapshot(TestStream, 1)
	in.Metadata = es.Metadata{"actor": "alice", "trace_id": "abc-123"}

	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("Save: %v", err)
	}

	out, ok, err := store.Latest(ctx, TestStream)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatalf("ok = false")
	}
	if out.Metadata["actor"] != "alice" || out.Metadata["trace_id"] != "abc-123" {
		t.Errorf("metadata round-trip failed: %+v", out.Metadata)
	}
}

func testSaveContextCanceled(t *testing.T, factory Factory) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := factory(t)
	if err := store.Save(ctx, MakeSnapshot(TestStream, 1)); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func testLatestContextCanceled(t *testing.T, factory Factory) {
	store := factory(t)
	if err := store.Save(t.Context(), MakeSnapshot(TestStream, 1)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err := store.Latest(ctx, TestStream)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func testConcurrent(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	const N = 64
	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() { _ = store.Save(ctx, MakeSnapshot(TestStream, uint64(i+1))) })
		wg.Go(func() { _, _, _ = store.Latest(ctx, TestStream) })
	}
	wg.Wait()

	snap, ok, err := store.Latest(ctx, TestStream)
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
