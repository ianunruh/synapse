// Package snapshotstorebench provides a benchmark harness any
// implementation of [es.SnapshotStore] can run. It is the benchmark
// counterpart to [snapshotstoretest].
//
// Usage from a backend's *_test.go:
//
//	func BenchmarkMyStore(b *testing.B) {
//	    snapshotstorebench.Run(b, func(b *testing.B) es.SnapshotStore {
//	        return mystore.New(b.TempDir())
//	    })
//	}
package snapshotstorebench

import (
	"fmt"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
)

// Factory returns a fresh [es.SnapshotStore] for each sub-benchmark.
type Factory func(b *testing.B) es.SnapshotStore

// BenchStream is the canonical stream id used by the harness.
const BenchStream es.StreamID = "bench-stream"

// makeSnapshot constructs a deterministic snapshot payload (~80 bytes
// of JSON) for a given version.
func makeSnapshot(version uint64) es.RawSnapshot {
	return es.RawSnapshot{
		StreamID:    BenchStream,
		Version:     version,
		Type:        "bench.snapshot.v1",
		ContentType: "application/json",
		RecordedAt:  time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Payload:     fmt.Appendf(nil, `{"version":%d,"value":"benchmark snapshot payload"}`, version),
	}
}

// Run drives the full benchmark suite.
func Run(b *testing.B, factory Factory) {
	b.Helper()
	b.Run("Save", func(b *testing.B) { benchSave(b, factory) })
	b.Run("Latest", func(b *testing.B) { benchLatest(b, factory) })
}

// benchSave measures the cost of one Save per iteration. Save is
// allowed to overwrite, so the same stream gets re-saved at advancing
// versions.
func benchSave(b *testing.B, factory Factory) {
	ctx := b.Context()
	store := factory(b)

	b.ReportAllocs()
	var version uint64
	for b.Loop() {
		version++
		if err := store.Save(ctx, makeSnapshot(version)); err != nil {
			b.Fatalf("Save: %v", err)
		}
	}
}

// benchLatest measures the cost of fetching the latest snapshot.
func benchLatest(b *testing.B, factory Factory) {
	ctx := b.Context()
	store := factory(b)

	if err := store.Save(ctx, makeSnapshot(1)); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := store.Latest(ctx, BenchStream); err != nil {
			b.Fatalf("Latest: %v", err)
		}
	}
}
