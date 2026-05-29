// Package checkpointstorebench provides a benchmark harness any
// implementation of [es.CheckpointStore] can run. It is the benchmark
// counterpart to [checkpointstoretest].
//
// Usage from a backend's *_test.go:
//
//	func BenchmarkMyStore(b *testing.B) {
//	    checkpointstorebench.Run(b, func(b *testing.B) es.CheckpointStore {
//	        return mystore.New(b.TempDir())
//	    })
//	}
package checkpointstorebench

import (
	"testing"

	"github.com/ianunruh/synapse/es"
)

// Factory returns a fresh [es.CheckpointStore] for each sub-benchmark.
type Factory func(b *testing.B) es.CheckpointStore

// BenchName is the canonical checkpoint name used by the harness.
const BenchName = "bench-checkpoint"

// Run drives the full benchmark suite.
func Run(b *testing.B, factory Factory) {
	b.Helper()
	b.Run("Save", func(b *testing.B) { benchSave(b, factory) })
	b.Run("Load", func(b *testing.B) { benchLoad(b, factory) })
}

// benchSave measures the cost of advancing a checkpoint once per
// iteration. Save is idempotent on the (name, position) pair so the
// same name receives advancing positions.
func benchSave(b *testing.B, factory Factory) {
	ctx := b.Context()
	store := factory(b)

	b.ReportAllocs()
	var position uint64
	for b.Loop() {
		position++
		if err := store.Save(ctx, BenchName, position); err != nil {
			b.Fatalf("Save: %v", err)
		}
	}
}

// benchLoad measures the cost of reading the current checkpoint.
func benchLoad(b *testing.B, factory Factory) {
	ctx := b.Context()
	store := factory(b)

	if err := store.Save(ctx, BenchName, 1); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := store.Load(ctx, BenchName); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}
