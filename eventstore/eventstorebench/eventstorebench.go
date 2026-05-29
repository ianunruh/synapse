// Package eventstorebench provides a benchmark harness any
// implementation of [es.EventStore] can run to measure append and
// load performance under standardized workloads. It is the benchmark
// counterpart to [eventstoretest].
//
// Usage from a backend's *_test.go:
//
//	func BenchmarkMyStore(b *testing.B) {
//	    eventstorebench.Run(b, func(b *testing.B) es.EventStore {
//	        return mystore.New(b.TempDir())
//	    })
//	}
//
// The factory returns a fresh store per sub-benchmark.
package eventstorebench

import (
	"fmt"
	"testing"

	"github.com/ianunruh/synapse/es"
)

// Factory returns a fresh [es.EventStore] for each sub-benchmark.
type Factory func(b *testing.B) es.EventStore

// BenchStream is the canonical stream id the harness uses for its
// single-stream workloads.
const BenchStream es.StreamID = "bench-stream"

// makeEvent constructs a deterministic event payload sized to model a
// small domain event (~50 bytes of JSON).
func makeEvent(stream es.StreamID, version uint64) es.RawEnvelope {
	return es.RawEnvelope{
		EventID:     fmt.Sprintf("evt-%s-%d", stream, version),
		StreamID:    stream,
		Version:     version,
		Type:        "bench.event",
		ContentType: "application/json",
		Payload:     fmt.Appendf(nil, `{"version":%d,"value":"benchmark payload"}`, version),
	}
}

func makeEvents(n int, stream es.StreamID, fromVersion uint64) []es.RawEnvelope {
	out := make([]es.RawEnvelope, n)
	for i := range n {
		out[i] = makeEvent(stream, fromVersion+uint64(i))
	}
	return out
}

// Run drives the full benchmark suite against the store returned by
// factory.
func Run(b *testing.B, factory Factory) {
	b.Helper()
	b.Run("Append_Single", func(b *testing.B) { benchAppendSingle(b, factory) })
	b.Run("Append_Batch_10", func(b *testing.B) { benchAppendBatch(b, factory, 10) })
	b.Run("Load_100", func(b *testing.B) { benchLoad(b, factory, 100) })
	b.Run("Load_1000", func(b *testing.B) { benchLoad(b, factory, 1000) })
}

// benchAppendSingle measures the cost of appending one event at a
// time, using es.Any so the store does not validate revision.
func benchAppendSingle(b *testing.B, factory Factory) {
	ctx := b.Context()
	store := factory(b)

	b.ReportAllocs()
	var version uint64
	for b.Loop() {
		version++
		if _, err := store.Append(ctx, BenchStream, es.Any, makeEvent(BenchStream, version)); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

// benchAppendBatch measures the cost of appending n events per call.
func benchAppendBatch(b *testing.B, factory Factory, n int) {
	ctx := b.Context()
	store := factory(b)

	batch := makeEvents(n, BenchStream, 1)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := store.Append(ctx, BenchStream, es.Any, batch...); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

// benchLoad measures the cost of loading a stream of n events,
// iterating every yielded envelope.
func benchLoad(b *testing.B, factory Factory, n int) {
	ctx := b.Context()
	store := factory(b)

	if _, err := store.Append(ctx, BenchStream, es.NoStream, makeEvents(n, BenchStream, 1)...); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		for _, err := range store.Load(ctx, BenchStream, es.ReadOptions{}) {
			if err != nil {
				b.Fatalf("Load: %v", err)
			}
		}
	}
}
