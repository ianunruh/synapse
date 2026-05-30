package postgres_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/eventstorebench"
)

func BenchmarkPostgresStore(b *testing.B) {
	eventstorebench.Run(b, func(b *testing.B) es.EventStore {
		return newStore(b)
	})
}

// BenchmarkPostgresStore_ParallelAppend measures Append throughput
// under N concurrent writers, each appending to its own stream so
// UNIQUE(stream_id, version) does not serialize them. Under the
// advisory-lock design (ADR-0024) every writer queued on one lock and
// the throughput plateaued at single-writer; under the parallel-writer
// design (ADR-0031) throughput should scale with writers up to
// whatever the database can absorb (typically WAL fsync).
//
// b.N appends are distributed across writers; the reported ns/op is
// wall-clock time per append, which falls as parallelism rises.
func BenchmarkPostgresStore_ParallelAppend(b *testing.B) {
	for _, writers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			store := newStore(b)
			ctx := b.Context()
			perWorker := (b.N + writers - 1) / writers

			b.ResetTimer()
			var wg sync.WaitGroup
			for w := range writers {
				stream := es.StreamID(fmt.Sprintf("bench-parallel-%d-%d", writers, w))
				ev := bareEvent(stream)
				wg.Go(func() {
					for range perWorker {
						if _, err := store.Append(ctx, stream, es.Any, ev); err != nil {
							if ctx.Err() != nil {
								return
							}
							b.Errorf("Append: %v", err)
							return
						}
					}
				})
			}
			wg.Wait()
			b.StopTimer()
		})
	}
}

func bareEvent(stream es.StreamID) es.RawEnvelope {
	return es.RawEnvelope{
		EventID:     "evt-" + string(stream),
		StreamID:    stream,
		Type:        "bench.event",
		ContentType: "application/json",
		Payload:     []byte(`{"v":1}`),
	}
}
