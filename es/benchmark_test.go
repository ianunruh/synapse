package es_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

// BenchmarkAggregateBase_Record measures the cost of staging one
// event on an aggregate (Record + the user's Apply). No serialization,
// no store I/O — this is the per-event cost a command method pays
// before the Repository ever sees it.
func BenchmarkAggregateBase_Record(b *testing.B) {
	c := testdomain.NewCounter(testdomain.CounterStream)
	b.ReportAllocs()
	for b.Loop() {
		c.Increment(1)
	}
}

// BenchmarkRegistry_Lookup measures codec lookup. It is on the hot
// path of every Save and every Load.
func BenchmarkRegistry_Lookup(b *testing.B) {
	reg := testdomain.NewRegistry()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = reg.Lookup("counter.incremented")
	}
}

// BenchmarkRepository_Save_1 measures one-event Save: codec marshal,
// metadata stamping, and store append. Each iteration appends a new
// event to the same stream, so it also exercises the aggregate's
// expected-revision tracking under realistic conditions.
func BenchmarkRepository_Save_1(b *testing.B) {
	ctx := b.Context()
	reg := testdomain.NewRegistry()
	repo := es.NewRepository(memory.New(), reg, testdomain.NewCounter)

	c := testdomain.NewCounter(testdomain.CounterStream)

	b.ReportAllocs()
	for b.Loop() {
		c.Increment(1)
		if err := repo.Save(ctx, c); err != nil {
			b.Fatalf("Save: %v", err)
		}
	}
}

// BenchmarkRepository_Save_10 measures a 10-event batch Save. The
// marshal + append work is amortized across the batch; per-iteration
// cost is one Save call carrying ten events.
func BenchmarkRepository_Save_10(b *testing.B) {
	ctx := b.Context()
	reg := testdomain.NewRegistry()
	repo := es.NewRepository(memory.New(), reg, testdomain.NewCounter)

	c := testdomain.NewCounter(testdomain.CounterStream)

	b.ReportAllocs()
	for b.Loop() {
		for range 10 {
			c.Increment(1)
		}
		if err := repo.Save(ctx, c); err != nil {
			b.Fatalf("Save: %v", err)
		}
	}
}

// BenchmarkRepository_Load_10 / _100 / _1000 measure Load latency
// dominated by codec unmarshal + Apply replay over a fixed-size stream.
// The store is seeded once; each iteration Loads the full history.

func BenchmarkRepository_Load_10(b *testing.B)   { benchLoad(b, 10) }
func BenchmarkRepository_Load_100(b *testing.B)  { benchLoad(b, 100) }
func BenchmarkRepository_Load_1000(b *testing.B) { benchLoad(b, 1000) }

func benchLoad(b *testing.B, n int) {
	ctx := b.Context()
	reg := testdomain.NewRegistry()
	repo := es.NewRepository(memory.New(), reg, testdomain.NewCounter)

	c := testdomain.NewCounter(testdomain.CounterStream)
	for range n {
		c.Increment(1)
	}
	if err := repo.Save(ctx, c); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := repo.Load(ctx, testdomain.CounterStream); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}

// BenchmarkExecute measures a full Execute round-trip: Load (with
// snapshot, so per-iteration cost is constant), handler, Save (which
// also writes a new snapshot because the policy fires every version).
// This is the steady-state command-execution latency on an aggregate
// with snapshots enabled.
func BenchmarkExecute(b *testing.B) {
	ctx := b.Context()
	reg := testdomain.NewRegistry()
	repo := es.NewRepository(memory.New(), reg, testdomain.NewCounter,
		es.WithSnapshotStore(snapmem.New()),
		es.WithSnapshotPolicy(es.EveryNVersions(1)))

	seed := testdomain.NewCounter(testdomain.CounterStream)
	seed.Increment(0)
	if err := repo.Save(ctx, seed); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := es.Execute(ctx, repo, testdomain.CounterStream,
			testdomain.IncrementCmd{By: 1}, testdomain.IncrementHandler); err != nil {
			b.Fatalf("Execute: %v", err)
		}
	}
}
