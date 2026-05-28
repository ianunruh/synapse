package memory_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/eventstoretest"
	"github.com/ianunruh/synapse/eventstore/memory"
)

// ----- Static interface assertions ------------------------------------

func TestStoreImplementsEventStore(t *testing.T) {
	var _ es.EventStore = memory.New()
}

func TestStoreImplementsSubscribable(t *testing.T) {
	var _ es.SubscribableEventStore = memory.New()
}

// ----- Shared contract suite ------------------------------------------

func TestMemoryStore_Contract(t *testing.T) {
	eventstoretest.RunSubscribableContract(t, func(_ *testing.T) es.SubscribableEventStore {
		return memory.New()
	})
}

// ----- Memory-specific tests ------------------------------------------

func TestLoad_SnapshotSemantics(t *testing.T) {
	// The memory store's Load snapshots at call time; later appends do
	// not appear in the iteration of an iterator returned earlier.
	// SQLite-style backends that query rows lazily wouldn't pass this
	// test, so it lives here as a memory-specific guarantee.
	ctx := t.Context()
	store := memory.New()

	stream := es.StreamID("snap-test")
	if _, err := store.Append(ctx, stream, es.NoStream, eventstoretest.MakeEvents(3, stream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seq := store.Load(ctx, stream, es.ReadOptions{})

	// Append more after Load returned.
	if _, err := store.Append(ctx, stream, es.Exact(3), eventstoretest.MakeEvents(2, stream, 4)...); err != nil {
		t.Fatalf("post-load append: %v", err)
	}

	got, err := eventstoretest.Collect(seq)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("snapshot length = %d, want 3 (snapshot taken before second append)", len(got))
	}
}
