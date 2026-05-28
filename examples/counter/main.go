// Command counter is an end-to-end demo of the synapse event sourcing
// toolkit. It wires the core packages — es, eventstore/memory,
// snapshotstore/memory, codec/json, idgen — and exercises aggregate
// creation, command execution, rehydration from history,
// optimistic-concurrency conflict detection, automatic snapshot
// taking via es.EveryNVersions, and snapshot-aware Load on a fresh
// Repository.
//
// Run it with:
//
//	go run ./examples/counter
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/idgen"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

// --- Domain ---------------------------------------------------------

// Counter is the aggregate. Embedding *es.AggregateBase gives it
// stream identity, version tracking, and a pending event buffer.
type Counter struct {
	*es.AggregateBase
	Name  string
	Value int
}

// NewCounter is the factory the Repository invokes to construct a
// fresh aggregate before rehydration.
func NewCounter(id es.StreamID) *Counter {
	return &Counter{AggregateBase: es.NewAggregateBase(id)}
}

type CounterCreated struct {
	Name string `json:"name"`
}

type CounterIncremented struct {
	By int `json:"by"`
}

type CounterReset struct{}

// CounterSnapshot is the serialized state of a Counter at a point in
// time. By convention the type name carries a version suffix so the
// codec registry can disambiguate schema evolutions.
type CounterSnapshot struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// Apply mutates in-memory state in response to a single event. It is
// invoked during rehydration AND immediately after a new event is
// recorded — implementations must be deterministic.
func (c *Counter) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case CounterCreated:
		c.Name = p.Name
	case CounterIncremented:
		c.Value += p.By
	case CounterReset:
		c.Value = 0
	}
}

// Create stages a CounterCreated event. A counter may be created at
// most once; subsequent calls fail.
func (c *Counter) Create(name string) error {
	if c.Version() != 0 {
		return fmt.Errorf("counter %q already created", c.StreamID())
	}
	c.Record("counter.created", CounterCreated{Name: name}, c.Apply)
	return nil
}

// Increment stages a CounterIncremented event.
func (c *Counter) Increment(by int) {
	c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}

// Reset stages a CounterReset event.
func (c *Counter) Reset() {
	c.Record("counter.reset", CounterReset{}, c.Apply)
}

// SnapshotType implements es.Snapshotter.
func (c *Counter) SnapshotType() string { return "counter.snapshot.v1" }

// Snapshot implements es.Snapshotter.
func (c *Counter) Snapshot() (any, error) {
	return CounterSnapshot{Name: c.Name, Value: c.Value}, nil
}

// Restore implements es.Snapshotter.
func (c *Counter) Restore(state any) error {
	s, ok := state.(CounterSnapshot)
	if !ok {
		return fmt.Errorf("invalid snapshot type %T", state)
	}
	c.Name = s.Name
	c.Value = s.Value
	return nil
}

// --- Commands -------------------------------------------------------

type IncrementCmd struct{ By int }

func IncrementHandler(_ context.Context, cmd IncrementCmd, c *Counter) error {
	c.Increment(cmd.By)
	return nil
}

// --- Main -----------------------------------------------------------

func main() {
	ctx := context.Background()

	// 1. Wire the library together. Event codecs and the snapshot
	//    codec live in the same Registry, disambiguated by their
	//    type-name namespace.
	events := memory.New()
	snaps := snapmem.New()
	reg := es.NewRegistry()
	es.Register(reg, "counter.created", jsoncodec.For[CounterCreated]())
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())
	es.Register(reg, "counter.reset", jsoncodec.For[CounterReset]())
	es.Register(reg, "counter.snapshot.v1", jsoncodec.For[CounterSnapshot]())

	repo := es.NewRepository(events, reg, NewCounter,
		es.WithIDGenerator(idgen.UUIDv7{}),
		es.WithSnapshotStore(snaps),
		es.WithSnapshotPolicy(es.EveryNVersions(3)))

	stream := es.StreamID("counter/hits")

	// 2. Loading a non-existent stream surfaces *es.StreamNotFoundError.
	fmt.Println("== loading non-existent stream")
	if _, err := repo.Load(ctx, stream); errors.Is(err, es.ErrStreamNotFound) {
		fmt.Printf("  ok: %v\n\n", err)
	}

	// 3. Create the counter and save it. Fresh aggregates use
	//    expected=NoStream automatically inside Save.
	fmt.Println("== creating the counter")
	c := NewCounter(stream)
	if err := c.Create("hits"); err != nil {
		log.Fatalf("Create: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		log.Fatalf("Save: %v", err)
	}
	fmt.Printf("  %q created (version %d)\n\n", c.Name, c.Version())

	// 4. Run a series of increment commands through Execute. With
	//    policy EveryNVersions(3), Save will silently take a
	//    snapshot when the version crosses a multiple of 3.
	fmt.Println("== executing increments")
	for _, by := range []int{1, 1, 5, 2} {
		if err := es.Execute(ctx, repo, stream, IncrementCmd{By: by}, IncrementHandler); err != nil {
			log.Fatalf("Execute: %v", err)
		}
		fmt.Printf("  +%d\n", by)
	}
	fmt.Println()

	// 5. Rehydrate and show the projected state.
	loaded, err := repo.Load(ctx, stream)
	if err != nil {
		log.Fatalf("Load: %v", err)
	}
	fmt.Printf("== current state\n  %s = %d (version %d)\n\n",
		loaded.Name, loaded.Value, loaded.Version())

	// 6. Walk the raw event log straight from the store. The store
	//    deals in opaque bytes; the payload column is whatever the
	//    codec emitted.
	fmt.Println("== event log")
	for env, err := range events.Load(ctx, stream, es.ReadOptions{}) {
		if err != nil {
			log.Fatalf("events.Load: %v", err)
		}
		fmt.Printf("  v%-2d %-21s %s  %s\n",
			env.Version, env.Type,
			env.RecordedAt.Format("15:04:05.000"),
			env.Payload)
	}
	fmt.Println()

	// 7. Load the counter twice, mutate both, save in order. The
	//    second Save loses because expected=Exact(loaded_version) no
	//    longer matches the head the first Save advanced.
	fmt.Println("== concurrent modification")
	a, _ := repo.Load(ctx, stream)
	b, _ := repo.Load(ctx, stream)
	a.Increment(10)
	b.Increment(20)
	if err := repo.Save(ctx, a); err != nil {
		log.Fatalf("Save a: %v", err)
	}
	fmt.Println("  a +=10 saved")
	if err := repo.Save(ctx, b); err != nil {
		fmt.Printf("  b +=20 blocked: %v\n", err)
	}
	fmt.Println()

	// 8. Inspect the snapshot store to see what the policy committed
	//    along the way. EveryNVersions(3) fires when a Save crosses
	//    a multiple of 3, so we expect the most recent snapshot at
	//    the largest such multiple seen.
	fmt.Println("== snapshot store")
	if snap, ok, err := snaps.Latest(ctx, stream); err != nil {
		log.Fatalf("snaps.Latest: %v", err)
	} else if ok {
		fmt.Printf("  v%d  %s  %s\n", snap.Version, snap.Type, snap.Payload)
	} else {
		fmt.Println("  (no snapshot)")
	}
	fmt.Println()

	// 9. Create a fresh Repository that points at the same event and
	//    snapshot stores, then Load. The aggregate state should be
	//    identical to what the original Repository sees — and Load
	//    will have started from the snapshot, replaying only events
	//    past the snapshot version.
	fmt.Println("== fresh Repository load via snapshot")
	fresh := es.NewRepository(events, reg, NewCounter,
		es.WithSnapshotStore(snaps))
	if snap, ok, _ := snaps.Latest(ctx, stream); ok {
		fmt.Printf("  starting from snapshot at v%d, replaying events with version > %d\n",
			snap.Version, snap.Version)
	}
	freshLoaded, err := fresh.Load(ctx, stream)
	if err != nil {
		log.Fatalf("fresh Load: %v", err)
	}
	fmt.Printf("  %s = %d (version %d)\n\n",
		freshLoaded.Name, freshLoaded.Value, freshLoaded.Version())

	// 10. Final state from the original Repository — only a's
	//     increment took.
	final, _ := repo.Load(ctx, stream)
	fmt.Printf("== final state\n  %s = %d (version %d)\n",
		final.Name, final.Value, final.Version())
}
