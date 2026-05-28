// Command projection demonstrates the synapse subscription /
// projection machinery. A TotalsProjection consumes events from every
// Counter stream via projection.Runner and aggregates a running total
// per stream plus a grand total across all streams.
//
// The demo walks through:
//
//  1. Catch-up: build derived state from the existing event log.
//  2. Resume: a fresh projection instance, sharing the same
//     CheckpointStore, sees only events past the saved checkpoint.
//     Projection state is the user's; the checkpoint just tracks
//     "how far through the log have I gotten".
//  3. Rebuild: Reset the checkpoint and full-replay.
//  4. Live mode: the Runner blocks waiting for new events and reacts
//     in real time as they're appended.
//
// Run it with:
//
//	go run ./examples/projection
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	checkpointmem "github.com/ianunruh/synapse/checkpointstore/memory"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
)

// --- Domain ---------------------------------------------------------

// Counter mirrors the aggregate from examples/counter, scoped down to
// the events the TotalsProjection cares about.
type Counter struct {
	*es.AggregateBase
	Name  string
	Value int
}

func NewCounter(id es.StreamID) *Counter {
	return &Counter{AggregateBase: es.NewAggregateBase(id)}
}

type CounterCreated struct {
	Name string `json:"name"`
}

type CounterIncremented struct {
	By int `json:"by"`
}

func (c *Counter) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case CounterCreated:
		c.Name = p.Name
	case CounterIncremented:
		c.Value += p.By
	}
}

func (c *Counter) Create(name string) {
	c.Record("counter.created", CounterCreated{Name: name}, c.Apply)
}

func (c *Counter) Increment(by int) {
	c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}

// --- Projection -----------------------------------------------------

// TotalsProjection is an in-memory read model: it tracks the running
// sum of CounterIncremented.By per stream plus a grand total across
// all streams. In production this state would live in a SQL table,
// Redis, etc.; here it is just a map under a mutex.
type TotalsProjection struct {
	mu         sync.Mutex
	perStream  map[es.StreamID]int
	grandTotal int
	eventsSeen int
}

func newTotalsProjection() *TotalsProjection {
	return &TotalsProjection{perStream: make(map[es.StreamID]int)}
}

// Project implements [es.Projection].
func (p *TotalsProjection) Project(_ context.Context, env es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.eventsSeen++
	if inc, ok := env.Payload.(CounterIncremented); ok {
		p.perStream[env.StreamID] += inc.By
		p.grandTotal += inc.By
	}
	return nil
}

func (p *TotalsProjection) Seen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eventsSeen
}

func (p *TotalsProjection) Print(indent string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("%s%d events processed; grand total = %d\n", indent, p.eventsSeen, p.grandTotal)
	for stream, total := range p.perStream {
		fmt.Printf("%s  %s: %d\n", indent, stream, total)
	}
}

// --- Main -----------------------------------------------------------

func main() {
	ctx := context.Background()

	// 1. Wire the library.
	events := memory.New()
	cps := checkpointmem.New()
	reg := es.NewRegistry()
	es.Register(reg, "counter.created", jsoncodec.For[CounterCreated]())
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())

	repo := es.NewRepository(events, reg, NewCounter)

	streams := []struct {
		name string
		id   es.StreamID
	}{
		{"hits", "counter/hits"},
		{"clicks", "counter/clicks"},
		{"views", "counter/views"},
	}

	// 2. Seed three counters with a few increments each.
	fmt.Println("== seeding 3 counters")
	for _, s := range streams {
		c := NewCounter(s.id)
		c.Create(s.name)
		for _, by := range []int{2, 3, 5} {
			c.Increment(by)
		}
		if err := repo.Save(ctx, c); err != nil {
			log.Fatalf("Save: %v", err)
		}
		fmt.Printf("  %q at %s: version %d, value %d\n", s.name, s.id, c.Version(), c.Value)
	}
	fmt.Println()

	// 3. Run 1: catch-up. Build derived state from the existing log.
	fmt.Println("== run 1: catch-up — build derived state from history")
	totals := newTotalsProjection()
	runner1 := projection.NewRunner("totals", events, reg, totals,
		projection.WithCheckpoint(cps))
	if err := runner1.Run(ctx); err != nil {
		log.Fatalf("run 1: %v", err)
	}
	totals.Print("  ")
	showCheckpoint(ctx, cps, "totals")
	fmt.Println()

	// 4. Append more events.
	fmt.Println("== appending +100 to each counter")
	for _, s := range streams {
		loaded, err := repo.Load(ctx, s.id)
		if err != nil {
			log.Fatalf("Load: %v", err)
		}
		loaded.Increment(100)
		if err := repo.Save(ctx, loaded); err != nil {
			log.Fatalf("Save: %v", err)
		}
	}
	fmt.Println()

	// 5. Run 2: resume from checkpoint with a FRESH projection.
	//    The fresh projection sees only events past the saved
	//    checkpoint — its grand_total reflects deltas, not the full
	//    history. Projection state lives in the projection; the
	//    CheckpointStore only persists log position.
	fmt.Println("== run 2: resume from checkpoint with a fresh projection")
	fmt.Println("  (fresh projection sees only events past the saved checkpoint)")
	fresh := newTotalsProjection()
	runner2 := projection.NewRunner("totals", events, reg, fresh,
		projection.WithCheckpoint(cps))
	if err := runner2.Run(ctx); err != nil {
		log.Fatalf("run 2: %v", err)
	}
	fresh.Print("  ")
	showCheckpoint(ctx, cps, "totals")
	fmt.Println()

	// 6. Run 3: rebuild. Reset the checkpoint and full-replay.
	fmt.Println("== run 3: rebuild — Reset the checkpoint and full-replay")
	if err := cps.Reset(ctx, "totals"); err != nil {
		log.Fatalf("Reset: %v", err)
	}
	rebuild := newTotalsProjection()
	runner3 := projection.NewRunner("totals", events, reg, rebuild,
		projection.WithCheckpoint(cps))
	if err := runner3.Run(ctx); err != nil {
		log.Fatalf("run 3: %v", err)
	}
	rebuild.Print("  ")
	showCheckpoint(ctx, cps, "totals")
	fmt.Println()

	// 7. Live mode: start the Runner in a goroutine, then append
	//    more events and watch the projection react in real time.
	fmt.Println("== live mode — Runner blocks waiting for new events")
	liveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	live := newTotalsProjection()
	liveRunner := projection.NewRunner("live-totals", events, reg, live,
		projection.WithCheckpoint(cps),
		projection.WithLive(true))
	done := make(chan error, 1)
	go func() { done <- liveRunner.Run(liveCtx) }()

	const initialEvents = 15 // 3 streams * (1 create + 3 increments) + 3 increments
	waitForSeen(live, initialEvents)
	fmt.Println("  caught up to head:")
	live.Print("    ")

	fmt.Println("  appending +50 to each counter…")
	for _, s := range streams {
		loaded, err := repo.Load(ctx, s.id)
		if err != nil {
			log.Fatalf("Load: %v", err)
		}
		loaded.Increment(50)
		if err := repo.Save(ctx, loaded); err != nil {
			log.Fatalf("Save: %v", err)
		}
	}

	waitForSeen(live, initialEvents+3)
	fmt.Println("  after live appends:")
	live.Print("    ")

	cancel()
	if err := <-done; err != nil {
		log.Fatalf("live Runner: %v", err)
	}
}

func showCheckpoint(ctx context.Context, cps *checkpointmem.Store, name string) {
	pos, found, err := cps.Load(ctx, name)
	if err != nil {
		log.Fatalf("checkpoint Load: %v", err)
	}
	if !found {
		fmt.Printf("  checkpoint %q: (none)\n", name)
		return
	}
	fmt.Printf("  checkpoint %q at GlobalPosition %d\n", name, pos)
}

// waitForSeen polls until the projection has processed at least want
// events, or fatally fails after 2 seconds. The example does not
// instrument cross-goroutine signaling; polling keeps the demo simple
// at the cost of a few wasted milliseconds.
func waitForSeen(p *TotalsProjection, want int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Seen() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Fatalf("projection did not reach %d events (saw %d) within 2s", want, p.Seen())
}
