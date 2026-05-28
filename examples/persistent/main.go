// Command persistent demonstrates the synapse SQL backends end to
// end. It wires the SQLite event store, snapshot store, and
// checkpoint store all against one *sql.DB and runs the full
// lifecycle of an aggregate, a snapshot, and a projection — then
// closes everything and reopens against the same file to show that
// state survives a process restart.
//
// Run it with:
//
//	go run ./examples/persistent
//
// The demo writes to a temporary directory and removes it on exit;
// to inspect the resulting database file by hand, comment out the
// deferred os.RemoveAll near the top of main.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	checkpointstoresqlite "github.com/ianunruh/synapse/checkpointstore/sqlite"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	eventstoresqlite "github.com/ianunruh/synapse/eventstore/sqlite"
	snapshotstoresqlite "github.com/ianunruh/synapse/snapshotstore/sqlite"

	_ "modernc.org/sqlite"
)

// --- Domain ---------------------------------------------------------

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

type CounterSnapshot struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func (c *Counter) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case CounterCreated:
		c.Name = p.Name
	case CounterIncremented:
		c.Value += p.By
	}
}

func (c *Counter) Create(name string) error {
	if c.Version() != 0 {
		return fmt.Errorf("counter %q already created", c.StreamID())
	}
	c.Record("counter.created", CounterCreated{Name: name}, c.Apply)
	return nil
}

func (c *Counter) Increment(by int) {
	c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}

// es.Snapshotter

func (c *Counter) SnapshotType() string { return "counter.snapshot.v1" }

func (c *Counter) Snapshot() (any, error) {
	return CounterSnapshot{Name: c.Name, Value: c.Value}, nil
}

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

// --- Projection ------------------------------------------------------

// TotalsProjection sums CounterIncremented.By across every event it
// sees. The projection's state is intentionally in-memory only — the
// CheckpointStore tracks "how far through the log have I gotten,"
// and a fresh projection instance after a restart sees only events
// past that point.
type TotalsProjection struct {
	mu    sync.Mutex
	total int
	seen  int
}

func newTotalsProjection() *TotalsProjection {
	return &TotalsProjection{}
}

func (p *TotalsProjection) Project(_ context.Context, env es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen++
	if inc, ok := env.Payload.(CounterIncremented); ok {
		p.total += inc.By
	}
	return nil
}

func (p *TotalsProjection) Stats() (seen, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen, p.total
}

// --- Wiring ----------------------------------------------------------

const streamID es.StreamID = "counter/hits"

func openDB(dsn string) *sql.DB {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	return db
}

func newRegistry() *es.Registry {
	reg := es.NewRegistry()
	es.Register(reg, "counter.created", jsoncodec.For[CounterCreated]())
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())
	es.Register(reg, "counter.snapshot.v1", jsoncodec.For[CounterSnapshot]())
	return reg
}

func newStores(ctx context.Context, db *sql.DB) (es.SubscribableEventStore, es.SnapshotStore, es.CheckpointStore) {
	events, err := eventstoresqlite.New(ctx, db)
	if err != nil {
		log.Fatalf("eventstore.New: %v", err)
	}
	snaps, err := snapshotstoresqlite.New(ctx, db)
	if err != nil {
		log.Fatalf("snapshotstore.New: %v", err)
	}
	cps, err := checkpointstoresqlite.New(ctx, db)
	if err != nil {
		log.Fatalf("checkpointstore.New: %v", err)
	}
	return events, snaps, cps
}

// --- Main ------------------------------------------------------------

func main() {
	ctx := context.Background()

	tmp, err := os.MkdirTemp("", "synapse-persistent-*")
	if err != nil {
		log.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmp)

	dsn := "file:" + filepath.Join(tmp, "store.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	fmt.Printf("== database\n  %s\n\n", dsn)

	runPhase1(ctx, dsn)
	fmt.Println()
	runPhase2(ctx, dsn)
	fmt.Println()
	fmt.Println("== teardown")
	fmt.Println("  temporary database removed")
}

func runPhase1(ctx context.Context, dsn string) {
	fmt.Println("== phase 1: first run")
	db := openDB(dsn)
	defer db.Close()

	events, snaps, cps := newStores(ctx, db)
	reg := newRegistry()

	repo := es.NewRepository(events, reg, NewCounter,
		es.WithSnapshotStore(snaps),
		es.WithSnapshotPolicy(es.EveryNVersions(3)))

	// Create the counter and one initial Save.
	c := NewCounter(streamID)
	if err := c.Create("hits"); err != nil {
		log.Fatal(err)
	}
	if err := repo.Save(ctx, c); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  created %q at version %d\n", c.Name, c.Version())

	// Four increments through Execute. EveryNVersions(3) will take a
	// snapshot when the version crosses a multiple of 3.
	for _, by := range []int{1, 1, 5, 2} {
		if err := es.Execute(ctx, repo, streamID, IncrementCmd{By: by}, IncrementHandler); err != nil {
			log.Fatal(err)
		}
	}

	// Load and show projected state.
	loaded, err := repo.Load(ctx, streamID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  state after 4 increments: %s = %d (version %d)\n",
		loaded.Name, loaded.Value, loaded.Version())

	// Take a manual snapshot at the latest version so phase 2 has a
	// recent restore point.
	if err := repo.SaveSnapshot(ctx, loaded); err != nil {
		log.Fatal(err)
	}
	snap, _, _ := snaps.Latest(ctx, streamID)
	fmt.Printf("  snapshot at version %d (type %q)\n", snap.Version, snap.Type)

	// Run the totals projection in catch-up mode.
	totals := newTotalsProjection()
	runner := projection.NewRunner("totals", events, reg, totals,
		projection.WithCheckpoint(cps))
	if err := runner.Run(ctx); err != nil {
		log.Fatal(err)
	}
	seen, total := totals.Stats()
	fmt.Printf("  projection processed %d %s; grand total = %d\n",
		seen, pluralize(seen, "event"), total)

	if pos, _, _ := cps.Load(ctx, "totals"); pos > 0 {
		fmt.Printf("  checkpoint %q at global position %d\n", "totals", pos)
	}

	showDBStats(ctx, db)
}

func runPhase2(ctx context.Context, dsn string) {
	fmt.Println("== phase 2: simulated restart")
	fmt.Println("  closing all stores; opening fresh ones against the same file…")

	db := openDB(dsn)
	defer db.Close()

	events, snaps, cps := newStores(ctx, db)
	reg := newRegistry()

	repo := es.NewRepository(events, reg, NewCounter,
		es.WithSnapshotStore(snaps))

	// Show the snapshot survived process restart.
	if snap, ok, _ := snaps.Latest(ctx, streamID); ok {
		fmt.Printf("  snapshot found at v%d (Load will start from there)\n", snap.Version)
	}

	loaded, err := repo.Load(ctx, streamID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  loaded counter: %s = %d (version %d) — restored via snapshot\n",
		loaded.Name, loaded.Value, loaded.Version())

	// Add a new command, advancing the stream.
	if err := es.Execute(ctx, repo, streamID, IncrementCmd{By: 100}, IncrementHandler); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  executed +100")

	// Show the checkpoint survived too, then resume.
	if pos, ok, _ := cps.Load(ctx, "totals"); ok {
		fmt.Printf("  checkpoint %q is at position %d; runner will resume from there\n",
			"totals", pos)
	}

	totals := newTotalsProjection()
	runner := projection.NewRunner("totals", events, reg, totals,
		projection.WithCheckpoint(cps))
	if err := runner.Run(ctx); err != nil {
		log.Fatal(err)
	}
	seen, delta := totals.Stats()
	fmt.Printf("  projection processed %d new %s; delta total = %d\n",
		seen, pluralize(seen, "event"), delta)

	if pos, _, _ := cps.Load(ctx, "totals"); pos > 0 {
		fmt.Printf("  checkpoint %q advanced to global position %d\n", "totals", pos)
	}

	showDBStats(ctx, db)
}

// showDBStats prints the row count for each of the three tables this
// demo touches. Useful for confirming that the stores are sharing one
// physical file.
func showDBStats(ctx context.Context, db *sql.DB) {
	fmt.Println("  database tables:")
	for _, tbl := range []string{"events", "snapshots", "checkpoints"} {
		var n int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n)
		if err != nil {
			fmt.Printf("    %s: error (%v)\n", tbl, err)
			continue
		}
		fmt.Printf("    %s: %d %s\n", tbl, n, pluralize(n, "row"))
	}
}

func pluralize(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}
