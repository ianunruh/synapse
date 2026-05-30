// Command postgres demonstrates the synapse Postgres backends end to
// end. It wires the Postgres event store, snapshot store, and
// checkpoint store all against one *pgxpool.Pool, runs the full
// lifecycle of an aggregate plus a snapshot plus a projection, then
// closes everything and reopens against the same database to show that
// state survives a process restart.
//
// It mirrors examples/persistent (which uses SQLite) so the differences
// between the two backends are easy to compare side by side.
//
// Run:
//
//	# Quick local setup:
//	docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name synapse-demo postgres:17-alpine
//
//	# Default DSN; override with POSTGRES_URL if your instance is elsewhere.
//	go run ./examples/postgres
//
//	# Teardown:
//	docker stop synapse-demo
//
// The program drops any prior tables it created so successive runs
// start from a clean state. Set POSTGRES_URL to point at a different
// instance:
//
//	POSTGRES_URL=postgres://user:pw@host:5432/db?sslmode=disable go run ./examples/postgres
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	checkpointstorepostgres "github.com/ianunruh/synapse/checkpointstore/postgres"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	eventstorepostgres "github.com/ianunruh/synapse/eventstore/postgres"
	snapshotstorepostgres "github.com/ianunruh/synapse/snapshotstore/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
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

type TotalsProjection struct {
	mu    sync.Mutex
	total int
	seen  int
}

func newTotalsProjection() *TotalsProjection { return &TotalsProjection{} }

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

// --- Wiring ---------------------------------------------------------

const streamID es.StreamID = "counter/hits"

func newRegistry() *es.Registry {
	reg := es.NewRegistry()
	es.Register(reg, "counter.created", jsoncodec.For[CounterCreated]())
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())
	es.Register(reg, "counter.snapshot.v1", jsoncodec.For[CounterSnapshot]())
	return reg
}

func openPool(ctx context.Context, dsn string) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pgxpool.New: %v", err)
	}
	return pool
}

// newStores builds all three stores against pool. The event-store's
// shared LISTEN goroutine (ADR-0025) holds one connection for its
// lifetime; the returned Close function stops it before the pool is
// torn down.
func newStores(
	ctx context.Context,
	pool *pgxpool.Pool,
) (events *eventstorepostgres.Store, snaps es.SnapshotStore, cps es.CheckpointStore, closeAll func()) {
	events, err := eventstorepostgres.New(ctx, pool)
	if err != nil {
		log.Fatalf("eventstore.New: %v", err)
	}
	snaps, err = snapshotstorepostgres.New(ctx, pool)
	if err != nil {
		log.Fatalf("snapshotstore.New: %v", err)
	}
	cps, err = checkpointstorepostgres.New(ctx, pool)
	if err != nil {
		log.Fatalf("checkpointstore.New: %v", err)
	}
	closeAll = func() {
		events.Close() // releases the LISTEN connection (ADR-0025)
	}
	return
}

func resetDatabase(ctx context.Context, pool *pgxpool.Pool) {
	// Idempotent reset so each run starts fresh. In a real service
	// migrations would be one-shot, not torn down between runs.
	for _, table := range []string{"events", "snapshots", "checkpoints"} {
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			log.Fatalf("DROP TABLE %s: %v", table, err)
		}
	}
}

// --- Main ----------------------------------------------------------

func main() {
	ctx := context.Background()

	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	fmt.Printf("== database\n  %s\n\n", dsn)

	// Pre-flight: reset state so the demo always starts fresh.
	resetPool := openPool(ctx, dsn)
	resetDatabase(ctx, resetPool)
	resetPool.Close()

	runPhase1(ctx, dsn)
	fmt.Println()
	runPhase2(ctx, dsn)
	fmt.Println()
	fmt.Println("== teardown")
	fmt.Println("  state retained in Postgres; rerun the program for a fresh start")
}

func runPhase1(ctx context.Context, dsn string) {
	fmt.Println("== phase 1: write")
	pool := openPool(ctx, dsn)
	defer pool.Close()

	events, snaps, cps, closeStores := newStores(ctx, pool)
	defer closeStores()
	reg := newRegistry()

	repo := es.NewRepository(events, reg, NewCounter,
		es.WithSnapshotStore(snaps),
		es.WithSnapshotPolicy(es.EveryNVersions(3)))

	// Create the aggregate.
	c := NewCounter(streamID)
	if err := c.Create("hits"); err != nil {
		log.Fatal(err)
	}
	if err := repo.Save(ctx, c); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  created %q at version %d\n", c.Name, c.Version())

	// Four increments through Execute. EveryNVersions(3) takes a
	// snapshot when the version crosses a multiple of 3.
	for _, by := range []int{1, 1, 5, 2} {
		if err := es.Execute(ctx, repo, streamID, IncrementCmd{By: by}, IncrementHandler); err != nil {
			log.Fatal(err)
		}
	}

	loaded, err := repo.Load(ctx, streamID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  state after 4 increments: %s = %d (version %d)\n",
		loaded.Name, loaded.Value, loaded.Version())

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

	showRowCounts(ctx, pool)
}

func runPhase2(ctx context.Context, dsn string) {
	fmt.Println("== phase 2: simulated restart")
	fmt.Println("  closing all stores; opening fresh ones against the same database…")
	pool := openPool(ctx, dsn)
	defer pool.Close()

	events, snaps, cps, closeStores := newStores(ctx, pool)
	defer closeStores()
	reg := newRegistry()

	repo := es.NewRepository(events, reg, NewCounter,
		es.WithSnapshotStore(snaps))

	if snap, ok, _ := snaps.Latest(ctx, streamID); ok {
		fmt.Printf("  snapshot found at v%d (Load will start from there)\n", snap.Version)
	}

	loaded, err := repo.Load(ctx, streamID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  loaded counter: %s = %d (version %d) — restored via snapshot\n",
		loaded.Name, loaded.Value, loaded.Version())

	if err := es.Execute(ctx, repo, streamID, IncrementCmd{By: 100}, IncrementHandler); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  executed +100")

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

	showRowCounts(ctx, pool)
}

// showRowCounts prints the row count for each of the three tables this
// demo touches. Useful for confirming all three stores share one
// database.
func showRowCounts(ctx context.Context, pool *pgxpool.Pool) {
	fmt.Println("  database tables:")
	for _, tbl := range []string{"events", "snapshots", "checkpoints"} {
		var n int
		err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n)
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
