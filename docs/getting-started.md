# Getting started

A walkthrough that builds a small event-sourced service end to end. By the time you finish, you'll have:

- A `Counter` aggregate with apply/event methods.
- A repository wired to an in-memory event store.
- A typed command and a handler.
- A projection that builds a read model from events.
- A snapshot for fast loads.
- The same code running against SQLite for persistence.

Each section adds one concept on top of the last. Finished versions of every snippet live under [`examples/`](../examples) — if you want a runnable starting point, [`examples/counter/main.go`](../examples/counter/main.go) is the closest match to this guide.

If you're new to event sourcing, Martin Fowler's [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) article is a good 10-minute primer. The rest of this doc assumes you know roughly what aggregates and events are.

## Contents

1. [The shape of the library](#1-the-shape-of-the-library)
2. [Build the aggregate](#2-build-the-aggregate)
3. [Wire a registry and a Repository](#3-wire-a-registry-and-a-repository)
4. [Commands](#4-commands)
5. [A projection](#5-a-projection)
6. [Snapshots](#6-snapshots)
7. [Switch to SQLite](#7-switch-to-sqlite)
8. [Where to go next](#8-where-to-go-next)

## Install

```
go get github.com/ianunruh/synapse
```

Requires Go 1.26 or newer.

## 1. The shape of the library

Three interfaces and one driver, in roughly the order you meet them:

- **`es.Aggregate`** — your domain type. State plus an `Apply` method that mutates state from events.
- **`es.EventStore`** — where events live. We start with in-memory; later we swap in SQLite without changing aggregate or repository code.
- **`es.Repository[A]`** — wires the two together. Save an aggregate; the repository serializes pending events through a codec registry and appends them to the store. Load an aggregate; it replays the events back through `Apply`.

Optional pieces we'll add along the way:

- **`es.Projection`** plus `projection.Runner` — consume events to build read models, run side effects, drive integrations.
- **`es.Snapshotter`** plus `SnapshotStore` — skip the replay on large aggregates.
- **`es.CheckpointStore`** — persist projection progress so a restarted runner picks up where it left off.
- **`es.Middleware`** — per-aggregate locking, retry on conflict, observability.

The core `es` package never imports a specific codec or backend. Both are user choices, registered explicitly.

## 2. Build the aggregate

The core domain type is an `es.Aggregate`. The library ships an embeddable `AggregateBase` that handles stream identity, version tracking, and a pending-events queue. Your aggregate writes only the domain-specific `Apply` and the methods that record events.

```go
package main

import (
    "github.com/ianunruh/synapse/es"
)

type Counter struct {
    *es.AggregateBase
    Value int
}

func NewCounter(id es.StreamID) *Counter {
    return &Counter{AggregateBase: es.NewAggregateBase(id)}
}
```

Events are plain Go types you define. The library imposes no base class, no marker interface, and no naming convention. Tag the fields for whichever codec you use:

```go
type CounterIncremented struct {
    By int `json:"by"`
}
```

`Apply` mutates state from a decoded event:

```go
func (c *Counter) Apply(env es.Envelope) {
    if inc, ok := env.Payload.(CounterIncremented); ok {
        c.Value += inc.By
    }
}
```

`Apply` does not return an error — events are facts that already happened; refusing to apply one during rehydration cannot unmake the past. Validate before recording, in the command method.

Methods that "do something" record an event via the embedded `Record` helper:

```go
func (c *Counter) Increment(by int) {
    c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}
```

The third argument tells `Record` how to apply the event — usually your own `Apply` method. `Record` appends the event to the pending queue and bumps the version; the repository writes the queue to the store when you call `Save`.

See [ADR-0004](adr/0004-aggregate-model.md) for the design reasoning (no required interfaces on events, embeddable base, etc.).

## 3. Wire a registry and a Repository

The library is serialization-agnostic. Codecs are registered per event type via an `es.Registry`:

```go
import (
    "github.com/ianunruh/synapse/es"
    jsoncodec "github.com/ianunruh/synapse/codec/json"
)

reg := es.NewRegistry()
es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())
```

A `Repository[A]` wires the event store, the registry, and the aggregate constructor:

```go
import "github.com/ianunruh/synapse/eventstore/memory"

repo := es.NewRepository(memory.New(), reg, NewCounter)
```

Save records pending events to the store:

```go
import "context"

ctx := context.Background()

c := NewCounter("counter/hits")
c.Increment(2)
c.Increment(3)
_ = repo.Save(ctx, c)
```

Load reconstructs the aggregate by replaying its history:

```go
loaded, _ := repo.Load(ctx, "counter/hits")
// loaded.Value == 5
```

Stream identity is just a string (`es.StreamID = type StreamID string`). Domain-level typed IDs live in your own package — define `type OrderID string`, hand `OrderID(x)` to the repository, and the library never has to care. See [ADR-0003](adr/0003-stream-id.md) for why.

## 4. Commands

For anything beyond toy code you'll want validators, retries, locking, or other cross-cutting concerns around the "load → mutate → save" loop. The library provides a typed `Handler` and a convenience `Execute`:

```go
type IncrementCmd struct {
    By int
}

func IncrementHandler(_ context.Context, cmd IncrementCmd, c *Counter) error {
    c.Increment(cmd.By)
    return nil
}

_ = es.Execute(ctx, repo, "counter/hits", IncrementCmd{By: 7}, IncrementHandler)
```

`Execute` loads the aggregate, runs the handler, and saves any events the handler recorded — all in one shot. Middleware in `es/middleware` adds cross-cutting concerns:

```go
import esmw "github.com/ianunruh/synapse/es/middleware"

repo := es.NewRepository(memory.New(), reg, NewCounter,
    es.WithMiddleware(
        esmw.PerAggregateLocking(),
        esmw.Retry(esmw.RetryConfig{MaxAttempts: 3}),
    ))
```

Middleware composes left-to-right: the first wraps the second wraps the third, around the load-handle-save pipeline. See [ADR-0012](adr/0012-command-middleware.md) for the model.

## 5. A projection

A projection consumes events to build derived state — a read model, a side effect, an integration. Implement the one-method `es.Projection`; the library's `projection.Runner` handles subscription, decoding, error policy, and checkpointing.

```go
import "github.com/ianunruh/synapse/es/projection"

type TotalsProjection struct {
    Total int
}

func (p *TotalsProjection) Project(_ context.Context, env es.Envelope) error {
    if inc, ok := env.Payload.(CounterIncremented); ok {
        p.Total += inc.By
    }
    return nil
}
```

The event store needs to support subscriptions for the runner to use it. The in-memory store does:

```go
import checkpointmem "github.com/ianunruh/synapse/checkpointstore/memory"

events := memory.New() // *Store satisfies es.SubscribableEventStore
cps := checkpointmem.New()

totals := &TotalsProjection{}

runner := projection.NewRunner(
    "totals",                          // checkpoint name
    events,                            // SubscribableEventStore
    reg,                               // codec registry
    totals,                            // your Projection
    projection.WithCheckpoint(cps),
)

_ = runner.Run(ctx)
```

`Run` returns when the log is caught up (or when the context is canceled). For real-time consumption, pass `projection.WithLive(true)` and the runner blocks waiting for new events:

```go
runner := projection.NewRunner("totals", events, reg, totals,
    projection.WithCheckpoint(cps),
    projection.WithLive(true))
```

The runner is single-threaded by design. For a real deployment, you run one per goroutine, one per process, or behind leader election — your call. See [ADR-0014](adr/0014-subscriptions-projections.md).

## 6. Snapshots

Loading a long-lived aggregate by replaying every event from the start gets expensive. Snapshots short-circuit that: the repository writes one periodically, and Load restores from it before replaying any newer events.

Add three methods to the aggregate:

```go
import "fmt"

type CounterSnapshot struct {
    Value int `json:"value"`
}

func (c *Counter) SnapshotType() string { return "counter.snapshot.v1" }

func (c *Counter) Snapshot() (any, error) {
    return CounterSnapshot{Value: c.Value}, nil
}

func (c *Counter) Restore(state any) error {
    s, ok := state.(CounterSnapshot)
    if !ok {
        return fmt.Errorf("invalid snapshot %T", state)
    }
    c.Value = s.Value
    return nil
}
```

Register the snapshot codec and wire a snapshot store with a policy:

```go
import snapshotmem "github.com/ianunruh/synapse/snapshotstore/memory"

es.Register(reg, "counter.snapshot.v1", jsoncodec.For[CounterSnapshot]())

repo := es.NewRepository(memory.New(), reg, NewCounter,
    es.WithSnapshotStore(snapshotmem.New()),
    es.WithSnapshotPolicy(es.EveryNVersions(100)),
)
```

`EveryNVersions(100)` takes a snapshot when the aggregate's version crosses a multiple of 100. The repository writes snapshots best-effort: events are committed first, then a snapshot is attempted; failures log via `slog.Default()` rather than failing the Save. See [ADR-0013](adr/0013-snapshotting.md) for the design.

## 7. Switch to SQLite

Everything above runs against in-memory stores. To persist, swap the backends. The interfaces don't change; only the constructors.

The SQLite backends live in sibling Go modules so the root module stays dep-free. Add them as separate `go get`s:

```
go get github.com/ianunruh/synapse/eventstore/sqlite
go get github.com/ianunruh/synapse/snapshotstore/sqlite
go get github.com/ianunruh/synapse/checkpointstore/sqlite
```

Then wire one `*sql.DB` to all three stores:

```go
import (
    "database/sql"

    checkpointsqlite "github.com/ianunruh/synapse/checkpointstore/sqlite"
    eventsqlite "github.com/ianunruh/synapse/eventstore/sqlite"
    snapsqlite "github.com/ianunruh/synapse/snapshotstore/sqlite"

    _ "modernc.org/sqlite"
)

db, _ := sql.Open("sqlite",
    "file:store.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")

events, _ := eventsqlite.New(ctx, db)
snaps,  _ := snapsqlite.New(ctx, db)
cps,    _ := checkpointsqlite.New(ctx, db)

repo := es.NewRepository(events, reg, NewCounter,
    es.WithSnapshotStore(snaps),
    es.WithSnapshotPolicy(es.EveryNVersions(100)))

runner := projection.NewRunner("totals", events, reg, totals,
    projection.WithCheckpoint(cps))
```

`WAL` journal mode and a non-zero `busy_timeout` are strongly recommended for any workload that involves concurrent reads or live subscribers. Each backend's `New` runs `CREATE TABLE IF NOT EXISTS` by default; pass `WithoutMigrate()` if you manage the schema with an external tool like goose or atlas. See [ADR-0017](adr/0017-sqlite-backend.md) and [ADR-0020](adr/0020-sql-schema-migration.md).

A complete program that wires all three SQLite stores together and demonstrates a process restart is in [`examples/persistent/main.go`](../examples/persistent/main.go).

## 8. Where to go next

Runnable examples:

- [`examples/counter`](../examples/counter) — the in-memory walkthrough that mirrors this guide.
- [`examples/projection`](../examples/projection) — projection runner with checkpoint resume, rebuild, and live mode.
- [`examples/persistent`](../examples/persistent) — the SQLite version, with a simulated process restart.

Deeper reading:

- [Architecture Decision Records](adr/) — 21 records covering everything from the aggregate model to the constructor pattern to the contract test suites. The ADRs are the canonical answer to "why is it shaped like this?"
- [`docs/adr/0002-package-layout.md`](adr/0002-package-layout.md) — why the repo is structured the way it is.
- Package docs: [pkg.go.dev/github.com/ianunruh/synapse/es](https://pkg.go.dev/github.com/ianunruh/synapse/es).

Things deliberately out of scope for now:

- Cross-store transactions across heterogeneous backends. We considered them; the conclusion is that the projection runner's at-least-once + idempotency story covers most of what people reach for. See the discussion preserved in the recent commit history if you're curious.
- Distributed coordination of multiple projection runners. The runner is intentionally single-threaded; horizontal scaling is the user's concern (leader election, sharded streams, etc.).
- Admin RPCs and a web UI. Planned as optional sibling subpackages users opt into; not implemented yet.

If you find a sharp edge or want to push back on a design choice, the ADRs are the place to start the conversation.
