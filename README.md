# synapse

A small Go event sourcing and CQRS toolkit. Composable primitives — aggregates, events, repositories, projections — with zero third-party deps in the core and pluggable backends for events, snapshots, and projection checkpoints.

## Status

Pre-1.0. The public surface is still moving; expect minor breaking changes between tagged versions. The persistent stack (SQLite events + snapshots + checkpoints, one file) is shaping up production-ready but has not yet seen production traffic.

## Install

```
go get github.com/ianunruh/synapse
```

Requires Go 1.26 or newer.

## Quick look

```go
package main

import (
    "context"

    jsoncodec "github.com/ianunruh/synapse/codec/json"
    "github.com/ianunruh/synapse/es"
    "github.com/ianunruh/synapse/eventstore/memory"
)

// Define an aggregate by embedding *es.AggregateBase.

type Counter struct {
    *es.AggregateBase
    Value int
}

func NewCounter(id es.StreamID) *Counter {
    return &Counter{AggregateBase: es.NewAggregateBase(id)}
}

type CounterIncremented struct {
    By int `json:"by"`
}

func (c *Counter) Apply(env es.Envelope) error {
    if inc, ok := env.Payload.(CounterIncremented); ok {
        c.Value += inc.By
    }
    return nil
}

func (c *Counter) Increment(by int) error {
    return c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}

func main() {
    ctx := context.Background()

    reg := es.NewRegistry()
    es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())

    repo := es.NewRepository(memory.New(), reg, NewCounter)

    c := NewCounter("counter/hits")
    _ = c.Increment(2)
    _ = c.Increment(3)
    _ = repo.Save(ctx, c)

    loaded, _ := repo.Load(ctx, "counter/hits")
    _ = loaded.Value // == 5
}
```

A full walkthrough is in [`docs/getting-started.md`](docs/getting-started.md). Runnable examples live under [`examples/`](examples).

## Documentation

- [Getting started](docs/getting-started.md) — a 20-minute walkthrough that builds the Counter aggregate above into a service with commands, a projection, snapshots, and a SQLite backend.
- [Architecture Decision Records](docs/adr/) — 21 records explaining why the library is shaped the way it is.
- Package docs: [pkg.go.dev/github.com/ianunruh/synapse/es](https://pkg.go.dev/github.com/ianunruh/synapse/es).

## Packages

The repo is a Go workspace. Core interfaces live in `es`; backends and contract suites are sibling packages — or sibling modules when they pull in third-party deps.

| Package | Module | What it is |
|---|---|---|
| `es` | root | Aggregate, Repository, Event, Envelope, Snapshotter, Projection, etc. |
| `es/middleware` | root | Built-in middleware: PerAggregateLocking, Retry |
| `es/projection` | root | Runner for read-model projections |
| `codec/json` | root | JSON event/snapshot codec |
| `idgen` | root | UUIDv7 identifier generator |
| `eventstore/memory` | root | In-memory event store |
| `eventstore/sqlite` | sibling | SQLite event store |
| `eventstore/eventstoretest` | root | Backend contract test suite |
| `snapshotstore/memory` | root | In-memory snapshot store |
| `snapshotstore/sqlite` | sibling | SQLite snapshot store |
| `snapshotstore/snapshotstoretest` | root | Backend contract test suite |
| `checkpointstore/memory` | root | In-memory checkpoint store |
| `checkpointstore/sqlite` | sibling | SQLite checkpoint store |
| `checkpointstore/checkpointstoretest` | root | Backend contract test suite |
| `examples/counter` | root | In-memory event sourcing walkthrough |
| `examples/projection` | root | Projection runner walkthrough |
| `examples/persistent` | sibling | SQLite-backed end-to-end demo |

Sibling modules each have their own `go.mod`. A `go.work` file at the repo root ties them together for local development. The root module has zero third-party deps; the SQLite backends transitively pull in [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) (pure Go, no CGo).

## Design principles

Recorded as Architecture Decision Records (starting at [ADR-0001](docs/adr/0001-foundation.md)):

- Go 1.26 toolchain, language features and stdlib used to current capability.
- Zero third-party deps in the root module. Backends that need them live as sibling Go modules.
- Modernization-clean. `gopls modernize ./...` exits 0 in every module.
- Serialization-agnostic core. Codecs are registered per event type; the `es` package never imports a specific codec.
- Type safety and performance are co-equal goals. Where they point the same direction (most cases), take both. Where they conflict, prefer the perf-friendly option in hot paths and document the trade-off.
- Admin RPCs and a web UI, when they exist, will be optional sibling subpackages users opt into.

## License

Apache 2.0. See [LICENSE](LICENSE).
