# ADR-0014: Subscriptions and projections

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0005 Event envelope](0005-event-envelope.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md), [ADR-0013 Snapshotting](0013-snapshotting.md)

## Context

Aggregates produce events; read models, side-effect handlers, and integrations consume them. The toolkit needs:

- Delivery of events in append order, from a caller-chosen position, across all streams or scoped to a single stream.
- A consumer abstraction that maps events to derived state (SQL rows, in-memory aggregates, search indices, outbound emails).
- Per-consumer progress tracking so consumers resume across restarts.
- A driver that wires the three together — subscribe → decode → project → checkpoint — and behaves reasonably on errors and shutdown.

The shape of the event store also has to admit *live* delivery (consumers see new events as they're appended) without requiring polling, while not forcing every backend to support it.

## Decision

### Data model: GlobalPosition

`Envelope` and `RawEnvelope` gain a `GlobalPosition uint64` field. The `EventStore` assigns positions monotonically across all streams during `Append`, on internal copies of the caller's input (the caller's slice is never mutated). `Append`'s signature is unchanged; positions surface through `Load`, `Subscribe`, and `SubscribeStream`. Per-stream `Version` remains; `GlobalPosition` is orthogonal.

### Subscription primitive

```go
type SubscriptionOptions struct {
    From uint64  // GlobalPosition or stream Version, per call site
    Live bool    // block waiting for new events when caught up
}

type SubscribableEventStore interface {
    EventStore
    Subscribe(ctx, opts) iter.Seq2[RawEnvelope, error]
    SubscribeStream(ctx, stream, opts) iter.Seq2[RawEnvelope, error]
}
```

`SubscribableEventStore` is an *extension* interface. Backends that only support catch-up reads through `Load` can omit it; consumers requiring live tail (like `projection.Runner`) fail to type-assert against them. This keeps `EventStore` minimal while making the live-tail capability discoverable.

The in-memory store implements live tail with a close-and-replace signal channel. Subscribers capture the channel under the read lock *before* snapshotting events, so any concurrent Append either appears in the snapshot or has not yet closed this notify channel — there is no missed-wake-up race.

### Projection and CheckpointStore

```go
type Projection interface {
    Project(ctx context.Context, env Envelope) error
}

type CheckpointStore interface {
    Save(ctx, name string, position uint64) error
    Load(ctx, name string) (position uint64, found bool, err error)
    Reset(ctx, name string) error
}
```

`Projection` is one method, deliberately. State lives in the implementation. Idempotency is a documented requirement: the Runner may present the same event twice across restarts.

`CheckpointStore` is symmetric with `SnapshotStore`: an interface in `es`, an in-memory backend in `checkpointstore/memory`, future SQL/Redis backends as siblings.

### Runner

Lives in `es/projection`. Constructed via `NewRunner` with positional required arguments and functional options, mirroring `NewRepository` (see [ADR-0016](0016-constructor-pattern.md)). The Runner type itself has unexported fields:

```go
func NewRunner(
    name string,
    store es.SubscribableEventStore,
    reg *es.Registry,
    proj es.Projection,
    opts ...RunnerOption,
) *Runner

// Optional configuration:
func WithCheckpoint(es.CheckpointStore) RunnerOption
func WithLive(bool) RunnerOption
func WithStream(es.StreamID) RunnerOption          // scope to one stream
func WithOnError(func(env, err) bool) RunnerOption
func WithLogger(*slog.Logger) RunnerOption
```

Flow:

1. Load checkpoint (if Checkpoint set), else start at 0.
2. Subscribe (global or per-stream based on `Stream`).
3. For each event: decode via Registry → Project → save checkpoint.
4. Projection errors: if `OnError` returns true, skip and checkpoint past; else return the error.
5. Context cancellation: return nil (graceful shutdown).
6. Codec missing for an event type: return `*CodecNotFoundError`.

### Layout

```
es/
├── envelope.go          (+ GlobalPosition field on Envelope and RawEnvelope)
├── subscription.go      (SubscriptionOptions, SubscribableEventStore)
└── projection.go        (Projection, CheckpointStore)

es/projection/
├── runner.go            (Runner)
└── runner_test.go

eventstore/memory/
├── memory.go            (stamps GlobalPosition; implements Subscribe and SubscribeStream)
├── subscription_test.go (new tests; existing memory_test.go unchanged)
└── ...

checkpointstore/memory/
├── memory.go
└── memory_test.go
```

## Consequences

- Projections are simple to write: implement `Project`, hand the Runner everything else.
- A single in-process Runner serves the common case. Distributed coordination is *not* in v0 — multiple Runners with the same Name and Checkpoint will race on Save and double-process some events. Idempotent projections survive this.
- Live tail in the in-memory store has unbounded fan-out: every Append broadcasts to every subscriber. For test/dev use this is fine; production backends will use their native push primitives (Postgres LISTEN/NOTIFY, Kafka consumer groups, EventStoreDB persistent subscriptions).
- The `GlobalPosition` addition is a structural change to envelopes, but it doesn't break existing code: structs use named-field initialization throughout, and the field default-zeros gracefully where backends don't assign positions.
- The `Stream`-scoped Runner reuses the same `SubscriptionOptions.From` field with different semantics (stream Version vs GlobalPosition). Documented per method; an alternative was two distinct options types, rejected as more API surface than the savings warrant.

## Alternatives considered

- **Subscribe on EventStore (required).** Rejected: forces every backend to implement live tail, including stores designed only for catch-up reads (file-backed, snapshot-only). Optional extension via `SubscribableEventStore` lets backends opt in.
- **Channels instead of `iter.Seq2`.** Rejected: matches ADR-0008's decision for `Load`. Channels force goroutines on the producer side and make error handling awkward; `iter.Seq2` is the idiomatic Go 1.23+ choice and composes naturally with `range` plus the runner's break/return on error.
- **Polling-based pseudo-live.** Rejected: bakes a bad pattern in (wasted I/O on remote backends, latency at the polling interval). The close-and-replace signal channel is trivial to implement in the memory store and unambiguous for backends to map to native push primitives.
- **Type filters in `SubscriptionOptions`.** Deferred: projections filter inside `Project`. Add when concrete demand shows it's worth the extra API and per-backend implementation work.
- **Per-stream subscription only, no global.** Rejected: cross-stream projections (revenue dashboards, integration outboxes) are common and would require subscribing to each stream individually.
- **`Limit` field in `SubscriptionOptions`.** Rejected: consumers break out of the iterator early to bound the read.
- **CheckpointStore baked into Runner.** Rejected: a separate interface lets users plug a SQL row, a Redis hash, or a file as they need. Matches the SnapshotStore pattern.
- **Default checkpoint every N events for batching.** Deferred: every-event checkpoint is safe by default; batching is an opt-in optimization that can be added without breaking the current interface.
- **Snapshot consumers (projections that consume snapshots instead of events).** Out of scope: snapshots are state checkpoints for aggregates, not a delivery primitive.
