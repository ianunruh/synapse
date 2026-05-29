# ADR-0027: Subscription type filters + checkpoint batching

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md) (implements two of its deferred items), [ADR-0018 Backend contract tests](0018-backend-contract-tests.md), [ADR-0025 Postgres shared listener](0025-postgres-shared-listener.md)

## Context

ADR-0014 shipped subscriptions and the projection `Runner` and explicitly **deferred** two items "until concrete demand":

- **Type filters in `SubscriptionOptions`** — until now a subscriber received every event type and filtered inside `Project`, which also meant a projection scoped to a few types still had to register codecs for (or otherwise tolerate) every other type in the log, or trip `CodecNotFoundError`.
- **Checkpoint-every-N batching** — the `Runner` saved a checkpoint after *every* event. Safe, but one checkpoint write per event is wasteful for high-throughput projections whose store round-trips dominate.

Both are now wanted, and both can be added without reshaping the existing interfaces.

## Decision

### Type filters

Add one field to `SubscriptionOptions`:

```go
type SubscriptionOptions struct {
    From  uint64
    Live  bool
    Types []string // nil/empty = all types; else deliver only these Type values
}
```

Matching is exact membership on `RawEnvelope.Type`. It is a **delivery** filter, orthogonal to stream scoping: it applies identically to `Subscribe` (global, across all streams) and `SubscribeStream` (one stream). Positions are unchanged — the cursor advances over filtered-out events, so a filtered stream's delivered positions are non-contiguous, and resuming from a delivered event's position stays correct.

Where the filter sits in the table → subscriber flow:

```
   Append (one tx): INSERT rows ──► [ events table ]
                          │
                          └── wake ──►(*)─── notify channel (close-and-replace)
                                                     │
   subscribeLoop  (one per Subscribe / SubscribeStream call)                │
   ┌─────────────────────────────────────────────────────────────────┐     │
   │ notify := currentNotify()         // capture BEFORE the read ◄────┼─────┘
   │ rows := SELECT … WHERE pos > cursor                               │
   │                      AND type IN (Types)    ◄── type filter here   │
   │ for row in rows:                                                  │
   │     yield(row); cursor = row.pos          // skipped rows still   │
   │                                            // advance the cursor   │
   │ if !Live { return }                                               │
   │ select { <-notify ; <-ctx.Done() }   // sleep, then loop & re-read │
   └─────────────────────────────────────────────────────────────────┘
                          │ yields matching RawEnvelopes
                          ▼
   projection.Runner:  decode ─► Project ─► checkpoint (every N; flush tail)
```

`(*)` The wake arrow is backend-specific. The memory and SQLite stores close-and-replace an in-process channel directly in `Append`. Postgres routes it through one shared `LISTEN` connection that fans `NOTIFY` out onto the same kind of channel (ADR-0025). The subscriber loop above is identical regardless.

Per backend the filter is pushed as far down as the backend allows:

- **memory** — membership check in the yield loop (`typeFilter` set), advancing the cursor past skipped events.
- **sqlite** — `AND type IN (?, ?, …)` appended to the catch-up query.
- **postgres** — `AND type = ANY($n::text[])` (pgx encodes the `[]string`).

`projection.WithTypes(types ...string)` threads the option through, so a projection subscribes only to the types it understands.

### Checkpoint batching

`projection.WithCheckpointEvery(n int)`: save once every `n` processed events instead of after each. `n <= 1` (default) preserves the every-event behavior. The Runner tracks a pending position and **flushes it when a non-live subscription drains cleanly**. Early returns — context cancel, iterator error, projection error — deliberately skip the flush.

The tradeoff: on an unclean stop, up to `n-1` already-processed events are redelivered next run. Projections are already required to be idempotent (ADR-0014), so this is safe; it trades durability granularity for fewer checkpoint writes.

## Consequences

- A projection can scope itself to the event types it has codecs for, instead of registering codecs for the whole log or relying on `OnError`. This is the concrete demand ADR-0014 waited for.
- The filter is verified by three new subtests wired into `RunSubscribableContract` (`Subscribe_TypeFilter_CatchUp`, `Subscribe_TypeFilter_Live`, `SubscribeStream_TypeFilter`), so every backend — memory, sqlite, postgres — proves the behavior automatically (ADR-0018).
- `SubscriptionOptions` and the `SubscribableEventStore` interface gain a field, not a method, so backends that ignore `Types` would simply over-deliver; all three in-tree backends honor it.
- Checkpoint batching is opt-in and backward-compatible in behavior (default `n=1`). High-throughput projections cut checkpoint writes by up to `n×`.
- A busy global log with a narrow type filter still scans (but does not deliver) non-matching events; for SQL backends the `WHERE` clause keeps that work in the database.

## Alternatives considered

- **Filter only in the Runner (status quo), not the store.** Rejected: the store still ships every row to the client (real cost for SQL backends), and it can't prevent `CodecNotFoundError` for unregistered types without extra Runner logic. Pushing the filter into the query is both cheaper and simpler for the consumer.
- **Glob/prefix matching on type names.** Rejected for v0: exact membership covers the known need; prefix matching invites per-backend SQL `LIKE` differences and ambiguous semantics. Can be layered on later as a separate option.
- **A separate `FilteredSubscriptionOptions` type.** Rejected: a nil slice is a clean "no filter" default, and one options struct keeps the interface small (consistent with ADR-0014 reusing `From` for both global and per-stream).
- **Time- or size-based checkpoint batching** (flush every N ms / N bytes). Rejected as premature; event-count batching is the simplest knob and matches how projections think about progress. A periodic flush can be added later without breaking `WithCheckpointEvery`.
- **Flush the checkpoint on context cancel.** Rejected: the cancel path's context is already dead, so `Save` would fail; relying on idempotent redelivery is simpler and already part of the Runner's contract.
