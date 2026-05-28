# ADR-0013: Snapshotting

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0004 Aggregate model](0004-aggregate-model.md), [ADR-0005 Event envelope](0005-event-envelope.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md)

## Context

Aggregates with long event histories grow expensive to rehydrate — every Load replays every event. Snapshotting addresses this by periodically writing the aggregate's domain state to a sidecar store; Load restores from the latest snapshot and replays only newer events. The design needs to:

- Stay optional and opt-in per aggregate type (some aggregates never warrant snapshots).
- Compose cleanly with the existing Repository, codec [Registry], and event store.
- Survive different storage technologies for events vs snapshots (event log + blob store is a common combination).
- Avoid coupling snapshot success to event-commit success — events are the source of truth; snapshots are optimizations.
- Use the same codec mechanism we already use for events.

## Decision

Snapshotting is modeled as four loosely coupled pieces in the `es` package, plus an in-memory backend in `snapshotstore/memory` mirroring `eventstore/memory`.

**Aggregate-side capability** is the `Snapshotter` interface:

```go
type Snapshotter interface {
    SnapshotType() string             // e.g. "counter.snapshot.v1"
    Snapshot() (state any, err error) // produce typed state value
    Restore(state any) error          // populate from prior snapshot
}
```

Aggregates that don't implement it pass through unchanged — the Repository's snapshot path silently no-ops for them.

**Storage** is a separate interface, deliberately decoupled from `EventStore`:

```go
type SnapshotStore interface {
    Save(ctx, snap RawSnapshot) error
    Latest(ctx, stream StreamID) (RawSnapshot, bool, error)
}
```

`RawSnapshot` parallels `RawEnvelope`: opaque byte payload, IANA `ContentType`, version, type name, metadata. The store knows nothing about codecs or domain types. Save replaces; Latest returns the most recent.

**Triggering** uses a policy plus a manual API:

```go
type SnapshotPolicy func(agg Aggregate, versionBefore, versionAfter uint64) bool
func EveryNVersions(n uint64) SnapshotPolicy
```

The Repository consults the policy after every successful `Save`. `EveryNVersions(n)` fires when `versionBefore/n != versionAfter/n` — at most once per multiple of `n` per Save, regardless of batch size. A version-pair signature lets the built-in policy stay stateless (no need to track last-snapshot version per stream).

Manual checkpoints go through `Repository.SaveSnapshot(ctx, agg)` for migration scripts, integration tests, and application-driven snapshotting.

**Codecs** use the same `Registry` as events. Snapshot type names are namespaced by convention (`counter.snapshot.v1`), keeping events and snapshots disjoint without introducing a parallel registry.

**Failure handling:**

- Snapshot codec missing on `Load`: returns `*CodecNotFoundError`. Don't silently fall back to full replay — that masks misconfiguration.
- Unmarshal / Restore failures on `Load`: returned, wrapped.
- Snapshot store transient failures on `Load`: returned, wrapped.
- Snapshot store / codec failures during automatic snapshot from `Save`: silently swallowed. Events are committed; the snapshot is an optimization. Marked `// TODO: surface via slog once logging story lands.` so we can revisit when the logging story is defined.
- Snapshot failures from manual `SaveSnapshot`: returned to the caller.
- `StreamNotFoundError` is returned only when both snapshot and events are absent.

## Consequences

- Adding snapshotting to an existing aggregate is a three-method change (`Snapshotter` plus a snapshot struct + codec registration) — no migration of stored events required.
- Repositories with mixed aggregate types where only some implement `Snapshotter` work without configuration changes; non-snapshotter aggregates simply skip the snapshot path.
- Snapshots are best-effort by design. The worst case is a slightly slower Load when a snapshot save fails — never a correctness issue, because event replay always reaches the head version.
- Backends are independent: a future Postgres event store and a blob-storage snapshot store interoperate through the Repository without either knowing about the other.
- Mixing event and snapshot codecs in one Registry is one less moving part, with the cost that users own the naming convention.

## Alternatives considered

- **Treat snapshots as special events in the event log.** Rejected: conflates two different access patterns (append-only history vs latest-only), forces every event store to grow snapshot semantics, and makes "find latest snapshot" an O(stream length) read.
- **Make `Snapshotter` required on every aggregate.** Rejected: aggregates with short histories don't need snapshots; forcing the interface adds boilerplate and a redundant code path for them.
- **Auto-snapshot policy with last-snapshot tracking in Repository.** Rejected for v0: tracking per-stream state in the Repository creates memory growth concerns at high stream cardinality (the same trade-off `PerAggregateLocking` already has). The version-pair signature avoids the issue while still expressing the common "every N versions" case cleanly.
- **Return snapshot save errors from `Save`.** Rejected: would conflate "events committed successfully" with "snapshot save failed," forcing every caller to disentangle them. Best-effort with later logging integration matches the optimization nature of snapshots.
- **Ship a default policy in `NewRepository`.** Rejected: snapshot frequency is workload-dependent; surprising users with a default could mask configuration mistakes. Snapshotting is opt-in.
