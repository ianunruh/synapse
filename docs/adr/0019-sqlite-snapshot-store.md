# ADR-0019: SQLite snapshot store and snapshot contract suite

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0013 Snapshotting](0013-snapshotting.md), [ADR-0017 SQLite event-store backend](0017-sqlite-backend.md), [ADR-0018 Backend contract tests](0018-backend-contract-tests.md)

## Context

With the SQLite event-store backend ([ADR-0017](0017-sqlite-backend.md)) shipped, the natural next step is a SQLite snapshot backend so users can persist both events and aggregate state in the same file. The snapshot interface ([ADR-0013](0013-snapshotting.md)) is small — Save (upsert) and Latest — so the implementation is straightforward; the interesting decisions are layout, schema, and what to do about the test duplication that's about to start.

## Decision

### Parallel sibling module `snapshotstore/sqlite`

Mirrors the existing structure. Users open one `*sql.DB`, hand it to both `eventstore/sqlite.New` and `snapshotstore/sqlite.New`. Each backend runs its own `CREATE TABLE IF NOT EXISTS` so they don't conflict. Independent modules with independent versioning, sharing only the `modernc.org/sqlite` driver dep.

```go
import (
    "database/sql"

    sqliteevents "github.com/ianunruh/synapse/eventstore/sqlite"
    sqlitesnaps  "github.com/ianunruh/synapse/snapshotstore/sqlite"
    _ "modernc.org/sqlite"
)

db, _ := sql.Open("sqlite", "file:store.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
events, _ := sqliteevents.New(ctx, db)
snaps,  _ := sqlitesnaps.New(ctx, db)

repo := es.NewRepository(events, reg, NewOrder,
    es.WithSnapshotStore(snaps),
    es.WithSnapshotPolicy(es.EveryNVersions(100)))
```

The repo's `go.work` adds the new module.

### Schema

One table, stream_id as the primary key, upsert via `ON CONFLICT(stream_id) DO UPDATE`:

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    stream_id    TEXT    PRIMARY KEY,
    version      INTEGER NOT NULL,
    type         TEXT    NOT NULL,
    content_type TEXT    NOT NULL,
    recorded_at  INTEGER NOT NULL,
    metadata     TEXT    NOT NULL DEFAULT '{}',
    payload      BLOB    NOT NULL
);
```

`stream_id` as PK matches the `Save replaces` semantic from [ADR-0013](0013-snapshotting.md): one snapshot per stream, latest wins. The single-row-per-stream model also makes `Latest` an O(1) lookup. Time-travel (multiple snapshots per stream, ORDER BY version DESC) is deferred; if a future user needs it, a composite PK plus indexing flips it on without breaking the existing data.

`recorded_at` is unix nanoseconds for precision. `metadata` is JSON text; SQLite has no native map type.

### Concurrency, atomicity, and WAL

- The Store uses a single `INSERT … ON CONFLICT` statement per Save — no explicit transaction, no read-then-write race.
- The Repository commits events first, then best-effort writes the snapshot in a separate statement. Snapshot failure is logged via `slog.WarnContext` ([ADR-0015](0015-slog-observability.md)) and does not fail Save. This matches the memory backend and works identically whether events and snapshots share a DB or not.
- WAL + busy_timeout (`?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)`) are required for concurrent workloads — same recommendation as the event store. Without them, contention on the snapshot table during heavy reads can produce SQLITE_BUSY.
- Cross-store atomicity (events + snapshot in one transaction) is intentionally out of scope. It would require either a new repository contract or driver-level transaction passing; neither is justified by the current best-effort snapshot model.

### Snapshot contract test suite

Adding the SQLite backend would otherwise duplicate every test in the memory backend, exactly the problem [ADR-0018](0018-backend-contract-tests.md) addressed for the event store. The fix is the same: a `snapshotstore/snapshotstoretest` package in the root module exporting one entry point.

```go
type Factory func(t *testing.T) es.SnapshotStore

func RunContract(t *testing.T, factory Factory)

func MakeSnapshot(stream es.StreamID, version uint64) es.RawSnapshot
```

Eight contract subtests: `Latest_EmptyStore`, `Save_Latest_RoundTrip`, `Save_Overwrites`, `Latest_PerStreamIsolation`, `MetadataRoundTrip`, `Save_ContextCanceled`, `Latest_ContextCanceled`, `Concurrent_SaveAndLatest`.

Both backends' test files collapse to ~20 lines (interface assertion + one `RunContract` call). The contract IS the spec — adding a future backend (Postgres, BoltDB) reduces to "implement the interface, call `RunContract`."

## Consequences

- Users get persistent snapshots with no schema work of their own and no extra runtime processes.
- One file, one backup. Events and snapshots travel together, restore together.
- The Repository's existing best-effort snapshot path needs no changes; the SQLite backend slots in behind `WithSnapshotStore` like any other implementation.
- The repository grows two more modules tracked by `go.work` — `eventstore/sqlite` and `snapshotstore/sqlite`. Each has its own `go.mod` and `go.sum`. CI / contributors need to run `go test ./...` from the relevant module dirs, as already documented in [CLAUDE.md](../../CLAUDE.md).
- The snapshot contract suite establishes that we'll likely have a `checkpointstore/checkpointstoretest` next time we add a non-memory CheckpointStore. The pattern is now uniform across all three store interfaces.

## Alternatives considered

- **Combined `sqlite` module exposing both event and snapshot stores.** Rejected: breaks the existing `eventstore/*` and `snapshotstore/*` parallel structure, and forces users who only want one store to pull in the schema and code for both. Independent modules let users mix backends (SQLite events + S3 snapshots, etc.).
- **Embed snapshots as special events in the event log.** Rejected: conflates two access patterns (append-only history vs latest-only), forces every event store to grow snapshot semantics, and turns `Latest` into a stream scan. Same reasoning as [ADR-0013](0013-snapshotting.md).
- **Cross-store atomicity (events + snapshot in one transaction).** Deferred: the current best-effort model — events committed first, snapshot logged on failure — has worked since [ADR-0013](0013-snapshotting.md) and survives this backend unchanged. Atomicity would require a richer Repository contract (e.g. taking a transaction handle) that we don't need yet.
- **Keep multiple historical snapshots per stream.** Deferred: useful for time-travel debugging but adds complexity and disk footprint. Single-row-per-stream is the documented `SnapshotStore.Latest` semantic; if a future user wants history, composite PK plus an explicit retention policy fits cleanly.
- **Parameterize the contract suite with skip flags.** Same answer as ADR-0018: both existing backends pass every test as-is. A future backend that genuinely can't satisfy a clause shouldn't claim the interface in the first place.
