# ADR-0021: SQLite checkpoint store

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md), [ADR-0017 SQLite event-store backend](0017-sqlite-backend.md), [ADR-0018 Backend contract tests](0018-backend-contract-tests.md), [ADR-0019 SQLite snapshot store](0019-sqlite-snapshot-store.md), [ADR-0020 SQL schema migration](0020-sql-schema-migration.md)

## Context

The toolkit's three store interfaces — `es.EventStore`, `es.SnapshotStore`, `es.CheckpointStore` — now each have an in-memory backend, and two of the three (events, snapshots) have a SQLite backend plus a shared contract test suite. The third (checkpoints) is the natural last piece: production projections need durable checkpoint state, and the most common deployment co-locates checkpoints with the same SQLite database that already holds events and snapshots.

## Decision

`checkpointstore/sqlite` ships as a sibling Go module, mirroring `eventstore/sqlite` and `snapshotstore/sqlite`. The accompanying `checkpointstore/checkpointstoretest` contract package extends the pattern from [ADR-0018](0018-backend-contract-tests.md).

### Schema

Single table, name as primary key:

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    name     TEXT    PRIMARY KEY,
    position INTEGER NOT NULL
);
```

- Save upserts via `ON CONFLICT(name) DO UPDATE SET position = excluded.position`.
- Load is `SELECT position WHERE name = ?`; `sql.ErrNoRows` translates to `(0, false, nil)`.
- Reset is `DELETE WHERE name = ?`.

No `updated_at` column; the memory backend tracks nothing equivalent and adding it asymmetrically would complicate the contract. Users who want audit metadata can run their own column.

### Position semantics

The contract codifies a subtle distinction: `Save(name, 0)` is **not** the same as "no checkpoint." After `Save(name, 0)`, `Load(name)` returns `(0, true, nil)`. After Reset (or before any Save), it returns `(0, false, nil)`. SQLite's UPSERT semantics preserve this naturally; the contract's `Save_Zero` test pins it down so any future backend has to honor it.

This matters because the `projection.Runner` uses position 0 as "start from the beginning of the log." `(0, true)` means "I have processed nothing yet, but I am tracking"; `(0, false)` means "I have never been started." The distinction is observable to operators inspecting checkpoint state.

### Schema management

Same opt-out, default-on pattern as [ADR-0020](0020-sql-schema-migration.md): exported `Schema` constant, standalone `Migrate(ctx, db)` function, `WithoutMigrate()` option on `New`. The convention is now uniform across all three SQL backends:

```go
sqlitestore.Schema             // []byte DDL, exported for external tooling
sqlitestore.Migrate(ctx, db)   // idempotent
sqlitestore.WithoutMigrate()   // opt-out
sqlitestore.New(ctx, db, opts...)
```

### Contract test suite

11 subtests, written once in `checkpointstoretest.RunContract`:

| Group | Subtests |
|---|---|
| Lifecycle | Load_EmptyStore, Save_Load_RoundTrip, Save_Overwrites, Reset_RemovesCheckpoint, Reset_NonExistentName_NoError |
| Isolation & semantics | PerNameIsolation (Save and Reset on one name don't affect another), Save_Zero (zero-position is distinct from missing) |
| Context propagation | Save_ContextCanceled, Load_ContextCanceled, Reset_ContextCanceled |
| Concurrency | Concurrent_SaveAndLoad (64 goroutines hammer Save and Load on one name; final position is in 1..N) |

Both backends pass all 11 (memory in 2 ms, sqlite in 80 ms). Each backend's test file is ~20 lines: compile-time interface assertion + one `RunContract` call + (for sqlite) the schema-management trio that's shared by all three SQL backends.

## Consequences

- All three store interfaces now have a uniform persistent backend and a contract-tested in-memory backend. A user can wire `eventstore/sqlite + snapshotstore/sqlite + checkpointstore/sqlite` against a single `*sql.DB` and get a complete persistent setup in one file.
- The contract pattern is locked in. Any future store interface gets a sibling `*storetest` package as part of its initial commit. Any future SQL backend follows the `Schema + Migrate + WithoutMigrate` recipe.
- The `Save_Zero` contract clause is the kind of subtle invariant that previously could have drifted silently between backends; pinning it in the contract is exactly the value the contract pattern was supposed to deliver.
- The repo's multi-module count grows to four (`.`, `eventstore/sqlite`, `snapshotstore/sqlite`, `checkpointstore/sqlite`), all tracked by `go.work`. CLAUDE.md's multi-module commands cover the SQL backends generically.

## Alternatives considered

- **Combined SQLite store under a single module exporting all three backends.** Same trade-off as ADR-0019: rejected to preserve the existing `eventstore/*`, `snapshotstore/*`, `checkpointstore/*` parallel structure and let users mix backends (SQLite events + Redis checkpoints, say) without pulling in unwanted code.
- **`updated_at` column for ops audit.** Deferred. The memory backend has no equivalent; adding it asymmetrically would mean a contract clause one backend can't pass. If a future user needs it, they add a column to their own table — `Schema` is exported precisely so the DDL is theirs to extend.
- **Multiple historical checkpoint positions per name** (for rebuild-to-point semantics). Out of scope: `CheckpointStore.Latest`-equivalent is `Load`, and `Reset` already provides "start over." Time-travel rebuilds would need a different interface, not a backend feature.
- **Bake `position` validation into the contract** (e.g., reject decreasing positions). Rejected: the `projection.Runner` saves positions monotonically by construction, and a contract that enforces monotonicity would prevent legitimate manual interventions (e.g., a SaaS operator manually resetting a position to recover from a bad event). Keep the store dumb; smartness lives in the Runner.
