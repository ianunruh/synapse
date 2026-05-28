# ADR-0017: SQLite event-store backend as a sibling Go module

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0001 Foundation](0001-foundation.md), [ADR-0002 Package layout](0002-package-layout.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md), [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md)

## Context

The in-memory `eventstore/memory` backend is great for tests and demos but loses everything on process exit. The first real persistent backend should be:

- Embedded (no separate server process to run).
- Cross-platform with no system requirements (so the toolkit stays easy to onboard).
- Compatible with the existing `EventStore` and `SubscribableEventStore` interfaces.
- Isolated from the root module so external deps don't leak into anyone who just uses `synapse/es`.

SQLite fits all four. The implementation needs to choose a driver, decide how live tail works against a relational store, and slot into the project's multi-module convention.

## Decision

### Sub-module under `eventstore/sqlite/`

Per [ADR-0001](0001-foundation.md), optional concerns with external deps live in sibling Go modules. The SQLite backend becomes a separate module at `eventstore/sqlite/` with its own `go.mod`. A `go.work` file at the repo root binds the modules for local development; once the root module is published at a real version, the sibling's `require` can pin it.

```
synapse/
├── go.mod                      # root: zero external deps
├── go.work                     # binds root + sqlite for local dev
├── eventstore/
│   ├── memory/                 # in root module
│   └── sqlite/
│       ├── go.mod              # SEPARATE module
│       ├── schema.sql          # embedded via go:embed
│       ├── sqlite.go           # Store implementation
│       └── sqlite_test.go
└── ...
```

The sibling module imports `github.com/ianunruh/synapse/es` for interface types and a `replace` directive in its `go.mod` points to `../..` so the module resolves cleanly even without `go.work`.

### Driver: modernc.org/sqlite (pure Go)

The store blank-imports `modernc.org/sqlite` (pure-Go, transpiled-from-C SQLite, no CGo). Users get cross-platform builds with no system requirements. The driver registers under the name `"sqlite"` so users open via `sql.Open("sqlite", dsn)`.

modernc adds compile time (the driver pulls a transpiled libc), but runtime performance is adequate for the typical event sourcing write rate. Users who need maximum throughput can fork the package and swap in `github.com/mattn/go-sqlite3` (CGo) — the Store is structured so the driver is the only thing that changes.

### Schema

A single `events` table embedded via `//go:embed schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL,
    stream_id       TEXT    NOT NULL,
    version         INTEGER NOT NULL,
    type            TEXT    NOT NULL,
    content_type    TEXT    NOT NULL,
    recorded_at     INTEGER NOT NULL,    -- unix nanoseconds
    causation       TEXT    NOT NULL DEFAULT '',
    correlation     TEXT    NOT NULL DEFAULT '',
    metadata        TEXT    NOT NULL DEFAULT '{}',
    payload         BLOB    NOT NULL,
    UNIQUE(stream_id, version)
);
```

- `INTEGER PRIMARY KEY AUTOINCREMENT` gives monotonic `GlobalPosition` for free.
- `UNIQUE(stream_id, version)` is both the per-stream ordering invariant and the backstop for optimistic concurrency on concurrent appenders.
- Metadata stored as JSON text; SQLite has no native map type.
- Timestamps as `INTEGER` (unix nanoseconds) for full precision without TIMESTAMP parsing ambiguity.
- No separate index for stream-scoped reads — the UNIQUE constraint creates one for free.

### Append

A single transaction: `SELECT MAX(version)` → validate against `expected` → `INSERT` one row per event → COMMIT. Conflict detection has two layers:

1. **Application check** against `expected` before any insert (fast fail, informative error).
2. **UNIQUE constraint** as a backstop — if two appenders race past the SELECT, the loser gets `SQLITE_CONSTRAINT_UNIQUE` at INSERT or COMMIT, which the store translates to `*es.ConflictError`.

Modern SQLite (with WAL) handles concurrent writers via the busy-timeout/retry mechanism. The store does not enforce any locking strategy on the caller's `*sql.DB`; callers are expected to configure `_pragma=journal_mode(WAL)` and `_pragma=busy_timeout(5000)` (or equivalent) at DSN time. The package doc and tests document this.

### Live tail: in-process broadcast

SQLite has no native LISTEN/NOTIFY. To match the contract from [ADR-0014](0014-subscriptions-projections.md) without polling, the Store maintains a close-and-replace `chan struct{}` — exactly the same pattern as the in-memory store. Each `Append` closes the current channel and installs a new one; `Subscribe`/`SubscribeStream` capture the channel before each query and wait on it after catching up.

**This is in-process only.** Cross-process consumers (a separate program subscribing to the same SQLite file) will not wake on remote writes — they'll see new events only when they re-Subscribe. v0 does not support cross-process live tail; a polling fallback or file-watch could be added later.

### Iter.Seq2 protocol compliance

The `Subscribe`/`SubscribeStream` loop is careful to honour the iter.Seq2 contract: when `yield` returns false (consumer breaks out of `range`), the inner read helper returns a sentinel `errIterStopped` that the outer loop catches and returns from cleanly, *without yielding again*. Calling `yield` after it returned false is a Go runtime panic ("range function continued iteration after function for loop body returned false") — easy to introduce, surfaced by the concurrent-subscriber tests.

## Consequences

- Users who want SQLite persistence opt in with one import (`synapse/eventstore/sqlite`) and one `sql.Open`. The toolkit's root module stays dep-free.
- The Store works wherever `database/sql` works. Users could in principle pass any SQLite-compatible `*sql.DB`; the only driver-specific code is the UNIQUE-constraint error inspection (via `modernc.org/sqlite/lib`), which would need an alternative path for non-modernc drivers.
- Live tail works in-process. Cross-process consumers fall back to catch-up reads, which is correct but not "live". Document.
- File-based DBs require WAL + busy_timeout pragmas for the SubscribableEventStore contract to behave well under contention. The test helper sets both; the package doc points users to do the same.
- `go test ./...` from the root no longer covers the sqlite module; CI / contributors run `cd eventstore/sqlite && go test ./...` separately. CLAUDE.md documents the multi-module commands.

## Alternatives considered

- **CGo driver (`mattn/go-sqlite3`).** Rejected for v0 due to system-requirement and cross-compilation friction. Users who need the throughput can swap the driver.
- **Postgres or MySQL backend first.** Defensible but adds a server dependency to onboarding. SQLite is a better first persistent backend; Postgres can come next with native LISTEN/NOTIFY for cross-process live tail.
- **Cross-process live tail via polling.** Deferred. Polling has obvious cost; users who need cross-process delivery should reach for a broker (Postgres, NATS, Kafka).
- **Embed `eventstore/sqlite` in the root module via build tags.** Rejected: build tags hide deps from `go.mod` but don't satisfy "zero external deps" — `go build` from a tagged-out import path still resolves them. A separate module is honest.
- **A common backend test suite.** Deferred. The SQLite tests deliberately mirror the memory store tests by hand. Extracting a contract test helper into a public package is a worthwhile future step but adds API surface; for v0 the duplication is small.
