# ADR-0024: Postgres backend — pgxpool, advisory-lock append, LISTEN/NOTIFY tail

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md), [ADR-0017 SQLite backend](0017-sqlite-backend.md), [ADR-0018 Backend contract tests](0018-backend-contract-tests.md), [ADR-0020 SQL schema migration](0020-sql-schema-migration.md)

## Context

The SQLite backend (ADR-0017) was the first concrete implementation of `es.EventStore`, `es.SnapshotStore`, and `es.CheckpointStore`. SQLite is great for embedded and single-process workloads but tops out long before multi-tenant production. We needed a second SQL backend, both because Postgres is the default operational choice for production services and because the contract suites are only meaningful once at least two implementations have proven the abstraction.

Postgres has three interesting differences from SQLite that the design had to account for:

- A real client/server protocol. Connection pooling matters; transaction round-trips are real network round-trips.
- Concurrent writers. SQLite's single-writer model serialized appends for free; Postgres does not, so global-position monotonicity is something we have to engineer.
- Native pub/sub via `LISTEN`/`NOTIFY`. Live subscriptions can be event-driven rather than polled.

## Decision

Three sibling Go modules — `eventstore/postgres`, `snapshotstore/postgres`, `checkpointstore/postgres` — mirror the SQLite split. Each takes a pre-built `*pgxpool.Pool` from the caller and applies its own schema by default (with a `WithoutMigrate()` opt-out, per ADR-0020). The caller owns pool lifecycle.

Key implementation choices:

- **Driver: `jackc/pgx/v5` via `pgxpool`.** Native pgx is required for `Conn.WaitForNotification`, which is the cleanest way to consume `LISTEN`/`NOTIFY` without a polling subprotocol. Pinned to v5; v6 is in development.
- **`BYTEA` for payload, `JSONB` for metadata.** Payload is opaque to the store (ADR-0005); BYTEA keeps it that way. Metadata is the only structured field the store knows about, and JSONB makes the natural Postgres operators available to operators inspecting the table.
- **Postgres 14+** as the supported floor. Earlier versions are EOL.

### Append: global advisory lock + `RETURNING global_position` + transactional `NOTIFY`

Every `Append` runs inside one transaction:

1. `SELECT pg_advisory_xact_lock(<constant>)` to serialize the assignment of `global_position` across all writers.
2. `SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1` for the expected-revision check.
3. One `INSERT ... RETURNING global_position` per event, accumulating the max global position.
4. `SELECT pg_notify('synapse_events', '<stream_id>:<max_global_position>')` so the notification fires on COMMIT.
5. `COMMIT`.

The advisory lock guarantees that committed `global_position` values are monotonic — no reader will ever see position N before position N-1 commits. The cost is that appenders queue rather than parallelize. ADR-0008's "single writer, many readers" framing is the workload this optimizes for; if a future workload needs parallel appends, a "safe high-water mark" pattern can replace the lock without breaking the contract.

`UNIQUE (stream_id, version)` is the second line of defense: two concurrent appenders racing through the lock (impossible by construction, but defended) would collide on the unique constraint, returning `*es.ConflictError` mapped from SQLSTATE `23505`.

### Subscribe: hold a connection, `LISTEN synapse_events`, drain + wait

Subscribers acquire a pgxpool connection for the lifetime of the iterator (a real concern for pool sizing). The connection executes `LISTEN synapse_events`; the loop alternates between SELECTing new events past a cursor and blocking on `WaitForNotification`. The cursor advances after each yield so the same SELECT serves both catch-up and post-notification reads idempotently.

The notification payload is `<stream_id>:<max_global_position>`. `SubscribeStream` consumers parse it and skip the SELECT entirely when the notification is for a different stream — turning a busy global channel into per-stream-targeted wake-ups without the operational complexity of per-stream channels.

The payload is treated as a hint, never authoritative: missed notifications (queue overflow, conn drop) fall through cleanly because the SELECT against the cursor is always correct.

### Snapshot and checkpoint stores

Same shape as their SQLite siblings, translated to Postgres syntax. `Save` is `INSERT ... ON CONFLICT DO UPDATE` for both. `Latest` is a single-row `SELECT`. `Reset` on the checkpoint store is `DELETE`. No advisory lock needed — these tables have natural primary keys that handle concurrent writes cleanly.

### Testing: `testcontainers-go`

Tests spin up `postgres:17-alpine` via `testcontainers-go`. One container per test package, lazily started on first `newStore`. Each test gets a fresh per-test database (`CREATE DATABASE test_<n>_<name>`) — `LISTEN`/`NOTIFY` channels are scoped per database, so this isolates pub/sub state across tests. Containers and per-test databases are torn down via `tb.Cleanup`.

Devs need Docker available. CI uses the default ubuntu-latest runner, which has Docker preinstalled.

## Consequences

- The library now has a production-grade backend story: SQLite for embedded/dev, Postgres for service-tier deployments. Same `es.SubscribableEventStore` / `es.SnapshotStore` / `es.CheckpointStore` interfaces; users pick by import.
- The `database/sql` divergence is real: SQLite takes `*sql.DB`, Postgres takes `*pgxpool.Pool`. Sibling modules are independent anyway, so users who want both can wire each independently.
- Append throughput is bounded by the advisory-lock serialization plus PG fsync. On loopback Docker the benchmarks show ~3.9ms per single-event append; on dedicated hardware with tuned WAL settings, the floor is lower. For workloads needing parallel-writer throughput, an unlocked-append + safe-high-water-mark design can replace the lock as a non-breaking change.
- LISTEN holds a pgxpool connection for the duration of the subscription. Pool size needs to accommodate `expected concurrent live subscribers + concurrent queries + concurrent appends`. Documented in the package doc.
- The contract suites validate that both backends satisfy the same semantics, which is the original purpose of the suites (ADR-0018). The Postgres implementation found no contract ambiguities, so the suites and the interfaces both held.
- The benchmark harnesses drop in without changes; the resulting numbers are useful comparison points but not perf claims — production sizing depends on hardware, network, and tuning.

## Alternatives considered

- **`database/sql` + `pgx/stdlib` driver.** Would have shared more code with the SQLite path and removed the divergent constructor type, but giving up `WaitForNotification` means polling-based live subscriptions, which is worse than what SQLite already does (in-process broadcast).
- **`lib/pq`.** Unmaintained; the Go community has migrated to pgx. Not a serious contender for new code.
- **`SERIALIZABLE` transaction isolation instead of advisory lock.** Equivalent correctness for the position-ordering problem, but a heavier hammer — every transaction pays the serializability cost, not just appends. Advisory lock targets the specific invariant.
- **Per-stream `NOTIFY` channels.** Would let `SubscribeStream` skip per-stream filtering, but Postgres channel names are `NAME`-typed (63 bytes, restricted character set) so user stream IDs would need sanitization, and global subscriptions would need to LISTEN on every stream's channel — impractical. Structured payload on one global channel gives the same skip behavior with simpler operational shape.
- **Empty `NOTIFY` payload.** Simpler but forces every consumer to SELECT on every notification, even if it's for a stream they don't care about. The structured payload cost is ~10 lines of parser and pays off whenever multiple SubscribeStream consumers run on a busy cluster.
- **Building the `pgxpool.Pool` from a DSN inside `New`.** Would have hidden pool ownership and made tuning (pool size, logger, tracing) opaque. Taking the pool from the caller mirrors the SQLite pattern of taking `*sql.DB` and keeps the library out of the way.
- **`testcontainers-go` vs. env-var DSN.** testcontainers wins on dev experience (works out of the box) and CI uniformity (no separate service container to configure). Its dep weight lives in test scope only.
