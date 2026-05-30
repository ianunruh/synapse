# ADR-0031: Postgres parallel-writer Append (safe high-water mark)

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0024 Postgres backend](0024-postgres-backend.md) (supersedes its "Append: global advisory lock" decision), [ADR-0025 Postgres shared listener](0025-postgres-shared-listener.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md)

## Context

ADR-0024's Append serializes every writer through `SELECT pg_advisory_xact_lock(<const>)` to guarantee that `BIGSERIAL global_position` values commit in monotonic order — so subscribers never see position N before position N-1 commits, and so no holes in the sequence can be skipped over.

The cost is single-writer throughput. Even when ten goroutines each append to ten different streams, they queue on one global lock. On loopback Docker this caps Append at roughly 250 ops/sec; on real hardware with concurrent network round-trips and tuned WAL settings it climbs but remains bounded by the serialization.

The advisory lock was not there for correctness of per-stream conflict detection — `UNIQUE(stream_id, version)` already handles that. It was there exclusively to keep the global-position visibility monotonic. The cleaner fix uses Postgres's own snapshot machinery instead.

## Decision

Replace the global advisory lock with a **safe-high-water-mark** filter on the subscriber side. Each event row records the writing transaction's id; subscribers reading the global log skip any row whose xid is still potentially in flight.

### Schema (one new column)

```sql
ALTER TABLE events
  ADD COLUMN xid xid8 NOT NULL DEFAULT pg_current_xact_id();
```

`pg_current_xact_id()` is the Postgres 14+ 64-bit xid of the current transaction. The DEFAULT means `Append` does not need to mention `xid` in its INSERT — the row gets it for free.

For new deployments, `schema.sql` ships with the column. For deployments that already created the table under the ADR-0024 schema, run the `ALTER` above once. The xid column on existing rows defaults to whatever transaction runs the ALTER, which is fine — those rows are committed by the time the ALTER returns and will always satisfy the high-water-mark filter.

### Append: drop the advisory lock

```go
tx, err := s.pool.Begin(ctx)
// (removed) SELECT pg_advisory_xact_lock($1)
// SELECT MAX(version) FROM events WHERE stream_id = $1
// (unchanged) checkRevision
// INSERT INTO events (...) VALUES (...) — DEFAULT supplies xid
// SELECT pg_notify(...)
// COMMIT
```

Concurrent appenders to the same stream still race on the read-modify-insert; the loser sees a `UNIQUE(stream_id, version)` violation and gets `*ConflictError`. Concurrent appenders to *different* streams no longer wait on each other.

### Subscribe: filter the global path with `pg_snapshot_xmin`

```sql
SELECT ... FROM events
WHERE global_position > $1
  AND xid < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY global_position
```

`pg_snapshot_xmin(pg_current_snapshot())` returns the smallest xid that may still be in flight at the start of the statement. Any row whose `xid < xmin` is **guaranteed committed**; any row whose `xid >= xmin` might be in flight or might be committed-but-newer-than-our-snapshot. The filter is conservative — it never includes a row whose transaction has not certainly completed — so the subscriber's cursor only advances over rows that, by Postgres's own snapshot machinery, will never have a still-in-flight predecessor reveal itself.

Per-stream subscribers (`SubscribeStream`) read `WHERE stream_id = $1 AND version > $cursor`. `UNIQUE(stream_id, version)` already enforces a strict per-stream commit order — only one transaction can commit `(s, version=5)` — so the xmin guard is unnecessary on the per-stream path and we keep that query unchanged.

## Consequences

- **Append throughput scales with writers**, bounded by the underlying WAL fsync (or row size). Benchmarks in `eventstore/postgres/bench_test.go` document the lift on loopback Docker; production figures depend on hardware and tuning.
- **Per-stream append semantics are unchanged.** Two writers racing on the same stream still produce one winner and one `*es.ConflictError`. Retry middleware (ADR-0012) recovers from this exactly as before.
- **Subscriber latency = duration of the oldest in-flight transaction.** A long-running write transaction — even one unrelated to events, such as a maintenance script — delays event delivery to subscribers until it commits or aborts. This is the explicit tradeoff. The standard `idle_in_transaction_session_timeout` GUC bounds the worst case; operators concerned with delivery latency under heavy load should set it.
- **No interface change.** `EventStore.Append`, `Subscribe`, `SubscribeStream`, `SubscriptionOptions` — all unchanged. The contract suite (ADR-0018) passes against the new implementation with no edits.
- **The shared listener (ADR-0025) keeps working as-is.** `NOTIFY` still fires at COMMIT; the listener broadcasts; subscribers wake and re-read. The xmin guard simply filters out rows whose transactions haven't committed yet — so a NOTIFY might wake a subscriber that finds nothing newly visible, which is harmless. The next NOTIFY (or even a subsequent poll) will pick the events up as their xids fall below the moving xmin.
- **Postgres 14+ floor remains.** `xid8`, `pg_current_xact_id()`, and `pg_snapshot_xmin(pg_current_snapshot())` are all PG14+. ADR-0024 already pinned the floor there.

## Alternatives considered

- **Keep the advisory lock; rely on PG's natural throughput at small scales.** Rejected as the long-term answer: it bakes a hard ceiling into a primitive that's otherwise designed to scale, and the fix is small.
- **`SERIALIZABLE` isolation across all transactions instead of the lock.** Equivalent correctness for the monotonicity property but a much heavier hammer — every transaction pays the serializability cost, not just appends. The advisory lock was the targeted-against-Append answer; the safe HWM is a targeted-against-Subscribe answer, paid for only by subscribers reading the global path.
- **Logical replication (`pg_logical`).** A different architecture entirely; the publisher decodes WAL into a stream. Powerful but far beyond a v0 toolkit's scope, and it adds operational requirements (a replication slot, WAL retention) that the current LISTEN/NOTIFY model deliberately avoids.
- **Subscriber computes the high-water mark in app code, then filters with a const xid.** One extra round-trip per poll. Lets app code log/metric the hwm value, but the in-SQL filter is simpler and atomic w.r.t. the subscriber's own statement boundary. Rejected for v0; can be added if a need for the hwm value emerges.
- **Auto-detect-and-ALTER the schema in `Migrate()`.** Rejected: ADR-0020 explicitly keeps `Migrate` to `CREATE TABLE IF NOT EXISTS`. Operators run migrations externally. The ADR-0031 `ALTER` is one statement and is documented above.
- **Index `xid` separately.** The global subscribe query scans `global_position > $1` (PK-ordered) and filters each row's xid. The xid check is a single cheap comparison against a constant per row; an index on xid doesn't pay for itself when the access path is already the `global_position` index. Revisit if benchmarks show the filter dominating.
