# ADR-0025: Postgres subscriptions — one shared LISTEN, in-process fan-out

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0024 Postgres backend](0024-postgres-backend.md) (supersedes its "Subscribe" decision), [ADR-0017 SQLite backend](0017-sqlite-backend.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md)

## Context

ADR-0024 shipped the Postgres backend with a per-subscriber `LISTEN`: each live subscription acquired a `pgxpool` connection and held it for the lifetime of the iterator, alternating between a cursor SELECT and `WaitForNotification`. ADR-0024 accepted, as a documented consequence, that "LISTEN holds a pgxpool connection for the duration of the subscription," so the pool had to be sized for `concurrent live subscribers + concurrent queries + concurrent appends`.

That coupling is sharper than it first looks:

- The number of concurrent live subscribers is hard-capped at `MaxConns`. Past that, new subscribers block on `Acquire` indefinitely — the pool is exhausted by long-lived holders that never release.
- A single live subscriber transiently needed a *second* connection for its catch-up SELECT (it ran the SELECT against the pool, not the held connection), so the real ceiling was `MaxConns / 2`. The contract's `Subscribe_Live_ManyConcurrentSubscribers` (8 subscribers) deadlocked CI against the default pool and only passed once the pool was widened — a symptom, not a fix.

The SQLite backend (ADR-0017) does not have this problem. It wakes subscribers with an in-process close-and-replace signal channel; subscribers hold no connection while waiting and run a transient cursor read when woken. The only reason Postgres did it differently is that its wake-up source is a real `NOTIFY` on a connection rather than an in-process append.

## Decision

Collapse the per-subscriber `LISTEN` into **one shared listener per `Store`** and reuse SQLite's in-process fan-out for the wake-up.

- **One background goroutine per Store** holds a single connection running `LISTEN synapse_events` and looping on `WaitForNotification`. On every notification it close-and-replaces an in-process `chan struct{}`, waking all live subscribers at once.
- **Live subscribers hold no connection.** They mirror the SQLite loop: capture the current notify channel, run a cursor SELECT on a pooled connection, release it, then wait on the channel. So a subscriber occupies a connection only for the duration of its catch-up query.
- **Lazy start, single stop.** The listener goroutine starts on the first live `Subscribe`/`SubscribeStream` (via `sync.Once`) and runs until `Store.Close`. There is exactly one start and one stop — no reference counting, no restart races. A Store used only for `Load` or non-live reads never starts it and never holds the connection.
- **`Store.Close`** cancels the listener's context, waits for the goroutine to release its connection, and is idempotent. It must be called before the caller closes the pool (otherwise `pool.Close` blocks on the held connection).
- **Reconnect with backoff + blanket wake.** If the listener connection drops, the goroutine wakes every subscriber (so they re-read against their cursor and pick up anything appended during the gap), backs off (50 ms → 5 s cap), and reconnects. On each successful (re)`LISTEN` it also broadcasts once, closing the window where an append landed before the listener was attached.

`Append` is unchanged: it still emits a transactional `pg_notify('synapse_events', '<stream_id>:<pos>')` on COMMIT. The payload is now advisory/operator-facing only — the shared listener treats any notification as a blanket wake.

### What this gives up: the per-stream skip

ADR-0024 had `SubscribeStream` parse the notification payload and skip its SELECT when the notification was for a different stream. That optimization required each subscriber to observe *every* notification individually. The shared listener's fan-out is edge-triggered and **coalescing** — a woken subscriber cannot know, from a single shared signal, which streams changed since its last read — so the skip is not correct to keep as-is and is **removed**. Per-stream subscribers now run a cheap indexed cursor SELECT (`WHERE stream_id = $1 AND version > $cursor`, usually returning zero rows) on each wake.

This is a deliberate trade: re-introducing the "every consumer SELECTs on every notification" cost that ADR-0024 avoided, in exchange for decoupling subscriber count from pool size. The skip can return later without an interface change by giving the listener a per-subscriber buffered queue of payloads with an overflow-means-resync sentinel; that complexity is not worth it in v0.

## Consequences

- **Subscriber count is independent of pool size.** A thousand live subscribers share one listener connection plus the pool for their transient catch-up reads. The pool needs `1 (listener) + peak concurrent catch-up reads + concurrent appends` — not one slot per subscriber. The contract's 8-subscriber test now passes against a small pool, and a small pool is kept in the test harness as a regression guard.
- **The Store now owns a resource.** ADR-0024's "the Store does not retain any goroutines of its own" no longer holds; the Store has a lifecycle and a `Close`. Callers must call `Store.Close` before closing the pool. This is a breaking change to the v0 surface, accepted because the toolkit has no external users yet.
- **Cross-process correctness is preserved.** Unlike SQLite's purely in-process broadcast, the Postgres listener consumes the real `NOTIFY`, so subscribers in other processes are still woken. This design keeps that property and adds the fan-out efficiency.
- **Wake-up latency gains one in-process hop** (NOTIFY → listener → channel → subscriber) versus each subscriber blocking directly on `WaitForNotification`. In practice the `NOTIFY` round-trip dominates and is sub-millisecond on a healthy connection.
- **A forgotten `Close` leaks the goroutine and one connection**, and will make `pool.Close` block on that connection. Documented on `Store` and `Close`.
- **Per-stream subscribers do more SELECTs** on a busy global channel than under ADR-0024 (see trade above).

## Alternatives considered

- **Keep per-subscriber `LISTEN`, just widen the pool.** What CI did as a stopgap. Does not remove the ceiling — it only moves it — and wastes a connection per idle-but-live subscriber. Rejected as the permanent design.
- **Per-subscriber buffered payload queues to preserve the skip.** Keeps the per-stream skip under fan-out, but needs an overflow-means-resync protocol and per-subscriber registration/teardown — materially more code and test surface for an optimization that only matters on busy multi-stream clusters. Deferred; the interface leaves room to add it.
- **Reference-counted listener (start on first live sub, stop on last).** Was the natural choice when preserving a no-`Close` constructor contract. With no back-compat constraint, an explicit `Close` is simpler and removes all start/stop and restart races. Rejected in favor of lazy-start-once + `Close`.
- **Eager listener start in `New`.** Simplest lifecycle, but holds a connection and a goroutine for Stores that only ever do catch-up reads. Lazy start avoids that for free.
- **Have `Append` also broadcast in-process** (like SQLite) to shave the `NOTIFY` round-trip for same-process subscribers. A latency optimization that adds a second wake path and double-wakes (harmless, coalesced). Out of scope for v0; the single listener path is the one source of truth.
