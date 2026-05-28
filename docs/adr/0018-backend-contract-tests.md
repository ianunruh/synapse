# ADR-0018: Shared contract test suite for event-store backends

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0002 Package layout](0002-package-layout.md), [ADR-0008 Event store and streaming](0008-event-store-and-streaming.md), [ADR-0014 Subscriptions and projections](0014-subscriptions-projections.md), [ADR-0017 SQLite event-store backend](0017-sqlite-backend.md)

## Context

Two event-store backends (memory and SQLite) ship with the toolkit, and more are expected (Postgres, BoltDB, NATS JetStream, …). Each was shipping its own hand-written tests for the same documented contract: Append revision matrix, atomicity on conflict, GlobalPosition assignment, Load with From/Limit, Subscribe catch-up and live tail, ordering, etc.

Two real problems with that:

1. **Duplication.** The memory and SQLite test files had near-identical helper functions (`makeEvent`, `makeEvents`, `collect`) and near-identical test logic for the contract pieces. Drift was inevitable — and indeed had already started.
2. **No enforcement.** A new backend could ship without ever testing some part of the contract because the test was specific to the old backends. The interface was the spec; the tests were per-backend reinterpretations of it.

We need a way for *the toolkit* to assert what an [`es.EventStore`](../../es/store.go) and [`es.SubscribableEventStore`](../../es/subscription.go) must do, and for every backend to opt in with a single line.

## Decision

Create [`eventstore/eventstoretest`](../../eventstore/eventstoretest/eventstoretest.go), a package in the root module that exports a contract test suite:

```go
type Factory func(t *testing.T) es.EventStore
type SubscribableFactory func(t *testing.T) es.SubscribableEventStore

func RunEventStoreContract(t *testing.T, factory Factory)
func RunSubscribableContract(t *testing.T, factory SubscribableFactory)

// Exported helpers for backend-specific tests:
func MakeEvent(stream es.StreamID, version uint64) es.RawEnvelope
func MakeEvents(n int, stream es.StreamID, fromVersion uint64) []es.RawEnvelope
func Collect(seq iter.Seq2[es.RawEnvelope, error]) ([]es.RawEnvelope, error)
```

The factory returns a fresh, independent store per invocation. Each contract subtest calls factory exactly once, so backends register cleanup via `t.Cleanup` inside the factory.

### What's in the contract

23 subtests, split between the base EventStore contract (15) and the SubscribableEventStore extension (8):

**EventStore.Append (8):** revision matrix (11 sub-subtests), atomicity on conflict, empty batch, multiple events advancing head, context cancellation, GlobalPosition assignment across streams, caller-input slice immutability, concurrent contention.

**EventStore.Load (7):** empty stream, full round-trip including all fields, From/Limit edge-case matrix (9 sub-subtests), Causation/Correlation/Metadata round-trip, consumer break-early, context cancellation, isolation from caller-side mutation.

**SubscribableEventStore (8):** empty store, global catch-up, catch-up with From, live tail sees new appends, live tail respects context cancel, many concurrent live subscribers each see all events, per-stream subscription filters correctly, per-stream subscription with From skips earlier versions.

### What's deliberately backend-specific

- **`memory.TestLoad_SnapshotSemantics`** — the in-memory store's Load takes a snapshot at call time; concurrent appends don't appear in the iteration. SQLite-style backends that query rows lazily would not pass this test. Kept as a memory-specific guarantee.
- **`sqlite.TestPersistence_AcrossStoreInstances`** — open a file-backed DB, write, close, reopen with a fresh `Store`, read. The memory store can't even articulate this test.
- **Compile-time interface assertions** (`var _ es.EventStore = newStore(t)`) — backend-specific; one line per backend.

### Layout

```
eventstore/
├── eventstoretest/
│   └── eventstoretest.go      # the contract suite + helpers
├── memory/
│   └── memory_test.go         # ~50 lines: assertion + 1 contract call + 1 memory-only test
├── sqlite/
│   └── sqlite_test.go         # ~110 lines: assertion + 1 contract call + 1 sqlite-only test
```

Memory's test file dropped from ~445 to ~55 lines. SQLite's dropped from ~390 to ~110 lines.

## Consequences

- Adding a new backend is now mechanical: implement `EventStore`/`SubscribableEventStore`, write a `factory` closure, call `RunSubscribableContract(t, factory)`, add any backend-specific tests, done. The contract enforces parity.
- The contract IS the executable spec for what a backend must do. Documentation in the interface comments and the contract code stay in sync because the contract code is what tests them.
- Per-test failures show up as `TestSQLiteStore_Contract/Append_Revision/exact-mismatch/nonempty/high`, which is a precise pointer to both the backend and the contract clause.
- Backend-specific tests live next to the backend; they don't accumulate in a shared file.
- The contract package imports `testing` but ships as a normal Go package (not `_test.go`-only), so it's importable from external modules — for example, sqlite's go.mod resolves it transparently through the existing `synapse` import.

## Alternatives considered

- **Stay per-backend, copy-paste tests.** Rejected: duplication had already started, and adding a third backend would compound it. No way to enforce parity short of code review.
- **Generated tests from a JSON spec.** Considered briefly; rejected as over-engineering for a Go-only library. The contract suite is already concise (~700 lines) and reads as a clear spec.
- **Put the contract in `es/storetest`.** Would have placed it alongside the interface definitions. Rejected because `eventstore/eventstoretest` sits naturally next to the backends it tests and matches the Go convention of `<thing>/<thing>test` packages (e.g., `net/http/httptest`, `io/fs/fstest`).
- **Make the factory return a `(store, cleanup)` tuple.** Rejected: `t.Cleanup` inside the factory closure is the idiomatic Go way and is already used widely (`t.TempDir`, `httptest.NewServer`). No need to reinvent.
- **Pass per-test options into the contract** (e.g. "skip the concurrent test", "use shorter timeouts"). Rejected for v0: every existing backend passes every test as-is, so optionality would be premature. If a future backend genuinely can't satisfy a contract clause, the clean answer is that backend doesn't implement the interface — split the interface or document the limitation.
