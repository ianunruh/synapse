# Benchmarks

Baseline numbers for the core hot paths and the included store backends, captured to make regression visible and to set order-of-magnitude expectations for users.

These are point measurements on a single machine, not a perf claim. Absolute numbers move with hardware, kernel, filesystem, and SQLite version; the **ratios** between memory and SQLite, and the **per-event scaling** within a single backend, are what to read carefully.

## Running them

```
# Core es package
go test ./es -bench=. -benchmem -run=^$

# Memory backends
go test ./eventstore/memory ./snapshotstore/memory ./checkpointstore/memory -bench=. -benchmem -run=^$

# SQLite backends (each lives in its own sibling module)
cd eventstore/sqlite       && go test -bench=. -benchmem -run=^$
cd snapshotstore/sqlite    && go test -bench=. -benchmem -run=^$
cd checkpointstore/sqlite  && go test -bench=. -benchmem -run=^$
```

Backends share a benchmark harness (`eventstorebench`, `snapshotstorebench`, `checkpointstorebench`) the same way they share the test contract suites. A new backend implementation gets standardized numbers by wiring `Run(b, factory)` from a `*_bench_test.go` file.

## Environment

- Go: `go1.26.3 linux/amd64`
- CPU: AMD Ryzen 9 7900X (24 logical cores)
- SQLite: `modernc.org/sqlite` (pure Go), file-backed with `journal_mode(WAL)` and `busy_timeout(5000)`
- Date: 2026-05-28

## Core package

Hot paths in `es` and the surrounding aggregate/repository plumbing. Memory event store, JSON codec, in-memory snapshot store where relevant.

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `AggregateBase.Record` | 2,204 | 933 | 0 | One `Record` + caller's `Apply`. The bytes/op reflect amortized slice growth on the pending queue. |
| `Registry.Lookup` | 79 | 0 | 0 | Codec lookup on the hot path of every Save and Load. |
| `Repository.Save` (1 event) | 10,094 | 2,384 | 5 | Codec marshal + memory-store append + ctx-derived stamping. |
| `Repository.Save` (10 events) | 119,377 | 24,372 | 23 | ~12 µs per event amortized. |
| `Repository.Load` (10 events) | 33,427 | 4,272 | 59 | ~3.3 µs per event including unmarshal + Apply. |
| `Repository.Load` (100 events) | 331,693 | 41,072 | 509 | Scales linearly with stream length. |
| `Repository.Load` (1000 events) | 3,309,786 | 396,273 | 5,009 | Same. |
| `Execute` (with snapshots) | 25,457 | 3,094 | 23 | Steady-state command round-trip: snapshot-restored Load + handler + Save. |

## Event store backend

Append-and-load workloads against the `es.EventStore` contract, driven by `eventstore/eventstorebench`.

| Op | Memory ns/op | SQLite ns/op | SQLite/Memory | Memory allocs | SQLite allocs |
|---|---:|---:|---:|---:|---:|
| `Append_Single` | 5,016 | 46,929 | 9.4× | 7 | 65 |
| `Append_Batch_10` | 20,030 | 165,658 | 8.3× | 2 | 238 |
| `Load_100` | 25,286 | 363,324 | 14.4× | 4 | 3,234 |
| `Load_1000` | 387,154 | 2,515,503 | 6.5× | 4 | 33,524 |

Batching pays off in both backends: 10-event SQLite appends are 3.5× a single append (not 10×), because the per-append fsync + transaction overhead amortizes across the batch.

## Snapshot store backend

`SnapshotStore` is small surface — Save + Latest — driven by `snapshotstore/snapshotstorebench`.

| Op | Memory ns/op | SQLite ns/op | SQLite/Memory | Memory allocs | SQLite allocs |
|---|---:|---:|---:|---:|---:|
| `Save` | 981 | 44,238 | 45× | 1 | 21 |
| `Latest` | 149 | 79,437 | 533× | 0 | 44 |

`Latest` on the memory store is a map lookup; on SQLite it's a `SELECT … ORDER BY version DESC LIMIT 1` round trip. The large ratio reflects the floor cost of any SQLite query, not a hot-path concern: snapshots are queried at most once per `Repository.Load`, not per event.

## Checkpoint store backend

`CheckpointStore` is two methods plus Reset; the bench harness covers Save and Load.

| Op | Memory ns/op | SQLite ns/op | SQLite/Memory | Memory allocs | SQLite allocs |
|---|---:|---:|---:|---:|---:|
| `Save` | 150 | 29,274 | 195× | 0 | 11 |
| `Load` | 98 | 40,930 | 418× | 0 | 21 |

In real projection workloads `Save` happens once per successfully projected event. At 29 µs the SQLite checkpoint is the rate-limiting step in a saturated runner, so this is the number to watch when tuning a backend or considering checkpoint batching.

## Notes

- Memory-backend benchmarks share one process-wide store across iterations and reuse the same stream/checkpoint name, so slice growth and map resize costs amortize across the bench run. SQLite benchmarks each get a fresh on-disk database, so I/O setup is a one-time cost outside the timed loop.
- The core `Execute` benchmark uses `WithSnapshotPolicy(EveryNVersions(1))` to keep Load latency constant across iterations. Without snapshots, an N-th `Execute` reads N-1 prior events, so the benchmark would measure replay scaling rather than steady-state command cost.
- Allocation counts are stable enough to be meaningful as regression signals; the bytes-per-op figures include amortized slice/map growth and are noisier.
