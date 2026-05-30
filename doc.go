// Package synapse is a toolkit for building event-sourced systems in Go.
//
// The library is organized as a set of small subpackages so applications
// import only what they need. This package itself is doc-only — there
// is nothing to import from it.
//
// Core (root module, no third-party deps):
//
//   - [github.com/ianunruh/synapse/es] — aggregates, envelopes, the
//     EventStore interface, the [es.Repository] (with snapshots and
//     middleware), the codec [es.Registry], typed command handlers,
//     [es.Execute].
//   - [github.com/ianunruh/synapse/es/middleware] — concrete repository
//     middleware: per-aggregate locking, retry-on-conflict.
//   - [github.com/ianunruh/synapse/es/projection] — a single-process
//     [projection.Runner] that subscribes, decodes, projects, and
//     checkpoints. Supports type filters and batched checkpoint saves.
//   - [github.com/ianunruh/synapse/es/commandbus] — a transport-facing
//     [commandbus.Bus] that routes named, byte-encoded commands to a
//     typed [es.Handler], with its own middleware (Logging, Recover,
//     Timeout).
//
// Codecs (per-event-type, opt-in):
//
//   - [github.com/ianunruh/synapse/codec/json] — encoding/json adapter,
//     root module, stdlib only.
//   - github.com/ianunruh/synapse/codec/proto — google.golang.org/protobuf
//     adapter, sibling module so the protobuf dep stays out of the core.
//
// Event, snapshot, and checkpoint store backends:
//
//   - in-memory: [github.com/ianunruh/synapse/eventstore/memory],
//     [github.com/ianunruh/synapse/snapshotstore/memory],
//     [github.com/ianunruh/synapse/checkpointstore/memory] — root
//     module, suitable for tests and development.
//   - SQLite: github.com/ianunruh/synapse/eventstore/sqlite (and the
//     matching snapshotstore/sqlite, checkpointstore/sqlite) — sibling
//     modules, pure-Go via modernc.org/sqlite.
//   - Postgres: github.com/ianunruh/synapse/eventstore/postgres (and
//     the matching snapshot/checkpoint variants) — sibling modules
//     via jackc/pgx/v5.
//
// Shared infrastructure:
//
//   - [github.com/ianunruh/synapse/idgen] — UUIDv7 generator.
//   - github.com/ianunruh/synapse/pgtest — Postgres testing harness
//     (testcontainers-go).
//   - [github.com/ianunruh/synapse/codec/codectest],
//     [github.com/ianunruh/synapse/eventstore/eventstoretest],
//     [github.com/ianunruh/synapse/snapshotstore/snapshotstoretest],
//     [github.com/ianunruh/synapse/checkpointstore/checkpointstoretest]
//     — shared contract test suites for the corresponding interfaces.
//
// Architectural decisions are recorded under docs/adr/. Read the
// relevant ADR before relitigating a decision.
package synapse
