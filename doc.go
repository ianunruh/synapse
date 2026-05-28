// Package synapse is a toolkit for building event-sourced systems in Go.
//
// The library is organized as a set of small subpackages so applications
// import only what they need. The current entry point is:
//
//   - [github.com/ianunruh/synapse/es] provides the core event sourcing
//     primitives: aggregates, envelopes, event stores, repositories,
//     codec registry, and command handlers.
//
// Future sibling packages will add codecs (encoding/json, protobuf),
// store backends (in-memory, SQL, log-oriented), an ID generator, admin
// RPCs, and an optional web UI.
package synapse
