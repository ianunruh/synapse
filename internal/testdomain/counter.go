// Package testdomain provides shared test fixtures — aggregates,
// events, commands, and a populated codec registry — for synapse's
// internal tests. The Counter aggregate is the canonical "hello
// world" example used across the es, middleware, snapshotting, and
// any future projection/subscription test suites.
//
// This package is internal to the synapse module and not intended for
// use outside it.
package testdomain

import (
	"context"
	"fmt"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
)

// CounterStream is the canonical stream id used by tests that exercise
// a single [Counter] aggregate.
const CounterStream es.StreamID = "test/counter"

// Counter is a simple aggregate that tracks an integer value mutated
// by [CounterIncremented] and [CounterReset] events. It implements
// [es.Snapshotter] so snapshot-related tests can exercise the full
// snapshot path.
//
// Tests inspect the [Counter.Count] field directly.
type Counter struct {
	*es.AggregateBase
	Count int
}

// NewCounter returns a fresh [Counter] bound to id.
func NewCounter(id es.StreamID) *Counter {
	return &Counter{AggregateBase: es.NewAggregateBase(id)}
}

// CounterIncremented is the event recorded by [Counter.Increment].
type CounterIncremented struct {
	By int `json:"by"`
}

// CounterReset is the event recorded by [Counter.Reset].
type CounterReset struct{}

// Apply implements [es.Aggregate].
func (c *Counter) Apply(env es.Envelope) error {
	switch p := env.Payload.(type) {
	case CounterIncremented:
		c.Count += p.By
	case CounterReset:
		c.Count = 0
	}
	return nil
}

// Increment stages a [CounterIncremented] event.
func (c *Counter) Increment(by int) error {
	return c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}

// Reset stages a [CounterReset] event.
func (c *Counter) Reset() error {
	return c.Record("counter.reset", CounterReset{}, c.Apply)
}

// CounterSnapshot is the serialized state of a [Counter] at a point
// in time.
type CounterSnapshot struct {
	Count int `json:"count"`
}

// SnapshotType implements [es.Snapshotter].
func (c *Counter) SnapshotType() string {
	return "counter.snapshot.v1"
}

// Snapshot implements [es.Snapshotter].
func (c *Counter) Snapshot() (any, error) {
	return CounterSnapshot{Count: c.Count}, nil
}

// Restore implements [es.Snapshotter].
func (c *Counter) Restore(state any) error {
	s, ok := state.(CounterSnapshot)
	if !ok {
		return fmt.Errorf("testdomain: invalid counter snapshot type %T", state)
	}
	c.Count = s.Count
	return nil
}

// IncrementCmd is a typed command targeting [Counter.Increment].
type IncrementCmd struct{ By int }

// IncrementHandler is the [es.Handler] for [IncrementCmd].
func IncrementHandler(_ context.Context, cmd IncrementCmd, c *Counter) error {
	return c.Increment(cmd.By)
}

// NewRegistry constructs an [es.Registry] populated with JSON codecs
// for the Counter event types and the snapshot codec.
func NewRegistry() *es.Registry {
	reg := NewRegistryWithoutSnapshot()
	es.Register(reg, "counter.snapshot.v1", jsoncodec.For[CounterSnapshot]())
	return reg
}

// NewRegistryWithoutSnapshot constructs an [es.Registry] populated
// with only the event codecs, omitting the snapshot codec. Used by
// tests that exercise the "snapshot codec missing" failure path.
func NewRegistryWithoutSnapshot() *es.Registry {
	reg := es.NewRegistry()
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())
	es.Register(reg, "counter.reset", jsoncodec.For[CounterReset]())
	return reg
}
