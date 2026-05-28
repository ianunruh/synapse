// Package testdomain provides shared test fixtures — aggregates,
// events, commands, and a populated codec registry — for synapse's
// internal tests. The Counter aggregate is the canonical "hello
// world" example used across the es, middleware, and any future
// projection/subscription test suites.
//
// This package is internal to the synapse module and not intended for
// use outside it.
package testdomain

import (
	"context"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
)

// CounterStream is the canonical stream id used by tests that exercise
// a single [Counter] aggregate.
const CounterStream es.StreamID = "test/counter"

// Counter is a simple aggregate that tracks an integer value mutated
// by [CounterIncremented] and [CounterReset] events. Tests inspect
// the [Counter.Count] field directly.
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

// IncrementCmd is a typed command targeting [Counter.Increment].
type IncrementCmd struct{ By int }

// IncrementHandler is the [es.Handler] for [IncrementCmd].
func IncrementHandler(_ context.Context, cmd IncrementCmd, c *Counter) error {
	return c.Increment(cmd.By)
}

// NewRegistry constructs an [es.Registry] populated with JSON codecs
// for the Counter event types.
func NewRegistry() *es.Registry {
	reg := es.NewRegistry()
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())
	es.Register(reg, "counter.reset", jsoncodec.For[CounterReset]())
	return reg
}
