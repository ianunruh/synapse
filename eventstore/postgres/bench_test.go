package postgres_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/eventstorebench"
)

func BenchmarkPostgresStore(b *testing.B) {
	eventstorebench.Run(b, func(b *testing.B) es.EventStore {
		return newStore(b)
	})
}
