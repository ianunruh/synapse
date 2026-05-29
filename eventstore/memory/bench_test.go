package memory_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/eventstorebench"
	"github.com/ianunruh/synapse/eventstore/memory"
)

func BenchmarkMemoryStore(b *testing.B) {
	eventstorebench.Run(b, func(_ *testing.B) es.EventStore {
		return memory.New()
	})
}
