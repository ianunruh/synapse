package memory_test

import (
	"testing"

	"github.com/ianunruh/synapse/checkpointstore/checkpointstorebench"
	"github.com/ianunruh/synapse/checkpointstore/memory"
	"github.com/ianunruh/synapse/es"
)

func BenchmarkMemoryStore(b *testing.B) {
	checkpointstorebench.Run(b, func(_ *testing.B) es.CheckpointStore {
		return memory.New()
	})
}
