package memory_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/snapshotstore/memory"
	"github.com/ianunruh/synapse/snapshotstore/snapshotstorebench"
)

func BenchmarkMemoryStore(b *testing.B) {
	snapshotstorebench.Run(b, func(_ *testing.B) es.SnapshotStore {
		return memory.New()
	})
}
