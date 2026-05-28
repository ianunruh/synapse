package memory_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/snapshotstore/memory"
	"github.com/ianunruh/synapse/snapshotstore/snapshotstoretest"
)

// ----- Static interface assertion -------------------------------------

func TestStoreImplementsSnapshotStore(t *testing.T) {
	var _ es.SnapshotStore = memory.New()
}

// ----- Shared contract suite ------------------------------------------

func TestMemorySnapshotStore_Contract(t *testing.T) {
	snapshotstoretest.RunContract(t, func(_ *testing.T) es.SnapshotStore {
		return memory.New()
	})
}
