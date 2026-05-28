package memory_test

import (
	"testing"

	"github.com/ianunruh/synapse/checkpointstore/checkpointstoretest"
	"github.com/ianunruh/synapse/checkpointstore/memory"
	"github.com/ianunruh/synapse/es"
)

// ----- Static interface assertion -------------------------------------

func TestStoreImplementsCheckpointStore(t *testing.T) {
	var _ es.CheckpointStore = memory.New()
}

// ----- Shared contract suite ------------------------------------------

func TestMemoryCheckpointStore_Contract(t *testing.T) {
	checkpointstoretest.RunContract(t, func(_ *testing.T) es.CheckpointStore {
		return memory.New()
	})
}
