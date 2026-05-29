package postgres_test

import (
	"testing"

	"github.com/ianunruh/synapse/checkpointstore/checkpointstorebench"
	"github.com/ianunruh/synapse/es"
)

func BenchmarkPostgresStore(b *testing.B) {
	checkpointstorebench.Run(b, func(b *testing.B) es.CheckpointStore {
		return newStore(b)
	})
}
