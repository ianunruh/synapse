package postgres_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/snapshotstore/snapshotstorebench"
)

func BenchmarkPostgresStore(b *testing.B) {
	snapshotstorebench.Run(b, func(b *testing.B) es.SnapshotStore {
		return newStore(b)
	})
}
