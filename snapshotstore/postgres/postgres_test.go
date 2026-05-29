package postgres_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/pgtest"
	pgstore "github.com/ianunruh/synapse/snapshotstore/postgres"
	"github.com/ianunruh/synapse/snapshotstore/snapshotstoretest"
)

func newStore(tb testing.TB) *pgstore.Store {
	tb.Helper()
	pool := pgtest.Pool(tb)
	store, err := pgstore.New(tb.Context(), pool)
	if err != nil {
		tb.Fatalf("pgstore.New: %v", err)
	}
	return store
}

func TestStoreImplementsSnapshotStore(t *testing.T) {
	var _ es.SnapshotStore = newStore(t)
}

func TestPostgresSnapshotStore_Contract(t *testing.T) {
	snapshotstoretest.RunContract(t, func(t *testing.T) es.SnapshotStore {
		return newStore(t)
	})
}
