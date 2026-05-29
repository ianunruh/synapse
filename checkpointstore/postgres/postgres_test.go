package postgres_test

import (
	"testing"

	"github.com/ianunruh/synapse/checkpointstore/checkpointstoretest"
	pgstore "github.com/ianunruh/synapse/checkpointstore/postgres"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/pgtest"
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

func TestStoreImplementsCheckpointStore(t *testing.T) {
	var _ es.CheckpointStore = newStore(t)
}

func TestPostgresCheckpointStore_Contract(t *testing.T) {
	checkpointstoretest.RunContract(t, func(t *testing.T) es.CheckpointStore {
		return newStore(t)
	})
}
