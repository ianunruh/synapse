package postgres_test

import (
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/eventstoretest"
	pgstore "github.com/ianunruh/synapse/eventstore/postgres"
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

func TestStoreImplementsSubscribable(t *testing.T) {
	var _ es.SubscribableEventStore = newStore(t)
}

func TestPostgresStore_Contract(t *testing.T) {
	eventstoretest.RunSubscribableContract(t, func(t *testing.T) es.SubscribableEventStore {
		return newStore(t)
	})
}
