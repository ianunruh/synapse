package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/eventstoretest"
	sqlitestore "github.com/ianunruh/synapse/eventstore/sqlite"

	_ "modernc.org/sqlite"
)

// newStore builds a Store backed by a fresh file-based SQLite DB.
// ":memory:" is per-connection in SQLite which breaks any test where
// Append and Subscribe goroutines might get different connections; a
// file-based DB plus WAL + busy_timeout pragmas let concurrent
// readers and a single writer coexist without SQLITE_BUSY failures.
func newStore(t *testing.T) *sqlitestore.Store {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "events.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := sqlitestore.New(t.Context(), db)
	if err != nil {
		t.Fatalf("sqlitestore.New: %v", err)
	}
	return store
}

// ----- Static interface assertion -------------------------------------

func TestStoreImplementsSubscribable(t *testing.T) {
	var _ es.SubscribableEventStore = newStore(t)
}

// ----- Shared contract suite ------------------------------------------

func TestSQLiteStore_Contract(t *testing.T) {
	eventstoretest.RunSubscribableContract(t, func(t *testing.T) es.SubscribableEventStore {
		return newStore(t)
	})
}

// ----- SQLite-specific tests ------------------------------------------

func TestPersistence_AcrossStoreInstances(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "events.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	stream := es.StreamID("persist")

	// Append via first instance.
	{
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("open 1: %v", err)
		}
		store, err := sqlitestore.New(ctx, db)
		if err != nil {
			t.Fatalf("New 1: %v", err)
		}
		if _, err := store.Append(ctx, stream, es.NoStream, eventstoretest.MakeEvents(3, stream, 1)...); err != nil {
			t.Fatalf("Append: %v", err)
		}
		db.Close()
	}

	// Read via second instance.
	{
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("open 2: %v", err)
		}
		defer db.Close()
		store, err := sqlitestore.New(ctx, db)
		if err != nil {
			t.Fatalf("New 2: %v", err)
		}
		got, err := eventstoretest.Collect(store.Load(ctx, stream, es.ReadOptions{}))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(got) != 3 {
			t.Errorf("len = %d, want 3", len(got))
		}
		for i, ev := range got {
			if ev.Version != uint64(i+1) {
				t.Errorf("events[%d].Version = %d, want %d", i, ev.Version, i+1)
			}
		}
	}
}
