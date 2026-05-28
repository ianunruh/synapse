package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/snapshotstore/snapshotstoretest"
	sqlitestore "github.com/ianunruh/synapse/snapshotstore/sqlite"

	_ "modernc.org/sqlite"
)

// newStore builds a Store backed by a fresh file-based SQLite DB.
// See the eventstore/sqlite README/doc for why we use file-based +
// WAL + busy_timeout in tests.
func newStore(t *testing.T) *sqlitestore.Store {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "snapshots.db") +
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

func TestStoreImplementsSnapshotStore(t *testing.T) {
	var _ es.SnapshotStore = newStore(t)
}

// ----- Shared contract suite ------------------------------------------

func TestSQLiteSnapshotStore_Contract(t *testing.T) {
	snapshotstoretest.RunContract(t, func(t *testing.T) es.SnapshotStore {
		return newStore(t)
	})
}
