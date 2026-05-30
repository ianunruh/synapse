package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"strings"
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
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"
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

// ----- Schema management ----------------------------------------------

func TestSchema_NonEmpty(t *testing.T) {
	if !strings.Contains(sqlitestore.Schema, "CREATE TABLE") {
		t.Errorf("Schema does not look like DDL: %q", sqlitestore.Schema)
	}
}

func TestNew_WithoutMigrate(t *testing.T) {
	// With WithoutMigrate, New does NOT create the snapshots table;
	// the caller is responsible for running Migrate or applying the
	// schema via some other tool first.
	ctx := t.Context()
	dsn := "file:" + filepath.Join(t.TempDir(), "noschema.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	store, err := sqlitestore.New(ctx, db, sqlitestore.WithoutMigrate())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Save should fail because the snapshots table does not exist.
	if err := store.Save(ctx, snapshotstoretest.MakeSnapshot("x", 1)); err == nil {
		t.Errorf("Save on unmigrated DB: expected error, got nil")
	}

	// After explicit Migrate, the same store works.
	if err := sqlitestore.Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := store.Save(ctx, snapshotstoretest.MakeSnapshot("x", 1)); err != nil {
		t.Errorf("Save after Migrate: %v", err)
	}
}

func TestMigrate_Idempotent(t *testing.T) {
	ctx := t.Context()
	dsn := "file:" + filepath.Join(t.TempDir(), "idem.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for range 3 {
		if err := sqlitestore.Migrate(ctx, db); err != nil {
			t.Errorf("Migrate: %v", err)
		}
	}
}
