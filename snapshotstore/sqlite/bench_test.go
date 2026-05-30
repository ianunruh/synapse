package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/ianunruh/synapse/es"
	sqlitestore "github.com/ianunruh/synapse/snapshotstore/sqlite"
	"github.com/ianunruh/synapse/snapshotstore/snapshotstorebench"

	_ "modernc.org/sqlite"
)

func newStoreB(b *testing.B) *sqlitestore.Store {
	b.Helper()
	dsn := "file:" + filepath.Join(b.TempDir(), "snapshots.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatalf("sql.Open: %v", err)
	}
	b.Cleanup(func() { db.Close() })
	store, err := sqlitestore.New(b.Context(), db)
	if err != nil {
		b.Fatalf("sqlitestore.New: %v", err)
	}
	return store
}

func BenchmarkSQLiteStore(b *testing.B) {
	snapshotstorebench.Run(b, func(b *testing.B) es.SnapshotStore {
		return newStoreB(b)
	})
}
