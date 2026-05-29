// Package pgtest provides a Postgres testing harness used by every
// Postgres-backed synapse store. It manages a single per-binary
// testcontainers Postgres instance and hands out fresh per-test
// databases backed by a pgxpool.
//
// Usage from a backend's *_test.go:
//
//	func newStore(tb testing.TB) *mystore.Store {
//	    tb.Helper()
//	    pool := pgtest.Pool(tb)
//	    store, err := mystore.New(tb.Context(), pool)
//	    if err != nil { tb.Fatalf("New: %v", err) }
//	    return store
//	}
//
// One container per test binary (initialized lazily on first Pool
// call), one database per test. Per-test databases are cleaned up
// after the test completes; the container is left running for the
// remainder of the binary's lifetime and is reaped when the process
// exits.
//
// LISTEN/NOTIFY channels in Postgres are scoped per database, so the
// per-test database isolation extends to pub/sub state — exactly what
// the Postgres event store's subscription tests need.
package pgtest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	containerOnce sync.Once
	containerDSN  string
	containerErr  error

	dbCounter atomic.Uint64
)

// Pool returns a *pgxpool.Pool connected to a fresh per-test database.
// Lazily starts a Postgres container the first time it is called in
// the test binary; subsequent calls reuse the container.
//
// The pool, the per-test database, and (eventually, on process exit)
// the container are cleaned up automatically. If Docker is unavailable
// or the container fails to start, Pool calls tb.Skip with a descriptive
// reason so tests degrade gracefully on machines without Docker.
func Pool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	baseDSN := ensureContainer(tb)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	maint, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		tb.Fatalf("pgtest: maintenance pool: %v", err)
	}
	defer maint.Close()

	dbName := fmt.Sprintf("test_%d_%s", dbCounter.Add(1), sanitize(tb.Name()))
	if _, err := maint.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		tb.Fatalf("pgtest: CREATE DATABASE: %v", err)
	}

	pool, err := pgxpool.New(ctx, withDatabase(baseDSN, dbName))
	if err != nil {
		tb.Fatalf("pgtest: test pool: %v", err)
	}
	tb.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		drop, err := pgxpool.New(dropCtx, baseDSN)
		if err == nil {
			_, _ = drop.Exec(dropCtx, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
			drop.Close()
		}
	})
	return pool
}

func ensureContainer(tb testing.TB) string {
	containerOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		c, err := tcpostgres.Run(ctx, "postgres:17-alpine",
			tcpostgres.WithDatabase("synapse"),
			tcpostgres.WithUsername("synapse"),
			tcpostgres.WithPassword("synapse"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second)),
		)
		if err != nil {
			containerErr = fmt.Errorf("start postgres container: %w", err)
			return
		}
		dsn, err := c.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			containerErr = fmt.Errorf("connection string: %w", err)
			return
		}
		containerDSN = dsn
	})
	if containerErr != nil {
		tb.Skipf("pgtest: postgres container unavailable: %v", containerErr)
	}
	return containerDSN
}

// sanitize maps a test name to a Postgres-safe identifier suffix.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + 32)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > 32 {
		out = out[:32]
	}
	return out
}

// withDatabase rewrites the database segment of a Postgres URL.
func withDatabase(dsn, db string) string {
	q := strings.Index(dsn, "?")
	prefix, suffix := dsn, ""
	if q >= 0 {
		prefix = dsn[:q]
		suffix = dsn[q:]
	}
	slash := strings.LastIndex(prefix, "/")
	if slash < 0 {
		return dsn
	}
	return prefix[:slash+1] + db + suffix
}
