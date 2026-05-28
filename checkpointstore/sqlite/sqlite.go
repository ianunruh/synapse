// Package sqlite provides a SQLite-backed [es.CheckpointStore] for the
// synapse event sourcing toolkit.
//
// Schema:
//
//	checkpoints(name PK, position)
//
// One row per checkpoint name. Save upserts via
// ON CONFLICT(name) DO UPDATE. Load returns (0, false, nil) for
// missing names; Reset deletes the row.
//
// The package blank-imports modernc.org/sqlite to register the
// pure-Go driver. WAL + busy_timeout pragmas are strongly recommended
// for concurrent workloads:
//
//	db, err := sql.Open("sqlite", "file:store.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
//	cps, err := sqlite.New(ctx, db)
//
// The checkpoint store may share the same *sql.DB and file with the
// SQLite event store and snapshot store, which is the common
// production case: one database to back up, one to restore.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // register the "sqlite" driver
)

// Schema is the SQL DDL this Store requires. It is exported so users
// who manage migrations externally (goose, golang-migrate, atlas, etc.)
// can feed it to their own tooling. [New] applies it by default;
// [WithoutMigrate] disables that. [Migrate] applies it explicitly.
//
//go:embed schema.sql
var Schema string

// Migrate applies [Schema] to db. It is idempotent (CREATE TABLE IF
// NOT EXISTS), so repeated calls are safe.
func Migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("synapse/checkpointstore/sqlite: migrate: %w", err)
	}
	return nil
}

// Option configures [New].
type Option func(*options)

type options struct {
	skipMigrate bool
}

// WithoutMigrate disables the automatic schema migration that [New]
// performs by default. Use this when the schema is managed by an
// external tool or by an explicit call to [Migrate].
func WithoutMigrate() Option {
	return func(o *options) { o.skipMigrate = true }
}

// Store is a SQLite-backed [es.CheckpointStore].
//
// A Store wraps a *sql.DB the caller provides; the caller retains
// ownership and is responsible for closing the database.
type Store struct {
	db *sql.DB
}

// New returns a Store wrapping db. By default New applies [Schema]
// (idempotent); pass [WithoutMigrate] to skip that step.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if !o.skipMigrate {
		if err := Migrate(ctx, db); err != nil {
			return nil, err
		}
	}
	return &Store{db: db}, nil
}

// Save implements [es.CheckpointStore].
func (s *Store) Save(ctx context.Context, name string, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO checkpoints (name, position) VALUES (?, ?)
		 ON CONFLICT(name) DO UPDATE SET position = excluded.position`,
		name, int64(position),
	)
	if err != nil {
		return fmt.Errorf("synapse/checkpointstore/sqlite: save: %w", err)
	}
	return nil
}

// Load implements [es.CheckpointStore]. Returns (0, false, nil) when
// no checkpoint has been saved for name.
func (s *Store) Load(ctx context.Context, name string) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	var position int64
	err := s.db.QueryRowContext(ctx,
		`SELECT position FROM checkpoints WHERE name = ?`,
		name,
	).Scan(&position)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("synapse/checkpointstore/sqlite: load: %w", err)
	}
	return uint64(position), true, nil
}

// Reset implements [es.CheckpointStore].
func (s *Store) Reset(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM checkpoints WHERE name = ?`,
		name,
	)
	if err != nil {
		return fmt.Errorf("synapse/checkpointstore/sqlite: reset: %w", err)
	}
	return nil
}
