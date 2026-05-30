// Package postgres provides a Postgres-backed [es.CheckpointStore]
// for the synapse event sourcing toolkit, built on jackc/pgx/v5 and
// pgxpool.
//
// Schema:
//
//	checkpoints(name PK, position)
//
// One row per checkpoint name. Save upserts via
// ON CONFLICT(name) DO UPDATE. Load returns (0, false, nil) for
// missing names; Reset deletes the row.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the SQL DDL this Store requires.
//
//go:embed schema.sql
var Schema string

// Migrate applies [Schema] to the pool. Idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("synapse: migrate: %w", err)
	}
	return nil
}

// Option configures [New].
type Option func(*options)

type options struct {
	skipMigrate bool
}

// WithoutMigrate disables the automatic schema migration that [New]
// performs by default.
func WithoutMigrate() Option {
	return func(o *options) { o.skipMigrate = true }
}

// Store is a Postgres-backed [es.CheckpointStore]. The caller owns
// the pool and is responsible for closing it.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store wrapping pool. By default applies [Schema];
// pass [WithoutMigrate] to skip.
func New(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Store, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if !o.skipMigrate {
		if err := Migrate(ctx, pool); err != nil {
			return nil, err
		}
	}
	return &Store{pool: pool}, nil
}

// Save implements [es.CheckpointStore].
func (s *Store) Save(ctx context.Context, name string, position uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO checkpoints (name, position) VALUES ($1, $2)
		 ON CONFLICT (name) DO UPDATE SET position = EXCLUDED.position`,
		name, int64(position),
	)
	if err != nil {
		return fmt.Errorf("synapse: save: %w", err)
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
	err := s.pool.QueryRow(ctx,
		`SELECT position FROM checkpoints WHERE name = $1`,
		name,
	).Scan(&position)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("synapse: load: %w", err)
	}
	return uint64(position), true, nil
}

// Reset implements [es.CheckpointStore].
func (s *Store) Reset(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM checkpoints WHERE name = $1`,
		name,
	)
	if err != nil {
		return fmt.Errorf("synapse: reset: %w", err)
	}
	return nil
}
