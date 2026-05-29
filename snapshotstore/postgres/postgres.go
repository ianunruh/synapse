// Package postgres provides a Postgres-backed [es.SnapshotStore] for
// the synapse event sourcing toolkit, built on jackc/pgx/v5 and
// pgxpool.
//
// Schema:
//
//	snapshots(stream_id PK, version, type, content_type,
//	          recorded_at, metadata JSONB, payload BYTEA)
//
// One row per stream — Save replaces any prior snapshot via
// ON CONFLICT(stream_id) DO UPDATE.
//
// The Store does not coordinate transactions with the event store;
// the Repository deliberately treats snapshot save as best-effort.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ianunruh/synapse/es"
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
		return fmt.Errorf("synapse/snapshotstore/postgres: migrate: %w", err)
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

// Store is a Postgres-backed [es.SnapshotStore]. The caller owns the
// pool and is responsible for closing it.
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

// Save implements [es.SnapshotStore]. Upserts the snapshot.
func (s *Store) Save(ctx context.Context, snap es.RawSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	metadataJSON := []byte("{}")
	if len(snap.Metadata) > 0 {
		b, err := json.Marshal(snap.Metadata)
		if err != nil {
			return fmt.Errorf("synapse/snapshotstore/postgres: marshal metadata: %w", err)
		}
		metadataJSON = b
	}

	_, err := s.pool.Exec(ctx,
		`INSERT INTO snapshots
			(stream_id, version, type, content_type, recorded_at, metadata, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (stream_id) DO UPDATE SET
			version = EXCLUDED.version,
			type = EXCLUDED.type,
			content_type = EXCLUDED.content_type,
			recorded_at = EXCLUDED.recorded_at,
			metadata = EXCLUDED.metadata,
			payload = EXCLUDED.payload`,
		string(snap.StreamID),
		int64(snap.Version),
		snap.Type,
		string(snap.ContentType),
		snap.RecordedAt.UTC(),
		metadataJSON,
		snap.Payload,
	)
	if err != nil {
		return fmt.Errorf("synapse/snapshotstore/postgres: save: %w", err)
	}
	return nil
}

// Latest implements [es.SnapshotStore]. Returns (zero, false, nil)
// when no snapshot exists for stream.
func (s *Store) Latest(ctx context.Context, stream es.StreamID) (es.RawSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return es.RawSnapshot{}, false, err
	}

	var (
		version      int64
		snapType     string
		contentType  string
		recordedAt   time.Time
		metadataJSON string
		payload      []byte
	)
	err := s.pool.QueryRow(ctx,
		`SELECT version, type, content_type, recorded_at, metadata, payload
		 FROM snapshots WHERE stream_id = $1`,
		string(stream),
	).Scan(&version, &snapType, &contentType, &recordedAt, &metadataJSON, &payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return es.RawSnapshot{}, false, nil
		}
		return es.RawSnapshot{}, false, fmt.Errorf("synapse/snapshotstore/postgres: latest: %w", err)
	}

	var metadata es.Metadata
	if metadataJSON != "" && metadataJSON != "{}" {
		metadata = make(es.Metadata)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return es.RawSnapshot{}, false, fmt.Errorf("synapse/snapshotstore/postgres: unmarshal metadata: %w", err)
		}
	}

	return es.RawSnapshot{
		StreamID:    stream,
		Version:     uint64(version),
		Type:        snapType,
		ContentType: es.ContentType(contentType),
		RecordedAt:  recordedAt.UTC(),
		Metadata:    metadata,
		Payload:     payload,
	}, true, nil
}
