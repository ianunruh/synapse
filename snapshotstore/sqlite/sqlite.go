// Package sqlite provides a SQLite-backed [es.SnapshotStore] for the
// synapse event sourcing toolkit.
//
// Schema:
//
//	snapshots(stream_id PK, version, type, content_type,
//	          recorded_at, metadata, payload)
//
// One row per stream — Save replaces any prior snapshot via
// ON CONFLICT(stream_id) DO UPDATE. The schema is applied on [New]
// via CREATE TABLE IF NOT EXISTS, so repeated calls are idempotent.
//
// The package blank-imports modernc.org/sqlite to register the
// pure-Go driver under the name "sqlite". Open the database with
// that driver (or any compatible driver) and pass the *sql.DB to
// [New]. WAL + busy_timeout + _txlock=immediate are strongly
// recommended for concurrent workloads (see the eventstore/sqlite
// package doc for the full rationale on _txlock):
//
//	db, err := sql.Open("sqlite",
//	    "file:store.db?_pragma=journal_mode(WAL)"+
//	    "&_pragma=busy_timeout(5000)"+
//	    "&_txlock=immediate")
//	snaps, err := sqlite.New(ctx, db)
//
// Events and snapshots may share the same *sql.DB and the same
// database file. The Store does not coordinate transactions with the
// event store; the Repository deliberately treats snapshot save as
// best-effort (events are committed first; a snapshot failure is
// logged at Warn level via slog and does not fail the Save).
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ianunruh/synapse/es"

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
// performs by default. Use this when the schema is managed by an
// external tool (goose, golang-migrate, atlas, etc.) or by an
// explicit call to [Migrate].
func WithoutMigrate() Option {
	return func(o *options) { o.skipMigrate = true }
}

// Store is a SQLite-backed [es.SnapshotStore].
//
// A Store wraps a *sql.DB the caller provides; the caller retains
// ownership and is responsible for closing the database.
type Store struct {
	db *sql.DB
}

// New returns a Store wrapping db. By default New applies [Schema]
// (idempotent); pass [WithoutMigrate] to skip that step when the
// schema is managed externally.
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

// Save implements [es.SnapshotStore]. It upserts the snapshot,
// replacing any prior snapshot for the same stream.
func (s *Store) Save(ctx context.Context, snap es.RawSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	metadataJSON := "{}"
	if len(snap.Metadata) > 0 {
		b, err := json.Marshal(snap.Metadata)
		if err != nil {
			return fmt.Errorf("synapse: marshal metadata: %w", err)
		}
		metadataJSON = string(b)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO snapshots
			(stream_id, version, type, content_type, recorded_at, metadata, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(stream_id) DO UPDATE SET
			version = excluded.version,
			type = excluded.type,
			content_type = excluded.content_type,
			recorded_at = excluded.recorded_at,
			metadata = excluded.metadata,
			payload = excluded.payload`,
		string(snap.StreamID),
		int64(snap.Version),
		snap.Type,
		string(snap.ContentType),
		snap.RecordedAt.UnixNano(),
		metadataJSON,
		snap.Payload,
	)
	if err != nil {
		return fmt.Errorf("synapse: save: %w", err)
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
		recordedNano int64
		metadataJSON string
		payload      []byte
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT version, type, content_type, recorded_at, metadata, payload
		 FROM snapshots WHERE stream_id = ?`,
		string(stream),
	).Scan(&version, &snapType, &contentType, &recordedNano, &metadataJSON, &payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return es.RawSnapshot{}, false, nil
		}
		return es.RawSnapshot{}, false, fmt.Errorf("synapse: latest: %w", err)
	}

	var metadata es.Metadata
	if metadataJSON != "" && metadataJSON != "{}" {
		metadata = make(es.Metadata)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return es.RawSnapshot{}, false, fmt.Errorf("synapse: unmarshal metadata: %w", err)
		}
	}

	return es.RawSnapshot{
		StreamID:    stream,
		Version:     uint64(version),
		Type:        snapType,
		ContentType: es.ContentType(contentType),
		RecordedAt:  time.Unix(0, recordedNano).UTC(),
		Metadata:    metadata,
		Payload:     payload,
	}, true, nil
}
