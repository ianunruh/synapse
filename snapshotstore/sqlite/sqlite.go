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
// [New]. WAL + busy_timeout pragmas are strongly recommended for
// concurrent workloads:
//
//	db, err := sql.Open("sqlite", "file:store.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
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

//go:embed schema.sql
var schemaSQL string

// Store is a SQLite-backed [es.SnapshotStore].
//
// A Store wraps a *sql.DB the caller provides; the caller retains
// ownership and is responsible for closing the database.
type Store struct {
	db *sql.DB
}

// New applies the snapshots schema (idempotent) and returns a Store
// wrapping db.
func New(ctx context.Context, db *sql.DB) (*Store, error) {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("synapse/snapshotstore/sqlite: init schema: %w", err)
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
			return fmt.Errorf("synapse/snapshotstore/sqlite: marshal metadata: %w", err)
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
		return fmt.Errorf("synapse/snapshotstore/sqlite: save: %w", err)
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
		return es.RawSnapshot{}, false, fmt.Errorf("synapse/snapshotstore/sqlite: latest: %w", err)
	}

	var metadata es.Metadata
	if metadataJSON != "" && metadataJSON != "{}" {
		metadata = make(es.Metadata)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return es.RawSnapshot{}, false, fmt.Errorf("synapse/snapshotstore/sqlite: unmarshal metadata: %w", err)
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
