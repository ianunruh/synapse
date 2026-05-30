// Package sqlite provides a SQLite-backed [es.SubscribableEventStore]
// for the synapse event sourcing toolkit.
//
// Schema:
//
//	events(global_position PK, event_id, stream_id, version,
//	       type, content_type, recorded_at,
//	       causation, correlation, metadata, payload,
//	       UNIQUE(stream_id, version))
//
// The schema is applied on [New] via CREATE TABLE IF NOT EXISTS, so
// repeated calls are idempotent. The store keeps no migration state of
// its own.
//
// Concurrency model:
//
//   - Append serializes through SQLite's single-writer lock. Two
//     concurrent appenders for the same stream race; the loser sees a
//     UNIQUE constraint violation and receives *[es.ConflictError].
//   - Live subscribers are woken via an in-process close-and-replace
//     signal channel maintained by the [Store] value. This means live
//     tail works for subscribers in the same process as the appender.
//     Cross-process consumers see new events only when they next
//     re-Subscribe (catch-up reads); long-poll/file-watch fallbacks
//     are deliberately out of scope for v0.
//
// The package blank-imports modernc.org/sqlite to register the pure-Go
// driver under the name "sqlite". Open the database with that driver
// (or any driver compatible with the schema and the registered driver
// name) and pass the *sql.DB to [New]:
//
//	db, err := sql.Open("sqlite",
//	    "file:events.db?_pragma=journal_mode(WAL)"+
//	    "&_pragma=busy_timeout(5000)"+
//	    "&_txlock=immediate")
//	store, err := sqlite.New(ctx, db)
//
// All three settings matter for a concurrent appender:
//
//   - journal_mode(WAL) lets readers proceed without blocking the
//     writer.
//   - busy_timeout(5000) makes the driver wait up to 5s for an
//     in-progress writer rather than failing with SQLITE_BUSY.
//   - _txlock=immediate makes every transaction begin as BEGIN
//     IMMEDIATE rather than the database/sql default of BEGIN
//     DEFERRED. This matters because Append's read-then-write
//     pattern under BEGIN DEFERRED can race with another writer that
//     committed after the read snapshot opened, producing
//     SQLITE_BUSY_SNAPSHOT (517) — which busy_timeout cannot recover
//     from. BEGIN IMMEDIATE acquires the writer lock at transaction
//     start, so late writers wait for the lock and open a fresh
//     snapshot when they get it.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"

	"github.com/ianunruh/synapse/es"
	sqlitedrv "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

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

// Store is a SQLite-backed [es.SubscribableEventStore].
//
// A Store wraps a *sql.DB the caller provides; the caller retains
// ownership and is responsible for closing the database.
type Store struct {
	db *sql.DB

	notifyMu sync.Mutex
	notify   chan struct{}
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
	return &Store{
		db:     db,
		notify: make(chan struct{}),
	}, nil
}

// Append implements [es.EventStore]. It runs inside a single
// transaction: SELECT current head, validate expected, INSERT one row
// per event in version order, COMMIT.
//
// A successful Append closes-and-replaces the internal signal channel
// to wake live subscribers in the same process.
func (s *Store) Append(
	ctx context.Context,
	stream es.StreamID,
	expected es.Revision,
	events ...es.RawEnvelope,
) (es.Revision, error) {
	if err := ctx.Err(); err != nil {
		return es.Revision{}, err
	}

	if len(events) == 0 {
		return s.head(ctx, stream)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // commit path returns the meaningful error

	var current int64
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?`,
		string(stream),
	).Scan(&current)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse: query head: %w", err)
	}
	currentU := uint64(current)

	if err := checkRevision(stream, expected, currentU); err != nil {
		return es.Revision{}, err
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO events
			(event_id, stream_id, version, type, content_type, recorded_at, causation, correlation, metadata, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse: prepare insert: %w", err)
	}
	defer stmt.Close()

	for i, ev := range events {
		metadataJSON := "{}"
		if len(ev.Metadata) > 0 {
			b, err := json.Marshal(ev.Metadata)
			if err != nil {
				return es.Revision{}, fmt.Errorf("synapse: marshal metadata: %w", err)
			}
			metadataJSON = string(b)
		}

		version := currentU + uint64(i) + 1
		_, err = stmt.ExecContext(ctx,
			ev.EventID,
			string(stream),
			int64(version),
			ev.Type,
			string(ev.ContentType),
			ev.RecordedAt.UnixNano(),
			ev.Causation,
			ev.Correlation,
			metadataJSON,
			ev.Payload,
		)
		if err != nil {
			if isUniqueConflict(err) {
				return es.Revision{}, &es.ConflictError{
					Stream:   stream,
					Expected: expected,
					Actual:   es.Exact(currentU + uint64(i)),
				}
			}
			return es.Revision{}, fmt.Errorf("synapse: insert v%d: %w", version, err)
		}
	}

	if err := tx.Commit(); err != nil {
		if isUniqueConflict(err) {
			return es.Revision{}, &es.ConflictError{
				Stream:   stream,
				Expected: expected,
				Actual:   es.Exact(currentU),
			}
		}
		return es.Revision{}, fmt.Errorf("synapse: commit: %w", err)
	}

	s.broadcast()

	return es.Exact(currentU + uint64(len(events))), nil
}

// Load implements [es.EventStore].
func (s *Store) Load(
	ctx context.Context,
	stream es.StreamID,
	opts es.ReadOptions,
) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(es.RawEnvelope{}, err)
			return
		}

		from := opts.From
		if from == 0 {
			from = 1
		}

		query := `SELECT global_position, event_id, stream_id, version, type, content_type,
			recorded_at, causation, correlation, metadata, payload
			FROM events WHERE stream_id = ? AND version >= ? ORDER BY version`
		args := []any{string(stream), int64(from)}
		if opts.Limit > 0 {
			query += " LIMIT ?"
			args = append(args, int64(opts.Limit))
		}

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			yield(es.RawEnvelope{}, fmt.Errorf("synapse: load query: %w", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}
			env, err := scanEvent(rows)
			if err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}
			if !yield(env, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(es.RawEnvelope{}, fmt.Errorf("synapse: load rows: %w", err))
		}
	}
}

// Subscribe implements [es.SubscribableEventStore]. Yields events
// from the global log with GlobalPosition > opts.From. When opts.Live
// is true the iterator captures the in-process notify channel before
// each query and waits on it after catching up; appends in the same
// process broadcast over that channel.
func (s *Store) Subscribe(ctx context.Context, opts es.SubscriptionOptions) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		from := opts.From
		for {
			if err := ctx.Err(); err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}

			notify := s.currentNotify()

			next, err := s.readGlobal(ctx, from, opts.Types, yield)
			if errors.Is(err, errIterStopped) {
				return
			}
			if err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}
			if next > 0 {
				from = next
			}
			if !opts.Live {
				return
			}

			select {
			case <-notify:
			case <-ctx.Done():
				yield(es.RawEnvelope{}, ctx.Err())
				return
			}
		}
	}
}

// SubscribeStream implements [es.SubscribableEventStore].
func (s *Store) SubscribeStream(ctx context.Context, stream es.StreamID, opts es.SubscriptionOptions) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		from := opts.From
		for {
			if err := ctx.Err(); err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}

			notify := s.currentNotify()

			next, err := s.readStream(ctx, stream, from, opts.Types, yield)
			if errors.Is(err, errIterStopped) {
				return
			}
			if err != nil {
				yield(es.RawEnvelope{}, err)
				return
			}
			if next > 0 {
				from = next
			}
			if !opts.Live {
				return
			}

			select {
			case <-notify:
			case <-ctx.Done():
				yield(es.RawEnvelope{}, ctx.Err())
				return
			}
		}
	}
}

// readGlobal yields events with GlobalPosition > from. Returns the
// highest position yielded (0 if none), or an error.
func (s *Store) readGlobal(
	ctx context.Context,
	from uint64,
	types []string,
	yield func(es.RawEnvelope, error) bool,
) (uint64, error) {
	query := `SELECT global_position, event_id, stream_id, version, type, content_type,
			recorded_at, causation, correlation, metadata, payload
			FROM events WHERE global_position > ?`
	args := []any{int64(from)}
	query, args = appendTypeFilter(query, args, types)
	query += " ORDER BY global_position"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("synapse: subscribe query: %w", err)
	}
	defer rows.Close()

	var last uint64
	for rows.Next() {
		env, err := scanEvent(rows)
		if err != nil {
			return last, err
		}
		if !yield(env, nil) {
			return last, errIterStopped
		}
		last = env.GlobalPosition
	}
	if err := rows.Err(); err != nil {
		return last, fmt.Errorf("synapse: subscribe rows: %w", err)
	}
	return last, nil
}

func (s *Store) readStream(
	ctx context.Context,
	stream es.StreamID,
	from uint64,
	types []string,
	yield func(es.RawEnvelope, error) bool,
) (uint64, error) {
	query := `SELECT global_position, event_id, stream_id, version, type, content_type,
			recorded_at, causation, correlation, metadata, payload
			FROM events WHERE stream_id = ? AND version > ?`
	args := []any{string(stream), int64(from)}
	query, args = appendTypeFilter(query, args, types)
	query += " ORDER BY version"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("synapse: subscribe-stream query: %w", err)
	}
	defer rows.Close()

	var last uint64
	for rows.Next() {
		env, err := scanEvent(rows)
		if err != nil {
			return last, err
		}
		if !yield(env, nil) {
			return last, errIterStopped
		}
		last = env.Version
	}
	if err := rows.Err(); err != nil {
		return last, fmt.Errorf("synapse: subscribe-stream rows: %w", err)
	}
	return last, nil
}

// errIterStopped sentinels caller-break, distinguishing it from real
// errors. It is never returned to callers (the read helpers translate
// it to a normal return).
var errIterStopped = errors.New("iterator stopped by consumer")

// appendTypeFilter adds an `AND type IN (...)` clause and the matching
// args when types is non-empty. Call it after the WHERE clause and
// before any ORDER BY.
func appendTypeFilter(query string, args []any, types []string) (string, []any) {
	if len(types) == 0 {
		return query, args
	}
	query += " AND type IN (" + strings.Repeat("?, ", len(types)-1) + "?)"
	for _, t := range types {
		args = append(args, t)
	}
	return query, args
}

func (s *Store) head(ctx context.Context, stream es.StreamID) (es.Revision, error) {
	var current int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?`,
		string(stream),
	).Scan(&current)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse: query head: %w", err)
	}
	return es.Exact(uint64(current)), nil
}

func (s *Store) currentNotify() chan struct{} {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	return s.notify
}

func (s *Store) broadcast() {
	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()
	close(s.notify)
	s.notify = make(chan struct{})
}

func checkRevision(stream es.StreamID, expected es.Revision, current uint64) error {
	switch expected {
	case es.Any:
		return nil
	case es.NoStream:
		if current == 0 {
			return nil
		}
	case es.StreamExists:
		if current > 0 {
			return nil
		}
	default:
		if v, ok := expected.Value(); ok && v == current {
			return nil
		}
	}
	return &es.ConflictError{
		Stream:   stream,
		Expected: expected,
		Actual:   es.Exact(current),
	}
}

func isUniqueConflict(err error) bool {
	if sqliteErr, ok := errors.AsType[*sqlitedrv.Error](err); ok {
		return sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
	}
	return false
}

func scanEvent(rows *sql.Rows) (es.RawEnvelope, error) {
	var (
		globalPos                                                                       int64
		eventID, streamID, eventType, contentType, causation, correlation, metadataJSON string
		version, recordedAtNano                                                         int64
		payload                                                                         []byte
	)
	if err := rows.Scan(
		&globalPos, &eventID, &streamID, &version, &eventType, &contentType,
		&recordedAtNano, &causation, &correlation, &metadataJSON, &payload,
	); err != nil {
		return es.RawEnvelope{}, fmt.Errorf("synapse: scan: %w", err)
	}

	var metadata es.Metadata
	if metadataJSON != "" && metadataJSON != "{}" {
		metadata = make(es.Metadata)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return es.RawEnvelope{}, fmt.Errorf("synapse: unmarshal metadata: %w", err)
		}
	}

	return es.RawEnvelope{
		EventID:        eventID,
		StreamID:       es.StreamID(streamID),
		Version:        uint64(version),
		GlobalPosition: uint64(globalPos),
		RecordedAt:     time.Unix(0, recordedAtNano).UTC(),
		Type:           eventType,
		ContentType:    es.ContentType(contentType),
		Causation:      causation,
		Correlation:    correlation,
		Metadata:       metadata,
		Payload:        payload,
	}, nil
}
