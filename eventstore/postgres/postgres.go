// Package postgres provides a Postgres-backed [es.SubscribableEventStore]
// for the synapse event sourcing toolkit, built on jackc/pgx/v5 and
// pgxpool.
//
// Schema:
//
//	events(global_position BIGSERIAL PK, event_id, stream_id, version,
//	       type, content_type, recorded_at,
//	       causation, correlation, metadata JSONB, payload BYTEA,
//	       UNIQUE(stream_id, version))
//
// The schema is applied on [New] via CREATE TABLE IF NOT EXISTS, so
// repeated calls are idempotent. Pass [WithoutMigrate] when the schema
// is managed by an external tool (goose, golang-migrate, atlas).
//
// Concurrency model:
//
//   - Append acquires a transaction-scoped advisory lock so all
//     appends serialize on a single global lock. This guarantees that
//     BIGSERIAL global_position values commit in monotonic order, so
//     subscribers never see a position N before position N-1 commits.
//     The cost is that concurrent appends queue rather than parallelize;
//     in v0 this is a deliberate simplicity-vs-throughput trade.
//   - Live subscribers LISTEN on the channel "synapse_events"; Append
//     emits a NOTIFY with payload "<stream_id>:<max_global_position>"
//     inside the same transaction, so the wake-up fires at COMMIT.
//     SubscribeStream consumers can skip the follow-up SELECT when
//     the notification's stream_id does not match their target.
//
// LISTEN holds a connection from the pool for the lifetime of the
// subscription. Pool sizing matters when many live subscribers run
// concurrently.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/ianunruh/synapse/es"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the SQL DDL this Store requires. It is exported so users
// who manage migrations externally can feed it to their own tooling.
//
//go:embed schema.sql
var Schema string

// notifyChannel is the LISTEN/NOTIFY channel name used by every
// [Store]. It is fixed at v0; configurability can be layered on later
// without breaking existing deployments.
const notifyChannel = "synapse_events"

// appendLockKey is the bigint key used with pg_advisory_xact_lock for
// the global Append serialization lock. The value is the first eight
// bytes of "synapse" in ASCII, padded — any constant works, but
// keeping it stable across versions matters so two synapse-using
// services don't collide on a single Postgres cluster.
const appendLockKey int64 = 0x73796E61707365 // "synapse"

// Migrate applies [Schema] to the pool. Idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("synapse/postgres: migrate: %w", err)
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

// Store is a Postgres-backed [es.SubscribableEventStore].
//
// The caller owns the pool and is responsible for closing it. The
// Store does not retain any goroutines of its own — every active
// LISTEN lives inside the goroutine consuming a Subscribe iterator.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store wrapping pool. By default applies [Schema];
// pass [WithoutMigrate] to skip when migrations are external.
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

// Append implements [es.EventStore].
//
// The flow inside one transaction: acquire the global advisory lock,
// SELECT the current head version for the stream, validate against
// expected, INSERT events with their per-stream versions, NOTIFY with
// the new max global_position, COMMIT.
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

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse/postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", appendLockKey); err != nil {
		return es.Revision{}, fmt.Errorf("synapse/postgres: advisory lock: %w", err)
	}

	var current int64
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1`,
		string(stream),
	).Scan(&current)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse/postgres: query head: %w", err)
	}
	currentU := uint64(current)

	if err := checkRevision(stream, expected, currentU); err != nil {
		return es.Revision{}, err
	}

	var maxGlobalPos int64
	for i, ev := range events {
		metadataJSON := []byte("{}")
		if len(ev.Metadata) > 0 {
			metadataJSON, err = json.Marshal(ev.Metadata)
			if err != nil {
				return es.Revision{}, fmt.Errorf("synapse/postgres: marshal metadata: %w", err)
			}
		}

		version := currentU + uint64(i) + 1
		var globalPos int64
		err := tx.QueryRow(ctx,
			`INSERT INTO events
				(event_id, stream_id, version, type, content_type, recorded_at, causation, correlation, metadata, payload)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			 RETURNING global_position`,
			ev.EventID,
			string(stream),
			int64(version),
			ev.Type,
			string(ev.ContentType),
			ev.RecordedAt.UTC(),
			ev.Causation,
			ev.Correlation,
			metadataJSON,
			ev.Payload,
		).Scan(&globalPos)
		if err != nil {
			if isUniqueConflict(err) {
				return es.Revision{}, &es.ConflictError{
					Stream:   stream,
					Expected: expected,
					Actual:   es.Exact(currentU + uint64(i)),
				}
			}
			return es.Revision{}, fmt.Errorf("synapse/postgres: insert v%d: %w", version, err)
		}
		maxGlobalPos = globalPos
	}

	payload := fmt.Sprintf("%s:%d", string(stream), maxGlobalPos)
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload); err != nil {
		return es.Revision{}, fmt.Errorf("synapse/postgres: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return es.Revision{}, fmt.Errorf("synapse/postgres: commit: %w", err)
	}

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
			 FROM events WHERE stream_id = $1 AND version >= $2 ORDER BY version`
		args := []any{string(stream), int64(from)}
		if opts.Limit > 0 {
			query += " LIMIT $3"
			args = append(args, int64(opts.Limit))
		}

		rows, err := s.pool.Query(ctx, query, args...)
		if err != nil {
			yield(es.RawEnvelope{}, fmt.Errorf("synapse/postgres: load query: %w", err))
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
			yield(es.RawEnvelope{}, fmt.Errorf("synapse/postgres: load rows: %w", err))
		}
	}
}

// Subscribe implements [es.SubscribableEventStore].
func (s *Store) Subscribe(ctx context.Context, opts es.SubscriptionOptions) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		s.subscribeLoop(ctx, opts, "", yield)
	}
}

// SubscribeStream implements [es.SubscribableEventStore].
func (s *Store) SubscribeStream(ctx context.Context, stream es.StreamID, opts es.SubscriptionOptions) iter.Seq2[es.RawEnvelope, error] {
	return func(yield func(es.RawEnvelope, error) bool) {
		s.subscribeLoop(ctx, opts, stream, yield)
	}
}

// subscribeLoop implements the catch-up + LISTEN/NOTIFY tail for both
// global and per-stream subscriptions. An empty filterStream means
// global. The cursor tracks the highest yielded global_position (or
// version for per-stream), so the same SELECT can serve catch-up and
// post-notification reads idempotently.
func (s *Store) subscribeLoop(
	ctx context.Context,
	opts es.SubscriptionOptions,
	filterStream es.StreamID,
	yield func(es.RawEnvelope, error) bool,
) {
	from := opts.From

	if !opts.Live {
		_, _, err := s.readSince(ctx, filterStream, from, yield)
		if err != nil {
			yield(es.RawEnvelope{}, err)
		}
		return
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		yield(es.RawEnvelope{}, fmt.Errorf("synapse/postgres: acquire: %w", err))
		return
	}
	defer conn.Release()
	defer func() { _, _ = conn.Exec(context.Background(), "UNLISTEN *") }()

	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		yield(es.RawEnvelope{}, fmt.Errorf("synapse/postgres: listen: %w", err))
		return
	}

	for {
		if err := ctx.Err(); err != nil {
			yield(es.RawEnvelope{}, err)
			return
		}

		next, stopped, err := s.readSince(ctx, filterStream, from, yield)
		if stopped {
			return
		}
		if err != nil {
			yield(es.RawEnvelope{}, err)
			return
		}
		if next > 0 {
			from = next
		}

		// Wait for notification. Use the raw pgx.Conn underneath the
		// pgxpool.Conn; WaitForNotification blocks until the next NOTIFY
		// on a LISTENed channel or ctx is canceled.
		notif, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				yield(es.RawEnvelope{}, ctx.Err())
				return
			}
			yield(es.RawEnvelope{}, fmt.Errorf("synapse/postgres: wait notify: %w", err))
			return
		}

		// For per-stream subscribers, skip the SELECT entirely when
		// the notification is for a different stream. The payload is
		// advisory — a missed or malformed payload falls back to a
		// SELECT, which is correct (just less efficient).
		if filterStream != "" {
			if notifStream, _, ok := parseNotifyPayload(notif.Payload); ok && notifStream != string(filterStream) {
				continue
			}
		}
	}
}

// readSince yields events past cursor for the given filter (global if
// filterStream is empty, per-stream otherwise). Returns the new
// cursor, whether the consumer broke out, and the first error.
func (s *Store) readSince(
	ctx context.Context,
	filterStream es.StreamID,
	cursor uint64,
	yield func(es.RawEnvelope, error) bool,
) (uint64, bool, error) {
	var (
		query string
		args  []any
	)
	if filterStream == "" {
		query = `SELECT global_position, event_id, stream_id, version, type, content_type,
				recorded_at, causation, correlation, metadata, payload
			 FROM events WHERE global_position > $1 ORDER BY global_position`
		args = []any{int64(cursor)}
	} else {
		query = `SELECT global_position, event_id, stream_id, version, type, content_type,
				recorded_at, causation, correlation, metadata, payload
			 FROM events WHERE stream_id = $1 AND version > $2 ORDER BY version`
		args = []any{string(filterStream), int64(cursor)}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return cursor, false, fmt.Errorf("synapse/postgres: subscribe query: %w", err)
	}
	defer rows.Close()

	last := cursor
	for rows.Next() {
		env, err := scanEvent(rows)
		if err != nil {
			return last, false, err
		}
		if !yield(env, nil) {
			return last, true, nil
		}
		if filterStream == "" {
			last = env.GlobalPosition
		} else {
			last = env.Version
		}
	}
	if err := rows.Err(); err != nil {
		return last, false, fmt.Errorf("synapse/postgres: subscribe rows: %w", err)
	}
	return last, false, nil
}

// parseNotifyPayload decodes a "<stream_id>:<max_global_position>"
// payload. Returns ok=false on any format error so callers can fall
// back to a full SELECT.
func parseNotifyPayload(payload string) (stream string, pos uint64, ok bool) {
	i := strings.LastIndexByte(payload, ':')
	if i < 0 {
		return "", 0, false
	}
	p, err := strconv.ParseUint(payload[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return payload[:i], p, true
}

func (s *Store) head(ctx context.Context, stream es.StreamID) (es.Revision, error) {
	var current int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1`,
		string(stream),
	).Scan(&current)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse/postgres: query head: %w", err)
	}
	return es.Exact(uint64(current)), nil
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
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23505" // unique_violation
	}
	return false
}

func scanEvent(rows pgx.Rows) (es.RawEnvelope, error) {
	var (
		globalPos                                                                       int64
		eventID, streamID, eventType, contentType, causation, correlation, metadataJSON string
		version                                                                         int64
		recordedAt                                                                      time.Time
		payload                                                                         []byte
	)
	if err := rows.Scan(
		&globalPos, &eventID, &streamID, &version, &eventType, &contentType,
		&recordedAt, &causation, &correlation, &metadataJSON, &payload,
	); err != nil {
		return es.RawEnvelope{}, fmt.Errorf("synapse/postgres: scan: %w", err)
	}

	var metadata es.Metadata
	if metadataJSON != "" && metadataJSON != "{}" {
		metadata = make(es.Metadata)
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return es.RawEnvelope{}, fmt.Errorf("synapse/postgres: unmarshal metadata: %w", err)
		}
	}

	return es.RawEnvelope{
		EventID:        eventID,
		StreamID:       es.StreamID(streamID),
		Version:        uint64(version),
		GlobalPosition: uint64(globalPos),
		RecordedAt:     recordedAt.UTC(),
		Type:           eventType,
		ContentType:    es.ContentType(contentType),
		Causation:      causation,
		Correlation:    correlation,
		Metadata:       metadata,
		Payload:        payload,
	}, nil
}
