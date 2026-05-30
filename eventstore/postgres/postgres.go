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
//   - Append emits a NOTIFY on the channel "synapse_events" inside the
//     same transaction, so the wake-up fires at COMMIT.
//   - A single shared goroutine per Store holds one connection running
//     LISTEN "synapse_events" and, on each notification, wakes every
//     live subscriber through an in-process broadcast. Woken subscribers
//     run a cursor SELECT on a pooled connection and release it; they
//     hold no connection while waiting. So the number of concurrent live
//     subscribers is independent of pool size — the pool only needs to
//     cover the one listener connection plus concurrent reads and
//     appends. The listener starts lazily on the first live subscription
//     and runs until [Store.Close]. See ADR-0025.
//
// Because the Store owns that goroutine and its connection, callers must
// call [Store.Close] before closing the pool.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
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

// Store is a Postgres-backed [es.SubscribableEventStore].
//
// The caller owns the pool and is responsible for closing it. The Store
// additionally owns a single shared LISTEN goroutine that is started
// lazily on the first live subscription and holds one connection for
// its lifetime; [Store.Close] stops it. Call Close before closing the
// pool. See ADR-0025.
type Store struct {
	pool *pgxpool.Pool

	// listenerCtx governs the shared LISTEN goroutine's lifetime; Close
	// cancels it. The goroutine starts once (startOnce) on the first
	// live subscription and closes listenerDone when it exits, so Close
	// can wait for the held connection to be released.
	listenerCtx  context.Context
	cancel       context.CancelFunc
	startOnce    sync.Once
	started      atomic.Bool
	listenerDone chan struct{}
	closeOnce    sync.Once

	// notify is the in-process broadcast channel. The listener
	// close-and-replaces it on every NOTIFY (and on each reconnect),
	// waking all live subscribers; each then runs a cursor SELECT.
	mu     sync.Mutex
	notify chan struct{}
}

// New returns a Store wrapping pool. By default applies [Schema];
// pass [WithoutMigrate] to skip when migrations are external.
//
// The returned Store must be closed with [Store.Close] before the pool
// is closed.
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
	lctx, cancel := context.WithCancel(context.Background())
	return &Store{
		pool:         pool,
		listenerCtx:  lctx,
		cancel:       cancel,
		listenerDone: make(chan struct{}),
		notify:       make(chan struct{}),
	}, nil
}

// Close stops the shared LISTEN goroutine and releases the connection
// it holds. Call Close before closing the underlying pool; otherwise
// pgxpool's Close blocks waiting for the listener's connection. Close is
// idempotent.
//
// After Close, live subscriptions no longer receive wake-ups (their
// one-shot catch-up read still works); cancel their contexts to stop
// them.
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		if s.started.Load() {
			<-s.listenerDone
		}
	})
}

// startListener launches the shared LISTEN goroutine exactly once, on
// the first live subscription.
func (s *Store) startListener() {
	s.startOnce.Do(func() {
		s.started.Store(true)
		go s.runListener()
	})
}

// listen backoff bounds for reconnecting the shared LISTEN connection.
const (
	listenBaseBackoff = 50 * time.Millisecond
	listenMaxBackoff  = 5 * time.Second
)

// runListener owns the shared LISTEN connection for the Store's
// lifetime. It reconnects with capped backoff and wakes all subscribers
// on every drop so they re-read against their cursor.
func (s *Store) runListener() {
	defer close(s.listenerDone)

	backoff := listenBaseBackoff
	for s.listenerCtx.Err() == nil {
		established := s.listenSession()
		if s.listenerCtx.Err() != nil {
			return
		}
		// The LISTEN connection dropped. Wake every subscriber so they
		// re-read against their cursor — covering anything appended
		// during the gap — then back off before reconnecting.
		s.broadcast()
		if established {
			backoff = listenBaseBackoff
		}
		select {
		case <-s.listenerCtx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, listenMaxBackoff)
	}
}

// listenSession acquires a connection, LISTENs, and broadcasts on every
// notification until the connection or context fails. It returns whether
// LISTEN was successfully established (used to reset reconnect backoff).
func (s *Store) listenSession() bool {
	ctx := s.listenerCtx
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false
	}
	defer conn.Release()
	defer func() { _, _ = conn.Exec(context.Background(), "UNLISTEN *") }()

	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return false
	}
	// Wake subscribers once on (re)establishing LISTEN, closing the
	// window where an append landed before the listener was attached.
	s.broadcast()

	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return true
		}
		s.broadcast()
	}
}

// currentNotify returns the channel a subscriber should wait on. It must
// be captured before the catch-up read so a NOTIFY arriving between the
// read and the wait is not lost.
func (s *Store) currentNotify() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

// broadcast wakes every waiting subscriber by closing the current notify
// channel and installing a fresh one.
func (s *Store) broadcast() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.notify)
	s.notify = make(chan struct{})
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
		return es.Revision{}, fmt.Errorf("synapse: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", appendLockKey); err != nil {
		return es.Revision{}, fmt.Errorf("synapse: advisory lock: %w", err)
	}

	var current int64
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1`,
		string(stream),
	).Scan(&current)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse: query head: %w", err)
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
				return es.Revision{}, fmt.Errorf("synapse: marshal metadata: %w", err)
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
			return es.Revision{}, fmt.Errorf("synapse: insert v%d: %w", version, err)
		}
		maxGlobalPos = globalPos
	}

	payload := fmt.Sprintf("%s:%d", string(stream), maxGlobalPos)
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, payload); err != nil {
		return es.Revision{}, fmt.Errorf("synapse: notify: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return es.Revision{}, fmt.Errorf("synapse: commit: %w", err)
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

// subscribeLoop implements the catch-up + live tail for both global and
// per-stream subscriptions. An empty filterStream means global. The
// cursor tracks the highest yielded global_position (or version for
// per-stream), so the same SELECT serves catch-up and post-wake reads
// idempotently.
//
// Live subscribers hold no connection while waiting: they capture the
// shared notify channel, read against the pool, release, then wait for
// the shared listener to broadcast. Capturing the channel before the
// read is what makes a NOTIFY arriving mid-read safe — it closes the
// channel we already hold, so the wait returns immediately and we
// re-read.
func (s *Store) subscribeLoop(
	ctx context.Context,
	opts es.SubscriptionOptions,
	filterStream es.StreamID,
	yield func(es.RawEnvelope, error) bool,
) {
	from := opts.From

	if opts.Live {
		s.startListener()
	}

	for {
		if err := ctx.Err(); err != nil {
			yield(es.RawEnvelope{}, err)
			return
		}

		var notify chan struct{}
		if opts.Live {
			notify = s.currentNotify()
		}

		next, stopped, err := s.readSince(ctx, filterStream, from, opts.Types, yield)
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

// readSince yields events past cursor for the given filter (global if
// filterStream is empty, per-stream otherwise). Returns the new
// cursor, whether the consumer broke out, and the first error.
func (s *Store) readSince(
	ctx context.Context,
	filterStream es.StreamID,
	cursor uint64,
	types []string,
	yield func(es.RawEnvelope, error) bool,
) (uint64, bool, error) {
	var (
		where string
		order string
		args  []any
	)
	if filterStream == "" {
		where = "global_position > $1"
		order = "global_position"
		args = []any{int64(cursor)}
	} else {
		where = "stream_id = $1 AND version > $2"
		order = "version"
		args = []any{string(filterStream), int64(cursor)}
	}
	if len(types) > 0 {
		args = append(args, types)
		where += fmt.Sprintf(" AND type = ANY($%d::text[])", len(args))
	}

	query := `SELECT global_position, event_id, stream_id, version, type, content_type,
			recorded_at, causation, correlation, metadata, payload
		 FROM events WHERE ` + where + " ORDER BY " + order

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return cursor, false, fmt.Errorf("synapse: subscribe query: %w", err)
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
		return last, false, fmt.Errorf("synapse: subscribe rows: %w", err)
	}
	return last, false, nil
}

func (s *Store) head(ctx context.Context, stream es.StreamID) (es.Revision, error) {
	var current int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1`,
		string(stream),
	).Scan(&current)
	if err != nil {
		return es.Revision{}, fmt.Errorf("synapse: query head: %w", err)
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
		RecordedAt:     recordedAt.UTC(),
		Type:           eventType,
		ContentType:    es.ContentType(contentType),
		Causation:      causation,
		Correlation:    correlation,
		Metadata:       metadata,
		Payload:        payload,
	}, nil
}
