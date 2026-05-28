package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
	sqlitestore "github.com/ianunruh/synapse/eventstore/sqlite"

	_ "modernc.org/sqlite"
)

const testStream es.StreamID = "test-stream"

func newStore(t *testing.T) *sqlitestore.Store {
	t.Helper()
	// File-based: ":memory:" is per-connection in SQLite, which breaks
	// tests where Append and Subscribe goroutines get different
	// connections. t.TempDir cleans up automatically. WAL + busy_timeout
	// let concurrent readers and a single writer coexist without
	// SQLITE_BUSY failures.
	dsn := "file:" + filepath.Join(t.TempDir(), "events.db") +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := sqlitestore.New(t.Context(), db)
	if err != nil {
		t.Fatalf("sqlitestore.New: %v", err)
	}
	return store
}

func makeEvent(stream es.StreamID, version uint64) es.RawEnvelope {
	return es.RawEnvelope{
		EventID:     fmt.Sprintf("evt-%s-%d", stream, version),
		StreamID:    stream,
		Version:     version,
		Type:        "test.event",
		ContentType: "application/json",
		RecordedAt:  time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Payload:     fmt.Appendf(nil, `{"version":%d}`, version),
	}
}

func makeEvents(n int, stream es.StreamID, fromVersion uint64) []es.RawEnvelope {
	out := make([]es.RawEnvelope, n)
	for i := range n {
		out[i] = makeEvent(stream, fromVersion+uint64(i))
	}
	return out
}

func collect(seq iter.Seq2[es.RawEnvelope, error]) ([]es.RawEnvelope, error) {
	var out []es.RawEnvelope
	for env, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, env)
	}
	return out, nil
}

// ----- Interface assertion ---------------------------------------------

func TestStoreImplementsSubscribable(t *testing.T) {
	var _ es.SubscribableEventStore = newStore(t)
}

// ----- Append: revisions ------------------------------------------------

func TestAppend_Revision(t *testing.T) {
	cases := []struct {
		name     string
		prefill  int
		expected es.Revision
		wantOK   bool
	}{
		{"any/empty", 0, es.Any, true},
		{"any/nonempty", 3, es.Any, true},
		{"noStream/empty", 0, es.NoStream, true},
		{"noStream/nonempty", 3, es.NoStream, false},
		{"streamExists/empty", 0, es.StreamExists, false},
		{"streamExists/nonempty", 3, es.StreamExists, true},
		{"exact-match/empty/0", 0, es.Exact(0), true},
		{"exact-match/nonempty", 3, es.Exact(3), true},
		{"exact-mismatch/empty/1", 0, es.Exact(1), false},
		{"exact-mismatch/nonempty/low", 3, es.Exact(2), false},
		{"exact-mismatch/nonempty/high", 3, es.Exact(4), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := newStore(t)

			if tc.prefill > 0 {
				if _, err := store.Append(ctx, testStream, es.Any, makeEvents(tc.prefill, testStream, 1)...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}

			ev := makeEvent(testStream, uint64(tc.prefill)+1)
			head, err := store.Append(ctx, testStream, tc.expected, ev)

			if tc.wantOK {
				if err != nil {
					t.Fatalf("Append: %v", err)
				}
				got, ok := head.Value()
				if !ok || got != uint64(tc.prefill)+1 {
					t.Errorf("head = %v, want Exact(%d)", head, tc.prefill+1)
				}
				return
			}
			if !errors.Is(err, es.ErrConflict) {
				t.Errorf("err = %v, want wrap of ErrConflict", err)
			}
		})
	}
}

func TestAppend_AtomicityOnConflict(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)

	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvent(testStream, 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := store.Append(ctx, testStream, es.NoStream,
		makeEvent(testStream, 2), makeEvent(testStream, 3))
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("Append: want ErrConflict, got %v", err)
	}

	got, err := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len(events) = %d, want 1 (no partial persistence on conflict)", len(got))
	}
}

func TestAppend_EmptyBatch(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)

	head, err := store.Append(ctx, testStream, es.Any)
	if err != nil {
		t.Fatalf("Append empty: %v", err)
	}
	if v, _ := head.Value(); v != 0 {
		t.Errorf("head = %v, want Exact(0)", head)
	}
}

// ----- Append: GlobalPosition ------------------------------------------

func TestAppend_AssignsGlobalPosition(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)

	if _, err := store.Append(ctx, "stream-a", es.NoStream, makeEvents(2, "stream-a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "stream-b", es.NoStream, makeEvents(3, "stream-b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	gotA, _ := collect(store.Load(ctx, "stream-a", es.ReadOptions{}))
	if len(gotA) != 2 || gotA[0].GlobalPosition != 1 || gotA[1].GlobalPosition != 2 {
		t.Errorf("stream-a positions = (%d, %d), want (1, 2)",
			safePos(gotA, 0), safePos(gotA, 1))
	}

	gotB, _ := collect(store.Load(ctx, "stream-b", es.ReadOptions{}))
	if len(gotB) != 3 {
		t.Fatalf("stream-b: len = %d", len(gotB))
	}
	for i, ev := range gotB {
		want := uint64(3 + i)
		if ev.GlobalPosition != want {
			t.Errorf("stream-b[%d].GlobalPosition = %d, want %d", i, ev.GlobalPosition, want)
		}
	}
}

func safePos(evs []es.RawEnvelope, i int) uint64 {
	if i < len(evs) {
		return evs[i].GlobalPosition
	}
	return 0
}

// ----- Load -------------------------------------------------------------

func TestLoad_EmptyStream(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)

	got, err := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestLoad_All_RoundTrip(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	want := makeEvents(5, testStream, 1)
	if _, err := store.Append(ctx, testStream, es.NoStream, want...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, env := range got {
		if env.EventID != want[i].EventID ||
			env.StreamID != want[i].StreamID ||
			env.Version != want[i].Version ||
			env.Type != want[i].Type ||
			env.ContentType != want[i].ContentType ||
			!env.RecordedAt.Equal(want[i].RecordedAt) ||
			!slices.Equal(env.Payload, want[i].Payload) {
			t.Errorf("events[%d]:\ngot  = %+v\nwant = %+v", i, env, want[i])
		}
	}
}

func TestLoad_FromAndLimit(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvents(10, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cases := []struct {
		name      string
		from      uint64
		limit     uint64
		wantFirst uint64
		wantLen   int
	}{
		{"from-0/all", 0, 0, 1, 10},
		{"from-1/all", 1, 0, 1, 10},
		{"from-5/all", 5, 0, 5, 6},
		{"from-0/limit-3", 0, 3, 1, 3},
		{"from-5/limit-3", 5, 3, 5, 3},
		{"from-9/limit-5/clip", 9, 5, 9, 2},
		{"from-11/past-end", 11, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collect(store.Load(ctx, testStream, es.ReadOptions{From: tc.from, Limit: tc.limit}))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(got) != tc.wantLen {
				t.Errorf("len = %d, want %d", len(got), tc.wantLen)
			}
			if tc.wantLen > 0 && got[0].Version != tc.wantFirst {
				t.Errorf("first version = %d, want %d", got[0].Version, tc.wantFirst)
			}
		})
	}
}

func TestLoad_MetadataRoundTrip(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)

	ev := makeEvent(testStream, 1)
	ev.Metadata = es.Metadata{"actor": "alice", "trace_id": "abc-123"}
	ev.Causation = "cmd-1"
	ev.Correlation = "req-1"

	if _, err := store.Append(ctx, testStream, es.NoStream, ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, _ := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	out := got[0]
	if out.Causation != "cmd-1" || out.Correlation != "req-1" {
		t.Errorf("causation/correlation lost: %q / %q", out.Causation, out.Correlation)
	}
	if out.Metadata["actor"] != "alice" || out.Metadata["trace_id"] != "abc-123" {
		t.Errorf("metadata round-trip failed: %+v", out.Metadata)
	}
}

// ----- Subscribe: catch-up ----------------------------------------------

func TestSubscribe_CatchUp_AllStreams(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	if _, err := store.Append(ctx, "a", es.NoStream, makeEvents(2, "a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "b", es.NoStream, makeEvents(3, "b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	got, err := collect(store.Subscribe(ctx, es.SubscriptionOptions{}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("len = %d, want 5", len(got))
	}
	var last uint64
	for i, ev := range got {
		if ev.GlobalPosition != last+1 {
			t.Errorf("events[%d].GlobalPosition = %d, want %d", i, ev.GlobalPosition, last+1)
		}
		last = ev.GlobalPosition
	}
}

func TestSubscribe_CatchUp_From(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	if _, err := store.Append(ctx, "a", es.NoStream, makeEvents(5, "a", 1)...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, _ := collect(store.Subscribe(ctx, es.SubscriptionOptions{From: 2}))
	if len(got) != 3 {
		t.Errorf("len = %d, want 3", len(got))
	}
	if got[0].GlobalPosition != 3 {
		t.Errorf("first.GlobalPosition = %d, want 3", got[0].GlobalPosition)
	}
}

func TestSubscribeStream_OnlyTargetStream(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	if _, err := store.Append(ctx, "a", es.NoStream, makeEvents(3, "a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "b", es.NoStream, makeEvents(4, "b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	got, _ := collect(store.SubscribeStream(ctx, "b", es.SubscriptionOptions{}))
	if len(got) != 4 {
		t.Errorf("len = %d, want 4", len(got))
	}
	for _, ev := range got {
		if ev.StreamID != "b" {
			t.Errorf("event from %q, want b", ev.StreamID)
		}
	}
}

// ----- Subscribe: live --------------------------------------------------

func TestSubscribe_Live_SeesNewAppends(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := newStore(t)

	results := make(chan es.RawEnvelope, 4)
	go func() {
		for env, err := range store.Subscribe(ctx, es.SubscriptionOptions{Live: true}) {
			if err != nil {
				close(results)
				return
			}
			results <- env
		}
		close(results)
	}()

	// Append two events; subscriber should see both.
	if _, err := store.Append(ctx, "live", es.NoStream, makeEvent("live", 1)); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if _, err := store.Append(ctx, "live", es.Exact(1), makeEvent("live", 2)); err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	collected := make([]es.RawEnvelope, 0, 2)
	timeout := time.After(2 * time.Second)
	for len(collected) < 2 {
		select {
		case env, ok := <-results:
			if !ok {
				t.Fatalf("subscriber closed; got %d events", len(collected))
			}
			collected = append(collected, env)
		case <-timeout:
			t.Fatalf("did not receive 2 events; got %d", len(collected))
		}
	}
	if collected[0].Version != 1 || collected[1].Version != 2 {
		t.Errorf("versions = (%d, %d), want (1, 2)", collected[0].Version, collected[1].Version)
	}
}

func TestSubscribe_Live_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := newStore(t)

	done := make(chan error, 1)
	go func() {
		var lastErr error
		for _, err := range store.Subscribe(ctx, es.SubscriptionOptions{Live: true}) {
			if err != nil {
				lastErr = err
				break
			}
		}
		done <- lastErr
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Errorf("subscriber did not exit on cancel")
	}
}

func TestSubscribe_Live_ManyConcurrentSubscribers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := newStore(t)

	const N = 8
	received := make([]int, N)
	done := make(chan int, N)

	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			count := 0
			for _, err := range store.Subscribe(ctx, es.SubscriptionOptions{Live: true}) {
				if err != nil {
					done <- count
					return
				}
				count++
				if count >= 3 {
					done <- count
					return
				}
			}
			done <- count
		})
	}

	time.Sleep(50 * time.Millisecond) // let subscribers reach the wait

	for i := range 3 {
		if _, err := store.Append(ctx, "s", es.Exact(uint64(i)), makeEvent("s", uint64(i+1))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	for i := range N {
		select {
		case received[i] = <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not finish", i)
		}
	}
	for i, c := range received {
		if c != 3 {
			t.Errorf("subscriber %d received %d events, want 3", i, c)
		}
	}
	wg.Wait()
}

// ----- Persistence across instances ------------------------------------

func TestPersistence_AcrossStoreInstances(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "events.db")

	// Append via first instance.
	{
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("open 1: %v", err)
		}
		store, err := sqlitestore.New(ctx, db)
		if err != nil {
			t.Fatalf("New 1: %v", err)
		}
		if _, err := store.Append(ctx, "persist", es.NoStream, makeEvents(3, "persist", 1)...); err != nil {
			t.Fatalf("Append: %v", err)
		}
		db.Close()
	}

	// Read via second instance.
	{
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("open 2: %v", err)
		}
		defer db.Close()
		store, err := sqlitestore.New(ctx, db)
		if err != nil {
			t.Fatalf("New 2: %v", err)
		}
		got, err := collect(store.Load(ctx, "persist", es.ReadOptions{}))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(got) != 3 {
			t.Errorf("len = %d, want 3", len(got))
		}
		for i, ev := range got {
			if ev.Version != uint64(i+1) {
				t.Errorf("events[%d].Version = %d, want %d", i, ev.Version, i+1)
			}
		}
	}
}
