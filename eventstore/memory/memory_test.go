package memory_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
)

const testStream es.StreamID = "test-stream"

func makeEvent(stream es.StreamID, version uint64) es.RawEnvelope {
	return es.RawEnvelope{
		EventID:     fmt.Sprintf("evt-%d", version),
		StreamID:    stream,
		Version:     version,
		Type:        "test.event",
		ContentType: "application/json",
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

// collect drains an iterator into a slice. The returned error is the
// iterator's terminal error, if any.
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

func envEqual(a, b es.RawEnvelope) bool {
	return a.EventID == b.EventID &&
		a.StreamID == b.StreamID &&
		a.Version == b.Version &&
		a.Type == b.Type &&
		a.ContentType == b.ContentType &&
		slices.Equal(a.Payload, b.Payload)
}

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
			store := memory.New()

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
				want := uint64(tc.prefill) + 1
				got, ok := head.Value()
				if !ok || got != want {
					t.Errorf("head = %v, want Exact(%d)", head, want)
				}
				return
			}

			if !errors.Is(err, es.ErrConflict) {
				t.Fatalf("err = %v, want wrap of ErrConflict", err)
			}
			var ce *es.ConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("err is not *ConflictError: %T", err)
			}
			if ce.Stream != testStream {
				t.Errorf("ConflictError.Stream = %q, want %q", ce.Stream, testStream)
			}
			gotActual, _ := ce.Actual.Value()
			if gotActual != uint64(tc.prefill) {
				t.Errorf("ConflictError.Actual = %v, want Exact(%d)", ce.Actual, tc.prefill)
			}
		})
	}
}

func TestAppend_AtomicityOnConflict(t *testing.T) {
	// A conflicting Append must not persist any of the events.
	ctx := t.Context()
	store := memory.New()

	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvent(testStream, 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Wrong expectation — should fail.
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
	// Append with zero events is a no-op returning the current head.
	ctx := t.Context()
	store := memory.New()

	head, err := store.Append(ctx, testStream, es.Any)
	if err != nil {
		t.Fatalf("Append empty/empty: %v", err)
	}
	if v, _ := head.Value(); v != 0 {
		t.Errorf("head = %v, want Exact(0)", head)
	}

	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvents(3, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}
	head, err = store.Append(ctx, testStream, es.Any)
	if err != nil {
		t.Fatalf("Append empty/nonempty: %v", err)
	}
	if v, _ := head.Value(); v != 3 {
		t.Errorf("head = %v, want Exact(3)", head)
	}
}

func TestAppend_MultipleEventsAdvanceHead(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	head, err := store.Append(ctx, testStream, es.NoStream, makeEvents(5, testStream, 1)...)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if v, _ := head.Value(); v != 5 {
		t.Errorf("head = %v, want Exact(5)", head)
	}

	head, err = store.Append(ctx, testStream, es.Exact(5), makeEvents(3, testStream, 6)...)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if v, _ := head.Value(); v != 8 {
		t.Errorf("head = %v, want Exact(8)", head)
	}
}

func TestAppend_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := memory.New()
	_, err := store.Append(ctx, testStream, es.NoStream, makeEvent(testStream, 1))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestLoad_EmptyStream(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	got, err := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(events) = %d, want 0", len(got))
	}
}

func TestLoad_All(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	want := makeEvents(5, testStream, 1)
	if _, err := store.Append(ctx, testStream, es.NoStream, want...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !slices.EqualFunc(got, want, envEqual) {
		t.Errorf("Load mismatch:\ngot  = %+v\nwant = %+v", got, want)
	}
}

func TestLoad_FromAndLimit(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvents(10, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cases := []struct {
		name      string
		from      uint64
		limit     uint64
		wantFirst uint64 // 0 when wantLen == 0
		wantLen   int
	}{
		{"from-0/all", 0, 0, 1, 10},
		{"from-1/all", 1, 0, 1, 10},
		{"from-5/all", 5, 0, 5, 6},
		{"from-0/limit-3", 0, 3, 1, 3},
		{"from-5/limit-3", 5, 3, 5, 3},
		{"from-9/limit-5/clip", 9, 5, 9, 2},
		{"from-10/limit-1/last", 10, 1, 10, 1},
		{"from-11/past-end", 11, 0, 0, 0},
		{"from-100/way-past", 100, 5, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collect(store.Load(ctx, testStream, es.ReadOptions{From: tc.from, Limit: tc.limit}))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(got) != tc.wantLen {
				t.Errorf("len(events) = %d, want %d", len(got), tc.wantLen)
			}
			if tc.wantLen > 0 && got[0].Version != tc.wantFirst {
				t.Errorf("first version = %d, want %d", got[0].Version, tc.wantFirst)
			}
		})
	}
}

func TestLoad_ContextCanceledBefore(t *testing.T) {
	store := memory.New()
	if _, err := store.Append(t.Context(), testStream, es.NoStream, makeEvents(3, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var got []es.RawEnvelope
	var gotErr error
	for env, err := range store.Load(ctx, testStream, es.ReadOptions{}) {
		if err != nil {
			gotErr = err
			break
		}
		got = append(got, env)
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", gotErr)
	}
	if len(got) != 0 {
		t.Errorf("len(events) = %d, want 0 on canceled context", len(got))
	}
}

func TestLoad_ContextCanceledDuring(t *testing.T) {
	store := memory.New()
	if _, err := store.Append(t.Context(), testStream, es.NoStream, makeEvents(5, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var got []es.RawEnvelope
	var gotErr error
	for env, err := range store.Load(ctx, testStream, es.ReadOptions{}) {
		if err != nil {
			gotErr = err
			break
		}
		got = append(got, env)
		if len(got) == 2 {
			cancel()
		}
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled mid-iteration", gotErr)
	}
	if len(got) != 2 {
		t.Errorf("len(events) = %d, want 2 before cancel", len(got))
	}
}

func TestLoad_BreakEarly(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvents(5, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got []es.RawEnvelope
	for env, err := range store.Load(ctx, testStream, es.ReadOptions{}) {
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		got = append(got, env)
		if len(got) >= 2 {
			break
		}
	}
	if len(got) != 2 {
		t.Errorf("len(events) = %d, want 2", len(got))
	}
}

func TestLoad_SnapshotSemantics(t *testing.T) {
	// Load snapshots at call time; later appends do not appear in the
	// iteration of an iterator returned earlier.
	ctx := t.Context()
	store := memory.New()
	if _, err := store.Append(ctx, testStream, es.NoStream, makeEvents(3, testStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	seq := store.Load(ctx, testStream, es.ReadOptions{})

	// Append more after Load returned.
	if _, err := store.Append(ctx, testStream, es.Exact(3), makeEvents(2, testStream, 4)...); err != nil {
		t.Fatalf("post-load append: %v", err)
	}

	got, err := collect(seq)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("snapshot length = %d, want 3 (snapshot taken before second append)", len(got))
	}
}

func TestLoad_IsolationFromCallerMutation(t *testing.T) {
	// Mutating the appended slice after Append must not affect the
	// store's view of the stream.
	ctx := t.Context()
	store := memory.New()

	events := makeEvents(3, testStream, 1)
	if _, err := store.Append(ctx, testStream, es.NoStream, events...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Mutate the original slice header — should not affect stored events.
	events[0].EventID = "tampered"
	events[1].Type = "tampered"

	got, err := collect(store.Load(ctx, testStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got[0].EventID == "tampered" || got[1].Type == "tampered" {
		t.Errorf("store retained mutated values: %+v", got)
	}
}

func TestConcurrent_AppendExactlyOneWins(t *testing.T) {
	// N goroutines race to be the first appender on a fresh stream.
	// With expected=NoStream, exactly one must succeed; the rest must
	// see ErrConflict.
	ctx := t.Context()
	store := memory.New()

	const N = 64
	type result struct{ err error }
	results := make(chan result, N)

	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() {
			ev := makeEvent(testStream, 1)
			ev.EventID = fmt.Sprintf("contender-%d", i)
			_, err := store.Append(ctx, testStream, es.NoStream, ev)
			results <- result{err}
		})
	}
	wg.Wait()
	close(results)

	var wins, losses int
	for r := range results {
		switch {
		case r.err == nil:
			wins++
		case errors.Is(r.err, es.ErrConflict):
			losses++
		default:
			t.Errorf("unexpected err: %v", r.err)
		}
	}
	if wins != 1 {
		t.Errorf("wins = %d, want 1", wins)
	}
	if wins+losses != N {
		t.Errorf("wins+losses = %d, want %d", wins+losses, N)
	}
}

func TestStoreImplementsEventStore(t *testing.T) {
	var _ es.EventStore = memory.New()
}
