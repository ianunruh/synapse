// Package eventstoretest provides a contract test suite that any
// implementation of [es.EventStore] or [es.SubscribableEventStore]
// can run to verify the documented behavior.
//
// Usage from a backend's *_test.go:
//
//	func TestMyStore_Contract(t *testing.T) {
//	    eventstoretest.RunEventStoreContract(t, func(t *testing.T) es.EventStore {
//	        return mystore.New(t.TempDir())
//	    })
//	}
//
//	func TestMyStore_SubscribableContract(t *testing.T) {
//	    eventstoretest.RunSubscribableContract(t, func(t *testing.T) es.SubscribableEventStore {
//	        return mystore.New(t.TempDir())
//	    })
//	}
//
// The factory returns a fresh, independent store per invocation. Each
// contract subtest calls factory exactly once, so backends should
// register any cleanup via t.Cleanup inside the factory.
//
// The package also exports a few helpers ([MakeEvent], [MakeEvents],
// [Collect]) that backend-specific tests can reuse.
package eventstoretest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
)

// Factory returns a fresh [es.EventStore] for each invocation.
type Factory func(t *testing.T) es.EventStore

// SubscribableFactory returns a fresh [es.SubscribableEventStore]
// for each invocation.
type SubscribableFactory func(t *testing.T) es.SubscribableEventStore

// TestStream is the canonical stream id used by the contract tests
// that exercise a single stream.
const TestStream es.StreamID = "test-stream"

// MakeEvent constructs a [es.RawEnvelope] with deterministic test
// values: EventID `evt-<stream>-<version>`, type `test.event`,
// `application/json` content type, a fixed RecordedAt, and a small
// JSON payload encoding the version.
func MakeEvent(stream es.StreamID, version uint64) es.RawEnvelope {
	return MakeTypedEvent(stream, version, "test.event")
}

// MakeTypedEvent is [MakeEvent] with an explicit event Type, used by the
// type-filter contract tests.
func MakeTypedEvent(stream es.StreamID, version uint64, eventType string) es.RawEnvelope {
	return es.RawEnvelope{
		EventID:     fmt.Sprintf("evt-%s-%d", stream, version),
		StreamID:    stream,
		Version:     version,
		Type:        eventType,
		ContentType: "application/json",
		RecordedAt:  time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Payload:     fmt.Appendf(nil, `{"version":%d}`, version),
	}
}

// MakeEvents constructs n events for stream starting at fromVersion.
func MakeEvents(n int, stream es.StreamID, fromVersion uint64) []es.RawEnvelope {
	out := make([]es.RawEnvelope, n)
	for i := range n {
		out[i] = MakeEvent(stream, fromVersion+uint64(i))
	}
	return out
}

// Collect drains an iterator into a slice. The returned error is the
// iterator's terminal error, if any. The collected slice contains
// every successful yield before the error.
func Collect(seq iter.Seq2[es.RawEnvelope, error]) ([]es.RawEnvelope, error) {
	var out []es.RawEnvelope
	for env, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, env)
	}
	return out, nil
}

// RunEventStoreContract runs every [es.EventStore] contract test
// against the store returned by factory. Each subtest constructs a
// fresh store.
func RunEventStoreContract(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("Append_Revision", func(t *testing.T) { testAppendRevision(t, factory) })
	t.Run("Append_AtomicityOnConflict", func(t *testing.T) { testAppendAtomicityOnConflict(t, factory) })
	t.Run("Append_EmptyBatch", func(t *testing.T) { testAppendEmptyBatch(t, factory) })
	t.Run("Append_MultipleEventsAdvanceHead", func(t *testing.T) { testAppendMultipleEvents(t, factory) })
	t.Run("Append_ContextCanceled", func(t *testing.T) { testAppendContextCanceled(t, factory) })
	t.Run("Append_AssignsGlobalPosition", func(t *testing.T) { testAppendAssignsGlobalPosition(t, factory) })
	t.Run("Append_InputSliceUnchanged", func(t *testing.T) { testAppendInputSliceUnchanged(t, factory) })
	t.Run("Append_ConcurrentExactlyOneWins", func(t *testing.T) { testAppendConcurrent(t, factory) })
	t.Run("Load_EmptyStream", func(t *testing.T) { testLoadEmptyStream(t, factory) })
	t.Run("Load_All_RoundTrip", func(t *testing.T) { testLoadAllRoundTrip(t, factory) })
	t.Run("Load_FromAndLimit", func(t *testing.T) { testLoadFromAndLimit(t, factory) })
	t.Run("Load_MetadataRoundTrip", func(t *testing.T) { testLoadMetadataRoundTrip(t, factory) })
	t.Run("Load_BreakEarly", func(t *testing.T) { testLoadBreakEarly(t, factory) })
	t.Run("Load_ContextCanceled", func(t *testing.T) { testLoadContextCanceled(t, factory) })
	t.Run("Load_IsolationFromCallerMutation", func(t *testing.T) { testLoadIsolationFromCallerMutation(t, factory) })
}

// RunSubscribableContract runs [RunEventStoreContract] plus the
// subscription tests against the store returned by factory.
func RunSubscribableContract(t *testing.T, factory SubscribableFactory) {
	t.Helper()

	RunEventStoreContract(t, func(t *testing.T) es.EventStore {
		return factory(t)
	})

	t.Run("Subscribe_EmptyStore", func(t *testing.T) { testSubscribeEmptyStore(t, factory) })
	t.Run("Subscribe_CatchUp_AllStreams", func(t *testing.T) { testSubscribeCatchUp(t, factory) })
	t.Run("Subscribe_CatchUp_From", func(t *testing.T) { testSubscribeCatchUpFrom(t, factory) })
	t.Run("Subscribe_Live_SeesNewAppends", func(t *testing.T) { testSubscribeLive(t, factory) })
	t.Run("Subscribe_Live_ContextCanceled", func(t *testing.T) { testSubscribeLiveContextCanceled(t, factory) })
	t.Run("Subscribe_Live_ManyConcurrentSubscribers", func(t *testing.T) { testSubscribeLiveMany(t, factory) })
	t.Run("SubscribeStream_OnlyTargetStream", func(t *testing.T) { testSubscribeStreamOnlyTarget(t, factory) })
	t.Run("SubscribeStream_FromVersion", func(t *testing.T) { testSubscribeStreamFromVersion(t, factory) })
	t.Run("Subscribe_TypeFilter_CatchUp", func(t *testing.T) { testSubscribeTypeFilterCatchUp(t, factory) })
	t.Run("Subscribe_TypeFilter_Live", func(t *testing.T) { testSubscribeTypeFilterLive(t, factory) })
	t.Run("SubscribeStream_TypeFilter", func(t *testing.T) { testSubscribeStreamTypeFilter(t, factory) })
}

// ============================================================================
// Append tests
// ============================================================================

func testAppendRevision(t *testing.T, factory Factory) {
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
			store := factory(t)

			if tc.prefill > 0 {
				if _, err := store.Append(ctx, TestStream, es.Any, MakeEvents(tc.prefill, TestStream, 1)...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}

			ev := MakeEvent(TestStream, uint64(tc.prefill)+1)
			head, err := store.Append(ctx, TestStream, tc.expected, ev)

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
				t.Errorf("err = %v, want wrap of ErrConflict", err)
			}
		})
	}
}

func testAppendAtomicityOnConflict(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if _, err := store.Append(ctx, TestStream, es.NoStream, MakeEvent(TestStream, 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := store.Append(ctx, TestStream, es.NoStream,
		MakeEvent(TestStream, 2), MakeEvent(TestStream, 3))
	if !errors.Is(err, es.ErrConflict) {
		t.Fatalf("Append: want ErrConflict, got %v", err)
	}

	got, err := Collect(store.Load(ctx, TestStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len(events) = %d, want 1 (no partial persistence on conflict)", len(got))
	}
}

func testAppendEmptyBatch(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	head, err := store.Append(ctx, TestStream, es.Any)
	if err != nil {
		t.Fatalf("Append empty/empty: %v", err)
	}
	if v, _ := head.Value(); v != 0 {
		t.Errorf("head = %v, want Exact(0)", head)
	}

	if _, err := store.Append(ctx, TestStream, es.NoStream, MakeEvents(3, TestStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}
	head, err = store.Append(ctx, TestStream, es.Any)
	if err != nil {
		t.Fatalf("Append empty/nonempty: %v", err)
	}
	if v, _ := head.Value(); v != 3 {
		t.Errorf("head = %v, want Exact(3)", head)
	}
}

func testAppendMultipleEvents(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	head, err := store.Append(ctx, TestStream, es.NoStream, MakeEvents(5, TestStream, 1)...)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if v, _ := head.Value(); v != 5 {
		t.Errorf("head = %v, want Exact(5)", head)
	}

	head, err = store.Append(ctx, TestStream, es.Exact(5), MakeEvents(3, TestStream, 6)...)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if v, _ := head.Value(); v != 8 {
		t.Errorf("head = %v, want Exact(8)", head)
	}
}

func testAppendContextCanceled(t *testing.T, factory Factory) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := factory(t)
	_, err := store.Append(ctx, TestStream, es.NoStream, MakeEvent(TestStream, 1))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func testAppendAssignsGlobalPosition(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	if _, err := store.Append(ctx, "stream-a", es.NoStream, MakeEvents(2, "stream-a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "stream-b", es.NoStream, MakeEvents(3, "stream-b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	gotA, _ := Collect(store.Load(ctx, "stream-a", es.ReadOptions{}))
	if len(gotA) != 2 || gotA[0].GlobalPosition != 1 || gotA[1].GlobalPosition != 2 {
		t.Errorf("stream-a positions = (%d, %d), want (1, 2)",
			safePos(gotA, 0), safePos(gotA, 1))
	}

	gotB, _ := Collect(store.Load(ctx, "stream-b", es.ReadOptions{}))
	if len(gotB) != 3 {
		t.Fatalf("stream-b: len = %d", len(gotB))
	}
	for i, ev := range gotB {
		want := uint64(3 + i) // continues from position 3
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

func testAppendInputSliceUnchanged(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	events := MakeEvents(3, "stream-x", 1)
	if _, err := store.Append(ctx, "stream-x", es.NoStream, events...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i, ev := range events {
		if ev.GlobalPosition != 0 {
			t.Errorf("input events[%d].GlobalPosition = %d; Append should not mutate caller's slice",
				i, ev.GlobalPosition)
		}
	}
}

func testAppendConcurrent(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	const N = 32
	type result struct{ err error }
	results := make(chan result, N)

	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() {
			ev := MakeEvent(TestStream, 1)
			ev.EventID = fmt.Sprintf("contender-%d", i)
			_, err := store.Append(ctx, TestStream, es.NoStream, ev)
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

// ============================================================================
// Load tests
// ============================================================================

func testLoadEmptyStream(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	got, err := Collect(store.Load(ctx, TestStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(events) = %d, want 0", len(got))
	}
}

func testLoadAllRoundTrip(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)
	want := MakeEvents(5, TestStream, 1)
	if _, err := store.Append(ctx, TestStream, es.NoStream, want...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := Collect(store.Load(ctx, TestStream, es.ReadOptions{}))
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

func testLoadFromAndLimit(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)
	if _, err := store.Append(ctx, TestStream, es.NoStream, MakeEvents(10, TestStream, 1)...); err != nil {
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
		{"from-10/limit-1/last", 10, 1, 10, 1},
		{"from-11/past-end", 11, 0, 0, 0},
		{"from-100/way-past", 100, 5, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Collect(store.Load(ctx, TestStream, es.ReadOptions{From: tc.from, Limit: tc.limit}))
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

func testLoadMetadataRoundTrip(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)

	ev := MakeEvent(TestStream, 1)
	ev.Metadata = es.Metadata{"actor": "alice", "trace_id": "abc-123"}
	ev.Causation = "cmd-1"
	ev.Correlation = "req-1"

	if _, err := store.Append(ctx, TestStream, es.NoStream, ev); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := Collect(store.Load(ctx, TestStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
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

func testLoadBreakEarly(t *testing.T, factory Factory) {
	ctx := t.Context()
	store := factory(t)
	if _, err := store.Append(ctx, TestStream, es.NoStream, MakeEvents(5, TestStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got []es.RawEnvelope
	for env, err := range store.Load(ctx, TestStream, es.ReadOptions{}) {
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		got = append(got, env)
		if len(got) >= 2 {
			break
		}
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
}

func testLoadContextCanceled(t *testing.T, factory Factory) {
	store := factory(t)
	if _, err := store.Append(t.Context(), TestStream, es.NoStream, MakeEvents(3, TestStream, 1)...); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var got []es.RawEnvelope
	var gotErr error
	for env, err := range store.Load(ctx, TestStream, es.ReadOptions{}) {
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
		t.Errorf("len = %d, want 0 on canceled context", len(got))
	}
}

func testLoadIsolationFromCallerMutation(t *testing.T, factory Factory) {
	// Mutating the appended slice after Append must not affect the
	// store's view of the stream.
	ctx := t.Context()
	store := factory(t)

	events := MakeEvents(3, TestStream, 1)
	if _, err := store.Append(ctx, TestStream, es.NoStream, events...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	events[0].EventID = "tampered"
	events[1].Type = "tampered"

	got, err := Collect(store.Load(ctx, TestStream, es.ReadOptions{}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got[0].EventID == "tampered" || got[1].Type == "tampered" {
		t.Errorf("store retained mutated values: %+v", got)
	}
}

// ============================================================================
// Subscribe tests
// ============================================================================

func testSubscribeEmptyStore(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)
	got, err := Collect(store.Subscribe(ctx, es.SubscriptionOptions{}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func testSubscribeCatchUp(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)
	if _, err := store.Append(ctx, "a", es.NoStream, MakeEvents(2, "a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "b", es.NoStream, MakeEvents(3, "b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	got, err := Collect(store.Subscribe(ctx, es.SubscriptionOptions{}))
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

func testSubscribeCatchUpFrom(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)
	if _, err := store.Append(ctx, "a", es.NoStream, MakeEvents(5, "a", 1)...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := Collect(store.Subscribe(ctx, es.SubscriptionOptions{From: 2}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("len = %d, want 3 (positions 3,4,5)", len(got))
	}
	if len(got) > 0 && got[0].GlobalPosition != 3 {
		t.Errorf("first.GlobalPosition = %d, want 3", got[0].GlobalPosition)
	}
}

func testSubscribeLive(t *testing.T, factory SubscribableFactory) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := factory(t)

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

	if _, err := store.Append(ctx, "live", es.NoStream, MakeEvent("live", 1)); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if _, err := store.Append(ctx, "live", es.Exact(1), MakeEvent("live", 2)); err != nil {
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

func testSubscribeLiveContextCanceled(t *testing.T, factory SubscribableFactory) {
	ctx, cancel := context.WithCancel(t.Context())
	store := factory(t)

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
	case <-time.After(2 * time.Second):
		t.Errorf("subscriber did not exit on cancel")
	}
}

func testSubscribeLiveMany(t *testing.T, factory SubscribableFactory) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := factory(t)

	const N = 8
	const eventsPer = 3
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
				if count >= eventsPer {
					done <- count
					return
				}
			}
			done <- count
		})
	}

	time.Sleep(50 * time.Millisecond) // let subscribers reach the wait

	for i := range eventsPer {
		if _, err := store.Append(ctx, "s", es.Exact(uint64(i)), MakeEvent("s", uint64(i+1))); err != nil {
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
		if c != eventsPer {
			t.Errorf("subscriber %d received %d events, want %d", i, c, eventsPer)
		}
	}
	wg.Wait()
}

func testSubscribeStreamOnlyTarget(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)
	if _, err := store.Append(ctx, "a", es.NoStream, MakeEvents(3, "a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "b", es.NoStream, MakeEvents(4, "b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	got, err := Collect(store.SubscribeStream(ctx, "b", es.SubscriptionOptions{}))
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("len = %d, want 4", len(got))
	}
	for _, ev := range got {
		if ev.StreamID != "b" {
			t.Errorf("event from %q, want b", ev.StreamID)
		}
	}
}

func testSubscribeStreamFromVersion(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)
	if _, err := store.Append(ctx, "a", es.NoStream, MakeEvents(5, "a", 1)...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := Collect(store.SubscribeStream(ctx, "a", es.SubscriptionOptions{From: 3}))
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
	if len(got) > 0 && got[0].Version != 4 {
		t.Errorf("first.Version = %d, want 4", got[0].Version)
	}
}

// testSubscribeTypeFilterCatchUp verifies that a non-live global
// subscription with SubscriptionOptions.Types yields only matching
// types, in order, with their true (non-contiguous) GlobalPositions.
func testSubscribeTypeFilterCatchUp(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)

	// Positions 1..5 with mixed types; only A and C are wanted.
	events := []es.RawEnvelope{
		MakeTypedEvent("s", 1, "A"),
		MakeTypedEvent("s", 2, "B"),
		MakeTypedEvent("s", 3, "A"),
		MakeTypedEvent("s", 4, "C"),
		MakeTypedEvent("s", 5, "A"),
	}
	if _, err := store.Append(ctx, "s", es.NoStream, events...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := Collect(store.Subscribe(ctx, es.SubscriptionOptions{Types: []string{"A", "C"}}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	wantPos := []uint64{1, 3, 4, 5}
	wantType := []string{"A", "A", "C", "A"}
	if len(got) != len(wantPos) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(wantPos), got)
	}
	for i, env := range got {
		if env.GlobalPosition != wantPos[i] || env.Type != wantType[i] {
			t.Errorf("got[%d] = {type:%s pos:%d}, want {type:%s pos:%d}",
				i, env.Type, env.GlobalPosition, wantType[i], wantPos[i])
		}
	}
}

// testSubscribeTypeFilterLive verifies that a live global subscription
// with a type filter delivers only matching types, including across a
// wake-up that also covered non-matching appends.
func testSubscribeTypeFilterLive(t *testing.T, factory SubscribableFactory) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := factory(t)

	if _, err := store.Append(ctx, "s", es.NoStream,
		MakeTypedEvent("s", 1, "A"), MakeTypedEvent("s", 2, "B")); err != nil {
		t.Fatalf("Append initial: %v", err)
	}

	got := make(chan es.RawEnvelope, 16)
	errc := make(chan error, 1)
	go func() {
		for env, err := range store.Subscribe(ctx, es.SubscriptionOptions{Live: true, Types: []string{"A"}}) {
			if err != nil {
				errc <- err
				return
			}
			got <- env
		}
	}()

	recv := func(wantVersion uint64) {
		t.Helper()
		select {
		case env := <-got:
			if env.Type != "A" || env.Version != wantVersion {
				t.Errorf("received {type:%s v:%d}, want {type:A v:%d}", env.Type, env.Version, wantVersion)
			}
		case err := <-errc:
			t.Fatalf("subscribe error: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatalf("did not receive A v%d", wantVersion)
		}
	}

	// Catch-up delivers the pre-existing A (v1), never the B (v2).
	recv(1)

	// A wake that bundles non-A appends must still deliver only the A's.
	if _, err := store.Append(ctx, "s", es.Exact(2),
		MakeTypedEvent("s", 3, "B"),
		MakeTypedEvent("s", 4, "A"),
		MakeTypedEvent("s", 5, "C"),
		MakeTypedEvent("s", 6, "A")); err != nil {
		t.Fatalf("Append more: %v", err)
	}
	recv(4)
	recv(6)
}

// testSubscribeStreamTypeFilter verifies the type filter composes with
// per-stream scoping: only matching types from the target stream.
func testSubscribeStreamTypeFilter(t *testing.T, factory SubscribableFactory) {
	ctx := t.Context()
	store := factory(t)

	if _, err := store.Append(ctx, "a", es.NoStream,
		MakeTypedEvent("a", 1, "A"),
		MakeTypedEvent("a", 2, "B"),
		MakeTypedEvent("a", 3, "A")); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "b", es.NoStream, MakeTypedEvent("b", 1, "A")); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	got, err := Collect(store.SubscribeStream(ctx, "a", es.SubscriptionOptions{Types: []string{"A"}}))
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (%v)", len(got), got)
	}
	wantVersions := []uint64{1, 3}
	for i, env := range got {
		if env.StreamID != "a" || env.Type != "A" || env.Version != wantVersions[i] {
			t.Errorf("got[%d] = {stream:%s type:%s v:%d}, want {stream:a type:A v:%d}",
				i, env.StreamID, env.Type, env.Version, wantVersions[i])
		}
	}
}
