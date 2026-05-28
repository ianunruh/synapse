package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
)

// ----- GlobalPosition assignment -----

func TestAppend_AssignsGlobalPosition(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	if _, err := store.Append(ctx, "stream-a", es.NoStream, makeEvents(2, "stream-a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "stream-b", es.NoStream, makeEvents(3, "stream-b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	// Read stream-a events and verify GlobalPosition is monotonic 1,2.
	gotA, _ := collect(store.Load(ctx, "stream-a", es.ReadOptions{}))
	if len(gotA) != 2 {
		t.Fatalf("stream-a: len = %d", len(gotA))
	}
	if gotA[0].GlobalPosition != 1 || gotA[1].GlobalPosition != 2 {
		t.Errorf("stream-a positions = (%d, %d), want (1, 2)",
			gotA[0].GlobalPosition, gotA[1].GlobalPosition)
	}

	gotB, _ := collect(store.Load(ctx, "stream-b", es.ReadOptions{}))
	if len(gotB) != 3 {
		t.Fatalf("stream-b: len = %d", len(gotB))
	}
	for i, ev := range gotB {
		want := uint64(3 + i) // continues from 3,4,5
		if ev.GlobalPosition != want {
			t.Errorf("stream-b[%d].GlobalPosition = %d, want %d", i, ev.GlobalPosition, want)
		}
	}
}

func TestAppend_InputSliceUnchanged(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	events := makeEvents(3, "stream-x", 1)
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

// ----- Catch-up Subscribe (Live=false) -----

func TestSubscribe_CatchUp_AllStreams(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
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
		t.Errorf("len(events) = %d, want 5", len(got))
	}
	// Verify monotonic GlobalPosition.
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
	store := memory.New()
	if _, err := store.Append(ctx, "a", es.NoStream, makeEvents(5, "a", 1)...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := collect(store.Subscribe(ctx, es.SubscriptionOptions{From: 2}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("len = %d, want 3 (positions 3,4,5)", len(got))
	}
	if got[0].GlobalPosition != 3 {
		t.Errorf("first.GlobalPosition = %d, want 3", got[0].GlobalPosition)
	}
}

func TestSubscribe_EmptyStore(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	got, err := collect(store.Subscribe(ctx, es.SubscriptionOptions{}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

// ----- Per-stream Subscribe -----

func TestSubscribeStream_OnlyTargetStream(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	if _, err := store.Append(ctx, "a", es.NoStream, makeEvents(3, "a", 1)...); err != nil {
		t.Fatalf("Append a: %v", err)
	}
	if _, err := store.Append(ctx, "b", es.NoStream, makeEvents(4, "b", 1)...); err != nil {
		t.Fatalf("Append b: %v", err)
	}

	got, err := collect(store.SubscribeStream(ctx, "b", es.SubscriptionOptions{}))
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

func TestSubscribeStream_FromVersion(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	if _, err := store.Append(ctx, "a", es.NoStream, makeEvents(5, "a", 1)...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := collect(store.SubscribeStream(ctx, "a", es.SubscriptionOptions{From: 3}))
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
	if got[0].Version != 4 {
		t.Errorf("first.Version = %d, want 4", got[0].Version)
	}
}

// ----- Live Subscribe -----

func TestSubscribe_Live_SeesNewAppends(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := memory.New()

	type result struct {
		env es.RawEnvelope
		err error
		ok  bool
	}
	results := make(chan result, 8)

	go func() {
		for env, err := range store.Subscribe(ctx, es.SubscriptionOptions{Live: true}) {
			results <- result{env: env, err: err, ok: true}
			if err != nil {
				close(results)
				return
			}
		}
		close(results)
	}()

	// Append a few events; the subscriber should receive each.
	if _, err := store.Append(ctx, "live", es.NoStream, makeEvent("live", 1)); err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if _, err := store.Append(ctx, "live", es.Exact(1), makeEvent("live", 2)); err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	collected := make([]es.RawEnvelope, 0, 2)
	timeout := time.After(time.Second)
	for len(collected) < 2 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("subscribe err: %v", r.err)
			}
			collected = append(collected, r.env)
		case <-timeout:
			t.Fatalf("did not receive 2 events; got %d", len(collected))
		}
	}
	if collected[0].Version != 1 || collected[1].Version != 2 {
		t.Errorf("versions = (%d, %d), want (1, 2)", collected[0].Version, collected[1].Version)
	}

	cancel()
}

func TestSubscribe_Live_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := memory.New()

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

	// Cancel after a brief delay so the subscriber is blocked in the
	// select waiting for events.
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

	store := memory.New()

	const N = 16
	received := make([]int, N)
	done := make(chan int, N)

	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() {
			count := 0
			for _, err := range store.Subscribe(ctx, es.SubscriptionOptions{Live: true}) {
				if err != nil {
					done <- count
					return
				}
				count++
				if count >= 4 {
					done <- count
					return
				}
			}
			done <- count
		})
		_ = i
	}

	// Make sure subscribers are blocked before appending.
	time.Sleep(20 * time.Millisecond)

	for i := range 4 {
		if _, err := store.Append(ctx, es.StreamID("s"), es.Exact(uint64(i)), makeEvent("s", uint64(i+1))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	for i := range N {
		select {
		case received[i] = <-done:
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not finish", i)
		}
	}
	for i, c := range received {
		if c != 4 {
			t.Errorf("subscriber %d received %d events, want 4", i, c)
		}
	}
	wg.Wait()
}

// ----- Interface assertion -----

func TestStoreImplementsSubscribable(t *testing.T) {
	var _ es.SubscribableEventStore = memory.New()
}
