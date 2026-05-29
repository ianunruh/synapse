package projection_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	checkpointmem "github.com/ianunruh/synapse/checkpointstore/memory"
	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/eventstoretest"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

// recordingProjection captures every event Project receives. Optional
// failOn callback returns a non-nil error for matching events.
type recordingProjection struct {
	mu     sync.Mutex
	events []es.Envelope
	failOn func(es.Envelope) error
}

func (p *recordingProjection) Project(_ context.Context, env es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, env)
	if p.failOn != nil {
		return p.failOn(env)
	}
	return nil
}

func (p *recordingProjection) Recorded() []es.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.events)
}

// ----- Helpers -----------------------------------------------------------

func seedCounters(t *testing.T, store *memory.Store, reg *es.Registry, streams int, events int) {
	t.Helper()
	ctx := t.Context()
	repo := es.NewRepository(store, reg, testdomain.NewCounter)
	for s := range streams {
		stream := es.StreamID(testdomain.CounterStream + es.StreamID("-") + es.StreamID(rune('a'+s)))
		c := testdomain.NewCounter(stream)
		for i := range events {
			c.Increment(i + 1)
		}
		if err := repo.Save(ctx, c); err != nil {
			t.Fatalf("seed Save: %v", err)
		}
	}
}

// ----- Catch-up subscriptions --------------------------------------------

func TestRunner_GlobalSubscription_CatchUp(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 2, 3) // 2 streams x 3 events = 6 events total

	proj := &recordingProjection{}
	r := projection.NewRunner("test", store, reg, proj)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(proj.Recorded()); got != 6 {
		t.Errorf("recorded %d events, want 6", got)
	}
	// Verify global ordering: GlobalPosition is monotonic.
	var last uint64
	for _, e := range proj.Recorded() {
		if e.GlobalPosition <= last {
			t.Errorf("GlobalPosition not monotonic: %d after %d", e.GlobalPosition, last)
		}
		last = e.GlobalPosition
	}
}

func TestRunner_PerStreamSubscription_CatchUp(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 3, 4) // 3 streams x 4 events

	target := es.StreamID(testdomain.CounterStream + "-b")
	proj := &recordingProjection{}
	r := projection.NewRunner("test", store, reg, proj, projection.WithStream(target))
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(proj.Recorded()); got != 4 {
		t.Errorf("recorded %d events, want 4 (one stream)", got)
	}
	for _, e := range proj.Recorded() {
		if e.StreamID != target {
			t.Errorf("event from %q, want %q", e.StreamID, target)
		}
	}
}

// ----- Live mode ---------------------------------------------------------

func TestRunner_LiveMode_SeesNewEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		store := memory.New()
		reg := testdomain.NewRegistry()

		// Seed two events first.
		seedCounters(t, store, reg, 1, 2)

		proj := &recordingProjection{}
		r := projection.NewRunner("live", store, reg, proj, projection.WithLive(true))

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()

		// Wait for the runner to consume the initial events.
		synctest.Wait()
		if got := len(proj.Recorded()); got != 2 {
			t.Fatalf("after seed: recorded %d, want 2", got)
		}

		// Append more events live.
		repo := es.NewRepository(store, reg, testdomain.NewCounter)
		stream := es.StreamID(testdomain.CounterStream + "-a")
		loaded, err := repo.Load(ctx, stream)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		loaded.Increment(99)
		if err := repo.Save(ctx, loaded); err != nil {
			t.Fatalf("Save: %v", err)
		}

		// Verify the runner sees the new event.
		synctest.Wait()
		if got := len(proj.Recorded()); got != 3 {
			t.Errorf("after live append: recorded %d, want 3", got)
		}

		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run after cancel: %v", err)
			}
		default:
			t.Errorf("Run did not exit after cancel")
		}
	})
}

// ----- Checkpoint integration --------------------------------------------

func TestRunner_Checkpoint_ResumesAfterRestart(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	cps := checkpointmem.New()
	seedCounters(t, store, reg, 1, 5)

	proj1 := &recordingProjection{}
	r1 := projection.NewRunner("resumable", store, reg, proj1, projection.WithCheckpoint(cps))
	if err := r1.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := len(proj1.Recorded()); got != 5 {
		t.Fatalf("first Run recorded %d, want 5", got)
	}

	// Append more events.
	repo := es.NewRepository(store, reg, testdomain.NewCounter)
	stream := es.StreamID(testdomain.CounterStream + "-a")
	loaded, err := repo.Load(ctx, stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for range 2 {
		loaded.Increment(10)
	}
	if err := repo.Save(ctx, loaded); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Second Runner should pick up only the new events.
	proj2 := &recordingProjection{}
	r2 := projection.NewRunner("resumable", store, reg, proj2, projection.WithCheckpoint(cps))
	if err := r2.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := len(proj2.Recorded()); got != 2 {
		t.Errorf("second Run recorded %d, want 2 (only new events past checkpoint)", got)
	}
}

func TestRunner_Reset_StartsFromBeginning(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	cps := checkpointmem.New()
	seedCounters(t, store, reg, 1, 3)

	proj1 := &recordingProjection{}
	r1 := projection.NewRunner("rebuild", store, reg, proj1, projection.WithCheckpoint(cps))
	if err := r1.Run(ctx); err != nil {
		t.Fatalf("Run 1: %v", err)
	}

	// Reset the checkpoint and re-run.
	if err := cps.Reset(ctx, "rebuild"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	proj2 := &recordingProjection{}
	r2 := projection.NewRunner("rebuild", store, reg, proj2, projection.WithCheckpoint(cps))
	if err := r2.Run(ctx); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := len(proj2.Recorded()); got != 3 {
		t.Errorf("after reset: recorded %d, want 3 (full replay)", got)
	}
}

// ----- Error policy ------------------------------------------------------

func TestRunner_ProjectionError_StopsByDefault(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 1, 5)

	boom := errors.New("kaboom")
	var calls atomic.Int32
	proj := &recordingProjection{
		failOn: func(_ es.Envelope) error {
			if calls.Add(1) == 3 {
				return boom
			}
			return nil
		},
	}

	r := projection.NewRunner("fail", store, reg, proj)
	err := r.Run(ctx)
	if !errors.Is(err, boom) {
		t.Errorf("Run: err = %v, want boom", err)
	}
	if got := len(proj.Recorded()); got != 3 {
		t.Errorf("recorded %d, want 3 (stopped at failing event)", got)
	}
}

func TestRunner_OnError_Skip_ContinuesAndCheckpoints(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	cps := checkpointmem.New()
	seedCounters(t, store, reg, 1, 5)

	boom := errors.New("transient")
	var calls atomic.Int32
	proj := &recordingProjection{
		failOn: func(_ es.Envelope) error {
			if calls.Add(1) == 3 {
				return boom
			}
			return nil
		},
	}

	skipped := false
	r := projection.NewRunner("skipper", store, reg, proj,
		projection.WithCheckpoint(cps),
		projection.WithOnError(func(_ es.Envelope, _ error) bool {
			skipped = true
			return true
		}),
	)
	if err := r.Run(ctx); err != nil {
		t.Errorf("Run: %v", err)
	}
	if !skipped {
		t.Errorf("OnError was never called")
	}
	if got := len(proj.Recorded()); got != 5 {
		t.Errorf("recorded %d, want 5 (skip + continue)", got)
	}

	// Checkpoint should be past the failing event.
	pos, found, err := cps.Load(ctx, "skipper")
	if err != nil {
		t.Fatalf("checkpoint Load: %v", err)
	}
	if !found || pos != 5 {
		t.Errorf("checkpoint = (%d, %v), want (5, true)", pos, found)
	}
}

func TestRunner_OnError_NoSkip_Stops(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 1, 5)

	boom := errors.New("permanent")
	var calls atomic.Int32
	proj := &recordingProjection{
		failOn: func(_ es.Envelope) error {
			if calls.Add(1) == 2 {
				return boom
			}
			return nil
		},
	}

	r := projection.NewRunner("nostop", store, reg, proj,
		projection.WithOnError(func(_ es.Envelope, _ error) bool { return false }),
	)
	err := r.Run(ctx)
	if !errors.Is(err, boom) {
		t.Errorf("Run: err = %v, want boom", err)
	}
}

func TestRunner_UnknownEventType_FailsWithCodecError(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	full := testdomain.NewRegistry()
	seedCounters(t, store, full, 1, 2)

	partial := es.NewRegistry() // no codecs registered
	proj := &recordingProjection{}
	r := projection.NewRunner("missing", store, partial, proj)
	err := r.Run(ctx)
	if !errors.Is(err, es.ErrCodecNotFound) {
		t.Errorf("Run: err = %v, want wrap of ErrCodecNotFound", err)
	}
}

// ----- Context cancellation ---------------------------------------------

func TestRunner_ContextCanceled_ReturnsCleanly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		store := memory.New()
		reg := testdomain.NewRegistry()
		proj := &recordingProjection{}
		r := projection.NewRunner("ctx", store, reg, proj, projection.WithLive(true))

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()

		// Let it block waiting for events, then cancel.
		synctest.Wait()
		cancel()
		synctest.Wait()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: err = %v, want nil on cancel", err)
			}
		default:
			t.Errorf("Run did not exit on cancel")
		}
	})
}

// ----- Type filtering ----------------------------------------------------

type filterPayload struct {
	Version int `json:"version"`
}

func TestRunner_WithTypes_FiltersBeforeDecode(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	if _, err := store.Append(ctx, "s", es.NoStream,
		eventstoretest.MakeTypedEvent("s", 1, "kept"),
		eventstoretest.MakeTypedEvent("s", 2, "dropped"),
		eventstoretest.MakeTypedEvent("s", 3, "kept"),
	); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// A codec is registered only for "kept"; without WithTypes the
	// Runner would hit ErrCodecNotFound on the "dropped" event.
	reg := es.NewRegistry()
	es.Register(reg, "kept", jsoncodec.For[filterPayload]())

	proj := &recordingProjection{}
	r := projection.NewRunner("typed", store, reg, proj, projection.WithTypes("kept"))
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rec := proj.Recorded()
	if len(rec) != 2 {
		t.Fatalf("recorded %d events, want 2 (only kept)", len(rec))
	}
	for _, e := range rec {
		if e.Type != "kept" {
			t.Errorf("recorded type %q, want kept", e.Type)
		}
	}
}

// ----- Checkpoint batching -----------------------------------------------

// recordingCheckpoint records every Save position so tests can assert
// the batching cadence, not just the final value.
type recordingCheckpoint struct {
	mu    sync.Mutex
	saved []uint64
	last  map[string]uint64
}

func newRecordingCheckpoint() *recordingCheckpoint {
	return &recordingCheckpoint{last: map[string]uint64{}}
}

func (c *recordingCheckpoint) Save(_ context.Context, name string, pos uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saved = append(c.saved, pos)
	c.last[name] = pos
	return nil
}

func (c *recordingCheckpoint) Load(_ context.Context, name string) (uint64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.last[name]
	return p, ok, nil
}

func (c *recordingCheckpoint) Reset(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.last, name)
	return nil
}

func (c *recordingCheckpoint) Saves() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.saved)
}

func TestRunner_CheckpointEvery_BatchesAndFlushes(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 1, 5) // positions 1..5

	cps := newRecordingCheckpoint()
	proj := &recordingProjection{}
	r := projection.NewRunner("batch", store, reg, proj,
		projection.WithCheckpoint(cps),
		projection.WithCheckpointEvery(2),
	)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(proj.Recorded()); got != 5 {
		t.Fatalf("recorded %d, want 5", got)
	}

	// Saves at positions 2 and 4 (every 2), plus a final flush of 5 on
	// clean drain.
	want := []uint64{2, 4, 5}
	if got := cps.Saves(); !slices.Equal(got, want) {
		t.Errorf("checkpoint saves = %v, want %v", got, want)
	}
}

func TestRunner_CheckpointEvery_NoFlushOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		store := memory.New()
		reg := testdomain.NewRegistry()
		cps := checkpointmem.New()
		seedCounters(t, store, reg, 1, 2) // 2 events, batch never reached

		proj := &recordingProjection{}
		r := projection.NewRunner("batch-cancel", store, reg, proj,
			projection.WithCheckpoint(cps),
			projection.WithLive(true),
			projection.WithCheckpointEvery(5),
		)

		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()

		synctest.Wait()
		if got := len(proj.Recorded()); got != 2 {
			t.Fatalf("recorded %d, want 2", got)
		}
		if _, found, _ := cps.Load(context.Background(), "batch-cancel"); found {
			t.Errorf("checkpoint saved before batch threshold; want none")
		}

		cancel()
		synctest.Wait()
		if err := <-done; err != nil {
			t.Errorf("Run after cancel: %v", err)
		}
		// No flush on cancel: up to checkpointEvery-1 events are
		// redelivered on the next run instead.
		if _, found, _ := cps.Load(context.Background(), "batch-cancel"); found {
			t.Errorf("checkpoint flushed on cancel; want none")
		}
	})
}
