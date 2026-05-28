package es_test

import (
	"errors"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

// Static interface assertion: testdomain.Counter must satisfy Snapshotter.
var _ es.Snapshotter = (*testdomain.Counter)(nil)

// ----- Policy unit tests --------------------------------------------------

func TestEveryNVersions(t *testing.T) {
	cases := []struct {
		name   string
		n      uint64
		before uint64
		after  uint64
		want   bool
	}{
		{"n=0/disabled", 0, 0, 100, false},
		{"crosses-100/0-100", 100, 0, 100, true},
		{"crosses-100/95-105", 100, 95, 105, true},
		{"crosses-100/99-100", 100, 99, 100, true},
		{"no-cross/100-150", 100, 100, 150, false},
		{"no-cross/0-99", 100, 0, 99, false},
		{"crosses-multiple/0-250-once", 100, 0, 250, true},
		{"crosses-50/50-100", 50, 50, 100, true},
		{"no-cross-50/50-99", 50, 50, 99, false},
		{"crosses-1-always", 1, 0, 1, true},
		{"crosses-1-second", 1, 1, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := es.EveryNVersions(tc.n)(nil, tc.before, tc.after); got != tc.want {
				t.Errorf("EveryNVersions(%d)(_, %d, %d) = %v, want %v",
					tc.n, tc.before, tc.after, got, tc.want)
			}
		})
	}
}

// ----- SaveSnapshot manual / configuration --------------------------------

func TestRepository_SaveSnapshot_NoStoreConfigured_Errors(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter)

	c := testdomain.NewCounter(testdomain.CounterStream)
	if err := c.Increment(1); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := repo.SaveSnapshot(ctx, c); err == nil {
		t.Errorf("SaveSnapshot: expected error when no snapshot store configured")
	}
}

func TestRepository_SaveSnapshot_Manual_PersistsSnapshot(t *testing.T) {
	ctx := t.Context()
	snaps := snapmem.New()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps))

	c := testdomain.NewCounter(testdomain.CounterStream)
	if err := c.Increment(5); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := c.Increment(3); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := repo.SaveSnapshot(ctx, c); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	snap, ok, err := snaps.Latest(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatalf("expected snapshot to exist after manual SaveSnapshot")
	}
	if snap.Version != 2 {
		t.Errorf("snap.Version = %d, want 2", snap.Version)
	}
	if snap.Type != "counter.snapshot.v1" {
		t.Errorf("snap.Type = %q, want counter.snapshot.v1", snap.Type)
	}
	if snap.ContentType != "application/json" {
		t.Errorf("snap.ContentType = %q, want application/json", snap.ContentType)
	}
	if string(snap.Payload) != `{"count":8}` {
		t.Errorf("snap.Payload = %s, want {\"count\":8}", snap.Payload)
	}
}

// ----- Load with snapshot ------------------------------------------------

func TestRepository_Load_WithSnapshot_ReplaysOnlyNewerEvents(t *testing.T) {
	ctx := t.Context()
	events := memory.New()
	snaps := snapmem.New()
	repo := es.NewRepository(events, testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps))

	// Seed 5 events, snapshot at v=5, then add 2 more events.
	c := testdomain.NewCounter(testdomain.CounterStream)
	for range 5 {
		if err := c.Increment(1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.SaveSnapshot(ctx, c); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if err := c.Increment(10); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := c.Increment(20); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.Load(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Count != 35 { // 5 * 1 + 10 + 20
		t.Errorf("loaded.Count = %d, want 35", loaded.Count)
	}
	if loaded.Version() != 7 {
		t.Errorf("loaded.Version() = %d, want 7", loaded.Version())
	}
}

func TestRepository_Load_NoSnapshotStore_FullReplay(t *testing.T) {
	// Without a snapshot store configured, Load behaves the same as before.
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter)

	c := testdomain.NewCounter(testdomain.CounterStream)
	for range 3 {
		if err := c.Increment(7); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.Load(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Count != 21 {
		t.Errorf("loaded.Count = %d, want 21", loaded.Count)
	}
}

func TestRepository_Load_SnapshotOnly_NoNewerEvents(t *testing.T) {
	// Snapshot exists and no events have been added after it. Load
	// returns the aggregate at the snapshot's version, no replay.
	ctx := t.Context()
	events := memory.New()
	snaps := snapmem.New()
	repo := es.NewRepository(events, testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps))

	c := testdomain.NewCounter(testdomain.CounterStream)
	if err := c.Increment(42); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.SaveSnapshot(ctx, c); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	loaded, err := repo.Load(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Count != 42 {
		t.Errorf("loaded.Count = %d, want 42", loaded.Count)
	}
	if loaded.Version() != 1 {
		t.Errorf("loaded.Version() = %d, want 1", loaded.Version())
	}
}

func TestRepository_Load_SnapshotCodecMissing_Errors(t *testing.T) {
	// Snapshot exists in the store, but the registry doesn't have a
	// codec registered for the snapshot's type. Load must surface
	// the misconfiguration, not silently fall back.
	ctx := t.Context()
	events := memory.New()
	snaps := snapmem.New()

	// Seed the snapshot with a fully populated registry.
	full := es.NewRepository(events, testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps))
	c := testdomain.NewCounter(testdomain.CounterStream)
	if err := c.Increment(3); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := full.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := full.SaveSnapshot(ctx, c); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Build a Repository whose registry omits the snapshot codec.
	reg := testdomain.NewRegistryWithoutSnapshot()
	partial := es.NewRepository(events, reg, testdomain.NewCounter,
		es.WithSnapshotStore(snaps))

	_, err := partial.Load(ctx, testdomain.CounterStream)
	if !errors.Is(err, es.ErrCodecNotFound) {
		t.Errorf("err = %v, want wrap of ErrCodecNotFound", err)
	}
}

// ----- Save policy --------------------------------------------------------

func TestRepository_Save_PolicyFires_SnapshotTaken(t *testing.T) {
	ctx := t.Context()
	snaps := snapmem.New()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps),
		es.WithSnapshotPolicy(es.EveryNVersions(5)))

	c := testdomain.NewCounter(testdomain.CounterStream)
	for range 5 {
		if err := c.Increment(1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	snap, ok, err := snaps.Latest(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatalf("expected snapshot after crossing v=5")
	}
	if snap.Version != 5 {
		t.Errorf("snap.Version = %d, want 5", snap.Version)
	}
}

func TestRepository_Save_PolicyDoesNotFire_NoSnapshot(t *testing.T) {
	ctx := t.Context()
	snaps := snapmem.New()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps),
		es.WithSnapshotPolicy(es.EveryNVersions(100)))

	c := testdomain.NewCounter(testdomain.CounterStream)
	for range 5 {
		if err := c.Increment(1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if _, ok, err := snaps.Latest(ctx, testdomain.CounterStream); err != nil {
		t.Fatalf("Latest: %v", err)
	} else if ok {
		t.Errorf("expected no snapshot before crossing v=100")
	}
}

func TestRepository_Save_NoPolicy_NoSnapshot(t *testing.T) {
	// Snapshot store configured but no policy: automatic snapshots
	// never fire. Manual SaveSnapshot still works.
	ctx := t.Context()
	snaps := snapmem.New()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps))

	c := testdomain.NewCounter(testdomain.CounterStream)
	for range 1000 {
		if err := c.Increment(1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if _, ok, err := snaps.Latest(ctx, testdomain.CounterStream); err != nil {
		t.Fatalf("Latest: %v", err)
	} else if ok {
		t.Errorf("expected no automatic snapshot without policy")
	}
}

func TestRepository_Save_PolicyAcrossMultipleSaves(t *testing.T) {
	// Policy should fire as the aggregate crosses successive multiples
	// of n across separate Save calls.
	ctx := t.Context()
	snaps := snapmem.New()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithSnapshotStore(snaps),
		es.WithSnapshotPolicy(es.EveryNVersions(3)))

	c := testdomain.NewCounter(testdomain.CounterStream)

	// First Save reaches v=3 — should snapshot at 3.
	for range 3 {
		_ = c.Increment(1)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	snap, ok, _ := snaps.Latest(ctx, testdomain.CounterStream)
	if !ok || snap.Version != 3 {
		t.Errorf("after first Save: snap.Version=%d ok=%v, want v=3 ok=true", snap.Version, ok)
	}

	// Second Save reaches v=4 — no crossing, no new snapshot.
	_ = c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save 2: %v", err)
	}
	snap, _, _ = snaps.Latest(ctx, testdomain.CounterStream)
	if snap.Version != 3 {
		t.Errorf("after second Save: snap.Version=%d, want still 3", snap.Version)
	}

	// Third Save reaches v=6 — crosses 3 -> 6, snapshot at 6.
	_ = c.Increment(1)
	_ = c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save 3: %v", err)
	}
	snap, _, _ = snaps.Latest(ctx, testdomain.CounterStream)
	if snap.Version != 6 {
		t.Errorf("after third Save: snap.Version=%d, want 6", snap.Version)
	}
}
