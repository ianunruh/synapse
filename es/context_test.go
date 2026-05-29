package es_test

import (
	"context"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

// loadOne returns the first RawEnvelope from the store for the given
// stream, failing the test if none exists.
func loadOne(t *testing.T, store *memory.Store, stream es.StreamID) es.RawEnvelope {
	t.Helper()
	ctx := t.Context()
	for env, err := range store.Load(ctx, stream, es.ReadOptions{}) {
		if err != nil {
			t.Fatalf("store.Load: %v", err)
		}
		return env
	}
	t.Fatalf("expected at least one stored event in %q", stream)
	return es.RawEnvelope{}
}

func TestSave_StampsContextCorrelationOnEmptyField(t *testing.T) {
	store := memory.New()
	repo := es.NewRepository(store, testdomain.NewRegistry(), testdomain.NewCounter)

	ctx := es.WithCorrelation(t.Context(), "corr-abc")
	c := testdomain.NewCounter(testdomain.CounterStream)
	c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got := loadOne(t, store, testdomain.CounterStream)
	if got.Correlation != "corr-abc" {
		t.Errorf("Correlation = %q, want %q", got.Correlation, "corr-abc")
	}
}

func TestSave_StampsContextCausationOnEmptyField(t *testing.T) {
	store := memory.New()
	repo := es.NewRepository(store, testdomain.NewRegistry(), testdomain.NewCounter)

	ctx := es.WithCausation(t.Context(), "cause-xyz")
	c := testdomain.NewCounter(testdomain.CounterStream)
	c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got := loadOne(t, store, testdomain.CounterStream)
	if got.Causation != "cause-xyz" {
		t.Errorf("Causation = %q, want %q", got.Causation, "cause-xyz")
	}
}

func TestSave_StampsContextMetadataOnEnvelope(t *testing.T) {
	store := memory.New()
	repo := es.NewRepository(store, testdomain.NewRegistry(), testdomain.NewCounter)

	ctx := es.WithMetadata(t.Context(), es.Metadata{
		"user":  "alice",
		"trace": "abc",
	})
	c := testdomain.NewCounter(testdomain.CounterStream)
	c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got := loadOne(t, store, testdomain.CounterStream)
	if got.Metadata["user"] != "alice" {
		t.Errorf("Metadata[user] = %q, want alice", got.Metadata["user"])
	}
	if got.Metadata["trace"] != "abc" {
		t.Errorf("Metadata[trace] = %q, want abc", got.Metadata["trace"])
	}
}

func TestWithMetadata_SuccessiveCallsMerge(t *testing.T) {
	// Two WithMetadata calls in sequence should combine, with the later
	// call winning on key collision. Verify via Save round-trip.
	store := memory.New()
	repo := es.NewRepository(store, testdomain.NewRegistry(), testdomain.NewCounter)

	ctx := es.WithMetadata(t.Context(), es.Metadata{"user": "alice", "trace": "abc"})
	ctx = es.WithMetadata(ctx, es.Metadata{"trace": "xyz", "extra": "1"})

	c := testdomain.NewCounter(testdomain.CounterStream)
	c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got := loadOne(t, store, testdomain.CounterStream)
	want := es.Metadata{"user": "alice", "trace": "xyz", "extra": "1"}
	for k, v := range want {
		if got.Metadata[k] != v {
			t.Errorf("Metadata[%q] = %q, want %q", k, got.Metadata[k], v)
		}
	}
	if len(got.Metadata) != len(want) {
		t.Errorf("Metadata = %v, want %v", got.Metadata, want)
	}
}

func TestSave_NoContextValues_LeavesFieldsEmpty(t *testing.T) {
	// Regression: when ctx is plain context.Background(), Save must
	// not invent any stamping.
	store := memory.New()
	repo := es.NewRepository(store, testdomain.NewRegistry(), testdomain.NewCounter)

	c := testdomain.NewCounter(testdomain.CounterStream)
	c.Increment(1)
	if err := repo.Save(context.Background(), c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got := loadOne(t, store, testdomain.CounterStream)
	if got.Correlation != "" {
		t.Errorf("Correlation = %q, want empty", got.Correlation)
	}
	if got.Causation != "" {
		t.Errorf("Causation = %q, want empty", got.Causation)
	}
	if got.Metadata != nil {
		t.Errorf("Metadata = %v, want nil", got.Metadata)
	}
}
