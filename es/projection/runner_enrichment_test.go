package projection_test

import (
	"context"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

// captureProjection records the first inbound event and issues a
// follow-up Save through the Repository, then captures the
// just-written raw envelope. Tests inspect the captured envelope's
// Causation/Correlation to verify the runner threaded the inbound
// event's identifiers through the context.
type captureProjection struct {
	repo   *es.Repository[*testdomain.Counter]
	store  *memory.Store
	target es.StreamID
	saved  *es.RawEnvelope
	fired  bool
}

func (p *captureProjection) Project(ctx context.Context, _ es.Envelope) error {
	if p.fired {
		return nil
	}
	p.fired = true

	c := testdomain.NewCounter(p.target)
	c.Increment(1)
	if err := p.repo.Save(ctx, c); err != nil {
		return err
	}
	for raw, err := range p.store.Load(ctx, p.target, es.ReadOptions{}) {
		if err != nil {
			return err
		}
		saved := raw
		p.saved = &saved
		return nil
	}
	return nil
}

func TestRunner_EnrichesProjectContext_StampsCausationAndCorrelation(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	repo := es.NewRepository(store, reg, testdomain.NewCounter)

	// Seed one event whose Correlation is set via ctx, so we can
	// verify both Causation (inbound EventID) and Correlation
	// (propagated from inbound) on the projection's outbound event.
	seedCtx := es.WithCorrelation(ctx, "orig-corr")
	seed := testdomain.NewCounter(testdomain.CounterStream)
	seed.Increment(1)
	if err := repo.Save(seedCtx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	var seededID string
	for raw, err := range store.Load(ctx, testdomain.CounterStream, es.ReadOptions{}) {
		if err != nil {
			t.Fatalf("seed Load: %v", err)
		}
		seededID = raw.EventID
		break
	}

	proj := &captureProjection{repo: repo, store: store, target: "test/derived"}
	r := projection.NewRunner("test", store, reg, proj)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if proj.saved == nil {
		t.Fatalf("projection did not save a follow-up event")
	}
	if proj.saved.Causation != seededID {
		t.Errorf("Causation = %q, want %q (inbound EventID)", proj.saved.Causation, seededID)
	}
	if proj.saved.Correlation != "orig-corr" {
		t.Errorf("Correlation = %q, want orig-corr", proj.saved.Correlation)
	}
}

func TestRunner_WithoutContextEnrichment_LeavesProjectCtxAlone(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()
	repo := es.NewRepository(store, reg, testdomain.NewCounter)

	seedCtx := es.WithCorrelation(ctx, "orig-corr")
	seed := testdomain.NewCounter(testdomain.CounterStream)
	seed.Increment(1)
	if err := repo.Save(seedCtx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	proj := &captureProjection{repo: repo, store: store, target: "test/derived"}
	r := projection.NewRunner("test", store, reg, proj,
		projection.WithoutContextEnrichment())
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if proj.saved == nil {
		t.Fatalf("projection did not save a follow-up event")
	}
	if proj.saved.Causation != "" {
		t.Errorf("Causation = %q, want empty (enrichment disabled)", proj.saved.Causation)
	}
	if proj.saved.Correlation != "" {
		t.Errorf("Correlation = %q, want empty (enrichment disabled)", proj.saved.Correlation)
	}
}
