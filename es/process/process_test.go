package process_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/process"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

// constant pmStream is the canonical PM aggregate id the tests route
// inbound events to. The "source" stream that produces events lives on
// a different stream id; the correlator maps both to pmStream.
const (
	sourceStream es.StreamID = "source"
	pmStream     es.StreamID = "pm"
)

// alwaysPM correlates every inbound event to pmStream. Useful for the
// happy-path tests that don't care about source-to-PM mapping.
func alwaysPM(_ es.Envelope) es.StreamID { return pmStream }

// incrementOnIncremented mirrors every CounterIncremented event from
// the source stream onto the PM aggregate. Trivial logic, but it
// exercises the load-handle-save path through Execute.
func incrementOnIncremented(_ context.Context, env es.Envelope, pm *testdomain.Counter) error {
	if p, ok := env.Payload.(testdomain.CounterIncremented); ok {
		pm.Increment(p.By)
	}
	return nil
}

// seedSource writes count CounterIncremented events to sourceStream.
func seedSource(t *testing.T, store *memory.Store, reg *es.Registry, count int) {
	t.Helper()
	repo := es.NewRepository(store, reg, testdomain.NewCounter)
	c := testdomain.NewCounter(sourceStream)
	for range count {
		c.Increment(1)
	}
	if err := repo.Save(t.Context(), c); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestManager_HappyPath_AppliesEveryEvent(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()

	seedSource(t, store, reg, 3) // 3 increments on sourceStream

	pmRepo := es.NewRepository(store, reg, testdomain.NewCounter)
	pm := process.New(pmRepo, alwaysPM, incrementOnIncremented)
	runner := projection.NewRunner("test", store, reg, pm)
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	loaded, err := pmRepo.Load(ctx, pmStream)
	if err != nil {
		t.Fatalf("Load PM: %v", err)
	}
	if loaded.Count != 3 {
		t.Errorf("PM Count = %d, want 3 (one per source event)", loaded.Count)
	}
}

func TestManager_FreshPM_CreatedByExecute(t *testing.T) {
	// The PM does not exist before the first event arrives. Execute's
	// create-on-missing (ADR-0030) builds a fresh aggregate; the PM
	// stream is created by the first Save. This test asserts the PM
	// stream did not exist beforehand.
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()

	pmRepo := es.NewRepository(store, reg, testdomain.NewCounter)

	// Pre-flight: the PM stream genuinely does not exist.
	if _, err := pmRepo.Load(ctx, pmStream); !errors.Is(err, es.ErrStreamNotFound) {
		t.Fatalf("pre-flight Load PM: err = %v, want ErrStreamNotFound", err)
	}

	seedSource(t, store, reg, 2)

	pm := process.New(pmRepo, alwaysPM, incrementOnIncremented)
	runner := projection.NewRunner("test", store, reg, pm)
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	loaded, err := pmRepo.Load(ctx, pmStream)
	if err != nil {
		t.Fatalf("Load PM after run: %v", err)
	}
	if loaded.Count != 2 || loaded.Version() != 2 {
		t.Errorf("after first events: Count=%d Version=%d, want 2, 2",
			loaded.Count, loaded.Version())
	}
}

func TestManager_CorrelateReturnsEmpty_SkipsEvent(t *testing.T) {
	// A correlator that returns "" must skip the event entirely: no
	// load, no save, no error. The PM stream stays empty.
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()

	seedSource(t, store, reg, 5)

	pmRepo := es.NewRepository(store, reg, testdomain.NewCounter)
	pm := process.New(pmRepo,
		func(_ es.Envelope) es.StreamID { return "" }, // skip everything
		incrementOnIncremented,
	)
	runner := projection.NewRunner("test", store, reg, pm)
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// PM stream must still not exist: handler never ran, nothing was
	// recorded, nothing was saved.
	if _, err := pmRepo.Load(ctx, pmStream); !errors.Is(err, es.ErrStreamNotFound) {
		t.Errorf("PM Load after skip-all: err = %v, want ErrStreamNotFound", err)
	}
}

func TestManager_HandlerError_Propagates(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()

	seedSource(t, store, reg, 1)

	sentinel := errors.New("handler refused")
	pmRepo := es.NewRepository(store, reg, testdomain.NewCounter)
	pm := process.New(pmRepo, alwaysPM,
		func(_ context.Context, _ es.Envelope, _ *testdomain.Counter) error {
			return sentinel
		},
	)
	runner := projection.NewRunner("test", store, reg, pm)
	err := runner.Run(ctx)
	if !errors.Is(err, sentinel) {
		t.Errorf("Run: err = %v, want wrap of sentinel", err)
	}
}

func TestManager_PartialCorrelation_RoutesOnlyMatching(t *testing.T) {
	// Mix of relevant and irrelevant events on the source stream; the
	// correlator routes only the relevant ones to the PM.
	ctx := t.Context()
	store := memory.New()
	reg := testdomain.NewRegistry()

	// 3 increments by 2, interleaved with 2 resets. Only the
	// increments should drive the PM.
	repo := es.NewRepository(store, reg, testdomain.NewCounter)
	c := testdomain.NewCounter(sourceStream)
	c.Increment(2)
	c.Reset()
	c.Increment(2)
	c.Reset()
	c.Increment(2)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("seed: %v", err)
	}

	pmRepo := es.NewRepository(store, reg, testdomain.NewCounter)
	pm := process.New(pmRepo,
		func(env es.Envelope) es.StreamID {
			if env.Type == "counter.incremented" {
				return pmStream
			}
			return ""
		},
		incrementOnIncremented,
	)
	runner := projection.NewRunner("test", store, reg, pm)
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	loaded, err := pmRepo.Load(ctx, pmStream)
	if err != nil {
		t.Fatalf("Load PM: %v", err)
	}
	if loaded.Count != 6 {
		t.Errorf("PM Count = %d, want 6 (three increments of 2)", loaded.Count)
	}
}
