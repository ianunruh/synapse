package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/middleware"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

func TestPerAggregateLocking_SerializesSameStream(t *testing.T) {
	mw := middleware.PerAggregateLocking()

	var active, peak atomic.Int32
	inner := es.Operation(func(_ context.Context, _ es.StreamID) error {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	op := mw(inner)

	const N = 32
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() { _ = op(t.Context(), "shared") })
	}
	wg.Wait()

	if got := peak.Load(); got != 1 {
		t.Errorf("peak concurrent = %d, want 1 (per-stream serialization)", got)
	}
}

func TestPerAggregateLocking_AllowsParallelDifferentStreams(t *testing.T) {
	mw := middleware.PerAggregateLocking()

	var active, peak atomic.Int32
	inner := es.Operation(func(_ context.Context, _ es.StreamID) error {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	op := mw(inner)

	const N = 8
	var wg sync.WaitGroup
	for i := range N {
		wg.Go(func() {
			_ = op(t.Context(), es.StreamID(fmt.Sprintf("stream-%d", i)))
		})
	}
	wg.Wait()

	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrent = %d, want >= 2 (cross-stream parallelism)", got)
	}
}

func TestPerAggregateLocking_RespectsContext(t *testing.T) {
	mw := middleware.PerAggregateLocking()

	release := make(chan struct{})
	inner := es.Operation(func(_ context.Context, _ es.StreamID) error {
		<-release
		return nil
	})
	op := mw(inner)

	holderStarted := make(chan struct{})
	go func() {
		close(holderStarted)
		_ = op(t.Context(), "s1")
	}()
	<-holderStarted
	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- op(ctx, "s1") }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("waiter did not respect context cancellation")
	}

	close(release)
}

func TestExecute_PerAggregateLocking_PreventsConflicts(t *testing.T) {
	// Without locking, parallel Executes on the same stream produce
	// ConflictErrors. With locking, all succeed serially.
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithMiddleware(middleware.PerAggregateLocking()))

	seed := testdomain.NewCounter(testdomain.CounterStream)
	seed.Increment(0)
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	const N = 32
	errs := make(chan error, N)
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			errs <- es.Execute(ctx, repo, testdomain.CounterStream,
				testdomain.IncrementCmd{By: 1}, testdomain.IncrementHandler)
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Execute: %v", err)
		}
	}

	final, err := repo.Load(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if final.Count != N {
		t.Errorf("count = %d, want %d (all N executes serialized)", final.Count, N)
	}
}
