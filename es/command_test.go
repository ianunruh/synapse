package es_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

func TestExecute_MiddlewareCompositionOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
	}
	tag := func(name string) es.Middleware {
		return func(next es.Operation) es.Operation {
			return func(ctx context.Context, stream es.StreamID) error {
				record(name + ":before")
				err := next(ctx, stream)
				record(name + ":after")
				return err
			}
		}
	}

	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithMiddleware(tag("a"), tag("b"), tag("c")))

	seed := testdomain.NewCounter(testdomain.CounterStream)
	seed.Increment(0)
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	if err := es.Execute(ctx, repo, testdomain.CounterStream,
		testdomain.IncrementCmd{By: 1}, testdomain.IncrementHandler); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	want := []string{"a:before", "b:before", "c:before", "c:after", "b:after", "a:after"}
	if !slices.Equal(order, want) {
		t.Errorf("order = %v\nwant = %v", order, want)
	}
}
