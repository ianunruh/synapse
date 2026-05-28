package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/middleware"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

func TestRetry_RetriesOnConflict(t *testing.T) {
	var calls int
	inner := es.Operation(func(_ context.Context, stream es.StreamID) error {
		calls++
		if calls < 3 {
			return &es.ConflictError{Stream: stream}
		}
		return nil
	})

	op := middleware.Retry(middleware.RetryConfig{
		MaxAttempts: 5,
		Backoff:     func(int) time.Duration { return 0 },
	})(inner)

	if err := op(t.Context(), "s1"); err != nil {
		t.Errorf("op: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	var calls int
	inner := es.Operation(func(_ context.Context, stream es.StreamID) error {
		calls++
		return &es.ConflictError{Stream: stream}
	})

	op := middleware.Retry(middleware.RetryConfig{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})(inner)

	err := op(t.Context(), "s1")
	if !errors.Is(err, es.ErrConflict) {
		t.Errorf("err = %v, want wrap of ErrConflict", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3 (MaxAttempts)", calls)
	}
}

func TestRetry_NoRetryOnNonRetryable(t *testing.T) {
	boom := errors.New("permanent")
	var calls int
	inner := es.Operation(func(_ context.Context, _ es.StreamID) error {
		calls++
		return boom
	})

	op := middleware.Retry(middleware.RetryConfig{
		MaxAttempts: 5,
		Backoff:     func(int) time.Duration { return 0 },
	})(inner)

	err := op(t.Context(), "s1")
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on non-retryable)", calls)
	}
}

func TestRetry_RespectsContext(t *testing.T) {
	var calls atomic.Int32
	inner := es.Operation(func(_ context.Context, stream es.StreamID) error {
		calls.Add(1)
		return &es.ConflictError{Stream: stream}
	})

	ctx, cancel := context.WithCancel(t.Context())
	op := middleware.Retry(middleware.RetryConfig{
		MaxAttempts: 10,
		Backoff:     func(int) time.Duration { return 50 * time.Millisecond },
	})(inner)

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := op(ctx, "s1")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got > 2 {
		t.Errorf("calls = %d, want <= 2 (canceled before more attempts)", got)
	}
}

func TestRetry_CustomRetryable(t *testing.T) {
	transient := errors.New("transient")
	var calls int
	inner := es.Operation(func(_ context.Context, _ es.StreamID) error {
		calls++
		if calls < 3 {
			return transient
		}
		return nil
	})

	op := middleware.Retry(middleware.RetryConfig{
		MaxAttempts: 5,
		Backoff:     func(int) time.Duration { return 0 },
		Retryable:   func(err error) bool { return errors.Is(err, transient) },
	})(inner)

	if err := op(t.Context(), "s1"); err != nil {
		t.Errorf("op: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetry_ZeroMaxAttemptsUsesDefault(t *testing.T) {
	var calls int
	inner := es.Operation(func(_ context.Context, stream es.StreamID) error {
		calls++
		return &es.ConflictError{Stream: stream}
	})

	op := middleware.Retry(middleware.RetryConfig{
		Backoff: func(int) time.Duration { return 0 },
	})(inner)
	_ = op(t.Context(), "s1")
	if calls != 3 {
		t.Errorf("calls = %d, want 3 (default MaxAttempts)", calls)
	}
}

func TestIsTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ConflictError", &es.ConflictError{}, true},
		{"ErrConflict", es.ErrConflict, true},
		{"wrapped ConflictError", fmt.Errorf("save: %w", &es.ConflictError{}), true},
		{"random error", errors.New("random"), false},
		{"context.Canceled", context.Canceled, false},
		{"StreamNotFound", &es.StreamNotFoundError{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := middleware.IsTransient(tc.err); got != tc.want {
				t.Errorf("IsTransient(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestExecute_RetryRecoversFromConflict(t *testing.T) {
	// First handler call sneaks a rival save in, forcing the
	// Repository's first Save to conflict. Retry reloads and re-runs;
	// the second handler call succeeds.
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithMiddleware(middleware.Retry(middleware.RetryConfig{
			MaxAttempts: 3,
			Backoff:     func(int) time.Duration { return 0 },
		})))

	seed := testdomain.NewCounter(testdomain.CounterStream)
	if err := seed.Increment(0); err != nil {
		t.Fatalf("seed Increment: %v", err)
	}
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	var attempts atomic.Int32
	conflictingHandler := func(ctx context.Context, _ testdomain.IncrementCmd, c *testdomain.Counter) error {
		if attempts.Add(1) == 1 {
			rival := testdomain.NewCounter(testdomain.CounterStream)
			rival.SetVersion(c.Version())
			if err := rival.Increment(7); err != nil {
				return err
			}
			if err := repo.Save(ctx, rival); err != nil {
				return err
			}
		}
		return c.Increment(1)
	}

	if err := es.Execute(ctx, repo, testdomain.CounterStream,
		testdomain.IncrementCmd{By: 1}, conflictingHandler); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2 (one conflict, one success)", got)
	}

	final, err := repo.Load(ctx, testdomain.CounterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if final.Count != 8 {
		t.Errorf("count = %d, want 8 (rival +7, retry +1)", final.Count)
	}
}
