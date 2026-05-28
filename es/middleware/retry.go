package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/ianunruh/synapse/es"
)

// RetryConfig configures a [Retry] middleware. The zero value is not
// useful — at minimum MaxAttempts should be set.
type RetryConfig struct {
	// MaxAttempts is the maximum number of times to invoke the
	// underlying operation, inclusive of the first attempt. Values
	// <= 0 default to 3.
	MaxAttempts int

	// Backoff returns the delay to wait before attempt+1. attempt is
	// 0-indexed (0 = the delay before the second attempt). Nil
	// defaults to exponential backoff starting at 50ms, doubling
	// each attempt, capped at attempt index 20 to prevent overflow.
	Backoff func(attempt int) time.Duration

	// Retryable reports whether err should trigger a retry. Nil
	// defaults to [IsTransient], which treats wraps of
	// [es.ErrConflict] as retryable.
	Retryable func(err error) bool
}

// Retry returns an [es.Middleware] that re-invokes its underlying
// [es.Operation] when it returns an error satisfying cfg.Retryable.
// It waits cfg.Backoff(attempt) between attempts and stops once
// cfg.MaxAttempts is reached or the context is canceled.
//
// Retry is typically chained outside [PerAggregateLocking] when both
// are used: locking eliminates same-process contention, retry covers
// conflicts from other processes.
func Retry(cfg RetryConfig) es.Middleware {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Backoff == nil {
		cfg.Backoff = defaultBackoff
	}
	if cfg.Retryable == nil {
		cfg.Retryable = IsTransient
	}

	return func(next es.Operation) es.Operation {
		return func(ctx context.Context, stream es.StreamID) error {
			var err error
			for attempt := range cfg.MaxAttempts {
				err = next(ctx, stream)
				if err == nil {
					return nil
				}
				if !cfg.Retryable(err) {
					return err
				}
				if attempt+1 >= cfg.MaxAttempts {
					return err
				}
				wait := cfg.Backoff(attempt)
				if wait <= 0 {
					continue
				}
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			return err
		}
	}
}

// defaultBackoff is the exponential backoff used when [RetryConfig]
// supplies no Backoff function. Starts at 50ms and doubles each
// attempt, capped at attempt index 20 to avoid duration overflow.
func defaultBackoff(attempt int) time.Duration {
	attempt = min(attempt, 20)
	return 50 * time.Millisecond << attempt
}

// IsTransient reports whether err is a transient failure suitable for
// retry. Currently it returns true exactly when err wraps
// [es.ErrConflict].
func IsTransient(err error) bool {
	return errors.Is(err, es.ErrConflict)
}
