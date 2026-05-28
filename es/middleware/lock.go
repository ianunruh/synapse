// Package middleware provides built-in [es.Middleware] implementations
// for common cross-cutting concerns around command execution:
// per-aggregate locking, retry on transient errors, and so on.
//
// Concrete middlewares from this package are passed through
// [es.WithMiddleware] at Repository construction time:
//
//	import (
//	    "github.com/ianunruh/synapse/es"
//	    esmw "github.com/ianunruh/synapse/es/middleware"
//	)
//
//	repo := es.NewRepository(store, reg, NewOrder,
//	    es.WithMiddleware(
//	        esmw.PerAggregateLocking(),
//	        esmw.Retry(esmw.RetryConfig{MaxAttempts: 5}),
//	    ))
package middleware

import (
	"context"
	"sync"

	"github.com/ianunruh/synapse/es"
)

// PerAggregateLocking returns an [es.Middleware] that serializes
// [es.Operation] calls per stream id. Concurrent [es.Execute] calls
// against different streams proceed in parallel; concurrent calls
// against the same stream queue up behind whichever caller arrived
// first.
//
// Locking eliminates the most common source of optimistic-concurrency
// conflicts (two in-process operations against the same stream) but
// does not protect against conflicts from other Repository instances
// or other processes — for those, combine with [Retry].
//
// The lock respects context cancellation: waiters whose context is
// canceled return ctx.Err() without ever holding the lock.
//
// The returned middleware retains one channel per stream that has
// ever been seen; entries are not garbage-collected. Long-running
// processes that see unbounded distinct stream ids should build a
// custom middleware with explicit cleanup.
func PerAggregateLocking() es.Middleware {
	var (
		mu    sync.Mutex
		locks = make(map[es.StreamID]chan struct{})
	)
	lockFor := func(stream es.StreamID) chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		ch, ok := locks[stream]
		if !ok {
			ch = make(chan struct{}, 1)
			locks[stream] = ch
		}
		return ch
	}

	return func(next es.Operation) es.Operation {
		return func(ctx context.Context, stream es.StreamID) error {
			ch := lockFor(stream)
			select {
			case ch <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-ch }()
			return next(ctx, stream)
		}
	}
}
