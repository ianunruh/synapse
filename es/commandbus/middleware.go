package commandbus

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"
)

// Logging returns a [Middleware] that records every dispatch on logger.
// Successful dispatches log at [slog.LevelDebug] with command (name)
// and duration attributes; failed dispatches log at [slog.LevelWarn]
// and include the err attribute. A nil logger falls back to
// [slog.Default].
//
// Debug is the default success level so production logs stay quiet by
// default — point a Debug-enabled handler at this if you want a full
// command audit log.
func Logging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next Operation) Operation {
		return func(ctx context.Context, name string, payload []byte) error {
			start := time.Now()
			err := next(ctx, name, payload)
			attrs := []slog.Attr{
				slog.String("command", name),
				slog.Duration("duration", time.Since(start)),
			}
			if err != nil {
				attrs = append(attrs, slog.Any("err", err))
				logger.LogAttrs(ctx, slog.LevelWarn, "synapse: command dispatch failed", attrs...)
				return err
			}
			logger.LogAttrs(ctx, slog.LevelDebug, "synapse: command dispatched", attrs...)
			return nil
		}
	}
}

// Recover returns a [Middleware] that recovers panics from any layer
// below it and returns them as *[PanicError] (wrapping [ErrPanic]).
// Compose it as an outer wrapper (e.g. earlier in [WithMiddleware]) so
// the panic is caught before any inner middleware sees it as a normal
// return.
func Recover() Middleware {
	return func(next Operation) Operation {
		return func(ctx context.Context, name string, payload []byte) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &PanicError{
						Name:  name,
						Value: r,
						Stack: debug.Stack(),
					}
				}
			}()
			return next(ctx, name, payload)
		}
	}
}

// Timeout returns a [Middleware] that wraps the dispatch context with
// [context.WithTimeout] for d. Useful when the transport does not
// already impose a deadline. d <= 0 disables the timeout.
func Timeout(d time.Duration) Middleware {
	return func(next Operation) Operation {
		if d <= 0 {
			return next
		}
		return func(ctx context.Context, name string, payload []byte) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, name, payload)
		}
	}
}
