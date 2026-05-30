package es

import (
	"context"
	"errors"
	"slices"
)

// Handler is a typed command handler. It receives a freshly loaded
// aggregate and is expected to call domain methods on it that
// internally enqueue events through (*AggregateBase).Record. The
// [Repository] takes care of persistence once the handler returns.
//
//	func PlaceOrder(ctx context.Context, cmd PlaceOrderCmd, o *Order) error {
//	    return o.Place(cmd.Items, cmd.Total)
//	}
type Handler[C any, A Aggregate] func(ctx context.Context, cmd C, agg A) error

// Operation is the type-erased form of an [Execute] call. It captures
// the load-handle-save pipeline as a single function that runs
// against a stream.
//
// Operations are produced by [Execute] internally and consumed by
// [Middleware]. User code rarely constructs Operation values
// directly; it provides Middleware that wrap them.
type Operation func(ctx context.Context, stream StreamID) error

// Middleware wraps an [Operation] to add behavior before, after, or
// around the underlying load-handle-save pipeline. The returned
// Operation must call next inside its body to execute the command.
//
// Middleware compose left-to-right: WithMiddleware(a, b, c) produces
// a chain where a wraps b wraps c wraps the underlying operation, so
// a's "before" code runs first and a's "after" code runs last.
//
// Concrete middlewares (per-aggregate locking, retry on transient
// errors, etc.) live in the github.com/ianunruh/synapse/es/middleware
// subpackage.
type Middleware func(next Operation) Operation

// chain composes middlewares into a single [Operation]. The leftmost
// middleware becomes the outermost wrapper.
func chain(mws []Middleware, op Operation) Operation {
	for _, mw := range slices.Backward(mws) {
		op = mw(op)
	}
	return op
}

// Execute is a convenience that loads an aggregate, runs the command
// handler, and saves any events the handler recorded. The
// load-handle-save pipeline is wrapped by the Repository's middleware
// chain (see [WithMiddleware]), so cross-cutting concerns such as
// locking and retries apply uniformly across all command types.
//
// When [Repository.Load] returns an error wrapping [ErrStreamNotFound],
// Execute treats it as "fresh aggregate": it builds one via the
// Repository's newFn (the same constructor passed to [NewRepository])
// and runs the handler against it. Save then appends with expected
// revision [NoStream], which is the natural shape for a create-style
// command. Handlers that require an existing aggregate must guard on
// agg.Version() == 0 and return an error. See ADR-0030.
//
// Any other Load error propagates unchanged. If the handler returns a
// non-nil error, Execute propagates it without attempting to save.
func Execute[C any, A Aggregate](
	ctx context.Context,
	r *Repository[A],
	id StreamID,
	cmd C,
	h Handler[C, A],
) error {
	op := func(ctx context.Context, stream StreamID) error {
		agg, err := r.Load(ctx, stream)
		if err != nil {
			if !errors.Is(err, ErrStreamNotFound) {
				return err
			}
			agg = r.newFn(stream)
		}
		if err := h(ctx, cmd, agg); err != nil {
			return err
		}
		return r.Save(ctx, agg)
	}
	return chain(r.middleware, op)(ctx, id)
}
