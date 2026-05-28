package es

import (
	"context"
	"errors"
)

// Handler is a typed command handler. It receives a freshly loaded
// aggregate and is expected to call domain methods on it that
// internally enqueue events through (*AggregateBase).Record. The [Repository]
// takes care of persistence once the handler returns.
//
//	func PlaceOrder(ctx context.Context, cmd PlaceOrderCmd, o *Order) error {
//	    return o.Place(cmd.Items, cmd.Total)
//	}
type Handler[C any, A Aggregate] func(ctx context.Context, cmd C, agg A) error

// Execute is a convenience that loads an aggregate, runs the command
// handler, and saves any events the handler recorded.
//
// Implementation deferred to a follow-up milestone.
func Execute[C any, A Aggregate](
	ctx context.Context,
	r *Repository[A],
	id StreamID,
	cmd C,
	h Handler[C, A],
) error {
	_ = ctx
	_ = r
	_ = id
	_ = cmd
	_ = h
	return errors.ErrUnsupported
}
