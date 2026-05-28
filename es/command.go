package es

import "context"

// Handler is a typed command handler. It receives a freshly loaded
// aggregate and is expected to call domain methods on it that
// internally enqueue events through (*AggregateBase).Record. The
// [Repository] takes care of persistence once the handler returns.
//
//	func PlaceOrder(ctx context.Context, cmd PlaceOrderCmd, o *Order) error {
//	    return o.Place(cmd.Items, cmd.Total)
//	}
type Handler[C any, A Aggregate] func(ctx context.Context, cmd C, agg A) error

// Execute is a convenience that loads an aggregate, runs the command
// handler, and saves any events the handler recorded.
//
// If the stream does not yet exist, Execute returns
// *[StreamNotFoundError]. For "load or create" semantics, construct an
// aggregate directly via the Repository's newFn and call Save.
//
// If the handler returns a non-nil error, Execute propagates it
// without attempting to save.
func Execute[C any, A Aggregate](
	ctx context.Context,
	r *Repository[A],
	id StreamID,
	cmd C,
	h Handler[C, A],
) error {
	agg, err := r.Load(ctx, id)
	if err != nil {
		return err
	}
	if err := h(ctx, cmd, agg); err != nil {
		return err
	}
	return r.Save(ctx, agg)
}
