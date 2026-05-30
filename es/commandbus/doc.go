// Package commandbus routes named, byte-encoded commands to the typed
// [es.Handler] registered for them, so HTTP and gRPC transports can
// dispatch commands without writing a per-route adapter by hand.
//
// The bus sits on top of [es.Execute]: registration captures the
// command and aggregate types at startup, and dispatch crosses the
// dynamic boundary exactly once — at the codec's Unmarshal — before
// calling [es.Execute] with statically typed arguments. Middleware
// configured on the repository (per-aggregate locking, retry, …) wraps
// each dispatched command identically to a direct [es.Execute] call;
// the bus adds no wrapping of its own. See ADR-0028.
//
// Commands implement the one-method [Command] interface so they can
// carry their own target stream id:
//
//	type PlaceOrder struct {
//	    OrderID string  `json:"order_id"`
//	    Items   []Item  `json:"items"`
//	}
//	func (c PlaceOrder) AggregateID() es.StreamID {
//	    return es.StreamID(c.OrderID)
//	}
//
// Wiring:
//
//	bus := commandbus.New()
//	commandbus.Register(bus, "order.place",
//	    orderRepo, PlaceHandler, jsoncodec.For[PlaceOrder]())
//
//	// In the transport handler:
//	if err := bus.Dispatch(ctx, name, body); err != nil {
//	    switch {
//	    case errors.Is(err, commandbus.ErrUnknownCommand):
//	        // 404 — no such command
//	    case errors.Is(err, commandbus.ErrDecode):
//	        // 400 — malformed body
//	    case errors.Is(err, es.ErrConflict):
//	        // 409 — optimistic concurrency
//	    default:
//	        // 5xx — handler failed
//	    }
//	}
//
// To propagate causation, correlation, and metadata into the events the
// command produces, wrap ctx with [es.WithCausation], [es.WithCorrelation],
// and [es.WithMetadata] before calling Dispatch. The Repository reads
// them off the context inside Save with no bus involvement.
package commandbus
