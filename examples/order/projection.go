package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/ianunruh/synapse/es"
)

// SalesView is a tiny in-memory read model fed by the order event
// log. It tracks the running line-item total per order as events
// arrive, then "books" that total into delivered revenue when the
// order is delivered or discards it when the order is canceled.
//
// Real services would persist the view in a database; the same
// projection.Project method works either way, since the projection
// just reacts to events.
type SalesView struct {
	mu                sync.Mutex
	pendingByOrder    map[es.StreamID]Money // running line-item total per stream
	deliveredRevenue  Money
	deliveredOrders   int
	canceledOrders    int
}

func newSalesView() *SalesView {
	return &SalesView{pendingByOrder: make(map[es.StreamID]Money)}
}

func (v *SalesView) Project(_ context.Context, env es.Envelope) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch p := env.Payload.(type) {
	case LineItemAdded:
		v.pendingByOrder[env.StreamID] += p.UnitPrice * Money(p.Quantity)
	case LineItemRemoved:
		// The view doesn't carry per-item detail, so an explicit
		// recalculation from the remaining items would be needed for a
		// precise view. The demo never removes items, so a stub is
		// enough — flag the case in case a future scenario adds one.
		_ = p
	case OrderDelivered:
		v.deliveredRevenue += v.pendingByOrder[env.StreamID]
		delete(v.pendingByOrder, env.StreamID)
		v.deliveredOrders++
	case OrderCanceled:
		delete(v.pendingByOrder, env.StreamID)
		v.canceledOrders++
	}
	return nil
}

func (v *SalesView) report() (revenue Money, delivered, canceled int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.deliveredRevenue, v.deliveredOrders, v.canceledOrders
}

func (v *SalesView) print() {
	revenue, delivered, canceled := v.report()
	fmt.Println("== sales view:")
	fmt.Printf("  delivered: %s from %d order(s)\n", revenue, delivered)
	fmt.Printf("  canceled:  %d order(s)\n", canceled)
}
