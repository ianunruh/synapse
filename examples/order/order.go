package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/ianunruh/synapse/es"
)

// Money is a non-negative integer count of the smallest currency unit
// (cents, for USD). Integer math avoids float drift; presentation
// rounding happens at the UI boundary.
type Money int64

func (m Money) String() string {
	if m < 0 {
		return "-" + (-m).String()
	}
	return fmt.Sprintf("$%d.%02d", m/100, m%100)
}

type LineItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	UnitPrice Money  `json:"unit_price"`
}

func (l LineItem) Total() Money { return l.UnitPrice * Money(l.Quantity) }

type Shipment struct {
	Carrier  string `json:"carrier"`
	Tracking string `json:"tracking"`
}

// Status is the Order's lifecycle position. Every command method
// inspects the current Status before staging a transition event, so
// invalid transitions never reach the event log.
type Status string

const (
	StatusPending   Status = "pending"
	StatusPlaced    Status = "placed"
	StatusShipped   Status = "shipped"
	StatusDelivered Status = "delivered"
	StatusCanceled  Status = "canceled"
)

// Order is the aggregate. State mutates only through Apply; the
// command methods stage events through (*es.AggregateBase).Record
// after they've validated the preconditions for the transition.
type Order struct {
	*es.AggregateBase
	CustomerID     string
	Status         Status
	Items          []LineItem
	Shipment       *Shipment
	CanceledReason string
}

func NewOrder(id es.StreamID) *Order {
	return &Order{AggregateBase: es.NewAggregateBase(id)}
}

func (o *Order) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case OrderCreated:
		o.CustomerID = p.CustomerID
		o.Status = StatusPending
	case LineItemAdded:
		o.Items = append(o.Items, LineItem{
			ProductID: p.ProductID, Quantity: p.Quantity, UnitPrice: p.UnitPrice,
		})
	case LineItemRemoved:
		o.Items = slices.DeleteFunc(o.Items, func(i LineItem) bool {
			return i.ProductID == p.ProductID
		})
	case OrderPlaced:
		o.Status = StatusPlaced
	case OrderShipped:
		o.Status = StatusShipped
		o.Shipment = &Shipment{Carrier: p.Carrier, Tracking: p.Tracking}
	case OrderDelivered:
		o.Status = StatusDelivered
	case OrderCanceled:
		o.Status = StatusCanceled
		o.CanceledReason = p.Reason
	}
}

// Total sums LineItem totals. It is a pure read of in-memory state
// and records no event.
func (o *Order) Total() Money {
	var total Money
	for _, i := range o.Items {
		total += i.Total()
	}
	return total
}

// --- Command methods ------------------------------------------------
//
// Validation lives in the command method, before Record is called.
// Apply takes no error return because every event in the log is a
// fact that already happened — refusing to apply it during rehydration
// cannot unmake the past.

func (o *Order) Create(customerID string) error {
	if o.Version() != 0 {
		return fmt.Errorf("order %q already created", o.StreamID())
	}
	if customerID == "" {
		return fmt.Errorf("customer id required")
	}
	o.Record("order.created", OrderCreated{CustomerID: customerID}, o.Apply)
	return nil
}

func (o *Order) AddItem(productID string, quantity int, unitPrice Money) error {
	if o.Status != StatusPending {
		return fmt.Errorf("cannot add items to %s order", o.Status)
	}
	if productID == "" {
		return fmt.Errorf("product id required")
	}
	if quantity <= 0 {
		return fmt.Errorf("quantity must be positive")
	}
	if unitPrice <= 0 {
		return fmt.Errorf("unit price must be positive")
	}
	o.Record("order.line_item_added",
		LineItemAdded{ProductID: productID, Quantity: quantity, UnitPrice: unitPrice},
		o.Apply)
	return nil
}

func (o *Order) RemoveItem(productID string) error {
	if o.Status != StatusPending {
		return fmt.Errorf("cannot modify %s order", o.Status)
	}
	if !slices.ContainsFunc(o.Items, func(i LineItem) bool {
		return i.ProductID == productID
	}) {
		return fmt.Errorf("item %q not in order", productID)
	}
	o.Record("order.line_item_removed", LineItemRemoved{ProductID: productID}, o.Apply)
	return nil
}

func (o *Order) Place() error {
	if o.Status != StatusPending {
		return fmt.Errorf("cannot place %s order", o.Status)
	}
	if len(o.Items) == 0 {
		return fmt.Errorf("cannot place empty order")
	}
	o.Record("order.placed", OrderPlaced{}, o.Apply)
	return nil
}

func (o *Order) Ship(carrier, tracking string) error {
	if o.Status != StatusPlaced {
		return fmt.Errorf("cannot ship %s order", o.Status)
	}
	if carrier == "" || tracking == "" {
		return fmt.Errorf("carrier and tracking required")
	}
	o.Record("order.shipped",
		OrderShipped{Carrier: carrier, Tracking: tracking},
		o.Apply)
	return nil
}

func (o *Order) Deliver() error {
	if o.Status != StatusShipped {
		return fmt.Errorf("cannot deliver %s order", o.Status)
	}
	o.Record("order.delivered", OrderDelivered{}, o.Apply)
	return nil
}

func (o *Order) Cancel(reason string) error {
	switch o.Status {
	case StatusDelivered:
		return fmt.Errorf("cannot cancel delivered order")
	case StatusCanceled:
		return fmt.Errorf("order already canceled")
	}
	if reason == "" {
		return fmt.Errorf("cancellation reason required")
	}
	o.Record("order.canceled", OrderCanceled{Reason: reason}, o.Apply)
	return nil
}

// --- Snapshotter ----------------------------------------------------

func (o *Order) SnapshotType() string { return "order.snapshot.v1" }

func (o *Order) Snapshot() (any, error) {
	return OrderSnapshot{
		CustomerID:     o.CustomerID,
		Status:         o.Status,
		Items:          slices.Clone(o.Items),
		Shipment:       o.Shipment,
		CanceledReason: o.CanceledReason,
	}, nil
}

func (o *Order) Restore(state any) error {
	s, ok := state.(OrderSnapshot)
	if !ok {
		return fmt.Errorf("invalid snapshot type %T", state)
	}
	o.CustomerID = s.CustomerID
	o.Status = s.Status
	o.Items = slices.Clone(s.Items)
	o.Shipment = s.Shipment
	o.CanceledReason = s.CanceledReason
	return nil
}

// --- Command structs + handlers -------------------------------------
//
// Typed commands give Execute a static signature. Each handler is a
// one-liner that forwards to the matching aggregate method; handlers
// exist so middleware (locking, retry, metrics) can wrap the
// load-handle-save pipeline without knowing about the aggregate.

type AddItemCmd struct {
	ProductID string
	Quantity  int
	UnitPrice Money
}

func AddItemHandler(_ context.Context, cmd AddItemCmd, o *Order) error {
	return o.AddItem(cmd.ProductID, cmd.Quantity, cmd.UnitPrice)
}

type PlaceCmd struct{}

func PlaceHandler(_ context.Context, _ PlaceCmd, o *Order) error {
	return o.Place()
}

type ShipCmd struct {
	Carrier  string
	Tracking string
}

func ShipHandler(_ context.Context, cmd ShipCmd, o *Order) error {
	return o.Ship(cmd.Carrier, cmd.Tracking)
}

type DeliverCmd struct{}

func DeliverHandler(_ context.Context, _ DeliverCmd, o *Order) error {
	return o.Deliver()
}

type CancelCmd struct {
	Reason string
}

func CancelHandler(_ context.Context, cmd CancelCmd, o *Order) error {
	return o.Cancel(cmd.Reason)
}
