package main

import (
	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
)

// Event payloads. Each is the serialized form of a state transition;
// the aggregate's Apply method is the only place that consumes them.
// Fields are tagged for the JSON codec, which is what newRegistry
// wires up below.

type OrderCreated struct {
	CustomerID string `json:"customer_id"`
}

type LineItemAdded struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	UnitPrice Money  `json:"unit_price"`
}

type LineItemRemoved struct {
	ProductID string `json:"product_id"`
}

type OrderPlaced struct{}

type OrderShipped struct {
	Carrier  string `json:"carrier"`
	Tracking string `json:"tracking"`
}

type OrderDelivered struct{}

type OrderCanceled struct {
	Reason string `json:"reason"`
}

// OrderSnapshot is the persisted state of an Order at a point in
// time. The version suffix in the registered type name leaves room
// for a future v2 schema without breaking existing snapshots.
type OrderSnapshot struct {
	CustomerID     string     `json:"customer_id"`
	Status         Status     `json:"status"`
	Items          []LineItem `json:"items"`
	Shipment       *Shipment  `json:"shipment,omitempty"`
	CanceledReason string     `json:"canceled_reason,omitempty"`
}

// newRegistry wires every event payload and the snapshot type to the
// JSON codec. A real service would build this once at startup.
func newRegistry() *es.Registry {
	reg := es.NewRegistry()
	es.Register(reg, "order.created", jsoncodec.For[OrderCreated]())
	es.Register(reg, "order.line_item_added", jsoncodec.For[LineItemAdded]())
	es.Register(reg, "order.line_item_removed", jsoncodec.For[LineItemRemoved]())
	es.Register(reg, "order.placed", jsoncodec.For[OrderPlaced]())
	es.Register(reg, "order.shipped", jsoncodec.For[OrderShipped]())
	es.Register(reg, "order.delivered", jsoncodec.For[OrderDelivered]())
	es.Register(reg, "order.canceled", jsoncodec.For[OrderCanceled]())
	es.Register(reg, "order.snapshot.v1", jsoncodec.For[OrderSnapshot]())
	return reg
}
