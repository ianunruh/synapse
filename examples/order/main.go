// Command order is an end-to-end demo of a richer event-sourced
// aggregate. It builds on examples/counter — codec registry,
// repository, snapshots, command middleware — and adds:
//
//   - A multi-stage Status lifecycle (pending → placed → shipped →
//     delivered, with cancellation as a side branch).
//   - Command methods that validate preconditions before recording an
//     event, so invalid transitions never reach the event log.
//   - LineItem value objects with Money arithmetic.
//
// Two streams are exercised to show both branches of the lifecycle:
// one runs the happy path through delivery, one is canceled after a
// single line item is added.
//
// Run it with:
//
//	go run ./examples/order
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

func main() {
	ctx := context.Background()

	events := memory.New()
	repo := es.NewRepository(events, newRegistry(), NewOrder,
		es.WithSnapshotStore(snapmem.New()),
		es.WithSnapshotPolicy(es.EveryNVersions(5)))

	happyPath(ctx, repo)
	fmt.Println()
	canceledPath(ctx, repo)
	fmt.Println()
	showEventLog(ctx, events, "order/alice")
}

func happyPath(ctx context.Context, repo *es.Repository[*Order]) {
	stream := es.StreamID("order/alice")
	fmt.Println("== happy path:", stream)

	// 1. Create.
	o := NewOrder(stream)
	must(o.Create("customer/alice"), "Create")
	must(repo.Save(ctx, o), "Save")
	fmt.Printf("  created for %s (status=%s)\n", o.CustomerID, o.Status)

	// 2. Add items via Execute.
	items := []AddItemCmd{
		{ProductID: "widget-7", Quantity: 2, UnitPrice: 1995},
		{ProductID: "gadget-3", Quantity: 1, UnitPrice: 4999},
	}
	for _, cmd := range items {
		must(es.Execute(ctx, repo, stream, cmd, AddItemHandler), "AddItem")
		fmt.Printf("  added %d × %s @ %s\n", cmd.Quantity, cmd.ProductID, cmd.UnitPrice)
	}

	// 3. Attempting to ship a pending order is rejected by the
	//    aggregate; Execute surfaces the handler error and saves
	//    nothing.
	err := es.Execute(ctx, repo, stream,
		ShipCmd{Carrier: "UPS", Tracking: "1Z..."}, ShipHandler)
	fmt.Printf("  ship (pending): rejected — %v\n", err)

	// 4. Place.
	must(es.Execute(ctx, repo, stream, PlaceCmd{}, PlaceHandler), "Place")
	fmt.Println("  placed")

	// 5. Adding an item after placement is rejected.
	err = es.Execute(ctx, repo, stream,
		AddItemCmd{ProductID: "widget-8", Quantity: 1, UnitPrice: 999},
		AddItemHandler)
	fmt.Printf("  add (placed): rejected — %v\n", err)

	// 6. Ship.
	must(es.Execute(ctx, repo, stream,
		ShipCmd{Carrier: "UPS", Tracking: "1Z999AA10123456784"},
		ShipHandler), "Ship")
	fmt.Println("  shipped via UPS 1Z999AA10123456784")

	// 7. Deliver.
	must(es.Execute(ctx, repo, stream, DeliverCmd{}, DeliverHandler), "Deliver")
	fmt.Println("  delivered")

	// 8. Cancellation of a delivered order is rejected.
	err = es.Execute(ctx, repo, stream,
		CancelCmd{Reason: "changed mind"}, CancelHandler)
	fmt.Printf("  cancel (delivered): rejected — %v\n", err)

	// 9. Final state.
	final, err := repo.Load(ctx, stream)
	if err != nil {
		log.Fatalf("Load: %v", err)
	}
	fmt.Printf("  final: status=%s total=%s version=%d\n",
		final.Status, final.Total(), final.Version())
}

func canceledPath(ctx context.Context, repo *es.Repository[*Order]) {
	stream := es.StreamID("order/bob")
	fmt.Println("== canceled path:", stream)

	o := NewOrder(stream)
	must(o.Create("customer/bob"), "Create")
	must(repo.Save(ctx, o), "Save")
	fmt.Printf("  created for %s\n", o.CustomerID)

	must(es.Execute(ctx, repo, stream,
		AddItemCmd{ProductID: "widget-1", Quantity: 5, UnitPrice: 599},
		AddItemHandler), "AddItem")
	fmt.Println("  added 5 × widget-1 @ $5.99")

	must(es.Execute(ctx, repo, stream,
		CancelCmd{Reason: "buyer's remorse"}, CancelHandler), "Cancel")
	fmt.Println(`  canceled ("buyer's remorse")`)

	// Subsequent transitions all rejected.
	err := es.Execute(ctx, repo, stream, PlaceCmd{}, PlaceHandler)
	fmt.Printf("  place (canceled): rejected — %v\n", err)

	err = es.Execute(ctx, repo, stream,
		CancelCmd{Reason: "still don't want it"}, CancelHandler)
	fmt.Printf("  cancel (canceled): rejected — %v\n", err)

	final, err := repo.Load(ctx, stream)
	if err != nil {
		log.Fatalf("Load: %v", err)
	}
	fmt.Printf("  final: status=%s reason=%q version=%d\n",
		final.Status, final.CanceledReason, final.Version())
}

func showEventLog(ctx context.Context, events *memory.Store, streamStr string) {
	fmt.Println("== event log:", streamStr)
	for env, err := range events.Load(ctx, es.StreamID(streamStr), es.ReadOptions{}) {
		if err != nil {
			log.Fatalf("events.Load: %v", err)
		}
		fmt.Printf("  v%-2d %-26s %s\n", env.Version, env.Type, env.Payload)
	}
}

func must(err error, op string) {
	if err != nil {
		log.Fatalf("%s: %v", op, err)
	}
}
