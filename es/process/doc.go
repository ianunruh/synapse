// Package process provides a thin wrapper for the process-manager
// pattern: an aggregate that consumes events from one or more streams
// and emits commands to drive a multi-step workflow.
//
// Process managers are the orchestration shape of a saga — a central
// PM aggregate observes events, tracks its own state, and dispatches
// the next command. (The other shape is choreography, where each
// service reacts to events independently. Choreography needs no new
// primitives; process.Manager exists for orchestration.) See ADR-0032
// for the full reasoning.
//
// The wiring is:
//
//	pm := process.New(transferRepo, correlateByTransferID, transferStep)
//	runner := projection.NewRunner("transfer-process", store, reg, pm,
//	    projection.WithLive(true),
//	    projection.WithTypes(
//	        "transfer.requested",
//	        "account.debited",
//	        "account.credited",
//	    ),
//	    projection.WithCheckpoint(checkpoints),
//	)
//	go runner.Run(ctx)
//
// transferStep is an [es.Handler] over the inbound [es.Envelope] and
// the loaded process-manager aggregate; inside, the user records
// events on the PM via domain methods and freely calls [es.Execute]
// on other repositories (or [commandbus.Bus.Dispatch]) to emit
// commands. Causation, correlation, and metadata propagate from the
// inbound event through the [projection.Runner]'s context enrichment
// (see ADR-0022), so the saga chain is stamped on every outbound
// event automatically.
//
// What this package does not provide:
//
//   - A scheduler for timeouts. Deadlines are out of scope; the
//     handler can implement them by emitting a "wake me at T"
//     command into a scheduling aggregate or external system.
//   - A compensation engine. Compensations are domain logic — the PM
//     decides to emit an UndoX command exactly the same way it emits
//     any other command.
//   - Distributed coordination. Multiple [projection.Runner] instances
//     with the same name will race on checkpoint saves; the underlying
//     gap is documented in ADR-0014 and applies to every projection,
//     not just process managers.
package process
