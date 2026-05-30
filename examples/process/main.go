// Command process is an end-to-end demo of the process-manager
// pattern: a Transfer aggregate that coordinates a multi-step
// workflow across two Account aggregates.
//
// Flow:
//
//	RequestTransfer (user)
//	    → Transfer aggregate records TransferRequested
//	        → process.Manager sees TransferRequested
//	            → debits source Account
//	                → AccountDebited (success) or AccountDebitRejected (insufficient funds)
//	                    → process.Manager sees the result
//	                        success → credits destination Account → Transfer.Completed
//	                        rejected → Transfer.Failed (no further action; compensation is a comment below)
//
// Every command emitted by the PM goes through es.Execute, so the
// PM's Repository middleware (PerAggregateLocking) applies; the
// causation/correlation context propagated by projection.Runner
// stamps the inbound event's id onto every outbound event,
// preserving the saga chain.
//
// Run:
//
//	go run ./examples/process
//
// The program runs a happy-path transfer (Alice → Bob, $25) followed
// by a failing one (Bob → Alice, $1000 against a zero balance),
// prints the final state of every aggregate, and exits.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"time"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	esmw "github.com/ianunruh/synapse/es/middleware"
	"github.com/ianunruh/synapse/es/process"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
)

// ----------------------------------------------------------------------------
// Domain — Account aggregate.
// ----------------------------------------------------------------------------

type Account struct {
	*es.AggregateBase
	Balance int
	Open    bool
}

func NewAccount(id es.StreamID) *Account {
	return &Account{AggregateBase: es.NewAggregateBase(id)}
}

type AccountOpened struct {
	InitialBalance int `json:"initial_balance"`
}

type AccountDebited struct {
	TransferID string `json:"transfer_id"`
	Amount     int    `json:"amount"`
}

type AccountCredited struct {
	TransferID string `json:"transfer_id"`
	Amount     int    `json:"amount"`
}

type AccountDebitRejected struct {
	TransferID string `json:"transfer_id"`
	Amount     int    `json:"amount"`
	Reason     string `json:"reason"`
}

func (a *Account) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case AccountOpened:
		a.Open = true
		a.Balance = p.InitialBalance
	case AccountDebited:
		a.Balance -= p.Amount
	case AccountCredited:
		a.Balance += p.Amount
	case AccountDebitRejected:
		// No balance change; the rejection is recorded for the PM to react to.
	}
}

type OpenAccountCmd struct {
	InitialBalance int
}

type DebitCmd struct {
	TransferID string
	Amount     int
}

type CreditCmd struct {
	TransferID string
	Amount     int
}

func openHandler(_ context.Context, cmd OpenAccountCmd, a *Account) error {
	if a.Open {
		return errors.New("account already open")
	}
	a.Record("account.opened", AccountOpened{InitialBalance: cmd.InitialBalance}, a.Apply)
	return nil
}

func debitHandler(_ context.Context, cmd DebitCmd, a *Account) error {
	if !a.Open {
		return errors.New("account not open")
	}
	if a.Balance < cmd.Amount {
		a.Record("account.debit_rejected", AccountDebitRejected{
			TransferID: cmd.TransferID,
			Amount:     cmd.Amount,
			Reason:     "insufficient funds",
		}, a.Apply)
		return nil
	}
	a.Record("account.debited", AccountDebited{
		TransferID: cmd.TransferID,
		Amount:     cmd.Amount,
	}, a.Apply)
	return nil
}

func creditHandler(_ context.Context, cmd CreditCmd, a *Account) error {
	if !a.Open {
		return errors.New("account not open")
	}
	a.Record("account.credited", AccountCredited{
		TransferID: cmd.TransferID,
		Amount:     cmd.Amount,
	}, a.Apply)
	return nil
}

// ----------------------------------------------------------------------------
// Domain — Transfer aggregate (the process-managed one).
// ----------------------------------------------------------------------------

type TransferStatus string

const (
	TransferPending   TransferStatus = "pending"
	TransferDebited   TransferStatus = "debited"
	TransferCompleted TransferStatus = "completed"
	TransferFailed    TransferStatus = "failed"
)

type Transfer struct {
	*es.AggregateBase
	From, To string
	Amount   int
	Status   TransferStatus
	Reason   string
}

func NewTransfer(id es.StreamID) *Transfer {
	return &Transfer{AggregateBase: es.NewAggregateBase(id)}
}

type TransferRequested struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int    `json:"amount"`
}

type TransferDebitedEvt struct{}

type TransferCompletedEvt struct{}

type TransferFailedEvt struct {
	Reason string `json:"reason"`
}

func (t *Transfer) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case TransferRequested:
		t.From, t.To, t.Amount = p.From, p.To, p.Amount
		t.Status = TransferPending
	case TransferDebitedEvt:
		t.Status = TransferDebited
	case TransferCompletedEvt:
		t.Status = TransferCompleted
	case TransferFailedEvt:
		t.Status = TransferFailed
		t.Reason = p.Reason
	}
}

type RequestTransferCmd struct {
	From, To string
	Amount   int
}

func requestHandler(_ context.Context, cmd RequestTransferCmd, t *Transfer) error {
	if t.Status != "" {
		return errors.New("transfer already requested")
	}
	if cmd.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	t.Record("transfer.requested", TransferRequested{
		From: cmd.From, To: cmd.To, Amount: cmd.Amount,
	}, t.Apply)
	return nil
}

// ----------------------------------------------------------------------------
// Process manager wiring.
// ----------------------------------------------------------------------------

// correlate maps every inbound event to the Transfer aggregate that
// should process it. TransferRequested arrives on a Transfer stream
// already; AccountDebited/Credited/DebitRejected carry the transfer
// id in their payload.
func correlate(env es.Envelope) es.StreamID {
	switch p := env.Payload.(type) {
	case TransferRequested:
		return env.StreamID
	case AccountDebited:
		return es.StreamID(p.TransferID)
	case AccountCredited:
		return es.StreamID(p.TransferID)
	case AccountDebitRejected:
		return es.StreamID(p.TransferID)
	}
	return ""
}

// transferStep is the per-event business logic of the process
// manager. The Transfer aggregate (t) has already had env applied; we
// decide what to do next: emit a command, or just record the outcome
// on the PM by calling a domain method that records.
//
// accountRepo is captured by main and threaded through here so the PM
// can dispatch debit/credit commands against Accounts. In a real
// service this would more typically be commandbus.Bus.Dispatch.
func makeTransferStep(accountRepo *es.Repository[*Account]) es.Handler[es.Envelope, *Transfer] {
	return func(ctx context.Context, env es.Envelope, t *Transfer) error {
		switch p := env.Payload.(type) {
		case TransferRequested:
			// Kick off the saga: debit the source account, tagging the
			// debit with this transfer's id so the PM can correlate
			// the resulting AccountDebited event back to itself.
			return es.Execute(ctx, accountRepo, es.StreamID(t.From),
				DebitCmd{TransferID: string(t.StreamID()), Amount: t.Amount},
				debitHandler)

		case AccountDebited:
			t.Record("transfer.debited", TransferDebitedEvt{}, t.Apply)
			return es.Execute(ctx, accountRepo, es.StreamID(t.To),
				CreditCmd{TransferID: p.TransferID, Amount: p.Amount},
				creditHandler)

		case AccountCredited:
			t.Record("transfer.completed", TransferCompletedEvt{}, t.Apply)
			return nil

		case AccountDebitRejected:
			t.Record("transfer.failed", TransferFailedEvt{Reason: p.Reason}, t.Apply)
			// Compensation would go here if the saga had gotten further:
			// for example, if Credit had failed AFTER Debit had succeeded,
			// the PM would emit a refund Debit-back on the source. This
			// saga fails before any irreversible step, so there is
			// nothing to undo.
			return nil
		}
		return nil
	}
}

// ----------------------------------------------------------------------------
// Main.
// ----------------------------------------------------------------------------

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	reg := es.NewRegistry()
	es.Register(reg, "account.opened", jsoncodec.For[AccountOpened]())
	es.Register(reg, "account.debited", jsoncodec.For[AccountDebited]())
	es.Register(reg, "account.credited", jsoncodec.For[AccountCredited]())
	es.Register(reg, "account.debit_rejected", jsoncodec.For[AccountDebitRejected]())
	es.Register(reg, "transfer.requested", jsoncodec.For[TransferRequested]())
	es.Register(reg, "transfer.debited", jsoncodec.For[TransferDebitedEvt]())
	es.Register(reg, "transfer.completed", jsoncodec.For[TransferCompletedEvt]())
	es.Register(reg, "transfer.failed", jsoncodec.For[TransferFailedEvt]())

	store := memory.New()
	accountRepo := es.NewRepository(store, reg, NewAccount,
		es.WithMiddleware(esmw.PerAggregateLocking()))
	transferRepo := es.NewRepository(store, reg, NewTransfer,
		es.WithMiddleware(esmw.PerAggregateLocking()))

	pm := process.New(transferRepo, correlate, makeTransferStep(accountRepo))
	runner := projection.NewRunner("transfer-process", store, reg, pm,
		projection.WithLive(true),
		// Only subscribe to the events the PM actually cares about —
		// notably not its own emitted Transfer events, which would
		// loop back through correlate and re-enter the handler.
		projection.WithTypes(
			"transfer.requested",
			"account.debited",
			"account.credited",
			"account.debit_rejected",
		),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()

	// Seed two accounts.
	mustExec(ctx, accountRepo, "alice", OpenAccountCmd{InitialBalance: 100}, openHandler)
	mustExec(ctx, accountRepo, "bob", OpenAccountCmd{InitialBalance: 0}, openHandler)

	// Happy path: Alice → Bob, $25. After the saga settles Alice = 75,
	// Bob = 25, transfer = Completed.
	mustExec(ctx, transferRepo, "xfer-happy",
		RequestTransferCmd{From: "alice", To: "bob", Amount: 25}, requestHandler)

	// Failure path: Bob → Alice, $1000 — Bob's balance is 0, so the
	// debit is rejected and the transfer fails before any irreversible
	// step.
	mustExec(ctx, transferRepo, "xfer-fail",
		RequestTransferCmd{From: "bob", To: "alice", Amount: 1000}, requestHandler)

	awaitSettled(ctx, transferRepo, "xfer-happy", TransferCompleted, TransferFailed)
	awaitSettled(ctx, transferRepo, "xfer-fail", TransferCompleted, TransferFailed)

	printState(ctx, accountRepo, transferRepo)

	cancel()
	if err := <-runnerDone; err != nil {
		log.Fatalf("runner: %v", err)
	}
}

func mustExec[C any, A es.Aggregate](
	ctx context.Context,
	repo *es.Repository[A],
	id es.StreamID,
	cmd C,
	h es.Handler[C, A],
) {
	if err := es.Execute(ctx, repo, id, cmd, h); err != nil {
		log.Fatalf("Execute on %q: %v", id, err)
	}
}

// awaitSettled polls the Transfer aggregate until its status is one of
// the terminal values, or 2 seconds pass.
func awaitSettled(
	ctx context.Context,
	repo *es.Repository[*Transfer],
	id es.StreamID,
	terminal ...TransferStatus,
) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		t, err := repo.Load(ctx, id)
		if err == nil && slices.Contains(terminal, t.Status) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Fatalf("transfer %q did not settle within 2s", id)
}

func printState(ctx context.Context, ar *es.Repository[*Account], tr *es.Repository[*Transfer]) {
	fmt.Println()
	fmt.Println("Final state:")
	for _, id := range []es.StreamID{"alice", "bob"} {
		a, err := ar.Load(ctx, id)
		if err != nil {
			log.Fatalf("Load %s: %v", id, err)
		}
		fmt.Printf("  account %s: balance=%d\n", id, a.Balance)
	}
	for _, id := range []es.StreamID{"xfer-happy", "xfer-fail"} {
		t, err := tr.Load(ctx, id)
		if err != nil {
			log.Fatalf("Load %s: %v", id, err)
		}
		fmt.Printf("  transfer %s: %s -> %s, amount=%d, status=%s",
			id, t.From, t.To, t.Amount, t.Status)
		if t.Reason != "" {
			fmt.Printf(" (%s)", t.Reason)
		}
		fmt.Println()
	}
}
