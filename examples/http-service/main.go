// Command http-service is an end-to-end demo of the synapse write/read
// loop behind a net/http transport:
//
//	HTTP request
//	  └─ commandbus.Dispatch (Logging, Recover, Timeout middleware)
//	       └─ es.Execute on a Repository wrapped in PerAggregateLocking
//	            └─ Counter aggregate + memory event store
//	                 └─ projection.Runner (live tail)
//	                      └─ in-memory View
//	                           └─ GET /counters/{id}
//
// The example registers two commands against a tiny Counter aggregate
// and serves three routes:
//
//	POST /commands/{name}      — dispatch a command (body is the JSON cmd)
//	GET  /counters             — list every counter in the view
//	GET  /counters/{id}        — read one counter
//
// The Dispatch handler maps the bus's error classes (which all unwrap
// to well-known sentinels) to HTTP statuses with errors.Is. See ADR-0028.
//
// Run:
//
//	go run ./examples/http-service
//
// Try it (in another shell):
//
//	# create a counter
//	curl -X POST http://localhost:8080/commands/counter.create \
//	     -d '{"stream":"counters/clicks","name":"clicks"}'
//	# increment it
//	curl -X POST http://localhost:8080/commands/counter.increment \
//	     -d '{"stream":"counters/clicks","by":7}'
//	# read it back through the projection view
//	curl http://localhost:8080/counters/counters/clicks
//	# unknown command -> 404; bad body -> 400; wrong revision -> 409;
//	# already-created -> 422 (handler error)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/commandbus"
	esmw "github.com/ianunruh/synapse/es/middleware"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
)

// -----------------------------------------------------------------------------
// Domain — tiny Counter aggregate, just enough to exercise two commands.
// -----------------------------------------------------------------------------

type CounterAggregate struct {
	*es.AggregateBase
	Name  string
	Value int
}

func newCounter(id es.StreamID) *CounterAggregate {
	return &CounterAggregate{AggregateBase: es.NewAggregateBase(id)}
}

type CounterCreated struct {
	Name string `json:"name"`
}

type CounterIncremented struct {
	By int `json:"by"`
}

func (c *CounterAggregate) Apply(env es.Envelope) {
	switch p := env.Payload.(type) {
	case CounterCreated:
		c.Name = p.Name
	case CounterIncremented:
		c.Value += p.By
	}
}

func (c *CounterAggregate) Create(name string) error {
	if c.Version() > 0 {
		return errors.New("counter already created")
	}
	if name == "" {
		return errors.New("name is required")
	}
	c.Record("counter.created", CounterCreated{Name: name}, c.Apply)
	return nil
}

func (c *CounterAggregate) Increment(by int) error {
	if c.Version() == 0 {
		return errors.New("counter not created")
	}
	c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
	return nil
}

// Commands (each implements commandbus.Command via AggregateID).

type CreateCounterCmd struct {
	Stream es.StreamID `json:"stream"`
	Name   string      `json:"name"`
}

func (c CreateCounterCmd) AggregateID() es.StreamID { return c.Stream }

type IncrementCounterCmd struct {
	Stream es.StreamID `json:"stream"`
	By     int         `json:"by"`
}

func (c IncrementCounterCmd) AggregateID() es.StreamID { return c.Stream }

func createHandler(_ context.Context, cmd CreateCounterCmd, c *CounterAggregate) error {
	return c.Create(cmd.Name)
}

func incrementHandler(_ context.Context, cmd IncrementCounterCmd, c *CounterAggregate) error {
	return c.Increment(cmd.By)
}

// -----------------------------------------------------------------------------
// Read side — an in-memory projection view fed by projection.Runner.
// -----------------------------------------------------------------------------

type CounterState struct {
	StreamID es.StreamID `json:"stream"`
	Name     string      `json:"name"`
	Value    int         `json:"value"`
}

type View struct {
	mu       sync.RWMutex
	counters map[es.StreamID]CounterState
}

func newView() *View {
	return &View{counters: make(map[es.StreamID]CounterState)}
}

func (v *View) Project(_ context.Context, env es.Envelope) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch p := env.Payload.(type) {
	case CounterCreated:
		v.counters[env.StreamID] = CounterState{StreamID: env.StreamID, Name: p.Name}
	case CounterIncremented:
		cur := v.counters[env.StreamID]
		cur.Value += p.By
		v.counters[env.StreamID] = cur
	}
	return nil
}

func (v *View) Get(id es.StreamID) (CounterState, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	c, ok := v.counters[id]
	return c, ok
}

func (v *View) List() []CounterState {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]CounterState, 0, len(v.counters))
	for _, c := range v.counters {
		out = append(out, c)
	}
	return out
}

// -----------------------------------------------------------------------------
// HTTP — dispatch + read handlers, error-class to status mapping.
// -----------------------------------------------------------------------------

func dispatchHandler(bus *commandbus.Bus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := bus.Dispatch(r.Context(), name, body); err != nil {
			writeDispatchError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// writeDispatchError maps the bus's documented error classes onto HTTP
// statuses by walking sentinels with errors.Is. Anything not classified
// is treated as a handler-domain failure (422), so 500 is reserved for
// recovered panics and other genuinely internal faults.
func writeDispatchError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, commandbus.ErrUnknownCommand):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, commandbus.ErrDecode):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, es.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, es.ErrStreamNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	case errors.Is(err, commandbus.ErrPanic):
		http.Error(w, "internal error", http.StatusInternalServerError)
	default:
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	}
}

func getCounterHandler(view *View) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := es.StreamID(r.PathValue("id"))
		c, ok := view.Get(id)
		if !ok {
			http.Error(w, fmt.Sprintf("counter %q not found", id), http.StatusNotFound)
			return
		}
		writeJSON(w, c)
	}
}

func listCountersHandler(view *View) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, view.List())
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// -----------------------------------------------------------------------------
// Wiring + lifecycle.
// -----------------------------------------------------------------------------

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	// Event registry — codecs for every event type we'll persist.
	reg := es.NewRegistry()
	es.Register(reg, "counter.created", jsoncodec.For[CounterCreated]())
	es.Register(reg, "counter.incremented", jsoncodec.For[CounterIncremented]())

	store := memory.New()

	// Repository — middleware here applies to load-handle-save (ADR-0012).
	repo := es.NewRepository(store, reg, newCounter,
		es.WithMiddleware(esmw.PerAggregateLocking()),
	)

	// Bus — middleware here wraps Dispatch (ADR-0029). Recover before
	// Logging so the panic still appears in the failure log; Timeout
	// innermost so the deadline scopes only the actual handler work.
	bus := commandbus.New(commandbus.WithMiddleware(
		commandbus.Logging(logger),
		commandbus.Recover(),
		commandbus.Timeout(5*time.Second),
	))
	commandbus.Register(bus, "counter.create", repo, createHandler,
		jsoncodec.For[CreateCounterCmd]())
	commandbus.Register(bus, "counter.increment", repo, incrementHandler,
		jsoncodec.For[IncrementCounterCmd]())

	// Projection — live tail off the same store, populates the View.
	view := newView()
	runner := projection.NewRunner("counter-view", store, reg, view,
		projection.WithLive(true),
	)

	// Routes.
	mux := http.NewServeMux()
	mux.Handle("POST /commands/{name}", dispatchHandler(bus))
	mux.Handle("GET /counters", listCountersHandler(view))
	mux.Handle("GET /counters/{id...}", getCounterHandler(view))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Lifecycle — one context cancels the projection runner on signal;
	// http.Server gets a separate shutdown deadline so in-flight requests
	// drain (then the bus's Timeout middleware backs them off).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()

	go func() {
		logger.Info("synapse: http-service listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("synapse: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown", "err", err)
	}

	if err := <-runnerDone; err != nil {
		logger.Error("projection runner", "err", err)
	}
}
