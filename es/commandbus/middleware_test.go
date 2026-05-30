package commandbus_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/commandbus"
	"github.com/ianunruh/synapse/internal/testdomain"
)

// recordingHandler is a [slog.Handler] that retains every record so the
// Logging middleware tests can assert on level, message, and attrs.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (*recordingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *recordingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(_ string) slog.Handler      { return h }
func (h *recordingHandler) Records() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.records)
}

func attrOf(r slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	var ok bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, ok = a.Value, true
			return false
		}
		return true
	})
	return v, ok
}

func TestLogging_DebugOnSuccess(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	h := &recordingHandler{}
	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Logging(slog.New(h))))
	commandbus.Register(bus, "counter.inc", repo, incHandler, jsoncodec.For[testCmd]())

	if err := bus.Dispatch(t.Context(), "counter.inc",
		[]byte(`{"stream":"counter","by":1}`)); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	recs := h.Records()
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	if recs[0].Level != slog.LevelDebug {
		t.Errorf("success level = %v, want Debug", recs[0].Level)
	}
	if v, ok := attrOf(recs[0], "command"); !ok || v.String() != "counter.inc" {
		t.Errorf("command attr = %v, want counter.inc", v)
	}
	if _, ok := attrOf(recs[0], "duration"); !ok {
		t.Errorf("duration attr missing")
	}
}

func TestLogging_WarnOnError(t *testing.T) {
	h := &recordingHandler{}
	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Logging(slog.New(h))))

	_ = bus.Dispatch(t.Context(), "no.such.command", nil)

	recs := h.Records()
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	if recs[0].Level != slog.LevelWarn {
		t.Errorf("error level = %v, want Warn", recs[0].Level)
	}
	if _, ok := attrOf(recs[0], "err"); !ok {
		t.Errorf("err attr missing on failure record")
	}
}

func TestLogging_NilLoggerUsesDefault(t *testing.T) {
	// Smoke test: nil logger must not panic. The middleware should fall
	// back to slog.Default(); we don't assert on its output.
	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Logging(nil)))
	_ = bus.Dispatch(t.Context(), "no.such.command", nil)
}

func TestRecover_PanicBecomesPanicError(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	panicHandler := func(_ context.Context, _ testCmd, _ *testdomain.Counter) error {
		panic("kaboom")
	}

	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Recover()))
	commandbus.Register(bus, "counter.panic", repo, panicHandler, jsoncodec.For[testCmd]())

	err := bus.Dispatch(t.Context(), "counter.panic",
		[]byte(`{"stream":"counter","by":1}`))
	if !errors.Is(err, commandbus.ErrPanic) {
		t.Errorf("err = %v, want wrap of ErrPanic", err)
	}
	var pe *commandbus.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err is not *PanicError: %T", err)
	}
	if pe.Name != "counter.panic" {
		t.Errorf("Name = %q, want counter.panic", pe.Name)
	}
	if pe.Value != "kaboom" {
		t.Errorf("Value = %v, want kaboom", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Errorf("Stack is empty")
	}
}

func TestRecover_PassesThroughNonPanic(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	sentinel := errors.New("handler boom")
	failHandler := func(_ context.Context, _ testCmd, _ *testdomain.Counter) error {
		return sentinel
	}

	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Recover()))
	commandbus.Register(bus, "counter.fail", repo, failHandler, jsoncodec.For[testCmd]())

	err := bus.Dispatch(t.Context(), "counter.fail",
		[]byte(`{"stream":"counter","by":1}`))
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want sentinel", err)
	}
	if errors.Is(err, commandbus.ErrPanic) {
		t.Errorf("non-panic error wrongly classified as ErrPanic: %v", err)
	}
}

func TestTimeout_AbortsSlowHandler(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	slowHandler := func(ctx context.Context, _ testCmd, _ *testdomain.Counter) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	}

	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Timeout(20 * time.Millisecond)))
	commandbus.Register(bus, "counter.slow", repo, slowHandler, jsoncodec.For[testCmd]())

	err := bus.Dispatch(t.Context(), "counter.slow",
		[]byte(`{"stream":"counter","by":1}`))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestTimeout_FastHandlerUnaffected(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Timeout(time.Second)))
	commandbus.Register(bus, "counter.inc", repo, incHandler, jsoncodec.For[testCmd]())

	if err := bus.Dispatch(t.Context(), "counter.inc",
		[]byte(`{"stream":"counter","by":1}`)); err != nil {
		t.Errorf("Dispatch: %v", err)
	}
}

func TestTimeout_ZeroDisables(t *testing.T) {
	// d <= 0 must short-circuit and return the inner Operation unchanged
	// — no deadline imposed, no spurious cancellation on a slow handler.
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	bus := commandbus.New(commandbus.WithMiddleware(commandbus.Timeout(0)))
	commandbus.Register(bus, "counter.inc", repo, incHandler, jsoncodec.For[testCmd]())

	if err := bus.Dispatch(t.Context(), "counter.inc",
		[]byte(`{"stream":"counter","by":1}`)); err != nil {
		t.Errorf("Dispatch: %v", err)
	}
}
