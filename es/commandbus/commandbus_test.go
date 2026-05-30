package commandbus_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/commandbus"
	esmw "github.com/ianunruh/synapse/es/middleware"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

// testCmd is a self-routing command used throughout the suite. The
// json tags lock the payload format the bus's Dispatch tests rely on.
type testCmd struct {
	Stream es.StreamID `json:"stream"`
	By     int         `json:"by"`
}

func (c testCmd) AggregateID() es.StreamID { return c.Stream }

func incHandler(_ context.Context, cmd testCmd, c *testdomain.Counter) error {
	c.Increment(cmd.By)
	return nil
}

func newRepo() *es.Repository[*testdomain.Counter] {
	return es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter)
}

// seed creates the stream by appending one zero-delta event, so a
// subsequent Load succeeds.
func seed(t *testing.T, repo *es.Repository[*testdomain.Counter], stream es.StreamID) {
	t.Helper()
	c := testdomain.NewCounter(stream)
	c.Increment(0)
	if err := repo.Save(t.Context(), c); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestDispatch_HappyPath(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	bus := commandbus.New()
	commandbus.Register(bus, "counter.inc", repo, incHandler, jsoncodec.For[testCmd]())

	if err := bus.Dispatch(t.Context(), "counter.inc",
		[]byte(`{"stream":"counter","by":3}`)); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Count != 3 {
		t.Errorf("Count = %d, want 3", loaded.Count)
	}
}

func TestDispatch_UnknownCommand(t *testing.T) {
	bus := commandbus.New()
	err := bus.Dispatch(t.Context(), "no.such.command", []byte(`{}`))
	if !errors.Is(err, commandbus.ErrUnknownCommand) {
		t.Errorf("err = %v, want wrap of ErrUnknownCommand", err)
	}
	var uce *commandbus.UnknownCommandError
	if !errors.As(err, &uce) {
		t.Fatalf("err is not *UnknownCommandError: %T", err)
	}
	if uce.Name != "no.such.command" {
		t.Errorf("Name = %q, want %q", uce.Name, "no.such.command")
	}
}

func TestDispatch_DecodeError(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	bus := commandbus.New()
	commandbus.Register(bus, "counter.inc", repo, incHandler, jsoncodec.For[testCmd]())

	err := bus.Dispatch(t.Context(), "counter.inc", []byte(`{`))
	if !errors.Is(err, commandbus.ErrDecode) {
		t.Errorf("err = %v, want wrap of ErrDecode", err)
	}
	var de *commandbus.DecodeError
	if !errors.As(err, &de) {
		t.Fatalf("err is not *DecodeError: %T", err)
	}
	if de.Name != "counter.inc" {
		t.Errorf("Name = %q, want counter.inc", de.Name)
	}

	// No event appended: the stream is still at version 1 (seed only)
	// and Count is 0.
	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Version() != 1 || loaded.Count != 0 {
		t.Errorf("after decode failure: Version=%d Count=%d, want 1, 0",
			loaded.Version(), loaded.Count)
	}
}

func TestDispatch_HandlerErrorPropagates(t *testing.T) {
	const stream es.StreamID = "counter"
	repo := newRepo()
	seed(t, repo, stream)

	sentinel := errors.New("handler boom")
	failHandler := func(_ context.Context, _ testCmd, _ *testdomain.Counter) error {
		return sentinel
	}

	bus := commandbus.New()
	commandbus.Register(bus, "counter.fail", repo, failHandler, jsoncodec.For[testCmd]())

	err := bus.Dispatch(t.Context(), "counter.fail",
		[]byte(`{"stream":"counter","by":1}`))
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want wrap of sentinel", err)
	}
	if errors.Is(err, commandbus.ErrUnknownCommand) {
		t.Errorf("handler error reported as ErrUnknownCommand; transport status would be wrong")
	}
}

func TestRegister_DuplicatePanics(t *testing.T) {
	bus := commandbus.New()
	repo := newRepo()
	codec := jsoncodec.For[testCmd]()
	commandbus.Register(bus, "x", repo, incHandler, codec)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value = %v (%T), want string", r, r)
		}
		if !strings.Contains(msg, "already registered") {
			t.Errorf("panic message = %q, want substring \"already registered\"", msg)
		}
	}()
	commandbus.Register(bus, "x", repo, incHandler, codec)
}

func TestRegister_NilRepoPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil repo")
		}
	}()
	bus := commandbus.New()
	var nilRepo *es.Repository[*testdomain.Counter]
	commandbus.Register(bus, "x", nilRepo, incHandler, jsoncodec.For[testCmd]())
}

func TestRegister_NilCodecPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil codec")
		}
	}()
	bus := commandbus.New()
	commandbus.Register(bus, "x", newRepo(), incHandler, es.TypedCodec[testCmd](nil))
}

func TestDispatch_MiddlewareAppliesUnderConcurrency(t *testing.T) {
	const stream es.StreamID = "counter-mw"
	const N = 8

	var calls atomic.Int64
	counting := func(next es.Operation) es.Operation {
		return func(ctx context.Context, sid es.StreamID) error {
			calls.Add(1)
			return next(ctx, sid)
		}
	}

	repo := es.NewRepository(memory.New(), testdomain.NewRegistry(), testdomain.NewCounter,
		es.WithMiddleware(counting, esmw.PerAggregateLocking()))
	seed(t, repo, stream) // direct repo.Save bypasses middleware

	bus := commandbus.New()
	commandbus.Register(bus, "counter.inc", repo, incHandler, jsoncodec.For[testCmd]())

	payload := []byte(`{"stream":"counter-mw","by":1}`)
	errs := make(chan error, N)
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() { errs <- bus.Dispatch(t.Context(), "counter.inc", payload) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("Dispatch: %v", err)
		}
	}

	if got := calls.Load(); got != int64(N) {
		t.Errorf("middleware calls = %d, want %d (one per Dispatch, no double-wrap, no skipped-wrap)",
			got, N)
	}

	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Count != N {
		t.Errorf("Count = %d, want %d (locking should have serialized all N)",
			loaded.Count, N)
	}
}

func TestDispatch_MiddlewareCompositionOrder(t *testing.T) {
	var log []string
	mw := func(label string) commandbus.Middleware {
		return func(next commandbus.Operation) commandbus.Operation {
			return func(ctx context.Context, name string, payload []byte) error {
				log = append(log, label+"-pre")
				err := next(ctx, name, payload)
				log = append(log, label+"-post")
				return err
			}
		}
	}

	// Use the unknown-command path to exercise the chain without needing
	// a real handler. The chain must still wrap the lookup failure.
	bus := commandbus.New(commandbus.WithMiddleware(mw("A"), mw("B")))
	err := bus.Dispatch(t.Context(), "missing", nil)
	if !errors.Is(err, commandbus.ErrUnknownCommand) {
		t.Errorf("err = %v, want ErrUnknownCommand", err)
	}

	want := []string{"A-pre", "B-pre", "B-post", "A-post"}
	if !slices.Equal(log, want) {
		t.Errorf("composition order = %v, want %v (A wraps B wraps core)", log, want)
	}
}

func TestNames(t *testing.T) {
	bus := commandbus.New()
	repo := newRepo()
	codec := jsoncodec.For[testCmd]()
	commandbus.Register(bus, "a", repo, incHandler, codec)
	commandbus.Register(bus, "b", repo, incHandler, codec)

	names := bus.Names()
	slices.Sort(names)
	if !slices.Equal(names, []string{"a", "b"}) {
		t.Errorf("Names() = %v, want [a b]", names)
	}

	names[0] = "MUTATED"
	for _, n := range bus.Names() {
		if n == "MUTATED" {
			t.Errorf("Names() returned a shared slice; mutation leaked: %v", bus.Names())
		}
	}
}
