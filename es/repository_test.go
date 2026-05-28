package es_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
)

// ----- Test aggregate: a counter -----

type Counter struct {
	*es.AggregateBase
	count int
}

func NewCounter(id es.StreamID) *Counter {
	return &Counter{AggregateBase: es.NewAggregateBase(id)}
}

type CounterIncremented struct {
	By int `json:"by"`
}

type CounterReset struct{}

func (c *Counter) Apply(env es.Envelope) error {
	switch p := env.Payload.(type) {
	case CounterIncremented:
		c.count += p.By
	case CounterReset:
		c.count = 0
	}
	return nil
}

func (c *Counter) Increment(by int) error {
	return c.Record("counter.incremented", CounterIncremented{By: by}, c.Apply)
}

func (c *Counter) Reset() error {
	return c.Record("counter.reset", CounterReset{}, c.Apply)
}

// ----- Test codec: minimal JSON, stdlib only -----

type jsonCodec[E any] struct{}

func (jsonCodec[E]) ContentType() es.ContentType            { return "application/json" }
func (jsonCodec[E]) Marshal(e E) ([]byte, error)            { return json.Marshal(e) }
func (jsonCodec[E]) Unmarshal(data []byte) (e E, err error) { return e, json.Unmarshal(data, &e) }

func testRegistry() *es.Registry {
	reg := es.NewRegistry()
	es.Register(reg, "counter.incremented", jsonCodec[CounterIncremented]{})
	es.Register(reg, "counter.reset", jsonCodec[CounterReset]{})
	return reg
}

const counterStream es.StreamID = "counter-1"

// ----- Test clock and id generator -----

type fixedClock struct{ now time.Time }

func (c fixedClock) NowUTC() time.Time { return c.now }

type stubIDGen struct {
	prefix string
	n      int
}

func (g *stubIDGen) NewEventID() string {
	g.n++
	return g.prefix + "-" + fmtInt(g.n)
}

func fmtInt(n int) string {
	// avoid strconv import in the test file; this is fine for small n
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// ----- Command for Execute tests -----

type IncrementCmd struct{ By int }

func IncrementHandler(_ context.Context, cmd IncrementCmd, c *Counter) error {
	return c.Increment(cmd.By)
}

// ----- Tests -----

func TestRepository_Load_StreamNotFound(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	_, err := repo.Load(ctx, counterStream)
	if !errors.Is(err, es.ErrStreamNotFound) {
		t.Errorf("err = %v, want wrap of ErrStreamNotFound", err)
	}
	var nf *es.StreamNotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("err is not *StreamNotFoundError: %T", err)
	} else if nf.Stream != counterStream {
		t.Errorf("Stream = %q, want %q", nf.Stream, counterStream)
	}
}

func TestRepository_SaveAndLoad_RoundTrip(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	c := NewCounter(counterStream)
	if err := c.Increment(5); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := c.Increment(3); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := c.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := c.Increment(2); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if c.count != 2 {
		t.Errorf("count after events = %d, want 2", c.count)
	}
	if c.Version() != 4 {
		t.Errorf("Version after events = %d, want 4", c.Version())
	}

	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(c.Pending()) != 0 {
		t.Errorf("Pending after Save = %d, want 0", len(c.Pending()))
	}

	c2, err := repo.Load(ctx, counterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c2.count != 2 {
		t.Errorf("loaded count = %d, want 2", c2.count)
	}
	if c2.Version() != 4 {
		t.Errorf("loaded Version = %d, want 4", c2.Version())
	}
}

func TestRepository_Save_EmptyAggregate_NoOp(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	repo := es.NewRepository(store, testRegistry(), NewCounter)

	c := NewCounter(counterStream)
	if err := repo.Save(ctx, c); err != nil {
		t.Errorf("Save: %v", err)
	}
	if _, err := repo.Load(ctx, counterStream); !errors.Is(err, es.ErrStreamNotFound) {
		t.Errorf("err = %v, want ErrStreamNotFound (Save should be a no-op)", err)
	}
}

func TestRepository_Save_ConcurrentModification(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	c := NewCounter(counterStream)
	_ = c.Increment(5)
	_ = c.Increment(3)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("seed: %v", err)
	}

	c1, err := repo.Load(ctx, counterStream)
	if err != nil {
		t.Fatalf("Load c1: %v", err)
	}
	c2, err := repo.Load(ctx, counterStream)
	if err != nil {
		t.Fatalf("Load c2: %v", err)
	}

	_ = c1.Increment(1)
	_ = c2.Increment(2)

	if err := repo.Save(ctx, c1); err != nil {
		t.Fatalf("Save c1: %v", err)
	}

	err = repo.Save(ctx, c2)
	if !errors.Is(err, es.ErrConflict) {
		t.Errorf("Save c2: err = %v, want wrap of ErrConflict", err)
	}
	var ce *es.ConflictError
	if !errors.As(err, &ce) {
		t.Errorf("err is not *ConflictError: %T", err)
	}
}

func TestRepository_Save_StampsIdentityAndTime(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	clock := fixedClock{now: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)}
	idgen := &stubIDGen{prefix: "evt"}
	repo := es.NewRepository(store, testRegistry(), NewCounter,
		es.WithClock(clock), es.WithIDGenerator(idgen))

	c := NewCounter(counterStream)
	_ = c.Increment(1)
	_ = c.Increment(2)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	var stored []es.RawEnvelope
	for env, err := range store.Load(ctx, counterStream, es.ReadOptions{}) {
		if err != nil {
			t.Fatalf("store.Load: %v", err)
		}
		stored = append(stored, env)
	}
	if len(stored) != 2 {
		t.Fatalf("stored = %d, want 2", len(stored))
	}
	for i, env := range stored {
		wantID := "evt-" + fmtInt(i+1)
		if env.EventID != wantID {
			t.Errorf("event[%d].EventID = %q, want %q", i, env.EventID, wantID)
		}
		if !env.RecordedAt.Equal(clock.now) {
			t.Errorf("event[%d].RecordedAt = %v, want %v", i, env.RecordedAt, clock.now)
		}
		if env.ContentType != "application/json" {
			t.Errorf("event[%d].ContentType = %q, want application/json", i, env.ContentType)
		}
	}
}

func TestRepository_Save_DefaultIDGenerator_UUIDv7Shape(t *testing.T) {
	ctx := t.Context()
	store := memory.New()
	repo := es.NewRepository(store, testRegistry(), NewCounter)

	c := NewCounter(counterStream)
	_ = c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	var got es.RawEnvelope
	for env, err := range store.Load(ctx, counterStream, es.ReadOptions{}) {
		if err != nil {
			t.Fatalf("store.Load: %v", err)
		}
		got = env
		break
	}
	id := got.EventID
	if len(id) != 36 {
		t.Errorf("EventID len = %d, want 36 (UUID format), got %q", len(id), id)
	}
	if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Errorf("EventID dashes misplaced: %q", id)
	}
	if id[14] != '7' {
		t.Errorf("EventID version nibble = %q, want '7' (UUIDv7)", string(id[14]))
	}
}

func TestRepository_Save_UnknownEventType_ReturnsCodecError(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), es.NewRegistry(), NewCounter)

	c := NewCounter(counterStream)
	_ = c.Increment(1)

	err := repo.Save(ctx, c)
	if !errors.Is(err, es.ErrCodecNotFound) {
		t.Errorf("err = %v, want wrap of ErrCodecNotFound", err)
	}
	var nf *es.CodecNotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("err is not *CodecNotFoundError: %T", err)
	}
}

func TestRepository_Load_UnknownEventType_ReturnsCodecError(t *testing.T) {
	ctx := t.Context()
	store := memory.New()

	// Seed with a full registry
	repo := es.NewRepository(store, testRegistry(), NewCounter)
	c := NewCounter(counterStream)
	_ = c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Load with an empty registry — should fail at codec lookup.
	repo2 := es.NewRepository(store, es.NewRegistry(), NewCounter)
	if _, err := repo2.Load(ctx, counterStream); !errors.Is(err, es.ErrCodecNotFound) {
		t.Errorf("err = %v, want wrap of ErrCodecNotFound", err)
	}
}

func TestExecute_LoadHandleSave(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	// Seed via Save.
	c := NewCounter(counterStream)
	_ = c.Increment(5)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := es.Execute(ctx, repo, counterStream, IncrementCmd{By: 3}, IncrementHandler); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	c2, err := repo.Load(ctx, counterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c2.count != 8 {
		t.Errorf("count after Execute = %d, want 8", c2.count)
	}
	if c2.Version() != 2 {
		t.Errorf("Version after Execute = %d, want 2", c2.Version())
	}
}

func TestExecute_StreamNotFound_Propagates(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	err := es.Execute(ctx, repo, counterStream, IncrementCmd{By: 1}, IncrementHandler)
	if !errors.Is(err, es.ErrStreamNotFound) {
		t.Errorf("err = %v, want wrap of ErrStreamNotFound", err)
	}
}

func TestExecute_HandlerError_NoSave(t *testing.T) {
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	// Seed
	c := NewCounter(counterStream)
	_ = c.Increment(1)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("seed: %v", err)
	}

	boom := errors.New("boom")
	err := es.Execute(ctx, repo, counterStream, IncrementCmd{By: 1},
		func(_ context.Context, _ IncrementCmd, _ *Counter) error { return boom })
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}

	// Verify nothing new was saved.
	c2, _ := repo.Load(ctx, counterStream)
	if c2.count != 1 {
		t.Errorf("count after failed Execute = %d, want 1", c2.count)
	}
	if c2.Version() != 1 {
		t.Errorf("Version after failed Execute = %d, want 1", c2.Version())
	}
}

func TestRepository_NewAggregate_FreshStream(t *testing.T) {
	// Demonstrates create semantics: build a fresh aggregate via newFn
	// and Save with expected=NoStream (handled by Save automatically).
	ctx := t.Context()
	repo := es.NewRepository(memory.New(), testRegistry(), NewCounter)

	c := NewCounter(counterStream)
	_ = c.Increment(42)
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := repo.Load(ctx, counterStream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.count != 42 {
		t.Errorf("loaded count = %d, want 42", loaded.count)
	}

	// Saving the same fresh aggregate type to the same stream a second
	// time must fail with conflict (expected=NoStream).
	c2 := NewCounter(counterStream)
	_ = c2.Increment(1)
	if err := repo.Save(ctx, c2); !errors.Is(err, es.ErrConflict) {
		t.Errorf("second Save: err = %v, want wrap of ErrConflict", err)
	}
}
