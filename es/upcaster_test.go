package es_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

// --- Domain ---------------------------------------------------------
//
// Three event versions of the "counter added" event let us exercise
// upcaster chains of arbitrary depth without entangling the existing
// testdomain Counter (whose schema we want to keep stable).

type CounterAddedV1 struct {
	Value int `json:"value"`
}

type CounterAddedV2 struct {
	Amount int    `json:"amount"`
	Source string `json:"source"`
}

type CounterAddedV3 struct {
	Amount   int    `json:"amount"`
	Source   string `json:"source"`
	Currency string `json:"currency"`
}

const (
	typeV1 = "counter.added.v1"
	typeV2 = "counter.added.v2"
	typeV3 = "counter.added.v3"
)

// upcasterCounter is an aggregate that knows only the *latest* event
// shape it cares about. Tests parameterize which version Apply
// recognizes so we can verify the upcaster delivered the right form.
type upcasterCounter struct {
	*es.AggregateBase
	stream    es.StreamID
	apply     func(env es.Envelope, c *upcasterCounter)
	Recorded  []es.Envelope
	Sum       int
	Source    string
	Currency  string
	Snapshot_ *upcasterSnapshotV2 // last restored snapshot
}

func newUpcasterCounter(stream es.StreamID, apply func(env es.Envelope, c *upcasterCounter)) *upcasterCounter {
	return &upcasterCounter{
		AggregateBase: es.NewAggregateBase(stream),
		stream:        stream,
		apply:         apply,
	}
}

func (c *upcasterCounter) Apply(env es.Envelope) {
	c.Recorded = append(c.Recorded, env)
	if c.apply != nil {
		c.apply(env, c)
	}
}

// Snapshot types for the snapshot-upcasting test.

type upcasterSnapshotV1 struct {
	Sum int `json:"sum"`
}

type upcasterSnapshotV2 struct {
	Sum    int    `json:"sum"`
	Source string `json:"source"`
}

const (
	snapTypeV1 = "counter.snapshot.v1"
	snapTypeV2 = "counter.snapshot.v2"
)

func (c *upcasterCounter) SnapshotType() string { return snapTypeV2 }

func (c *upcasterCounter) Snapshot() (any, error) {
	return upcasterSnapshotV2{Sum: c.Sum, Source: c.Source}, nil
}

func (c *upcasterCounter) Restore(state any) error {
	s, ok := state.(upcasterSnapshotV2)
	if !ok {
		return fmt.Errorf("invalid snapshot type %T", state)
	}
	c.Snapshot_ = &s
	c.Sum = s.Sum
	c.Source = s.Source
	return nil
}

// --- Helpers --------------------------------------------------------

// registerAllEventCodecs registers JSON codecs for every test event
// version. Codecs and upcasters live in the same Registry — tests
// just opt in to the upcasters they care about.
func registerAllEventCodecs(reg *es.Registry) {
	es.Register(reg, typeV1, jsoncodec.For[CounterAddedV1]())
	es.Register(reg, typeV2, jsoncodec.For[CounterAddedV2]())
	es.Register(reg, typeV3, jsoncodec.For[CounterAddedV3]())
}

// appendRaw stores a single payload of the given type as a v1-version
// event in the store, bypassing the Repository so tests can simulate
// "old events were written by yesterday's code".
func appendRaw[E any](t *testing.T, store *memory.Store, stream es.StreamID, eventType string, payload E, version uint64) {
	t.Helper()
	codec := jsoncodec.For[E]()
	data, err := codec.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	expected := es.NoStream
	if version > 1 {
		expected = es.Exact(version - 1)
	}
	if _, err := store.Append(t.Context(), stream, expected, es.RawEnvelope{
		EventID:     fmt.Sprintf("evt-%d", version),
		StreamID:    stream,
		Version:     version,
		Type:        eventType,
		ContentType: codec.ContentType(),
		Payload:     data,
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// --- Tests ----------------------------------------------------------

func TestUpcaster_SingleHop_OnLoad(t *testing.T) {
	store := memory.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)
	es.RegisterUpcaster(reg, typeV1, typeV2, func(in CounterAddedV1) (CounterAddedV2, error) {
		return CounterAddedV2{Amount: in.Value, Source: "legacy"}, nil
	})

	stream := es.StreamID("test/upcast-single")
	appendRaw(t, store, stream, typeV1, CounterAddedV1{Value: 7}, 1)

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, func(env es.Envelope, c *upcasterCounter) {
			if v2, ok := env.Payload.(CounterAddedV2); ok {
				c.Sum += v2.Amount
				c.Source = v2.Source
			}
		})
	})

	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Sum != 7 {
		t.Errorf("Sum = %d, want 7", loaded.Sum)
	}
	if loaded.Source != "legacy" {
		t.Errorf("Source = %q, want %q", loaded.Source, "legacy")
	}
	if len(loaded.Recorded) != 1 || loaded.Recorded[0].Type != typeV2 {
		t.Errorf("Apply saw Type = %q, want %q", loaded.Recorded[0].Type, typeV2)
	}
}

func TestUpcaster_Chain_v1_v2_v3(t *testing.T) {
	store := memory.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)
	es.RegisterUpcaster(reg, typeV1, typeV2, func(in CounterAddedV1) (CounterAddedV2, error) {
		return CounterAddedV2{Amount: in.Value, Source: "v1->v2"}, nil
	})
	es.RegisterUpcaster(reg, typeV2, typeV3, func(in CounterAddedV2) (CounterAddedV3, error) {
		return CounterAddedV3{Amount: in.Amount, Source: in.Source, Currency: "USD"}, nil
	})

	stream := es.StreamID("test/upcast-chain")
	appendRaw(t, store, stream, typeV1, CounterAddedV1{Value: 3}, 1)

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, func(env es.Envelope, c *upcasterCounter) {
			if v3, ok := env.Payload.(CounterAddedV3); ok {
				c.Sum += v3.Amount
				c.Source = v3.Source
				c.Currency = v3.Currency
			}
		})
	})

	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Sum != 3 || loaded.Source != "v1->v2" || loaded.Currency != "USD" {
		t.Errorf("got Sum=%d Source=%q Currency=%q; want 3, v1->v2, USD",
			loaded.Sum, loaded.Source, loaded.Currency)
	}
	if loaded.Recorded[0].Type != typeV3 {
		t.Errorf("Apply saw Type = %q, want %q (final upcasted type)", loaded.Recorded[0].Type, typeV3)
	}
}

func TestUpcaster_CycleDetected(t *testing.T) {
	store := memory.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)
	es.RegisterUpcaster(reg, typeV1, typeV2, func(in CounterAddedV1) (CounterAddedV2, error) {
		return CounterAddedV2{Amount: in.Value}, nil
	})
	es.RegisterUpcaster(reg, typeV2, typeV1, func(in CounterAddedV2) (CounterAddedV1, error) {
		return CounterAddedV1{Value: in.Amount}, nil
	})

	stream := es.StreamID("test/upcast-cycle")
	appendRaw(t, store, stream, typeV1, CounterAddedV1{Value: 1}, 1)

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, nil)
	})

	_, err := repo.Load(t.Context(), stream)
	var cycleErr *es.UpcasterCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("err = %v, want *UpcasterCycleError", err)
	}
	if !slices.Contains(cycleErr.Chain, typeV1) || !slices.Contains(cycleErr.Chain, typeV2) {
		t.Errorf("Chain = %v, want both %q and %q present", cycleErr.Chain, typeV1, typeV2)
	}
}

func TestUpcaster_UserFnError_Propagates(t *testing.T) {
	store := memory.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)
	boom := errors.New("upcaster boom")
	es.RegisterUpcaster(reg, typeV1, typeV2, func(_ CounterAddedV1) (CounterAddedV2, error) {
		return CounterAddedV2{}, boom
	})

	stream := es.StreamID("test/upcast-err")
	appendRaw(t, store, stream, typeV1, CounterAddedV1{Value: 1}, 1)

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, nil)
	})

	_, err := repo.Load(t.Context(), stream)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want wrap of %v", err, boom)
	}
}

func TestUpcaster_NoUpcaster_Passthrough(t *testing.T) {
	// Regression: a registry with no upcasters behaves exactly as
	// before — Envelope.Type stays the raw type, payload is the codec's
	// decoded value.
	store := memory.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)

	stream := es.StreamID("test/upcast-passthrough")
	appendRaw(t, store, stream, typeV2, CounterAddedV2{Amount: 5, Source: "direct"}, 1)

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, func(env es.Envelope, c *upcasterCounter) {
			if v2, ok := env.Payload.(CounterAddedV2); ok {
				c.Sum = v2.Amount
				c.Source = v2.Source
			}
		})
	})

	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Sum != 5 || loaded.Source != "direct" {
		t.Errorf("got Sum=%d Source=%q; want 5, direct", loaded.Sum, loaded.Source)
	}
	if loaded.Recorded[0].Type != typeV2 {
		t.Errorf("Type = %q, want %q (unchanged)", loaded.Recorded[0].Type, typeV2)
	}
}

func TestUpcaster_OnSnapshotPath(t *testing.T) {
	store := memory.New()
	snaps := snapmem.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)
	es.Register(reg, snapTypeV1, jsoncodec.For[upcasterSnapshotV1]())
	es.Register(reg, snapTypeV2, jsoncodec.For[upcasterSnapshotV2]())
	es.RegisterUpcaster(reg, snapTypeV1, snapTypeV2, func(in upcasterSnapshotV1) (upcasterSnapshotV2, error) {
		return upcasterSnapshotV2{Sum: in.Sum, Source: "snap-upcast"}, nil
	})

	stream := es.StreamID("test/upcast-snap")
	appendRaw(t, store, stream, typeV2, CounterAddedV2{Amount: 1, Source: "x"}, 1)

	// Seed the snapshot store with a v1 snapshot at version 1.
	snapCodec := jsoncodec.For[upcasterSnapshotV1]()
	snapData, err := snapCodec.Marshal(upcasterSnapshotV1{Sum: 42})
	if err != nil {
		t.Fatalf("snap marshal: %v", err)
	}
	if err := snaps.Save(t.Context(), es.RawSnapshot{
		StreamID: stream, Version: 1, Type: snapTypeV1,
		ContentType: snapCodec.ContentType(), Payload: snapData,
	}); err != nil {
		t.Fatalf("snap Save: %v", err)
	}

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, func(_ es.Envelope, _ *upcasterCounter) {})
	},
		es.WithSnapshotStore(snaps),
	)

	loaded, err := repo.Load(t.Context(), stream)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Snapshot_ == nil {
		t.Fatalf("Restore was not called with upcasted v2 snapshot")
	}
	if loaded.Snapshot_.Sum != 42 || loaded.Snapshot_.Source != "snap-upcast" {
		t.Errorf("Restored = %+v, want {Sum:42 Source:snap-upcast}", *loaded.Snapshot_)
	}
}

// recordingProj captures the payload types Project sees. Used to
// verify the projection runner sees the final upcasted shape.
type recordingProj struct {
	mu    sync.Mutex
	types []string
}

func (p *recordingProj) Project(_ context.Context, env es.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.types = append(p.types, env.Type)
	return nil
}

func TestUpcaster_ProjectionRunner_SeesUpcastedPayload(t *testing.T) {
	store := memory.New()
	reg := es.NewRegistry()
	registerAllEventCodecs(reg)
	es.RegisterUpcaster(reg, typeV1, typeV2, func(in CounterAddedV1) (CounterAddedV2, error) {
		return CounterAddedV2{Amount: in.Value, Source: "p"}, nil
	})

	stream := es.StreamID("test/upcast-proj")
	appendRaw(t, store, stream, typeV1, CounterAddedV1{Value: 1}, 1)

	proj := &recordingProj{}
	r := projection.NewRunner("test", store, reg, proj)
	if err := r.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(proj.types) != 1 || proj.types[0] != typeV2 {
		t.Errorf("Project saw Types = %v, want [%q]", proj.types, typeV2)
	}
}

func TestUpcaster_TypeMismatch_Errors(t *testing.T) {
	// Register an upcaster from typeV1 expecting CounterAddedV1, but
	// have the codec produce a CounterAddedV2 value. The upcaster
	// wrapper should return *UpcasterTypeError.
	store := memory.New()
	reg := es.NewRegistry()
	// Deliberately register the v2 codec under the v1 type name.
	es.Register(reg, typeV1, jsoncodec.For[CounterAddedV2]())
	es.RegisterUpcaster(reg, typeV1, typeV2, func(in CounterAddedV1) (CounterAddedV2, error) {
		return CounterAddedV2{Amount: in.Value}, nil
	})

	stream := es.StreamID("test/upcast-mismatch")
	appendRaw(t, store, stream, typeV1, CounterAddedV2{Amount: 1}, 1)

	repo := es.NewRepository(store, reg, func(id es.StreamID) *upcasterCounter {
		return newUpcasterCounter(id, nil)
	})

	_, err := repo.Load(t.Context(), stream)
	var typeErr *es.UpcasterTypeError
	if !errors.As(err, &typeErr) {
		t.Fatalf("err = %v, want *UpcasterTypeError", err)
	}
	if typeErr.FromType != typeV1 {
		t.Errorf("FromType = %q, want %q", typeErr.FromType, typeV1)
	}
	if !strings.Contains(typeErr.Expected, "CounterAddedV1") {
		t.Errorf("Expected = %q, want it to mention CounterAddedV1", typeErr.Expected)
	}
}
