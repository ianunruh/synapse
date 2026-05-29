// Package codectest provides a shared contract suite for [es.TypedCodec]
// implementations, mirroring the role eventstoretest plays for event
// stores (ADR-0018). A codec package opts in with a single call:
//
//	func TestContract(t *testing.T) {
//	    codectest.RunContract(t, jsoncodec.For[Order], "order.placed",
//	        Order{ID: "o-1", Total: 99},
//	        func(a, b Order) bool { return reflect.DeepEqual(a, b) })
//	}
//
// The suite asserts only the behavior every codec must share regardless
// of wire format: a stable, non-empty content type; round-trip fidelity;
// statelessness across instances; integration through [es.Register] and
// the [es.Registry]; and rejection of payloads whose dynamic type does
// not match the registered one. Format-specific concerns — struct-tag
// handling, wire layout, what counts as malformed input — belong in the
// codec package's own tests.
package codectest

import (
	"errors"
	"testing"

	"github.com/ianunruh/synapse/es"
)

// Factory constructs a fresh codec for event type E. A codec package's
// generic For[E] constructor satisfies it directly, so callers pass it
// unadorned: codectest.RunContract(t, jsoncodec.For[Order], ...).
type Factory[E any] func() es.TypedCodec[E]

// RunContract runs the codec contract against newCodec. eventType is the
// name to register under; sample is a representative non-zero value of
// E; equal reports whether two values of E are equivalent (E is often
// not comparable with ==, and proto messages must be compared with
// proto.Equal, so the caller supplies this).
func RunContract[E any](t *testing.T, newCodec Factory[E], eventType string, sample E, equal func(a, b E) bool) {
	t.Helper()
	t.Run("ContentType", func(t *testing.T) { testContentType(t, newCodec) })
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, newCodec, sample, equal) })
	t.Run("CrossInstance", func(t *testing.T) { testCrossInstance(t, newCodec, sample, equal) })
	t.Run("RegistryIntegration", func(t *testing.T) { testRegistryIntegration(t, newCodec, eventType, sample, equal) })
	t.Run("RegistryTypeMismatch", func(t *testing.T) { testRegistryTypeMismatch(t, newCodec, eventType) })
}

func testContentType[E any](t *testing.T, newCodec Factory[E]) {
	ct := newCodec().ContentType()
	if ct == "" {
		t.Error("ContentType is empty; codecs must report a wire format")
	}
	if other := newCodec().ContentType(); other != ct {
		t.Errorf("ContentType not stable across instances: %q vs %q", ct, other)
	}
}

func testRoundTrip[E any](t *testing.T, newCodec Factory[E], sample E, equal func(a, b E) bool) {
	c := newCodec()
	data, err := c.Marshal(sample)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !equal(sample, out) {
		t.Errorf("round-trip mismatch:\nin  = %+v\nout = %+v", sample, out)
	}
}

func testCrossInstance[E any](t *testing.T, newCodec Factory[E], sample E, equal func(a, b E) bool) {
	// Codecs are expected to be stateless: a value marshaled by one
	// instance must unmarshal through another.
	data, err := newCodec().Marshal(sample)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := newCodec().Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !equal(sample, out) {
		t.Errorf("cross-instance round-trip mismatch:\nin  = %+v\nout = %+v", sample, out)
	}
}

func testRegistryIntegration[E any](t *testing.T, newCodec Factory[E], eventType string, sample E, equal func(a, b E) bool) {
	reg := es.NewRegistry()
	es.Register(reg, eventType, newCodec())

	c, ok := reg.Lookup(eventType)
	if !ok {
		t.Fatalf("Lookup(%q): not found after Register", eventType)
	}
	if got, want := c.ContentType(), newCodec().ContentType(); got != want {
		t.Errorf("registered ContentType = %q, want %q", got, want)
	}

	data, err := c.Marshal(sample)
	if err != nil {
		t.Fatalf("EventCodec.Marshal: %v", err)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("EventCodec.Unmarshal: %v", err)
	}
	got, ok := out.(E)
	if !ok {
		t.Fatalf("Unmarshal returned %T, want %T", out, sample)
	}
	if !equal(sample, got) {
		t.Errorf("registry round-trip mismatch:\nin  = %+v\nout = %+v", sample, got)
	}
}

// mismatch is a private sentinel: no caller's payload type E can equal
// it, so passing it through the erased codec always fails the adapter's
// type assertion.
type mismatch struct{}

func testRegistryTypeMismatch[E any](t *testing.T, newCodec Factory[E], eventType string) {
	reg := es.NewRegistry()
	es.Register(reg, eventType, newCodec())

	c, _ := reg.Lookup(eventType)
	_, err := c.Marshal(mismatch{})
	if !errors.Is(err, es.ErrPayloadType) {
		t.Errorf("Marshal of wrong type: err = %v, want wrap of ErrPayloadType", err)
	}
	var pe *es.PayloadTypeError
	if !errors.As(err, &pe) {
		t.Errorf("Marshal of wrong type: err is %T, want *es.PayloadTypeError", err)
	}
}
