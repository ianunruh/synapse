package json_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	jsoncodec "github.com/ianunruh/synapse/codec/json"
	"github.com/ianunruh/synapse/es"
)

type Order struct {
	ID    string `json:"id"`
	Items []Item `json:"items,omitzero"`
	Total int    `json:"total"`
}

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

func TestFor_ContentType(t *testing.T) {
	if got := jsoncodec.For[Order]().ContentType(); got != "application/json" {
		t.Errorf("ContentType = %q, want application/json", got)
	}
	if got := jsoncodec.ContentType; got != "application/json" {
		t.Errorf("package-level ContentType = %q, want application/json", got)
	}
}

func TestFor_RoundTrip(t *testing.T) {
	c := jsoncodec.For[Order]()
	in := Order{
		ID:    "order-1",
		Items: []Item{{SKU: "abc", Quantity: 2}, {SKU: "xyz", Quantity: 1}},
		Total: 4250,
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.ID != in.ID || out.Total != in.Total || len(out.Items) != len(in.Items) {
		t.Errorf("round-trip mismatch:\nin  = %+v\nout = %+v", in, out)
	}
	if out.Items[0].SKU != "abc" || out.Items[0].Quantity != 2 {
		t.Errorf("Items[0] = %+v, want {SKU:abc Quantity:2}", out.Items[0])
	}
}

func TestFor_TagsRespected(t *testing.T) {
	data, err := jsoncodec.For[Order]().Marshal(Order{ID: "x", Total: 5})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(data)
	for _, want := range []string{`"id":"x"`, `"total":5`} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %s: %s", want, s)
		}
	}
}

func TestFor_OmitZero(t *testing.T) {
	// Items uses `omitzero` (Go 1.24+). An empty slice should be elided.
	data, err := jsoncodec.For[Order]().Marshal(Order{ID: "x"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), `"items"`) {
		t.Errorf("expected items field elided via omitzero, got %s", data)
	}
}

// Money implements custom (Un)Marshaler so we can verify stdlib hooks
// are honored by the codec.
type Money struct{ Cents int }

func (m Money) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `"$%d.%02d"`, m.Cents/100, m.Cents%100), nil
}

func (m *Money) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"$`)
	dollarsStr, centsStr, ok := strings.Cut(s, ".")
	if !ok {
		return errors.New("money: missing decimal point")
	}
	dollars, err := strconv.Atoi(dollarsStr)
	if err != nil {
		return fmt.Errorf("money dollars: %w", err)
	}
	cents, err := strconv.Atoi(centsStr)
	if err != nil {
		return fmt.Errorf("money cents: %w", err)
	}
	m.Cents = dollars*100 + cents
	return nil
}

type Receipt struct {
	Total Money `json:"total"`
}

func TestFor_CustomMarshalerHonored(t *testing.T) {
	c := jsoncodec.For[Receipt]()
	data, err := c.Marshal(Receipt{Total: Money{Cents: 1234}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"$12.34"`) {
		t.Errorf("custom Marshaler not invoked: %s", data)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.Total.Cents != 1234 {
		t.Errorf("custom Unmarshaler not invoked: cents = %d, want 1234", out.Total.Cents)
	}
}

func TestFor_MarshalError(t *testing.T) {
	type unsupported struct{ Ch chan int }
	_, err := jsoncodec.For[unsupported]().Marshal(unsupported{Ch: make(chan int)})
	if err == nil {
		t.Errorf("expected error marshaling channel field")
	}
}

func TestFor_UnmarshalError(t *testing.T) {
	_, err := jsoncodec.For[Order]().Unmarshal([]byte("{not-json"))
	if err == nil {
		t.Errorf("expected error unmarshaling malformed JSON")
	}
}

func TestFor_RegistryIntegration(t *testing.T) {
	reg := es.NewRegistry()
	es.Register(reg, "order.placed", jsoncodec.For[Order]())

	c, ok := reg.Lookup("order.placed")
	if !ok {
		t.Fatalf("Lookup: not found")
	}
	if c.ContentType() != "application/json" {
		t.Errorf("ContentType = %q, want application/json", c.ContentType())
	}

	in := Order{ID: "via-registry", Total: 99}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	order, ok := out.(Order)
	if !ok {
		t.Fatalf("Unmarshal returned %T, want Order", out)
	}
	if order.ID != "via-registry" || order.Total != 99 {
		t.Errorf("registry round-trip mismatch: %+v", order)
	}
}

func TestFor_RegistryTypeMismatch(t *testing.T) {
	// The registry adapter (typedAdapter[E]) rejects payloads whose
	// dynamic type does not match the registered E.
	reg := es.NewRegistry()
	es.Register(reg, "order.placed", jsoncodec.For[Order]())

	c, _ := reg.Lookup("order.placed")
	_, err := c.Marshal("not an Order")
	if !errors.Is(err, es.ErrPayloadType) {
		t.Errorf("err = %v, want wrap of ErrPayloadType", err)
	}
	var pe *es.PayloadTypeError
	if !errors.As(err, &pe) {
		t.Errorf("err is not *PayloadTypeError: %T", err)
	}
}

func TestFor_NestedSliceMap(t *testing.T) {
	type Config struct {
		Name    string          `json:"name"`
		Flags   map[string]bool `json:"flags"`
		Listens []string        `json:"listens"`
	}
	c := jsoncodec.For[Config]()
	in := Config{
		Name:    "service-a",
		Flags:   map[string]bool{"a": true, "b": false},
		Listens: []string{"0.0.0.0", "::"},
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.Name != in.Name || len(out.Flags) != 2 || len(out.Listens) != 2 {
		t.Errorf("nested round-trip mismatch:\nin  = %+v\nout = %+v", in, out)
	}
}

func TestFor_StatelessSharing(t *testing.T) {
	// Two instances of For[Order]() are interchangeable; the codec is
	// stateless and safe to share across goroutines.
	a := jsoncodec.For[Order]()
	b := jsoncodec.For[Order]()
	data, err := a.Marshal(Order{ID: "shared"})
	if err != nil {
		t.Fatalf("Marshal a: %v", err)
	}
	out, err := b.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal b: %v", err)
	}
	if out.ID != "shared" {
		t.Errorf("cross-instance round-trip failed: %+v", out)
	}
}

func TestFor_EquivalentToDirectMarshal(t *testing.T) {
	// Our codec is a thin wrapper around encoding/json with no
	// transformations — output should match a direct json.Marshal
	// byte-for-byte.
	in := Order{ID: "compare", Total: 7}
	direct, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	via, err := jsoncodec.For[Order]().Marshal(in)
	if err != nil {
		t.Fatalf("codec.Marshal: %v", err)
	}
	if string(direct) != string(via) {
		t.Errorf("output differs from direct json.Marshal:\ndirect = %s\nvia    = %s", direct, via)
	}
}
