package json_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ianunruh/synapse/codec/codectest"
	jsoncodec "github.com/ianunruh/synapse/codec/json"
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

// TestContract runs the shared codec contract; format-specific behavior
// is covered by the tests below.
func TestContract(t *testing.T) {
	codectest.RunContract(t,
		jsoncodec.For[Order],
		"order.placed",
		Order{
			ID:    "order-1",
			Items: []Item{{SKU: "abc", Quantity: 2}, {SKU: "xyz", Quantity: 1}},
			Total: 4250,
		},
		func(a, b Order) bool { return reflect.DeepEqual(a, b) },
	)
}

func TestContentType(t *testing.T) {
	if got := jsoncodec.For[Order]().ContentType(); got != "application/json" {
		t.Errorf("ContentType = %q, want application/json", got)
	}
	if got := jsoncodec.ContentType; got != "application/json" {
		t.Errorf("package-level ContentType = %q, want application/json", got)
	}
}

func TestTagsRespected(t *testing.T) {
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

func TestOmitZero(t *testing.T) {
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

func TestCustomMarshalerHonored(t *testing.T) {
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

func TestMarshalError(t *testing.T) {
	type unsupported struct{ Ch chan int }
	_, err := jsoncodec.For[unsupported]().Marshal(unsupported{Ch: make(chan int)})
	if err == nil {
		t.Errorf("expected error marshaling channel field")
	}
}

func TestUnmarshalError(t *testing.T) {
	_, err := jsoncodec.For[Order]().Unmarshal([]byte("{not-json"))
	if err == nil {
		t.Errorf("expected error unmarshaling malformed JSON")
	}
}

func TestEquivalentToDirectMarshal(t *testing.T) {
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

func TestNestedSliceMap(t *testing.T) {
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
