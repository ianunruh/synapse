package proto_test

import (
	"bytes"
	"testing"

	"github.com/ianunruh/synapse/codec/codectest"
	protocodec "github.com/ianunruh/synapse/codec/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestContract runs the shared codec contract against a well-known
// protobuf message type, so no .proto/codegen is needed in the test.
func TestContract(t *testing.T) {
	codectest.RunContract(t,
		protocodec.For[*wrapperspb.StringValue],
		"wkt.string_value",
		wrapperspb.String("hello, synapse"),
		func(a, b *wrapperspb.StringValue) bool { return proto.Equal(a, b) },
	)
}

func TestContentType(t *testing.T) {
	const want = "application/vnd.google.protobuf"
	if got := protocodec.For[*wrapperspb.StringValue]().ContentType(); got != want {
		t.Errorf("ContentType() = %q, want %q", got, want)
	}
	if protocodec.ContentType != want {
		t.Errorf("package-level ContentType = %q, want %q", protocodec.ContentType, want)
	}
}

func TestRoundTripNested(t *testing.T) {
	c := protocodec.For[*structpb.Struct]()
	in, err := structpb.NewStruct(map[string]any{
		"id":    "order-1",
		"total": 42.0,
		"tags":  []any{"a", "b"},
	})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := c.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Errorf("nested round-trip mismatch:\nin  = %v\nout = %v", in, out)
	}
}

func TestEquivalentToDirectMarshal(t *testing.T) {
	// The codec is a thin wrapper over proto.Marshal with default
	// options, so its output must match a direct proto.Marshal of the
	// same message.
	in := wrapperspb.Int64(1234)
	direct, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	via, err := protocodec.For[*wrapperspb.Int64Value]().Marshal(in)
	if err != nil {
		t.Fatalf("codec.Marshal: %v", err)
	}
	if !bytes.Equal(direct, via) {
		t.Errorf("output differs from direct proto.Marshal:\ndirect = %x\nvia    = %x", direct, via)
	}
}

func TestUnmarshalError(t *testing.T) {
	// Field 1 (StringValue.value) is length-delimited; this declares a
	// 5-byte payload but supplies one byte — a truncated field the
	// runtime rejects.
	_, err := protocodec.For[*wrapperspb.StringValue]().Unmarshal([]byte{0x0a, 0x05, 0x61})
	if err == nil {
		t.Error("expected error unmarshaling truncated protobuf")
	}
}

func TestUnmarshalEmptyAllocatesZero(t *testing.T) {
	// Empty input must still yield a fresh, non-nil zero message — this
	// exercises the nil-receiver ProtoReflect().New() allocation path,
	// since the zero value of a pointer message type is nil.
	out, err := protocodec.For[*wrapperspb.StringValue]().Unmarshal(nil)
	if err != nil {
		t.Fatalf("Unmarshal(nil): %v", err)
	}
	if out == nil {
		t.Fatal("Unmarshal(nil) returned a nil message, want an allocated zero value")
	}
	if out.GetValue() != "" {
		t.Errorf("Value = %q, want empty", out.GetValue())
	}
}
