package es

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// Upcaster transforms a decoded payload of one event (or snapshot)
// type into the next version of that type, returning the new payload
// and the new type name. Upcasters compose through the [Registry]:
// when a payload is decoded on Load, [Registry.Upcast] applies every
// upcaster whose from-type matches the current type, in sequence,
// until no upcaster matches. The aggregate's Apply (or, for
// snapshots, Restore) sees the final upcasted shape.
//
// User code does not construct Upcaster values directly — register
// typed upcasters through [RegisterUpcaster], which takes care of
// the type-erasure on the hot path.
type Upcaster func(in any) (out any, newType string, err error)

// RegisterUpcaster registers a typed upcaster on the Registry. The
// function receives an In value (the decoded payload of an event at
// fromType) and returns the Out value at the next version, identified
// by toType.
//
//	es.RegisterUpcaster[OrderPlacedV1, OrderPlacedV2](reg,
//	    "order.placed.v1", "order.placed.v2",
//	    func(in OrderPlacedV1) (OrderPlacedV2, error) {
//	        return OrderPlacedV2{Total: in.Amount, Currency: "USD"}, nil
//	    })
//
// The same Registry is shared by event codecs and snapshot codecs, so
// the same mechanism upcasts both. Registration is typically done at
// startup; the Registry is safe for concurrent use afterwards.
//
// Registering an upcaster for a fromType that already has one
// replaces the previous entry.
func RegisterUpcaster[In, Out any](
	r *Registry,
	fromType, toType string,
	fn func(In) (Out, error),
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upcasters[fromType] = func(in any) (any, string, error) {
		typed, ok := in.(In)
		if !ok {
			return nil, "", &UpcasterTypeError{
				FromType: fromType,
				Expected: reflect.TypeFor[In]().String(),
				Got:      fmt.Sprintf("%T", in),
			}
		}
		out, err := fn(typed)
		if err != nil {
			return nil, "", err
		}
		return out, toType, nil
	}
}

// LookupUpcaster returns the [Upcaster] registered for fromType. The
// boolean is false when no upcaster has been registered for that type.
func (r *Registry) LookupUpcaster(fromType string) (Upcaster, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.upcasters[fromType]
	return u, ok
}

// upcastMaxHops bounds the number of upcaster applications during a
// single [Registry.Upcast] call. It is a defense in depth on top of
// the visited-set cycle detector — a legitimately deep evolution
// chain is unlikely to need more than a handful of hops.
const upcastMaxHops = 32

// Upcast applies the upcaster chain rooted at typeName to payload,
// returning the final payload and final type name. When no upcaster
// is registered for typeName, the inputs pass through unchanged with
// a nil error.
//
// A cycle in the registered upcasters returns *[UpcasterCycleError]
// without further mutation. Exceeding [upcastMaxHops] returns the
// same error with the full chain attached.
//
// Errors returned by the user-registered upcaster function are
// wrapped with the from-type for context. [UpcasterTypeError] is
// returned without wrapping so callers can use [errors.As] directly.
func (r *Registry) Upcast(payload any, typeName string) (any, string, error) {
	chain := []string{typeName}
	for range upcastMaxHops {
		u, ok := r.LookupUpcaster(typeName)
		if !ok {
			return payload, typeName, nil
		}
		next, nextType, err := u(payload)
		if err != nil {
			return nil, typeName, err
		}
		if slices.Contains(chain, nextType) {
			return nil, typeName, &UpcasterCycleError{Chain: append(chain, nextType)}
		}
		chain = append(chain, nextType)
		payload, typeName = next, nextType
	}
	return nil, typeName, &UpcasterCycleError{Chain: chain}
}

// UpcasterTypeError is returned when a registered upcaster receives a
// payload whose dynamic type does not match the In type the upcaster
// was registered with — typically a sign that the codec for FromType
// was reconfigured but the upcaster was not.
type UpcasterTypeError struct {
	FromType string
	Expected string
	Got      string
}

func (e *UpcasterTypeError) Error() string {
	return fmt.Sprintf("synapse: upcaster for %s expected %s, got %s",
		e.FromType, e.Expected, e.Got)
}

func (*UpcasterTypeError) Unwrap() error { return ErrUpcasterType }

// UpcasterCycleError is returned when the registered upcasters form
// a cycle or when [upcastMaxHops] is exceeded. Chain is the sequence
// of type names traversed, with the repeating type appended for
// visibility.
type UpcasterCycleError struct {
	Chain []string
}

func (e *UpcasterCycleError) Error() string {
	return fmt.Sprintf("synapse: upcaster cycle: %s",
		strings.Join(e.Chain, " -> "))
}

func (*UpcasterCycleError) Unwrap() error { return ErrUpcasterCycle }
