package commandbus

import (
	"errors"
	"fmt"
)

// ErrUnknownCommand is the sentinel returned (via [UnknownCommandError])
// when [Bus.Dispatch] is called with a name that has not been
// registered. Use [errors.Is] to classify the failure — typically a
// transport maps it to "no such route" (HTTP 404).
var ErrUnknownCommand = errors.New("synapse: command not registered")

// ErrDecode is the sentinel returned (via [DecodeError]) when the
// command's codec fails to decode the payload. Use [errors.Is] to
// classify it — typically a transport maps it to "bad request"
// (HTTP 400). Use [errors.As] with [*DecodeError] (or [errors.Unwrap])
// to recover the underlying codec error.
var ErrDecode = errors.New("synapse: command decode failed")

// UnknownCommandError carries the offending name. It wraps
// [ErrUnknownCommand].
type UnknownCommandError struct {
	Name string
}

func (e *UnknownCommandError) Error() string {
	return fmt.Sprintf("synapse: commandbus: command %q not registered", e.Name)
}

func (*UnknownCommandError) Unwrap() error { return ErrUnknownCommand }

// DecodeError carries the offending command name and the underlying
// codec error. It unwraps to both [ErrDecode] and the wrapped error so
// callers can match either via [errors.Is].
type DecodeError struct {
	Name string
	Err  error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("synapse: commandbus: decode %q: %s", e.Name, e.Err)
}

func (e *DecodeError) Unwrap() []error { return []error{ErrDecode, e.Err} }
