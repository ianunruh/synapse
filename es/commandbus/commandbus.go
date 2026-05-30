package commandbus

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/ianunruh/synapse/es"
)

// Command is implemented by every command type registered with a [Bus].
// [Bus.Dispatch] decodes the payload, then reads the target stream from
// the command itself — keeping commands self-contained and freeing
// transports from extracting the id separately. See ADR-0028.
type Command interface {
	AggregateID() es.StreamID
}

// Bus routes named, byte-encoded commands to typed [es.Handler]
// implementations through their [es.Repository] (and, transitively,
// through the repository's middleware chain).
//
// A Bus is safe for concurrent use. Registration is expected at startup;
// [Bus.Dispatch] runs concurrently. See ADR-0028.
type Bus struct {
	mu       sync.RWMutex
	entries  map[string]entry
	dispatch Operation // pre-built chain of middleware wrapping the core lookup
}

// Operation is the type-erased shape of [Bus.Dispatch]. It is what
// [Middleware] wraps. See [WithMiddleware] and ADR-0029.
type Operation func(ctx context.Context, name string, payload []byte) error

// Middleware wraps an [Operation] to add behavior before, after, or
// around dispatch — for example logging, panic recovery, or per-call
// deadlines. Middlewares compose left-to-right as outer wrappers:
// WithMiddleware(A, B, C) yields A wrapping B wrapping C wrapping the
// core lookup. See ADR-0029.
//
// Bus middleware operates at the dispatch boundary
// (ctx, name, []byte) and is orthogonal to [es.Middleware], which
// operates at the aggregate boundary (ctx, [es.StreamID]) and lives on
// the [es.Repository] (ADR-0012). The two compose without ever
// double-wrapping.
type Middleware func(next Operation) Operation

// entry is the non-generic value stored in the map. The closure inside
// captures the command and aggregate type parameters, the repository,
// the handler, and the codec — so dispatch is a single map lookup plus
// the closure call, with no type assertion on the hot path.
type entry struct {
	run func(ctx context.Context, data []byte) error
}

type options struct {
	middleware []Middleware
}

// Option configures a [Bus] at construction time.
type Option func(*options)

// WithMiddleware installs bus-level middleware that wraps every
// [Bus.Dispatch] call. Successive calls accumulate; middlewares compose
// left-to-right as outer wrappers, matching [es.WithMiddleware]'s
// convention (ADR-0012). See ADR-0029 for the built-ins and how bus
// middleware composes with repository middleware.
func WithMiddleware(mws ...Middleware) Option {
	return func(o *options) { o.middleware = append(o.middleware, mws...) }
}

// New returns an empty [Bus] ready to be populated with [Register]. Any
// middleware supplied via [WithMiddleware] is composed once here; per-
// Dispatch work is one chain call.
func New(opts ...Option) *Bus {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	b := &Bus{entries: make(map[string]entry)}
	core := func(ctx context.Context, name string, payload []byte) error {
		e, ok := b.lookup(name)
		if !ok {
			return &UnknownCommandError{Name: name}
		}
		return e.run(ctx, payload)
	}
	b.dispatch = chain(o.middleware, core)
	return b
}

// chain wraps op with mws so that the leftmost middleware becomes the
// outermost wrapper, matching [es.Middleware] composition (ADR-0012).
func chain(mws []Middleware, op Operation) Operation {
	for _, mw := range slices.Backward(mws) {
		op = mw(op)
	}
	return op
}

// Register binds name to a typed handler. The command type C must
// implement [Command]; the aggregate type A must implement [es.Aggregate].
// codec decodes payload bytes into a C; the closure then reads the
// target stream from c.AggregateID() and calls [es.Execute], which
// applies the repository's middleware chain.
//
// Register panics at startup if name is already registered, repo is nil,
// or codec is nil — these are programmer errors that surface only at
// dispatch time otherwise. See ADR-0028 for the rationale on panicking
// (vs. [es.Registry]'s silent last-wins).
func Register[C Command, A es.Aggregate](
	b *Bus,
	name string,
	repo *es.Repository[A],
	h es.Handler[C, A],
	codec es.TypedCodec[C],
) {
	if repo == nil {
		panic(fmt.Sprintf("synapse: commandbus: Register(%q): nil repository", name))
	}
	if codec == nil {
		panic(fmt.Sprintf("synapse: commandbus: Register(%q): nil codec", name))
	}

	e := entry{
		run: func(ctx context.Context, data []byte) error {
			c, err := codec.Unmarshal(data)
			if err != nil {
				return &DecodeError{Name: name, Err: err}
			}
			return es.Execute(ctx, repo, c.AggregateID(), c, h)
		},
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.entries[name]; exists {
		panic(fmt.Sprintf("synapse: commandbus: command %q already registered", name))
	}
	b.entries[name] = e
}

// Dispatch decodes payload into the command registered under name and
// executes it against the registered repository. The target stream is
// taken from the decoded command's [Command.AggregateID] method.
//
// Dispatch returns:
//
//   - *[UnknownCommandError] (wrapping [ErrUnknownCommand]) if name has
//     no registered handler.
//   - *[DecodeError] (wrapping [ErrDecode] and the codec's error) if the
//     payload fails to decode.
//   - any error returned by the handler or by [es.Execute], including
//     *[es.ConflictError] and *[es.StreamNotFoundError], propagated
//     verbatim so transports can classify them with [errors.Is] /
//     [errors.As].
func (b *Bus) Dispatch(ctx context.Context, name string, payload []byte) error {
	return b.dispatch(ctx, name, payload)
}

// Names returns the command names currently registered, in unspecified
// order. The returned slice is a fresh copy and may be retained.
func (b *Bus) Names() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.entries))
	for n := range b.entries {
		out = append(out, n)
	}
	return out
}

// lookup returns the entry for name under the read lock and releases it
// before the caller runs the closure, so a slow handler does not block
// concurrent registrations or dispatches.
func (b *Bus) lookup(name string) (entry, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	e, ok := b.entries[name]
	return e, ok
}
