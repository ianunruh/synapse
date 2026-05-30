# ADR-0028: CommandBus — dynamic command routing for transports

**Status:** Accepted (2026-05-29)
**Relates to:** [ADR-0009 Command handler](0009-command-handler.md) (implements its deferred bus), [ADR-0007 Codec registry](0007-codec-registry.md) (precedent for the typed→untyped erasure), [ADR-0012 Command middleware](0012-command-middleware.md), [ADR-0002 Package layout](0002-package-layout.md), [ADR-0016 Constructor pattern](0016-constructor-pattern.md), [ADR-0010 Error model](0010-error-model.md)

## Context

ADR-0009 shipped the typed command primitives — `Handler[C, A]` and the `Execute[C, A]` load-handle-save convenience — and explicitly **deferred a dynamic `CommandBus` to a future subpackage**. It named the alternative the bus would adopt: a `Command` interface exposing `AggregateID() StreamID`, so commands self-describe their target stream once decoded.

That deferral is now blocking the transport story. A service receiving commands over HTTP or gRPC arrives at a uniform `(name string, payload []byte)` boundary and needs to route each to its typed `Execute[C, A]` call without writing a per-route adapter by hand. The toolkit's typed layer is exactly the right primitive *underneath* — what's missing is the registry that turns a name + bytes into a typed call.

## Decision

Build the bus as a sibling driver subpackage at `es/commandbus`, alongside `es/projection` and `es/middleware` — in the root module, depending only on `es`, no third-party deps. Mirror the proven `typedAdapter[E]` erasure pattern from `es/codec.go` (ADR-0007): generics at registration where types are statically known; a non-generic boundary at dispatch.

### Public surface

```go
package commandbus

type Command interface {
    AggregateID() es.StreamID
}

type Bus struct { /* mu sync.RWMutex; entries map[string]entry */ }
type Option func(*options) // variadic in v0 per ADR-0016; no concrete options yet

func New(opts ...Option) *Bus

func Register[C Command, A es.Aggregate](
    b *Bus,
    name string,
    repo *es.Repository[A],
    h es.Handler[C, A],
    codec es.TypedCodec[C],
)

func (b *Bus) Dispatch(ctx context.Context, name string, payload []byte) error
func (b *Bus) Names() []string
```

### Erasure mechanics

A non-generic `entry struct { run func(ctx context.Context, data []byte) error }` is stored in the map. `Register[C, A]` builds the closure with `C`/`A`/`repo`/`h`/`codec` baked in:

```go
run: func(ctx context.Context, data []byte) error {
    c, err := codec.Unmarshal(data)                          // -> C, never any
    if err != nil { return &DecodeError{Name: name, Err: err} }
    return es.Execute(ctx, repo, c.AggregateID(), c, h)      // typed throughout
}
```

`C` never escapes the closure as `any`, so the dispatch path has **no type assertion** — the only erasure is the variadic-return on `codec.Unmarshal`'s call into the boxed interface, which the compiler types back to `C` immediately. This is strictly cleaner than the codec registry's `typedAdapter.Marshal` (which must do a `payload.(E)` assertion because payloads enter at the untyped boundary); here payloads enter as `[]byte` and become `C` exactly once.

### Self-routing commands

Commands implement `commandbus.Command` and return their target `es.StreamID` from `AggregateID()`. The bus reads it after decoding, so a transport never has to extract an id separately from the body — the command is self-contained. This is the alternative ADR-0009 named.

### Error model

Two sentinels and two typed wrappers, following the `es/errors.go` brand-prefix pattern.

```go
var (
    ErrUnknownCommand = errors.New("synapse: command not registered")
    ErrDecode         = errors.New("synapse: command decode failed")
)

type UnknownCommandError struct{ Name string }
type DecodeError        struct{ Name string; Err error }
```

`DecodeError.Unwrap() []error` returns both `ErrDecode` and the underlying codec error (Go 1.20+ multi-unwrap), so `errors.Is(err, ErrDecode)` and `errors.Is(err, originalCodecErr)` both succeed. Transports can therefore map cleanly: unknown route → 404, malformed body → 400, handler/conflict errors → 5xx/422 via the existing `*es.ConflictError` / `es.ErrConflict` sentinels that `Execute` already propagates.

### Duplicate registration panics

A duplicate `Register` call for the same name panics at startup with a clear `"synapse: commandbus: command %q already registered"` message. This **diverges** from `es.Registry.Register`, which silently last-wins. The rationale: a duplicate codec swap is generally a harmless re-init, but a duplicate route silently re-mapping a command to a different handler is the kind of bug that surfaces only in production. Registration is always startup-time, so a panic at init is the right loud failure.

Nil `repo` and nil `codec` arguments to `Register` also panic for the same fail-fast reason — they'd otherwise nil-deref deep inside `es.Execute` on first dispatch.

### Middleware is the Repository's concern, not the bus's

`es.Execute` already wraps the load-handle-save operation in the repository's middleware chain (ADR-0012). `Dispatch` calls `es.Execute` exactly once per command and adds **no wrapping of its own**, so `WithMiddleware(PerAggregateLocking(), Retry(...))` on the repo applies identically whether a command arrived through `es.Execute` directly or through the bus — neither doubled nor skipped. Bus-level middleware (auth, request logging) is intentionally out of v0; users wrap their HTTP/gRPC handler or repo as appropriate.

### Context propagation

`Dispatch` passes the caller's `ctx` straight through. `Repository.Save` already stamps causation, correlation, and metadata from the context, so a transport that wraps `ctx` with `es.WithCorrelation` / `es.WithCausation` / `es.WithMetadata` *before* calling `Dispatch` gets full saga chaining without the bus knowing anything about it.

## Consequences

- Transports gain a uniform `(name, payload)` → typed command dispatch in ~150 lines of bus code, with zero changes to the `es` core. Commands carry their target stream id; transports don't have to extract it.
- The bus is generic at registration only — call sites that already type-check (`PlaceHandler` for `PlaceOrder` against `*Order`) are still type-checked. Dispatch crosses the dynamic boundary exactly once, at `codec.Unmarshal`.
- All existing middleware (`PerAggregateLocking`, `Retry`, future logging/tracing) applies to bus-dispatched commands with no additional wiring — the Repository owns middleware, the bus owns routing. Clean separation.
- The transport status-mapping use case is fully served by `errors.Is(err, ErrUnknownCommand)` / `errors.Is(err, ErrDecode)` / `errors.Is(err, es.ErrConflict)`. Handler errors propagate verbatim.
- Commands must implement `Command` (a one-method interface). For aggregates that already key on a domain id field (most do), this is a one-line method. ADR-0009 weighed this cost and named it the right choice for the bus.
- The bus has a single implementation, so there is no `commandbustest` contract harness (ADR-0018's harness pattern exists for *interchangeable* backends/codecs, not one-offs).

## Alternatives considered

- **Caller-supplied `StreamID` at `Dispatch`** (`Dispatch(ctx, name, id, payload)`). Simpler — no `Command` interface on user types — and natural when a REST route already names the id. Rejected: it forces the transport to parse the id out of the URL *and* the body separately for any command that needs to also embed the id in its payload (most do, for replay and audit). Self-routing commands keep the boundary single-source-of-truth and match ADR-0009's named alternative.
- **Extractor function at registration** (`Register(..., func(C) StreamID)`). Avoids the interface but adds a registration argument every call site has to pass. The interface is one line of code per command type; the extractor is one line of code per *registration*. Same cost, less ergonomic.
- **`DispatchValue(ctx, name, id, cmd any)`** for already-typed in-process callers. Rejected for v0: in-process callers should use `es.Execute` directly — it's *more* type-safe (the compiler enforces `C`/`A` pairing) and has no runtime assertion. The bus exists for the untyped transport boundary; adding a typed-in-process path reintroduces the `cmd.(C)` assertion and a command-type error for no win. Can be added non-breakingly later if a name-based replay tool needs it.
- **Silent last-wins on duplicate registration**, matching `es.Registry`. Rejected: routing errors are higher-stakes than codec swaps; loud failure at startup beats silent miswiring at runtime.
- **Bus-level middleware** wrapping `Dispatch`. Rejected for v0: middleware lives on the `Repository` per ADR-0012, and adding a second wrapping layer at the bus invites double-wrap bugs (which middleware is the lock? the retry?). Transports can wrap the HTTP handler or compose at the repo. Revisit if a use case shows up that the repo layer can't serve.
- **Reusing `es.Registry` for command codecs.** Rejected: events and commands are different concerns. The command codec is registered per-command in the bus and lives only inside the entry closure; no conflation with the event registry's `Lookup` / `Upcast`.
- **A `commandbustest` contract harness** mirroring `eventstoretest` / `codectest`. Rejected per ADR-0018's rationale: contract suites exist for interchangeable backends. The bus has one implementation; tests live next to it.
- **Bus owns command-name → schema mapping for the transport** (e.g. an OpenAPI/protobuf descriptor registry). Out of scope: that's a transport-adapter concern, layered above the bus.
