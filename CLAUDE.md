# CLAUDE.md

Context for Claude sessions working on synapse.

## Project

`github.com/ianunruh/synapse` is an open-source Go event sourcing / CQRS toolkit. The core lives in `synapse/es`. Optional concerns (codec implementations, store backends, future admin RPCs, future web UI) live in sibling subpackages or sibling modules so users import only what they need.

Architectural decisions are recorded in `docs/adr/`. Read the relevant ADR before relitigating a decision.

## Constraints from the project owner

These were stated at project start (or added during early planning) and must not be violated without explicit user agreement.

1. **Go 1.26 toolchain.** Use language features and stdlib through 1.26: `iter.Seq2`, `clear()`, generic type aliases, `testing/synctest`, `t.Context()`, `fmt.Appendf`, range-over-int, etc.

2. **Modernization hints from the linter applied at all times.** Run `go vet` and gopls modernize before claiming work is done. No `interface{}` (use `any`), no channels for streaming reads (use `iter.Seq2`), no callback-style iteration, no manual `for i := 0; i < n; i++` where `for i := range n` reads cleaner.

3. **Zero third-party dependencies in the core module.** The root `go.mod` stays clean. Optional concerns that require external libraries live as sibling Go modules under the same repo so users opt in.

4. **Core is serialization-agnostic.** `synapse/es` never imports any specific codec. Codecs register per event type via `es.Registry`.

5. **Type safety AND performance are co-equal goals.** When they point the same direction (most cases), take both. When they conflict, prefer the perf-friendly option in hot paths (Apply/replay, codec marshal/unmarshal, store I/O) and document the trade-off. Avoid interface dispatch and boxing inside hot loops.

6. **The API must be ergonomic.** Generic type parameters infect every signature touching them — avoid them at the core boundary unless the type-safety win is concrete and worth the call-site noise.

7. **Admin RPCs and web UI are optional opt-ins.** They will live as sibling subpackages. The core must not depend on them, and they must be removable without breaking the core.

## Commands

```
go build ./...
go vet ./...
go test ./...
CGO_ENABLED=1 go test -race ./...   # requires gcc
```

## Conventions worth remembering

- Error message strings use the `"synapse:"` brand prefix, not `"es:"`. Mirrors `net/http` returning `"http:"`. See ADR-0002.
- The root `synapse` package is doc-only — do not add re-exports there. See ADR-0002.
- New significant decisions get a numbered ADR under `docs/adr/`. Keep them short (Context / Decision / Consequences / Alternatives), and cross-link related ADRs.
