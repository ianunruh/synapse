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

The repo is a Go multi-module workspace. The root module is dep-free; sibling modules under `eventstore/`, `codec/`, etc. that need third-party deps live in their own go.mod files. `go.work` ties them together for local dev.

```
# Root module
go build ./...
go vet ./...
go test ./...
go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./...

# Sibling module (run from the module directory)
cd eventstore/sqlite && go test ./...
cd eventstore/sqlite && go vet ./...
cd eventstore/sqlite && go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./...

# Race detector (root, requires gcc)
CGO_ENABLED=1 go test -race ./...
```

Run modernize on both the root and any modified sibling modules before committing. Exit 0 with no output means clean.

## Conventions worth remembering

- **Error message brand prefix.** One brand: `"synapse:"`. No sub-paths (no `"synapse/postgres:"`, `"synapse/sqlite:"`, etc.) — mirrors `net/http`'s `"http:"`, not `crypto/rand`'s `"crypto/rand:"`. See ADR-0002.
  - **Apply the prefix at two places only:** (1) leaf errors (`errors.New("synapse: ...")`, the originating `fmt.Errorf`), and (2) wraps where `%w` wraps an error from *outside* synapse — a third-party library (`pgx`, `pgxpool`, `database/sql`, `encoding/json`, `google.golang.org/protobuf`, OS errors). Those wraps are effectively new synapse-layer error messages.
  - **Drop the prefix at internal wraps** — anywhere `fmt.Errorf("... %w", err)` wraps an already-branded synapse error (anything returned from another `es.*` function, sibling store, sibling codec). The brand is already in the chain; adding another produces `"synapse: ... synapse: ..."` noise. Just describe the operation: `fmt.Errorf("load checkpoint %q: %w", name, err)`.
  - Rule of thumb when reviewing a new `fmt.Errorf`: trace the `%w` argument. Third-party origin → brand it. Synapse origin → no brand.
- The root `synapse` package is doc-only — do not add re-exports there. See ADR-0002.
- New significant decisions get a numbered ADR under `docs/adr/`. Keep them short (Context / Decision / Consequences / Alternatives), and cross-link related ADRs.
