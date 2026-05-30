# ADR-0002: Package layout — core lives in `synapse/es`

**Status:** Accepted (2026-05-28)

## Context

The toolkit will eventually include sibling concerns beyond the core event-sourcing types: admin RPCs, a web UI, projection/read-model machinery, codec implementations, store backends, ID generators. We had to decide whether the root package holds the core types or whether the root is a doc-only entry point and the core lives in a subpackage.

## Decision

- Core types live in `synapse/es`.
- The root `synapse` package is doc-only.
- Sibling concerns are added as `synapse/<concern>` subpackages: `synapse/codec/json`, `synapse/codec/proto`, `synapse/eventstore/memory`, `synapse/eventstore/sqlite`, `synapse/eventstore/postgres`, `synapse/idgen`, `synapse/es/middleware`, `synapse/es/projection`, `synapse/es/commandbus`, plus the matching snapshot- and checkpoint-store backends. Future: `synapse/admin`, `synapse/web`.
- Error message strings use the `"synapse: ..."` brand prefix rather than `"es: ..."`. This mirrors `net/http` returning `"http: ..."`: one short brand identifies the library to a reader of a log line, not the internal package short-name. No sub-package variants (no `"synapse/postgres: ..."`); apply the brand at leaves and at wraps over third-party errors only — internal wraps over already-branded errors drop the prefix so chains don't compound `"synapse: ... synapse: ..."`. See CLAUDE.md "Conventions worth remembering" for the full rule.

## Consequences

- All subpackages have meaningful short-name qualifiers at call sites (`es.Aggregate`, future `admin.Server`, `projection.Runner`); the root carries the brand.
- Uniform layout: no package is "special" by being at the root.
- One extra path segment in imports (`synapse/es` vs `synapse`).
- The root may evolve into a meta-package that re-exports nothing; tempting "convenience re-exports" must be resisted (Go aliases multiply imports without simplifying them).
- Pre-1.0 the refactor cost minutes; post-1.0 it would have required every consumer to update imports and qualifiers.

## Alternatives considered

- **Keep the core at root, add siblings later.** Rejected because the resulting layout is non-uniform and post-1.0 rename costs are high.
- **Name the subpackage `eventsource` or `core`.** Rejected: `eventsource` is verbose at every call site; `core` duplicates the brand idea and becomes meaningless once admin/web exist. `es` is short, conventional in this domain, and reads cleanly in qualified identifiers.
