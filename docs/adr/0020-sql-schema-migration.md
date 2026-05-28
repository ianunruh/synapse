# ADR-0020: Schema migration is opt-out in SQL backends

**Status:** Accepted (2026-05-28)
**Relates to:** [ADR-0016 Constructor pattern](0016-constructor-pattern.md), [ADR-0017 SQLite event-store backend](0017-sqlite-backend.md), [ADR-0019 SQLite snapshot store](0019-sqlite-snapshot-store.md)

## Context

The SQLite backends originally ran `CREATE TABLE IF NOT EXISTS …` inside `New(ctx, db)`. That is friendly for tests, examples, and quick demos: import the package, open a `*sql.DB`, call `New`, and the schema appears. No setup steps to forget.

For production usage that pattern fights with the rest of the Go ecosystem. Teams typically own schema lifecycle via a dedicated tool — goose, golang-migrate, atlas, sql-migrate, sqlc/dbmate, an internal wrapper — so the application code does not touch DDL at runtime. Our library running an extra `CREATE TABLE IF NOT EXISTS` is benign but unwanted: it muddies the audit trail, it can race with deploy-time migration tooling, and it expresses an opinion the user has already decided otherwise on.

We need both audiences served without shifting the cost onto either.

## Decision

Schema management on SQL backends is **opt-out, default-on**. Each SQL backend exposes three knobs:

```go
// Exported so external tools can apply the schema:
//   go:embed schema.sql
var Schema string

// Standalone runner (idempotent, same SQL):
func Migrate(ctx context.Context, db *sql.DB) error

// Opt-out option per ADR-0016 conventions:
func WithoutMigrate() Option

// Default behavior: New applies Schema unless WithoutMigrate is passed.
func New(ctx context.Context, db *sql.DB, opts ...Option) (*Store, error)
```

Three usage patterns are now first-class:

```go
// 1. Quick start (tests, examples): default migrate
store, _ := sqlitestore.New(ctx, db)

// 2. Explicit migration step (deploy-time runner)
sqlitestore.Migrate(ctx, db)
store, _ := sqlitestore.New(ctx, db, sqlitestore.WithoutMigrate())

// 3. External tooling owns the schema
//    (goose, golang-migrate, atlas etc. apply sqlitestore.Schema)
store, _ := sqlitestore.New(ctx, db, sqlitestore.WithoutMigrate())
```

This rule applies to both `eventstore/sqlite` and `snapshotstore/sqlite`, and is the established pattern for any future SQL backend (Postgres, MySQL, etc.).

### What's inside the option

Functional option per [ADR-0016](0016-constructor-pattern.md). Internal `options` struct holds `skipMigrate bool`. The option name `WithoutMigrate` is a negative form because the default *is* to migrate; the opt-out reads as "without that step." `With(SkipMigrate=true)` and `WithMigrate(false)` were both considered and rejected as more verbose for the only case anyone actually writes.

### Backward compatibility

Existing callers using `New(ctx, db)` continue to migrate. No code change is required. The new option is purely additive.

### Naming of `Schema`

Capitalized, package-level variable, embedded via `go:embed schema.sql`. Users can read it as a string, hash it, log it, or pipe it into their migration tool. The variable doc points at `Migrate` and `WithoutMigrate` so the relationship is discoverable.

## Consequences

- The default behavior matches what most first-time readers expect (test code, the example programs, the doc examples in package comments still "just work").
- Production teams own the schema lifecycle without arguing with us: `WithoutMigrate()` is the single line they add.
- The `Schema` export means tooling integration is free — no need for users to reimplement the DDL or copy it from our source code, and no risk of it drifting from what the runtime expects.
- `Migrate` is idempotent (CREATE TABLE IF NOT EXISTS), so calling it multiple times or alongside `WithoutMigrate` is safe. The test `TestMigrate_Idempotent` enforces this.
- Adding a future Postgres or MySQL backend follows the same three-knob recipe (Schema + Migrate + WithoutMigrate), so users only have to learn the pattern once.

## Alternatives considered

- **Default-off, opt-in `WithMigrate()`.** Matches the Go ecosystem convention (libraries do not auto-migrate). Rejected because it changes existing behavior silently, breaks the demos / examples without recourse, and trades a small benefit for a friction tax on every first-time reader.
- **Drop the auto-migrate entirely, keep only `Migrate`.** Strictest separation, cleanest semantics, but the worst onboarding story.
- **Versioned migrations table inside the package.** Useful for libraries that own schema evolution across releases. Out of scope: the current schemas are tiny single-table designs; adding a versioning system before there's a v2 schema would be premature.
- **Pass the migration tool as a dependency** (e.g., an `Applier` interface that runs the schema). Rejected as over-abstraction; users with a tool already use it externally and pass `WithoutMigrate()`.
