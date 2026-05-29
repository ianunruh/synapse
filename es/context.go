package es

import (
	"context"
	"maps"
)

// ctxKey is unexported so external code cannot collide with our keys.
type ctxKey int

const (
	ctxKeyCorrelation ctxKey = iota
	ctxKeyCausation
	ctxKeyMetadata
)

// WithCorrelation returns a child context carrying id as the
// correlation identifier for events recorded under it. [Repository]
// stamps it onto each saved event's Correlation field where that
// field is otherwise empty; an explicit value on the [Envelope] takes
// precedence.
func WithCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyCorrelation, id)
}

// WithCausation returns a child context carrying id as the causation
// identifier. The same precedence rule as for correlation applies:
// per-event values win over the context value at [Repository.Save]
// time.
func WithCausation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyCausation, id)
}

// WithMetadata returns a child context carrying meta as event
// metadata. Successive calls merge; on key collision, the later call
// wins. At [Repository.Save] time the context map is the base and the
// per-event [Envelope.Metadata] map overrides on key collision, so
// callers can establish a baseline (user, trace id) for a request and
// still tag individual events explicitly.
func WithMetadata(ctx context.Context, meta Metadata) context.Context {
	if len(meta) == 0 {
		return ctx
	}
	prev, _ := ctx.Value(ctxKeyMetadata).(Metadata)
	next := make(Metadata, len(prev)+len(meta))
	maps.Copy(next, prev)
	maps.Copy(next, meta)
	return context.WithValue(ctx, ctxKeyMetadata, next)
}

func correlationFromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyCorrelation).(string)
	return id
}

func causationFromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyCausation).(string)
	return id
}

func metadataFromContext(ctx context.Context) Metadata {
	md, _ := ctx.Value(ctxKeyMetadata).(Metadata)
	return md
}

// mergeMetadata returns a fresh Metadata with base keys first and
// override keys on top. Returns nil when both inputs are empty so the
// store sees a nil Metadata for events with no annotations.
func mergeMetadata(base, override Metadata) Metadata {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(Metadata, len(base)+len(override))
	maps.Copy(out, base)
	maps.Copy(out, override)
	return out
}
