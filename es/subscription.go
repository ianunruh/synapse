package es

import (
	"context"
	"iter"
)

// SubscriptionOptions controls a [SubscribableEventStore.Subscribe] or
// [SubscribableEventStore.SubscribeStream] call.
type SubscriptionOptions struct {
	// From is the position to start at. For
	// [SubscribableEventStore.Subscribe] this is interpreted as a
	// [RawEnvelope.GlobalPosition]; for
	// [SubscribableEventStore.SubscribeStream] it is a
	// [RawEnvelope.Version] within the targeted stream. Events with
	// the relevant position > From are yielded; 0 means start from
	// the beginning.
	From uint64

	// Live, when true, blocks waiting for new events after yielding
	// all existing events past From. When false, the iterator
	// terminates cleanly once caught up.
	Live bool
}

// SubscribableEventStore is the optional capability an [EventStore]
// implements to support live subscriptions. Backends that only
// support catch-up reads through [EventStore.Load] can omit it;
// consumers that need live tail (e.g., the projection.Runner) will
// fail to type-assert against them.
type SubscribableEventStore interface {
	EventStore

	// Subscribe yields events across all streams in global append
	// order, starting at the position past opts.From. When opts.Live
	// is true, the iterator blocks waiting for new events after
	// exhausting the existing log; otherwise it terminates when
	// caught up.
	//
	// Context cancellation terminates the iterator with a single
	// terminal (zero, ctx.Err()) yield.
	Subscribe(ctx context.Context, opts SubscriptionOptions) iter.Seq2[RawEnvelope, error]

	// SubscribeStream is the per-stream variant of [Subscribe]. It
	// yields only events for stream, ordered by [RawEnvelope.Version].
	// opts.From is interpreted as a stream Version (the iterator
	// yields events with Version > opts.From).
	SubscribeStream(ctx context.Context, stream StreamID, opts SubscriptionOptions) iter.Seq2[RawEnvelope, error]
}
