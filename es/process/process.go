package process

import (
	"context"

	"github.com/ianunruh/synapse/es"
)

// Correlate maps an inbound event to the process-manager stream id
// that should consume it. Returning the empty string skips the event —
// no aggregate is loaded, no handler runs, and the [projection.Runner]
// continues to the next event (its checkpoint still advances).
type Correlate func(env es.Envelope) es.StreamID

// Manager implements [es.Projection] by routing each inbound event to
// a process-manager aggregate M determined by a [Correlate] function.
// For every routed event, [Manager.Project] calls [es.Execute], which
// loads (or creates) the aggregate, runs the handler, and saves any
// pending events through the repository's middleware chain.
//
// Manager is not generic at the type level even though [New] is
// generic over M; the closure built by [New] captures the
// repository, the correlator, and the handler, so the user sees a
// non-generic *Manager at call sites. Wrap it in
// [projection.NewRunner] to subscribe and dispatch. See ADR-0032.
//
// Manager is safe for concurrent calls to [Manager.Project], with the
// same caveats as [es.Execute] on the underlying repository.
type Manager struct {
	project func(ctx context.Context, env es.Envelope) error
}

// New returns a [Manager] that drives the process-manager aggregate M.
// For every event delivered by a [projection.Runner]:
//
//   - id := correlate(env). If id is "", the event is skipped.
//   - [es.Execute] loads M at id (or builds a fresh one via the
//     Repository's newFn if the stream does not yet exist —
//     ADR-0030), runs handle, and saves any pending events.
//
// Inside handle the user mutates M via its domain methods (which
// record events on the PM's own stream) and is free to call
// [es.Execute] on other repositories or [commandbus.Bus.Dispatch] to
// emit commands to other aggregates — the causation/correlation
// context propagated by the runner (see ADR-0022) stamps every
// outbound event with the correct saga chain.
func New[M es.Aggregate](
	repo *es.Repository[M],
	correlate Correlate,
	handle es.Handler[es.Envelope, M],
) *Manager {
	return &Manager{
		project: func(ctx context.Context, env es.Envelope) error {
			id := correlate(env)
			if id == "" {
				return nil
			}
			return es.Execute(ctx, repo, id, env, handle)
		},
	}
}

// Project implements [es.Projection].
func (m *Manager) Project(ctx context.Context, env es.Envelope) error {
	return m.project(ctx, env)
}
