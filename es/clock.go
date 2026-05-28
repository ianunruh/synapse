package es

import "time"

// Clock abstracts the wall-clock source used by the [Repository] to
// stamp RecordedAt on outgoing events. Tests can supply a virtual
// clock (for instance via testing/synctest) to make timestamps
// deterministic.
type Clock interface {
	// NowUTC returns the current time in UTC.
	NowUTC() time.Time
}

// SystemClock is a [Clock] backed by time.Now. It is the default for
// constructors that take an optional clock.
type SystemClock struct{}

// NowUTC implements [Clock].
func (SystemClock) NowUTC() time.Time { return time.Now().UTC() }
