package idgen_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ianunruh/synapse/idgen"
)

// Static interface assertions.
var (
	_ idgen.Generator = idgen.UUIDv7{}
	_ idgen.Generator = idgen.GeneratorFunc(nil)
)

func TestUUIDv7_FormatShape(t *testing.T) {
	id := idgen.UUIDv7{}.NewEventID()
	if len(id) != 36 {
		t.Errorf("len = %d, want 36, id = %q", len(id), id)
	}
	for _, pos := range []int{8, 13, 18, 23} {
		if id[pos] != '-' {
			t.Errorf("dash at index %d missing: %q", pos, id)
		}
	}
	if id[14] != '7' {
		t.Errorf("version nibble = %q, want '7' (UUIDv7)", string(id[14]))
	}
	switch id[19] {
	case '8', '9', 'a', 'b':
	default:
		t.Errorf("variant nibble = %q, want one of 8/9/a/b", string(id[19]))
	}
}

func TestUUIDv7_EncodedTimestamp(t *testing.T) {
	fixed := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	g := idgen.UUIDv7{Now: func() time.Time { return fixed }}
	id := g.NewEventID()

	want := fmt.Sprintf("%012x", uint64(fixed.UnixMilli()))
	got := id[:8] + id[9:13]
	if got != want {
		t.Errorf("timestamp prefix = %q, want %q (full id %q)", got, want, id)
	}
}

func TestUUIDv7_DefaultClockIsTimeNow(t *testing.T) {
	// Without Now set, two calls bracketing time.Now should produce a
	// timestamp inside the bracket.
	before := time.Now()
	id := idgen.UUIDv7{}.NewEventID()
	after := time.Now()

	var ts uint64
	if _, err := fmt.Sscanf(id[:8]+id[9:13], "%012x", &ts); err != nil {
		t.Fatalf("Sscanf: %v", err)
	}
	idTime := time.UnixMilli(int64(ts))
	if idTime.Before(before.Add(-time.Millisecond)) || idTime.After(after.Add(time.Millisecond)) {
		t.Errorf("id timestamp %v outside bracket [%v, %v]", idTime, before, after)
	}
}

func TestUUIDv7_TimestampSortable(t *testing.T) {
	earlier := time.Unix(1_700_000_000, 0)
	later := time.Unix(1_700_000_001, 0)

	a := idgen.UUIDv7{Now: func() time.Time { return earlier }}.NewEventID()
	b := idgen.UUIDv7{Now: func() time.Time { return later }}.NewEventID()
	if a >= b {
		t.Errorf("earlier id %q should sort before later id %q", a, b)
	}
}

func TestUUIDv7_Uniqueness(t *testing.T) {
	g := idgen.UUIDv7{}
	const N = 4096
	seen := make(map[string]struct{}, N)
	for range N {
		id := g.NewEventID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id after %d generations: %q", len(seen), id)
		}
		seen[id] = struct{}{}
	}
}

func TestUUIDv7_ConcurrentSafe(t *testing.T) {
	g := idgen.UUIDv7{}
	const N = 256

	results := make(chan string, N)
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() { results <- g.NewEventID() })
	}
	wg.Wait()
	close(results)

	seen := make(map[string]struct{}, N)
	for id := range results {
		if _, dup := seen[id]; dup {
			t.Errorf("duplicate id under concurrency: %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != N {
		t.Errorf("got %d unique ids, want %d", len(seen), N)
	}
}

func TestGeneratorFunc(t *testing.T) {
	counter := 0
	var g idgen.Generator = idgen.GeneratorFunc(func() string {
		counter++
		return fmt.Sprintf("id-%d", counter)
	})

	if got := g.NewEventID(); got != "id-1" {
		t.Errorf("first call: got %q, want id-1", got)
	}
	if got := g.NewEventID(); got != "id-2" {
		t.Errorf("second call: got %q, want id-2", got)
	}
}
