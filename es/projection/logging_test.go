package projection_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/es/projection"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
)

func TestRunner_OnError_Skip_LogsWarning(t *testing.T) {
	ctx := t.Context()

	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 1, 3) // 1 stream, 3 events

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	boom := errors.New("projection boom")
	var calls atomic.Int32
	proj := &recordingProjection{
		failOn: func(_ es.Envelope) error {
			if calls.Add(1) == 2 {
				return boom
			}
			return nil
		},
	}

	r := projection.NewRunner("log-skipper", store, reg, proj,
		projection.WithOnError(func(_ es.Envelope, _ error) bool { return true }),
		projection.WithLogger(logger),
	)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("expected WARN in log output, got: %s", out)
	}
	if !strings.Contains(out, "projection error, skipping event") {
		t.Errorf("expected skip message in log output, got: %s", out)
	}
	if !strings.Contains(out, "name=log-skipper") {
		t.Errorf("expected projection name in log output, got: %s", out)
	}
	if !strings.Contains(out, "err=") {
		t.Errorf("expected err attribute in log output, got: %s", out)
	}
}

func TestRunner_NoSkip_DoesNotLog(t *testing.T) {
	// All events succeed; logger should see nothing.
	ctx := t.Context()

	store := memory.New()
	reg := testdomain.NewRegistry()
	seedCounters(t, store, reg, 1, 3)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	proj := &recordingProjection{}
	r := projection.NewRunner("log-quiet", store, reg, proj,
		projection.WithLogger(logger),
	)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if out := buf.String(); out != "" {
		t.Errorf("expected empty log output on clean run, got: %s", out)
	}
}
