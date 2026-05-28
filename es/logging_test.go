package es_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/ianunruh/synapse/es"
	"github.com/ianunruh/synapse/eventstore/memory"
	"github.com/ianunruh/synapse/internal/testdomain"
	snapmem "github.com/ianunruh/synapse/snapshotstore/memory"
)

func TestRepository_Save_SnapshotFailure_LogsWarning(t *testing.T) {
	// Configure the Repository so the automatic snapshot path runs
	// AND fails: WithSnapshotStore + WithSnapshotPolicy that always
	// fires, but a registry missing the snapshot codec. trySaveSnapshot
	// returns *CodecNotFoundError; Save should log it at Warn level
	// and return nil.
	ctx := t.Context()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	repo := es.NewRepository(memory.New(),
		testdomain.NewRegistryWithoutSnapshot(),
		testdomain.NewCounter,
		es.WithSnapshotStore(snapmem.New()),
		es.WithSnapshotPolicy(es.EveryNVersions(1)),
		es.WithLogger(logger))

	c := testdomain.NewCounter(testdomain.CounterStream)
	if err := c.Increment(1); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v (events should have committed despite snapshot failure)", err)
	}

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("expected WARN level in log output, got: %s", out)
	}
	if !strings.Contains(out, "snapshot save failed") {
		t.Errorf("expected 'snapshot save failed' in log output, got: %s", out)
	}
	if !strings.Contains(out, "stream="+string(testdomain.CounterStream)) {
		t.Errorf("expected stream attribute in log output, got: %s", out)
	}
	if !strings.Contains(out, "err=") {
		t.Errorf("expected err attribute in log output, got: %s", out)
	}
}

func TestRepository_Save_SnapshotSuccess_DoesNotLog(t *testing.T) {
	// With a fully populated registry, the snapshot succeeds and the
	// logger should see nothing.
	ctx := t.Context()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	repo := es.NewRepository(memory.New(),
		testdomain.NewRegistry(),
		testdomain.NewCounter,
		es.WithSnapshotStore(snapmem.New()),
		es.WithSnapshotPolicy(es.EveryNVersions(1)),
		es.WithLogger(logger))

	c := testdomain.NewCounter(testdomain.CounterStream)
	if err := c.Increment(1); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if out := buf.String(); out != "" {
		t.Errorf("expected empty log output on successful snapshot, got: %s", out)
	}
}
