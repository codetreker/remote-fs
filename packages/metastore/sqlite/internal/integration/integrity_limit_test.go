package integration_test

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func integrityOptions(limit int64) sqlite.Options {
	return sqlite.Options{Window: sqlite.DefaultWindow(), MaxIntegrityRecords: limit}
}

func integrityByteOptions(limit int64) sqlite.Options {
	return sqlite.Options{Window: sqlite.DefaultWindow(), MaxIntegrityBytes: limit}
}

func TestIntegrityWorkLimitAcceptsItsBoundaryAndRefusesTheNextRecord(t *testing.T) {
	path := database(t)
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "two"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// One namespace, three nodes, two entries, one log row, and four retained changes.
	atBoundary, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityOptions(11))
	if err != nil {
		t.Fatalf("opening at the exact integrity work limit: %v", err)
	}
	if err := atBoundary.Close(); err != nil {
		t.Fatal(err)
	}

	overLimit, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityOptions(10))
	if err == nil {
		overLimit.Close()
		t.Fatal("opening one integrity record above the limit succeeded")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("opening one integrity record above the limit: %v, want EFBIG", err)
	}
}

func TestObjectStatusRefusesIntegrityWorkAboveItsConfiguredLimit(t *testing.T) {
	store, err := sqlite.OpenWithOptions(
		t.Context(), database(t), "workspace", 0, integrityOptions(5),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Create(t.Context(), "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "two"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("status one integrity record above its limit: %v, want EFBIG", err)
	}
	if _, err := store.Stat(t.Context(), "one"); err != nil {
		t.Fatalf("a refused status changed the namespace: %v", err)
	}
}

func TestIntegrityWorkCountIncludesForeignLabelsTouchingTheNamespace(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenWithOptions(
		t.Context(), path, "workspace", 0, integrityOptions(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	neighbour, err := sqlite.Open(t.Context(), path, "neighbour", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := neighbour.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		INSERT INTO entries (namespace, parent, name, node)
		SELECT foreign_ns.id, local_ns.root, names.name, foreign_ns.root
		FROM namespaces local_ns, namespaces foreign_ns,
			(SELECT CAST('hidden-one' AS BLOB) AS name UNION ALL SELECT CAST('hidden-two' AS BLOB)) names
		WHERE local_ns.name = 'workspace' AND foreign_ns.name = 'neighbour'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("foreign-labeled edges above the integrity limit returned %v, want EFBIG", err)
	}
}

func TestCanceledObjectStatusDoesNotPoisonTheStore(t *testing.T) {
	store, err := sqlite.Open(t.Context(), database(t), "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.ObjectStatus(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled integrity status returned %v, want context.Canceled", err)
	}
	if _, err := store.ObjectStatus(t.Context()); err != nil {
		t.Fatalf("status after cancellation: %v", err)
	}
}

func TestLegacyIntegrityWorkLimitRefusesBeforeMigration(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	for _, version := range versions {
		t.Run(version.name, func(t *testing.T) {
			path := database(t)
			version.write(t, path)
			before := schemaOf(t, path)
			store, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityOptions(5))
			if err == nil {
				store.Close()
				t.Fatal("migrating one integrity record above the limit succeeded")
			}
			if !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("migrating one integrity record above the limit: %v, want EFBIG", err)
			}
			if after := schemaOf(t, path); after != before {
				t.Fatalf("the refused integrity limit changed schema version %d", version.version)
			}
		})
	}
}

func TestCanceledLegacyOpenDoesNotMigrate(t *testing.T) {
	path := database(t)
	writeVersionTwo(t, path)
	before := schemaOf(t, path)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	store, err := sqlite.OpenWithOptions(ctx, path, "workspace", 0, integrityOptions(6))
	if err == nil {
		store.Close()
		t.Fatal("a canceled legacy open succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled legacy open returned %v, want context.Canceled", err)
	}
	if after := schemaOf(t, path); after != before {
		t.Fatal("a canceled legacy open changed the schema")
	}
}

func TestIntegrityByteLimitAcceptsItsExactBoundary(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	for _, name := range []string{"a", "bc"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Entry names and their Created log records each retain three bytes.
	atBoundary, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityByteOptions(6))
	if err != nil {
		t.Fatalf("opening at the exact integrity byte limit: %v", err)
	}
	if err := atBoundary.Close(); err != nil {
		t.Fatal(err)
	}
	over, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityByteOptions(5))
	if err == nil {
		over.Close()
		t.Fatal("opening one byte above the integrity limit succeeded")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("opening one byte above the integrity limit: %v, want EFBIG", err)
	}
}

func TestOversizedCorruptNameIsRejectedByLengthAdmission(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		UPDATE entries
		SET name = CAST(zeroblob(8388608) AS BLOB)
		WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
		  AND name = CAST('file' AS BLOB)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityByteOptions(1024))
	if err == nil {
		opened.Close()
		t.Fatal("opening an oversized corrupt name succeeded")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("opening an oversized corrupt name: %v, want EFBIG", err)
	}
}
