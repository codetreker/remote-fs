package sqlite

import (
	"errors"
	"math"
	"os"
	"syscall"
	"testing"

	_ "modernc.org/sqlite"
)

func TestReaderConnectionOptionsAreBoundedAndValidatedBeforeOpening(t *testing.T) {
	defaults := DefaultOptions()
	if defaults.MaxReaderConnections != DefaultMaxReaderConnections || defaults.MaxReaderConnections < 1 ||
		defaults.MaxSnapshotReaderConnections != DefaultMaxSnapshotReaderConnections ||
		defaults.MaxSnapshotReaderConnections < 1 ||
		defaults.MaxIntegrityRecords != DefaultMaxIntegrityRecords || defaults.MaxIntegrityRecords < 1 ||
		defaults.MaxIntegrityBytes != DefaultMaxIntegrityBytes || defaults.MaxIntegrityBytes < 1 ||
		defaults.MaxMetadataBytes != DefaultMaxMetadataBytes || defaults.MaxMetadataBytes < 1 ||
		defaults.MaxDirectoryEntries != DefaultMaxDirectoryEntries || defaults.MaxDirectoryEntries < 1 ||
		defaults.MaxDirectoryBytes != DefaultMaxDirectoryBytes || defaults.MaxDirectoryBytes < 1 {
		t.Fatalf("default SQLite options are %+v", defaults)
	}
	effective, err := (Options{Window: DefaultWindow()}).Effective()
	if err != nil {
		t.Fatal(err)
	}
	if effective != defaults {
		t.Fatalf("zero-valued resource options resolve to %+v, want %+v", effective, defaults)
	}
	store, err := Open(t.Context(), t.TempDir()+"/metastore.db", "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if got := store.read.Stats().MaxOpenConnections; got != DefaultMaxReaderConnections {
		store.Close()
		t.Fatalf("Open configured %d reader connections, want default %d", got, DefaultMaxReaderConnections)
	}
	if got := store.snapshotRead.Stats().MaxOpenConnections; got != DefaultMaxSnapshotReaderConnections {
		store.Close()
		t.Fatalf("Open configured %d snapshot reader connections, want default %d",
			got, DefaultMaxSnapshotReaderConnections)
	}
	if store.maxIntegrityRecords != DefaultMaxIntegrityRecords {
		store.Close()
		t.Fatalf("Open configured an integrity record limit of %d, want default %d",
			store.maxIntegrityRecords, DefaultMaxIntegrityRecords)
	}
	if store.maxIntegrityBytes != DefaultMaxIntegrityBytes {
		store.Close()
		t.Fatalf("Open configured an integrity byte limit of %d, want default %d",
			store.maxIntegrityBytes, DefaultMaxIntegrityBytes)
	}
	if store.maxMetadataBytes != DefaultMaxMetadataBytes {
		store.Close()
		t.Fatalf("Open configured a metadata byte limit of %d, want default %d",
			store.maxMetadataBytes, DefaultMaxMetadataBytes)
	}
	if store.maxDirectoryEntries != DefaultMaxDirectoryEntries || store.maxDirectoryBytes != DefaultMaxDirectoryBytes {
		store.Close()
		t.Fatalf("Open configured directory limits %d/%d, want %d/%d",
			store.maxDirectoryEntries, store.maxDirectoryBytes, DefaultMaxDirectoryEntries, DefaultMaxDirectoryBytes)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	minimum, err := OpenWithOptions(
		t.Context(), t.TempDir()+"/metastore.db", "workspace", 0,
		Options{Window: DefaultWindow(), MaxIntegrityRecords: MinIntegrityRecords},
	)
	if err != nil {
		t.Fatalf("opening an empty volume at the minimum integrity record limit: %v", err)
	}
	if err := minimum.Close(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name  string
		limit int
	}{
		{"negative", -1},
		{"unbounded", math.MaxInt},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window:               DefaultWindow(),
				MaxReaderConnections: test.limit,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with an invalid SQLite reader-connection limit succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with reader-connection limit %d: %v, want EINVAL", test.limit, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid options touched the database path: %v", statErr)
			}
		})
	}

	for _, test := range []struct {
		name  string
		limit int
	}{
		{"negative snapshot readers", -1},
		{"unbounded snapshot readers", math.MaxInt},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window:                       DefaultWindow(),
				MaxSnapshotReaderConnections: test.limit,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with an invalid SQLite snapshot reader limit succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with snapshot reader limit %d: %v, want EINVAL", test.limit, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid options touched the database path: %v", statErr)
			}
		})
	}

	for _, test := range []struct {
		name  string
		limit int64
	}{
		{"negative integrity records", -1},
		{"below an empty volume", MinIntegrityRecords - 1},
		{"unbounded integrity records", math.MaxInt64},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window:              DefaultWindow(),
				MaxIntegrityRecords: test.limit,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with an invalid SQLite integrity record limit succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with integrity record limit %d: %v, want EINVAL", test.limit, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid options touched the database path: %v", statErr)
			}
		})
	}

	for _, limit := range []int64{-1, math.MaxInt64} {
		path := t.TempDir() + "/nested/metastore.db"
		store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
			Window:            DefaultWindow(),
			MaxIntegrityBytes: limit,
		})
		if err == nil {
			store.Close()
			t.Fatalf("opening with integrity byte limit %d succeeded", limit)
		}
		if !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("opening with integrity byte limit %d: %v, want EINVAL", limit, err)
		}
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("invalid integrity byte limit touched the database path: %v", statErr)
		}
	}
	for _, limit := range []int64{-1, math.MaxInt64} {
		path := t.TempDir() + "/nested/metastore.db"
		store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
			Window:           DefaultWindow(),
			MaxMetadataBytes: limit,
		})
		if err == nil {
			store.Close()
			t.Fatalf("opening with metadata byte limit %d succeeded", limit)
		}
		if !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("opening with metadata byte limit %d: %v, want EINVAL", limit, err)
		}
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("invalid metadata byte limit touched the database path: %v", statErr)
		}
	}
	for _, test := range []struct {
		name    string
		entries int
		bytes   int64
	}{
		{"negative directory entries", -1, 0},
		{"excess directory entries", DefaultMaxDirectoryEntries + 1, 0},
		{"negative directory bytes", 0, -1},
		{"excess directory bytes", 0, DefaultMaxDirectoryBytes + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := t.TempDir() + "/nested/metastore.db"
			store, err := OpenWithOptions(t.Context(), path, "workspace", 0, Options{
				Window: DefaultWindow(), MaxDirectoryEntries: test.entries, MaxDirectoryBytes: test.bytes,
			})
			if err == nil {
				store.Close()
				t.Fatal("opening with invalid directory limits succeeded")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("opening with directory limits %d/%d: %v, want EINVAL", test.entries, test.bytes, err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid directory limits touched the database path: %v", statErr)
			}
		})
	}
}

func TestObjectLimitValidationKeepsFiniteBoundsAndDefaults(t *testing.T) {
	for _, limits := range []ObjectLimits{{}, {MaxPendingObjects: 1, MaxPendingBytes: 1}, {MaxPendingObjects: math.MaxInt64 - 1, MaxPendingBytes: math.MaxInt64 - 1}} {
		if err := limits.Validate(); err != nil {
			t.Fatalf("valid limits %+v: %v", limits, err)
		}
		effective, err := limits.Effective()
		if err != nil || effective.MaxPendingObjects <= 0 || effective.MaxPendingBytes <= 0 {
			t.Fatalf("effective limits %+v: %+v, %v", limits, effective, err)
		}
	}
	for _, limits := range []ObjectLimits{{MaxPendingObjects: -1}, {MaxPendingBytes: -1}, {MaxPendingObjects: math.MaxInt64}, {MaxPendingBytes: math.MaxInt64}} {
		if err := limits.Validate(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid limits %+v: %v", limits, err)
		}
	}
	if effective, err := (ObjectLimits{}).Effective(); err != nil || effective != DefaultObjectLimits() {
		t.Fatalf("zero limits = %+v, %v", effective, err)
	}
}
