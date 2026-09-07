package sqlite_test

import (
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestSinceRefusesAnOversizedStoredNameBeforeExposingAPartialPage(t *testing.T) {
	store := open(t, database(t), "workspace", 0)
	if err := store.Create(t.Context(), strings.Repeat("x", 1<<20)); err != nil {
		t.Fatal(err)
	}
	result, err := metastore.NewChangeResult(128, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return lengths.Name + lengths.FromName + lengths.Content + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Since(t.Context(), 0, 1024, result); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized stored change returned %v, want EFBIG", err)
	}
	if changes, err := result.Changes(); !errors.Is(err, syscall.EFBIG) || changes != nil {
		t.Fatalf("oversized stored change exposed %+v, %v", changes, err)
	}
}

func TestSnapshotBoundedMakesAProductionErrorTerminalWithoutExposingPrefixRows(t *testing.T) {
	store := open(t, database(t), "workspace", 0)
	if err := store.Create(t.Context(), strings.Repeat("x", 1<<20)); err != nil {
		t.Fatal(err)
	}
	snap, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	newResult := func(max int64) *metastore.RowResult {
		result, err := metastore.NewRowResult(max, 0, func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
			return lengths.Name + lengths.Content + 1, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := newResult(128)
	if _, err := snap.Next(t.Context(), 1024, first); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized snapshot row returned %v, want EFBIG", err)
	}
	if rows, err := first.Rows(); !errors.Is(err, syscall.EFBIG) || rows != nil {
		t.Fatalf("oversized snapshot row exposed prefix %+v, %v", rows, err)
	}
	second := newResult(2 << 20)
	if _, err := snap.Next(t.Context(), 1024, second); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("snapshot continued after its production failure with %v", err)
	}
	if rows, err := second.Rows(); !errors.Is(err, syscall.EFBIG) || rows != nil {
		t.Fatalf("failed snapshot later exposed %+v, %v", rows, err)
	}
}

func TestSinceRefusesLiveChangeCorruptionWithoutExposingAPartialPage(t *testing.T) {
	tests := []struct {
		name   string
		damage string
	}{
		{"text kind", `UPDATE changes SET kind = 'created' WHERE position = (SELECT min(position) FROM changes)`},
		{"text mode", `UPDATE changes SET mode = 'regular' WHERE position = (SELECT min(position) FROM changes)`},
		{"text name", `UPDATE changes SET name = 'file' WHERE position = (SELECT min(position) FROM changes)`},
		{"missing created name", `UPDATE changes SET name = NULL WHERE position = (SELECT min(position) FROM changes)`},
		{"slash in name", `UPDATE changes SET name = CAST('bad/name' AS BLOB) WHERE position = (SELECT min(position) FROM changes)`},
		{"invalid recorded nanoseconds", `UPDATE changes SET recorded_nsec = 1000000000 WHERE position = (SELECT min(position) FROM changes)`},
		{"file bytes without content", `UPDATE changes SET size = 1, content = NULL WHERE position = (SELECT min(position) FROM changes)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			store := open(t, path, "workspace", 0)
			defer store.Close()
			if err := store.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			damageDatabase(t, path, test.damage)

			result, err := metastore.NewChangeResult(1<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
				return 256 + lengths.Name + lengths.FromName + lengths.Content, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Since(t.Context(), 0, 100, result); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Since returned %v, want EIO", err)
			}
			if changes, err := result.Changes(); !errors.Is(err, syscall.EIO) || changes != nil {
				t.Fatalf("corrupt change exposed %+v, %v", changes, err)
			}
		})
	}
}

func TestSnapshotRefusesLiveStorageClassCorruptionBeforeReturningAPicture(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	defer store.Close()
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	damageDatabase(t, path,
		`UPDATE entries SET name = 'text-name' WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')`)

	snap, _, err := store.Snapshot(t.Context())
	if err == nil {
		if snap != nil {
			snap.Close()
		}
		t.Fatal("Snapshot returned a picture of storage-class-corrupt entries")
	}
	if !errors.Is(err, syscall.EIO) || snap != nil {
		t.Fatalf("Snapshot returned (%v, %v), want nil and EIO", snap, err)
	}
}
