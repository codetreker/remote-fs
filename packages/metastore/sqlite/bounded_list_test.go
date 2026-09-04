package sqlite

import (
	"errors"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestListBoundedRefusesAStoredHugeNameBeforeLoadingItsBlob(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Create(t.Context(), "small"); err != nil {
		t.Fatal(err)
	}
	const hugeNameBytes = 8 << 20
	if _, err := store.write.ExecContext(t.Context(),
		`UPDATE entries SET name = zeroblob(?) WHERE namespace = ? AND parent = ? AND name = ?`,
		hugeNameBytes, store.namespace, store.root, []byte("small")); err != nil {
		t.Fatal(err)
	}

	result, err := storage.NewListResult(1, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded returned %v, want EIO", err)
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatalf("the refused huge name left a usable result: %+v, %v", entries, err)
	}
}

func TestListBoundedDoesNotLoadContentBeforeReservingAnEntry(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	const contentBytes = 32 << 20
	writer, err := store.write.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(t.Context(), `
		UPDATE nodes SET content = zeroblob(?) WHERE id = (
			SELECT node FROM entries WHERE namespace = ? AND parent = ? AND name = CAST('file' AS BLOB)
		)`, contentBytes, store.namespace, store.root); err != nil {
		t.Fatal(err)
	}
	reservationFailure := errors.New("reservation refused")
	result, err := storage.NewListResult(1024, 0, func(_ int, _ int64, _ storage.Attr) (int64, error) {
		return 0, reservationFailure
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = store.ListBounded(t.Context(), "", result)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, reservationFailure) {
		t.Fatalf("ListBounded returned %v, want reservation failure", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= contentBytes/4 {
		t.Fatalf("ListBounded allocated %d bytes before reservation refused a row with a %d-byte content key", allocated, contentBytes)
	}
	if entries, err := result.Entries(); !errors.Is(err, reservationFailure) || entries != nil {
		t.Fatalf("the refused reservation left a usable result: %+v, %v", entries, err)
	}
}

func TestListBoundedRefusesTwoNamesForOneNodeWithoutExposingEither(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Create(t.Context(), "aa"); err != nil {
		t.Fatal(err)
	}
	var node int64
	if err := store.write.QueryRowContext(t.Context(),
		`SELECT node FROM entries WHERE namespace = ? AND parent = ? AND name = ?`,
		store.namespace, store.root, []byte("aa")).Scan(&node); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(),
		`INSERT INTO entries (namespace, parent, name, node) VALUES (?, ?, ?, ?)`,
		store.namespace, store.root, []byte("bb"), node); err != nil {
		t.Fatal(err)
	}

	result, err := storage.NewListResult(100, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded returned %v, want EIO", err)
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatalf("the duplicate node left a usable result: %+v, %v", entries, err)
	}
}

func TestListBoundedRefusesDifferentLengthNamesForOneNodeWithoutExposingEither(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Create(t.Context(), "short"); err != nil {
		t.Fatal(err)
	}
	var node int64
	if err := store.write.QueryRowContext(t.Context(),
		`SELECT node FROM entries WHERE namespace = ? AND parent = ? AND name = ?`,
		store.namespace, store.root, []byte("short")).Scan(&node); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(),
		`INSERT INTO entries (namespace, parent, name, node) VALUES (?, ?, ?, ?)`,
		store.namespace, store.root, []byte("a-much-longer-alias"), node); err != nil {
		t.Fatal(err)
	}

	result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded returned %v, want EIO", err)
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatalf("the different-length alias left a usable result: %+v, %v", entries, err)
	}
}
