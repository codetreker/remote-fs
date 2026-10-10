package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestListBoundedRejectsMalformedNamesBeforeReservation(t *testing.T) {
	for _, test := range []struct {
		name string
		leaf any
	}{
		{"text", "z-text"}, {"empty", []byte{}}, {"dot", []byte(".")},
		{"dotdot", []byte("..")}, {"slash", []byte("z/bad")}, {"nul", []byte("z\x00bad")},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for _, name := range []string{"a-valid", "z-invalid"} {
				if err := store.Create(t.Context(), name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.write.ExecContext(t.Context(), `UPDATE entries SET name=? WHERE volume=? AND parent=? AND name=?`, test.leaf, store.volume, store.root, []byte("z-invalid")); err != nil {
				t.Fatal(err)
			}
			result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
				return nameBytes + metadataBytes, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
				t.Fatalf("ListBounded=%v", err)
			}
			if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
				t.Fatalf("entries=%+v error=%v", entries, err)
			}
		})
	}
}

func TestListBoundedReservesEveryHeaderBeforeLoadingPayloads(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	largeName := "a" + strings.Repeat("x", 8<<20)
	for _, name := range []string{"a", "z"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE entries SET name=? WHERE volume=? AND parent=? AND name=?`, []byte(largeName), store.volume, store.root, []byte("a")); err != nil {
		t.Fatal(err)
	}
	refused := errors.New("second reservation refused")
	result, err := storage.NewListResult(16<<20, 0, func(index int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		if index == 1 {
			return 0, refused
		}
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = store.ListBounded(t.Context(), "", result)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, refused) {
		t.Fatalf("ListBounded=%v", err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got >= uint64(len(largeName))/4 {
		t.Fatalf("loaded payload before all reservations: allocated %d bytes", got)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, refused) {
		t.Fatalf("entries=%+v error=%v", entries, err)
	}
}

func TestListBoundedKeepsHeaderAndPayloadInOneSnapshot(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, name := range []string{"a", "b"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	result, err := storage.NewListResult(1024, 0, func(index int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		if index == 1 {
			if err := store.Rename(t.Context(), "a", "z"); err != nil {
				t.Fatal(err)
			}
		}
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(t.Context(), "", result); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name)
	}
	if !reflect.DeepEqual(names, []string{"a", "b"}) {
		t.Fatalf("snapshot names=%v", names)
	}
	if _, err := store.Stat(t.Context(), "z"); err != nil {
		t.Fatal(err)
	}
}

func TestListBoundedInvalidatesPayloadPrefixOnLateMetadataFailure(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, name := range []string{"a", "z"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=zeroblob(6) WHERE id=(SELECT node FROM entries WHERE volume=? AND parent=? AND name=?)`, store.volume, store.root, []byte("z")); err != nil {
		t.Fatal(err)
	}
	result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded=%v", err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("entries=%+v error=%v", entries, err)
	}
}

func TestListBoundedCancellationAfterReservationsKeepsResultInvalid(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, name := range []string{"a", "z"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, err := storage.NewListResult(1024, 0, func(index int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		if index == 1 {
			cancel()
		}
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(ctx, "", result); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListBounded=%v", err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("entries=%+v error=%v", entries, err)
	}
}

func TestListBoundedRejectsAliasesOutsideTheObservedDirectory(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Create(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := store.Mkdir(t.Context(), "other"); err != nil {
		t.Fatal(err)
	}
	var node, parent int64
	if err := store.write.QueryRowContext(t.Context(), `SELECT node FROM entries WHERE volume=? AND parent=? AND name=?`, store.volume, store.root, []byte("a")).Scan(&node); err != nil {
		t.Fatal(err)
	}
	if err := store.write.QueryRowContext(t.Context(), `SELECT node FROM entries WHERE volume=? AND parent=? AND name=?`, store.volume, store.root, []byte("other")).Scan(&parent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `INSERT INTO entries(volume,parent,name,node) VALUES(?,?,?,?)`, store.volume, parent, []byte("alias"), node); err != nil {
		t.Fatal(err)
	}
	result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ListBounded(t.Context(), "", result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("ListBounded=%v", err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("entries=%+v error=%v", entries, err)
	}
}

func TestReservedPayloadCursorRejectsDifferentHeader(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func([]reservedChild) []reservedChild
	}{
		{"unreserved", func(rows []reservedChild) []reservedChild { return nil }},
		{"missing", func(rows []reservedChild) []reservedChild { return append(rows, rows[0]) }},
		{"identity", func(rows []reservedChild) []reservedChild { rows[0].node++; return rows }},
		{"name length", func(rows []reservedChild) []reservedChild { rows[0].nameBytes++; return rows }},
		{"metadata length", func(rows []reservedChild) []reservedChild { rows[0].metadataBytes++; return rows }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", 0, DefaultWindow())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Create(t.Context(), "a"); err != nil {
				t.Fatal(err)
			}
			attr, err := store.Stat(t.Context(), "a")
			if err != nil {
				t.Fatal(err)
			}
			result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
				return nameBytes + metadataBytes, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			reservation, err := result.Reserve(1, 6, attr.Attr())
			if err != nil {
				t.Fatal(err)
			}
			headers := test.change([]reservedChild{{node: int64(attr.ID), nameBytes: 1, metadataBytes: 6, reservation: reservation}})
			err = store.inspect(t.Context(), func(tx *sql.Tx) error { return store.loadReservedChildren(t.Context(), tx, store.root, headers) })
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("cursor=%v", err)
			}
		})
	}
}
