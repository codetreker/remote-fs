package sqlite

import (
	"database/sql"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestVirtualAllocationChargesWholeUnitsAndPreservesLogicalUsage(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "volume.db"), "volume", 8192, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CheckAllocationReporting(); err != nil {
		t.Fatalf("SQLite cannot promise its stored allocation facts: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	check := func(wantSize, wantAllocated int64) {
		t.Helper()
		node, err := store.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		if node.Size != wantSize || node.AllocationSize != wantAllocated {
			t.Fatalf("node size/allocation = %d/%d, want %d/%d", node.Size, node.AllocationSize, wantSize, wantAllocated)
		}
		attr := node.Attr()
		if !attr.AllocationKnown || attr.AllocationSize != wantAllocated {
			t.Fatalf("attribute allocation = %+v", attr)
		}
		used, err := store.Usage(t.Context())
		if err != nil || used != wantSize {
			t.Fatalf("logical usage = %d, %v; want %d", used, err, wantSize)
		}
		space, err := store.Space(t.Context())
		if err != nil || space != (storage.Space{Total: 8192, Used: wantAllocated, Avail: 8192 - wantAllocated}) {
			t.Fatalf("space = %+v, %v", space, err)
		}
	}
	write := func(size int64) {
		t.Helper()
		if size == 0 {
			if err := store.Commit(t.Context(), "file", metastore.Object{ModTime: time.Now()}); err != nil {
				t.Fatal(err)
			}
			return
		}
		key, err := store.Reserve(t.Context(), "file", size)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Commit(t.Context(), "file", metastore.Object{Key: key, Size: size, ModTime: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	write(1)
	check(1, 4096)
	write(4096)
	check(4096, 4096)
	write(4097)
	check(4097, 8192)
	if _, err := store.Reserve(t.Context(), "file", 8193); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("growth beyond virtual quota = %v, want EDQUOT", err)
	}
	write(0)
	check(0, 0)
}

func TestReplicaUpdateCannotChargeAnotherVolume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	first, err := Open(t.Context(), path, "first", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Error(err)
		}
	})
	second, err := Open(t.Context(), path, "second", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Error(err)
		}
	})
	other, err := second.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := first.write.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := updateNode(t.Context(), tx, first.volume, other); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-volume replica update = %v, want no row", err)
	}
}

func TestDetachedFileKeepsVirtualAllocationUntilLastReferenceCloses(t *testing.T) {
	config := lockingTestConfig(t)
	config.Allowance = 4096
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	key, err := store.Reserve(t.Context(), "file", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(t.Context(), "file", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	file, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if space, err := store.Space(t.Context()); err != nil || space.Used != 4096 {
		t.Fatalf("detached allocation = %+v, %v; want 4096", space, err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if space, err := store.Space(t.Context()); err != nil || space.Used != 0 || space.Avail != 4096 {
		t.Fatalf("released allocation = %+v, %v; want full availability", space, err)
	}
}

func TestLoweredAllowanceReportsOverQuotaAndPermitsRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume.db")
	store, err := Open(t.Context(), path, "volume", 8192, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second"} {
		key, err := store.Reserve(t.Context(), name, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Commit(t.Context(), name, metastore.Object{Key: key, Size: 1, ModTime: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), path, "volume", 4096, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if space, err := store.Space(t.Context()); err != nil || space != (storage.Space{Total: 4096, Used: 8192, Avail: 0}) {
		t.Fatalf("lowered allowance = %+v, %v", space, err)
	}
	if _, err := store.Reserve(t.Context(), "third", 1); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("over-quota reservation = %v, want EDQUOT", err)
	}
	if err := store.Commit(t.Context(), "first", metastore.Object{ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if space, err := store.Space(t.Context()); err != nil || space != (storage.Space{Total: 4096, Used: 4096, Avail: 0}) {
		t.Fatalf("released allocation = %+v, %v", space, err)
	}
}
