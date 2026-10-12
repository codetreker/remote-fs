package sqlite

import (
	"context"
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

func allocationUnitStore(t *testing.T, allowance int64) (*Store, func() error) {
	t.Helper()
	config := lockingTestConfig(t)
	config.Allowance = allowance
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store.Store, store.Close
}

func TestAllocationUnitBytesDoesNotRequireAllowance(t *testing.T) {
	for _, test := range []struct {
		name      string
		allowance int64
	}{{"unbounded", 0}, {"bounded", 8192}} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := allocationUnitStore(t, test.allowance)
			if err := store.CheckAllocationReporting(); err != nil {
				t.Fatal(err)
			}
			if got, err := store.AllocationUnitBytes(t.Context()); err != nil || got != 4096 {
				t.Fatalf("allocation unit = %d, %v", got, err)
			}
			if test.allowance == 0 {
				if _, err := store.Space(t.Context()); !errors.Is(err, syscall.ENOSYS) {
					t.Fatalf("space without allowance = %v", err)
				}
			}
		})
	}
}

func TestAllocationUnitBytesZeroesPublicationAndLifetimeFailures(t *testing.T) {
	store, closeStore := allocationUnitStore(t, 0)
	denied := errors.New("allocation publication denied")
	for _, failAt := range []int{1, 2} {
		calls := 0
		ctx := metastore.WithFilePublicationGuard(t.Context(), func() error {
			calls++
			if calls >= failAt {
				return denied
			}
			return nil
		})
		if got, err := store.AllocationUnitBytes(ctx); got != 0 || !errors.Is(err, denied) || calls != failAt {
			t.Fatalf("guard %d = %d, %v (%d calls)", failAt, got, err, calls)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := store.AllocationUnitBytes(ctx); got != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled allocation query = %d, %v", got, err)
	}
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
	if got, err := store.AllocationUnitBytes(t.Context()); got != 0 || !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed allocation query = %d, %v", got, err)
	}
}

func TestAllocationUnitBytesRejectsUnverifiedVolumeIdentity(t *testing.T) {
	store, _ := allocationUnitStore(t, 0)
	var databaseID string
	if err := store.read.QueryRowContext(t.Context(), `SELECT database_id FROM database_state`).Scan(&databaseID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := store.write.ExecContext(context.Background(), `UPDATE volumes SET root=? WHERE id=?`, store.root, store.volume); err != nil {
			t.Error(err)
		}
		if _, err := store.write.ExecContext(context.Background(), `UPDATE database_state SET database_id=?`, databaseID); err != nil {
			t.Error(err)
		}
	})
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET root=? WHERE id=?`, store.root+1, store.volume); err != nil {
		t.Fatal(err)
	}
	if got, err := store.AllocationUnitBytes(t.Context()); got != 0 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unverified root allocation query = %d, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET root=? WHERE id=?`, store.root, store.volume); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE database_state SET database_id='INVALID0000000000000000000000000'`); err != nil {
		t.Fatal(err)
	}
	if got, err := store.AllocationUnitBytes(t.Context()); got != 0 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unverified database allocation query = %d, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE database_state SET database_id=?`, databaseID); err != nil {
		t.Fatal(err)
	}
	if got, err := store.AllocationUnitBytes(t.Context()); err != nil || got != 4096 {
		t.Fatalf("restored allocation query = %d, %v", got, err)
	}
}

func TestAllocationUnitBytesPreservesDurabilityFailure(t *testing.T) {
	store, _ := allocationUnitStore(t, 0)
	failure := errors.New("allocation durability failure")
	store.coordinator.poisonWith(failure)
	t.Cleanup(func() {
		store.coordinator.health.Lock()
		store.coordinator.poison = nil
		store.coordinator.health.Unlock()
	})
	if got, err := store.AllocationUnitBytes(t.Context()); got != 0 || !errors.Is(err, syscall.EIO) || !errors.Is(err, failure) {
		t.Fatalf("poisoned allocation query = %d, %v", got, err)
	}
}

func TestAllocationUnitBytesRequiresLockingAuthority(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "volume.db"), "volume", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.CheckAllocationReporting(); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil || !root.Attr().AllocationKnown || root.AllocationSize != 0 {
		t.Fatalf("plain store allocation fact = %+v, %v", root, err)
	}
	if got, err := store.AllocationUnitBytes(t.Context()); got != 0 || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("allocation unit without locking authority = %d, %v", got, err)
	}
}
