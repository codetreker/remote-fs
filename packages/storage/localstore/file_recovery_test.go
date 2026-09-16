package localstore_test

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"golang.org/x/sys/unix"
)

func TestDefaultStoreBindsFileRecoveryWithoutEnablingStrongLocks(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	if store.LockService() != nil {
		t.Fatal("default file service enabled strong locks")
	}
	if _, err := store.FileState(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".file-leases.intent", ".file-leases.witness"} {
		info, err := os.Lstat(filepath.Join(config.Root, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("file recovery evidence %s: %v, %v", name, info, err)
		}
	}
	if size, err := unix.Getxattr(config.Root, "user.remote-fs.file-lease-state", nil); err != nil || size <= 0 {
		t.Fatalf("file recovery binding = %d, %v", size, err)
	}
	if _, err := unix.Getxattr(config.Root, "user.remote-fs.lease-state", nil); !errors.Is(err, syscall.ENODATA) {
		t.Fatalf("unexpected strong recovery binding: %v", err)
	}
}

func TestReadyFileRecoveryCannotBeReinitializedAfterEvidenceLoss(t *testing.T) {
	for _, damage := range []string{"intent", "witness", "binding", "all"} {
		t.Run(damage, func(t *testing.T) {
			config := testConfig(privateRoot(t))
			store := open(t, config)
			closeStore(t, store)
			for _, part := range []string{"intent", "witness", "binding"} {
				if damage != part && damage != "all" {
					continue
				}
				var err error
				if part == "binding" {
					err = unix.Removexattr(config.Root, "user.remote-fs.file-lease-state")
				} else {
					err = os.Remove(filepath.Join(config.Root, ".file-leases."+part))
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			restored, err := localstore.Open(t.Context(), config)
			if restored != nil {
				closeStore(t, restored)
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("opening Ready file service after %s loss = %v", damage, err)
			}
			for _, part := range []string{"intent", "witness", "binding"} {
				if damage != part && damage != "all" {
					continue
				}
				if part == "binding" {
					if _, err := unix.Getxattr(config.Root, "user.remote-fs.file-lease-state", nil); !errors.Is(err, syscall.ENODATA) {
						t.Fatalf("Ready binding was repaired: %v", err)
					}
				} else if _, err := os.Lstat(filepath.Join(config.Root, ".file-leases."+part)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("Ready %s evidence was repaired: %v", part, err)
				}
			}
		})
	}
}
