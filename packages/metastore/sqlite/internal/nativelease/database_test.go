package nativelease

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func ownedDatabase(t *testing.T, path string, exclusive bool) *Database {
	t.Helper()
	owner, err := AcquireDatabase(path, exclusive, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	return owner
}

func assertDatabaseRefused(t *testing.T, path string, exclusive, create bool, want error) {
	t.Helper()
	owner, err := AcquireDatabase(path, exclusive, create)
	if owner != nil {
		if closeErr := owner.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal("refused acquisition returned ownership")
	}
	if !errors.Is(err, want) {
		t.Fatalf("acquisition error = %v; want %v", err, want)
	}
}

func TestDatabaseAcquisitionOwnsTheNativeFileUntilClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	before := time.Now()
	owner := ownedDatabase(t, path, true)
	if owner.FD() < 0 || owner.Path() != path || !owner.Exclusive() ||
		owner.Acquired().Before(before) || owner.Acquired().After(time.Now()) {
		t.Fatalf("ownership = fd %d, path %q, exclusive %t, acquired %v", owner.FD(), owner.Path(), owner.Exclusive(), owner.Acquired())
	}
	var held unix.Stat_t
	if err := unix.Fstat(owner.FD(), &held); err != nil {
		t.Fatal(err)
	}
	if held.Mode&unix.S_IFMT != unix.S_IFREG || held.Mode&0o077 != 0 || held.Nlink != 1 {
		t.Fatalf("metadata inode is not private and single-linked: %+v", held)
	}
	flags, err := unix.FcntlInt(uintptr(owner.FD()), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("descriptor flags = %#x, %v", flags, err)
	}
	if err := VerifyDatabase(owner); err != nil {
		t.Fatal(err)
	}
	if err := VerifyExclusiveOwnership(owner); err != nil {
		t.Fatal(err)
	}
	for _, exclusive := range []bool{false, true} {
		assertDatabaseRefused(t, path, exclusive, false, syscall.EBUSY)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if owner.FD() != -1 || owner.Path() != path || owner.Acquired().Before(before) {
		t.Fatal("close lost ownership identity or left its descriptor live")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := ownedDatabase(t, path, true)
	if err := VerifyExclusiveOwnership(reopened); err != nil {
		t.Fatal(err)
	}
}

func TestSharedDatabaseOwnersExcludeTakeoverUntilEveryOwnerCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	first := ownedDatabase(t, path, false)
	second := ownedDatabase(t, path, false)
	if first.Exclusive() || second.Exclusive() {
		t.Fatal("shared acquisition advertised exclusive ownership")
	}
	if err := VerifyExclusiveOwnership(first); !errors.Is(err, syscall.EIO) {
		t.Fatalf("shared owner passed exclusive verification: %v", err)
	}
	assertDatabaseRefused(t, path, true, false, syscall.EBUSY)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	assertDatabaseRefused(t, path, true, false, syscall.EBUSY)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	owner := ownedDatabase(t, path, true)
	if err := VerifyExclusiveOwnership(owner); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseAcquisitionRefusesUnusableNativeTargets(t *testing.T) {
	t.Run("missing without creation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing")
		assertDatabaseRefused(t, path, true, false, syscall.ENOENT)
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("refused acquisition created the file: %v", err)
		}
	})
	t.Run("symbolic link", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "metadata.sqlite")
		owner := ownedDatabase(t, path, true)
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		alias := path + ".alias"
		if err := os.Symlink(path, alias); err != nil {
			t.Fatal(err)
		}
		assertDatabaseRefused(t, alias, true, false, syscall.ELOOP)
		ownedDatabase(t, path, true)
	})
	t.Run("writable by others", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "metadata.sqlite")
		if err := os.WriteFile(path, []byte("preserve"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o622); err != nil {
			t.Fatal(err)
		}
		assertDatabaseRefused(t, path, true, false, syscall.EIO)
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		ownedDatabase(t, path, true)
		contents, err := os.ReadFile(path)
		if err != nil || string(contents) != "preserve" {
			t.Fatalf("refused ownership changed bytes: %q, %v", contents, err)
		}
	})
	t.Run("multiple links", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "metadata.sqlite")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		alias := path + ".alias"
		if err := os.Link(path, alias); err != nil {
			t.Fatal(err)
		}
		assertDatabaseRefused(t, path, true, false, syscall.EIO)
		if err := os.Remove(alias); err != nil {
			t.Fatal(err)
		}
		ownedDatabase(t, path, true)
	})
	t.Run("fifo", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "metadata.pipe")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		assertDatabaseRefused(t, path, true, false, syscall.EIO)
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
			t.Fatalf("refused target was changed: %v, %v", info, err)
		}
	})
}

func TestDatabaseVerificationRefusesIdentityChangesWithoutReleasingOwnership(t *testing.T) {
	for _, mutation := range []string{"replacement", "missing", "extra link", "permissions"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata.sqlite")
			owner := ownedDatabase(t, path, true)
			heldPath := path
			switch mutation {
			case "replacement", "missing":
				heldPath = path + ".held"
				if err := os.Rename(path, heldPath); err != nil {
					t.Fatal(err)
				}
				if mutation == "replacement" {
					if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			case "extra link":
				if err := os.Link(path, path+".alias"); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(path, 0o622); err != nil {
					t.Fatal(err)
				}
			}
			for _, verify := range []func(*Database) error{VerifyDatabase, VerifyExclusiveOwnership} {
				err := verify(owner)
				if !errors.Is(err, syscall.EIO) || mutation == "missing" && !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("identity verification = %v", err)
				}
			}
			var stat unix.Stat_t
			if err := unix.Fstat(owner.FD(), &stat); err != nil {
				t.Fatalf("failed verification released its descriptor: %v", err)
			}
			assertDatabaseRefused(t, heldPath, false, false, syscall.EBUSY)
		})
	}
}

func TestExclusiveVerificationRequiresALiveAcquisitionAndChecksTheLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	owner := ownedDatabase(t, path, true)
	for _, candidate := range []*Database{
		nil,
		{fd: owner.FD(), path: path, exclusive: false, acquired: owner.Acquired()},
		{fd: -1, path: path, exclusive: true, acquired: owner.Acquired()},
		{fd: owner.FD(), path: path, exclusive: true},
	} {
		if err := VerifyExclusiveOwnership(candidate); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid ownership passed verification: %v", err)
		}
	}
	if err := VerifyDatabase(&Database{fd: -1, path: path}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("invalid descriptor error = %v", err)
	}
	if err := unix.Flock(owner.FD(), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	reader := ownedDatabase(t, path, false)
	if err := VerifyExclusiveOwnership(owner); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("a conflicting reader did not prevent exclusive verification: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyExclusiveOwnership(owner); err != nil {
		t.Fatalf("exclusive verification after conflicting reader closed: %v", err)
	}
	assertDatabaseRefused(t, path, false, false, syscall.EBUSY)
}

func TestDatabaseOwnerMustMatchTheAnchoredDatabaseOrRoot(t *testing.T) {
	for _, binding := range []string{"database", "root"} {
		t.Run(binding, func(t *testing.T) {
			root := t.TempDir()
			owner := ownedDatabase(t, filepath.Join(root, "metadata.sqlite"), true)
			config := Config{Directory: root, Name: ".leases", Identity: "database-owner-test",
				BindingFD: owner.FD(), RecoveryStart: owner.Acquired(), Initialize: true}
			if binding == "root" {
				fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := unix.Close(fd); err != nil {
						t.Error(err)
					}
				})
				if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
				config.BindingFD = fd
			}
			a := mustLeaseAnchor(t, config)
			if err := VerifyDatabaseOwner(a, owner); err != nil {
				t.Fatal(err)
			}
			other := ownedDatabase(t, filepath.Join(t.TempDir(), "other.sqlite"), true)
			if err := VerifyDatabaseOwner(a, other); !errors.Is(err, syscall.EIO) {
				t.Fatalf("database outside the anchor directory was accepted: %v", err)
			}
			missingParent := *owner
			missingParent.path = filepath.Join(root, "missing", "database.sqlite")
			if err := VerifyDatabaseOwner(a, &missingParent); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("missing database directory = %v", err)
			}
			closedOwner := *owner
			closedOwner.fd = -1
			if err := VerifyDatabaseOwner(a, &closedOwner); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("invalid database descriptor = %v", err)
			}
			if binding == "database" {
				neighbour := ownedDatabase(t, filepath.Join(root, "neighbour.sqlite"), true)
				if err := VerifyDatabaseOwner(a, neighbour); !errors.Is(err, syscall.EIO) {
					t.Fatalf("a different database inode reused the binding: %v", err)
				}
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			if err := VerifyDatabaseOwner(a, owner); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("closed anchor passed ownership verification: %v", err)
			}
			assertDatabaseRefused(t, owner.Path(), false, false, syscall.EBUSY)
		})
	}
}

func TestOpeningValidationRequiresTheLeaseOwnerForBoundNativePaths(t *testing.T) {
	for _, bound := range []string{"file", "parent"} {
		t.Run(bound, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata.sqlite")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateOpening(path, false); err != nil {
				t.Fatal(err)
			}
			target := path
			if bound == "parent" {
				target = filepath.Dir(path)
			}
			if err := unix.Setxattr(target, leaseBindingAttribute, []byte("bound"), unix.XATTR_CREATE); err != nil {
				t.Fatal(err)
			}
			if err := ValidateOpening(path, false); !errors.Is(err, syscall.EIO) {
				t.Fatalf("unowned bound database opening = %v", err)
			}
			if err := ValidateOpening(path, true); err != nil {
				t.Fatalf("owned bound database opening = %v", err)
			}
			if err := unix.Removexattr(target, leaseBindingAttribute); err != nil {
				t.Fatal(err)
			}
			if err := ValidateOpening(path, false); err != nil {
				t.Fatalf("unbound database opening = %v", err)
			}
		})
	}
	for _, name := range []string{"metadata%20.sqlite", "metadata?mode=ro", "metadata#fragment", "metadata\x00.sqlite"} {
		for _, owned := range []bool{false, true} {
			if err := ValidateOpening(name, owned); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("invalid native path %q, owned=%t: %v", name, owned, err)
			}
		}
	}
	if err := ValidateOpening(filepath.Join(t.TempDir(), "new.sqlite"), false); err != nil {
		t.Fatalf("missing unbound database opening = %v", err)
	}
	path := filepath.Join(t.TempDir(), strings.Repeat("x", 256))
	if err := ValidateOpening(path, false); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("native xattr failure lost its cause: %v", err)
	}
}

func TestLeaseDatabaseCloseNeverRetriesAReusedDescriptor(t *testing.T) {
	fd, err := unix.Open(filepath.Join(t.TempDir(), "held"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("close reported failure after descriptor release")
	owner := &Database{fd: fd, closeFD: func(fd int) error {
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
		return fault
	}}
	if err := owner.Close(); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	reused, err := unix.Open(filepath.Join(t.TempDir(), "replacement"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(reused)
	owner.closeFD = func(int) error { t.Fatal("descriptor closure was retried"); return nil }
	if err := owner.Close(); !errors.Is(err, fault) {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(reused, &stat); err != nil {
		t.Fatalf("replacement descriptor was closed: %v", err)
	}
}
