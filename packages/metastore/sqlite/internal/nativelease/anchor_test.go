package nativelease

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func leaseAnchorFixture(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return Config{
		Directory: root, Name: ".leases", Identity: "volume/objects",
		BindingFD: fd, RecoveryStart: time.Now(), Initialize: true,
	}
}

func mustLeaseAnchor(t *testing.T, config Config) *Anchor {
	t.Helper()
	a, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	return a
}

func anchorEvidence(a *Anchor) Evidence {
	return Evidence{
		DatabaseID: strings.Repeat("a", 32), StateID: a.StateID(),
		Generation: 0, MaxLease: 10 * time.Second,
	}
}

func TestLeaseAnchorInitializeAdvanceAndReopen(t *testing.T) {
	config := leaseAnchorFixture(t)
	a := mustLeaseAnchor(t, config)
	if !a.Initializing() || !validLeaseAnchorID(a.StateID()) || a.RecoveryStart() != config.RecoveryStart {
		t.Fatal("anchor did not preserve its initialization identity and ownership time")
	}
	if _, exists, err := a.Load(); err != nil || exists {
		t.Fatalf("initial Load = %v, %v", exists, err)
	}
	if err := a.Complete(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Complete without witness = %v", err)
	}
	first := anchorEvidence(a)
	if err := a.Advance(first); err != nil {
		t.Fatal(err)
	}
	if err := a.Complete(); err != nil {
		t.Fatal(err)
	}
	next := first
	next.Generation++
	next.MaxLease += time.Second
	if err := a.Advance(next); err != nil {
		t.Fatal(err)
	}
	if err := a.Advance(next); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	config.RecoveryStart = time.Now()
	reopened := mustLeaseAnchor(t, config)
	actual, exists, err := reopened.Load()
	if err != nil || !exists || actual != next || reopened.Initializing() {
		t.Fatalf("reopened witness = %+v, %v, %v", actual, exists, err)
	}
	if reopened.RecoveryStart() != config.RecoveryStart {
		t.Fatal("reopen used an old ownership time")
	}
}

func TestLeaseAnchorRefusesLostOrForeignEvidence(t *testing.T) {
	for _, name := range []string{"intent", "witness", "binding", "identity", "state-slot", "state-location"} {
		t.Run(name, func(t *testing.T) {
			config := leaseAnchorFixture(t)
			a := mustLeaseAnchor(t, config)
			if err := a.Advance(anchorEvidence(a)); err != nil {
				t.Fatal(err)
			}
			if err := a.Complete(); err != nil {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "intent", "witness":
				if err := os.Remove(filepath.Join(config.Directory, config.Name+"."+name)); err != nil {
					t.Fatal(err)
				}
			case "binding":
				if err := unix.Fremovexattr(config.BindingFD, leaseBindingAttribute); err != nil {
					t.Fatal(err)
				}
			case "identity":
				config.Identity = "another-volume"
			case "state-slot":
				config.Name = ".other-leases"
			case "state-location":
				config.Directory = t.TempDir()
			}
			for _, initialize := range []bool{false, true} {
				config.Initialize = initialize
				if opened, err := Open(config); !errors.Is(err, syscall.EIO) {
					if opened != nil {
						_ = opened.Close()
					}
					t.Fatalf("Initialize=%v after %s loss/change = %v", initialize, name, err)
				}
			}
		})
	}
}

func TestLeaseAnchorRecoversOnlyMatchingInitialization(t *testing.T) {
	for _, point := range []string{"intent-only", "binding", "witness"} {
		t.Run(point, func(t *testing.T) {
			config := leaseAnchorFixture(t)
			a := mustLeaseAnchor(t, config)
			id := a.StateID()
			if point == "witness" {
				if err := a.Advance(anchorEvidence(a)); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			if point == "intent-only" {
				if err := unix.Fremovexattr(config.BindingFD, leaseBindingAttribute); err != nil {
					t.Fatal(err)
				}
			}
			reopened := mustLeaseAnchor(t, config)
			if reopened.StateID() != id || !reopened.Initializing() {
				t.Fatal("interrupted initialization changed its durable identity")
			}
			if err := reopened.Advance(anchorEvidence(reopened)); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Complete(); err != nil {
				t.Fatal(err)
			}
		})
	}
	config := leaseAnchorFixture(t)
	config.Initialize = false
	if a, err := Open(config); !errors.Is(err, syscall.EIO) {
		if a != nil {
			_ = a.Close()
		}
		t.Fatalf("Open without any proof = %v", err)
	}
}

func TestLeaseAnchorDiscardsInterruptedStages(t *testing.T) {
	for name, body := range map[string][]byte{
		"empty": nil, "partial": []byte("partial"), "oversized": bytes.Repeat([]byte{0}, leaseAnchorMaxBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			config := leaseAnchorFixture(t)
			a := mustLeaseAnchor(t, config)
			accepted := anchorEvidence(a)
			if err := a.Advance(accepted); err != nil {
				t.Fatal(err)
			}
			if err := a.Complete(); err != nil {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			for _, suffix := range []string{".intent.stage", ".witness.stage"} {
				if err := os.WriteFile(filepath.Join(config.Directory, config.Name+suffix), body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			config.Initialize = false
			reopened := mustLeaseAnchor(t, config)
			got, exists, err := reopened.Load()
			if err != nil || !exists || got != accepted {
				t.Fatalf("published witness after interrupted stages = %+v, %v, %v", got, exists, err)
			}
			for _, suffix := range []string{".intent.stage", ".witness.stage"} {
				if _, err := os.Lstat(filepath.Join(config.Directory, config.Name+suffix)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("stage was not removed: %v", err)
				}
			}
		})
	}
}

func TestLeaseAnchorRejectsRegressionsAndIdentityChanges(t *testing.T) {
	config := leaseAnchorFixture(t)
	a := mustLeaseAnchor(t, config)
	accepted := anchorEvidence(a)
	accepted.Generation = 3
	if err := a.Advance(accepted); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Evidence){
		"decrease":   func(e *Evidence) { e.MaxLease-- },
		"skip":       func(e *Evidence) { e.Generation++ },
		"replay":     func(e *Evidence) { e.Generation = 1 },
		"database":   func(e *Evidence) { e.DatabaseID = strings.Repeat("b", 32) },
		"state":      func(e *Evidence) { e.StateID = strings.Repeat("c", 32) },
		"bad-id":     func(e *Evidence) { e.DatabaseID = "corrupt" },
		"negative":   func(e *Evidence) { e.MaxLease = -1 },
		"generation": func(e *Evidence) { e.Generation = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			next := accepted
			next.Generation++
			change(&next)
			if err := a.Advance(next); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Advance = %v", err)
			}
			got, exists, err := a.Load()
			if err != nil || !exists || got != accepted {
				t.Fatalf("failed Advance changed accepted witness: %+v, %v, %v", got, exists, err)
			}
		})
	}
}

func TestLeaseAnchorRejectsCorruptPublishedRecords(t *testing.T) {
	for _, name := range []string{"intent", "witness"} {
		for _, corruption := range []string{"empty", "checksum", "header", "symlink", "hardlink", "oversized", "public"} {
			t.Run(name+"/"+corruption, func(t *testing.T) {
				config := leaseAnchorFixture(t)
				a := mustLeaseAnchor(t, config)
				if err := a.Advance(anchorEvidence(a)); err != nil {
					t.Fatal(err)
				}
				if err := a.Complete(); err != nil {
					t.Fatal(err)
				}
				if err := a.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(config.Directory, config.Name+"."+name)
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				switch corruption {
				case "empty":
					body = nil
				case "checksum":
					body[len(body)-1] ^= 1
				case "header":
					body[0] ^= 1
				case "oversized":
					body = bytes.Repeat([]byte{0}, leaseAnchorMaxBytes+1)
				case "public":
					if err := os.Chmod(path, 0o644); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					if err := os.Rename(path, path+".saved"); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(path+".saved", path); err != nil {
						t.Fatal(err)
					}
				case "hardlink":
					if err := os.Link(path, path+".saved"); err != nil {
						t.Fatal(err)
					}
				}
				if corruption == "empty" || corruption == "checksum" || corruption == "header" || corruption == "oversized" {
					if err := os.WriteFile(path, body, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				config.Initialize = false
				if opened, err := Open(config); !errors.Is(err, syscall.EIO) {
					if opened != nil {
						_ = opened.Close()
					}
					t.Fatalf("Open corrupt %s = %v", name, err)
				}
			})
		}
	}
}

func TestLeaseAnchorPreservesLifetimeOwnership(t *testing.T) {
	config := leaseAnchorFixture(t)
	a := mustLeaseAnchor(t, config)
	other, err := unix.Open(config.Directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(other)
	if err := unix.Flock(other, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("parallel writable ownership = %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(other, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("anchor Close released caller's ownership = %v", err)
	}
	if _, _, err := a.Load(); !errors.Is(err, syscall.EIO) || !errors.Is(err, syscall.EBADF) {
		t.Fatalf("Load after Close = %v", err)
	}
}

func TestLeaseAnchorRefusesDirectoryReplacement(t *testing.T) {
	config := leaseAnchorFixture(t)
	a := mustLeaseAnchor(t, config)
	if err := a.Advance(anchorEvidence(a)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(config.Directory, config.Directory+"-moved"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(config.Directory + "-moved") })
	if err := os.Mkdir(config.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Load(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Load after directory replacement = %v", err)
	}
	if err := a.Complete(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Complete after directory replacement = %v", err)
	}
}

func TestLeaseAnchorFileBindingAndRuntimeLoss(t *testing.T) {
	config := leaseAnchorFixture(t)
	path := filepath.Join(config.Directory, "metadata.sqlite")
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	config.BindingFD = fd
	a := mustLeaseAnchor(t, config)
	if err := a.Advance(anchorEvidence(a)); err != nil {
		t.Fatal(err)
	}
	if err := a.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := a.Complete(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fremovexattr(fd, leaseBindingAttribute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Load(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Load after binding removal = %v", err)
	}
	if err := a.Advance(anchorEvidence(a)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Advance after binding removal = %v", err)
	}
}

func TestLeaseAnchorInterruptedPublicationFailurePreservesAccepted(t *testing.T) {
	config := leaseAnchorFixture(t)
	a := mustLeaseAnchor(t, config)
	accepted := anchorEvidence(a)
	if err := a.Advance(accepted); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(config.Directory, config.Name+".witness.stage")
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	next := accepted
	next.Generation++
	next.MaxLease++
	if err := a.Advance(next); !errors.Is(err, syscall.EIO) || !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("Advance with unremovable stage = %v", err)
	}
	got, exists, err := a.Load()
	if err != nil || !exists || got != accepted {
		t.Fatalf("failed publication changed witness = %+v, %v, %v", got, exists, err)
	}
}

func TestLeaseAnchorChecksConfigurationAndRecordEncoding(t *testing.T) {
	config := leaseAnchorFixture(t)
	for name, change := range map[string]func(*Config){
		"relative": func(c *Config) { c.Directory = "relative" },
		"name":     func(c *Config) { c.Name = "../elsewhere" },
		"identity": func(c *Config) { c.Identity = "" },
		"fd":       func(c *Config) { c.BindingFD = -1 },
		"time":     func(c *Config) { c.RecoveryStart = time.Time{} },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := config
			change(&invalid)
			if a, err := Open(invalid); !errors.Is(err, syscall.EINVAL) {
				if a != nil {
					_ = a.Close()
				}
				t.Fatalf("invalid configuration = %v", err)
			}
		})
	}
	encoded, err := encodeLeaseRecord("intent", leaseAnchorIntent{})
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeLeaseRecord(encoded, "witness", &Evidence{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("decode wrong record kind = %v", err)
	}
	if _, err := encodeLeaseRecord("oversized", strings.Repeat("a", leaseAnchorMaxBytes)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("encode oversized record = %v", err)
	}
	if _, err := encodeLeaseRecord("unsupported", make(chan struct{})); !errors.Is(err, syscall.EIO) {
		t.Fatalf("encode unsupported value = %v", err)
	}
}

func TestLeaseAnchorReconcilesUncertainPublication(t *testing.T) {
	for _, failDirectory := range []bool{false, true} {
		name := "stage-sync"
		if failDirectory {
			name = "directory-sync"
		}
		t.Run(name, func(t *testing.T) {
			config := leaseAnchorFixture(t)
			a := mustLeaseAnchor(t, config)
			accepted := anchorEvidence(a)
			if err := a.Advance(accepted); err != nil {
				t.Fatal(err)
			}
			if err := a.Complete(); err != nil {
				t.Fatal(err)
			}
			next := accepted
			next.Generation++
			next.MaxLease++
			injected := errors.New("injected lease sync failure")
			a.syncFile = func(fd int) error {
				if (fd == a.directoryFD) == failDirectory {
					return injected
				}
				return unix.Fsync(fd)
			}
			if err := a.Advance(next); !errors.Is(err, injected) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("Advance with sync failure = %v", err)
			}
			got, exists, err := a.Load()
			want := accepted
			if failDirectory {
				want = next
			}
			if err != nil || !exists || got != want {
				t.Fatalf("Load after uncertain publication = %+v, %v, %v; want %+v", got, exists, err, want)
			}
			a.syncFile = unix.Fsync
			if err := a.Advance(next); err != nil {
				t.Fatal(err)
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			config.Initialize = false
			reopened := mustLeaseAnchor(t, config)
			got, exists, err = reopened.Load()
			if err != nil || !exists || got != next {
				t.Fatalf("Load after reconciled reopen = %+v, %v, %v", got, exists, err)
			}
		})
	}
}

func TestLeaseAnchorRejectsRemoteFilesystemsBeforeMutation(t *testing.T) {
	for _, filesystem := range []int64{
		unix.NFS_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC, unix.AFS_FS_MAGIC, unix.AFS_SUPER_MAGIC, unix.CEPH_SUPER_MAGIC,
		unix.CODA_SUPER_MAGIC, unix.NCP_SUPER_MAGIC,
	} {
		t.Run(fmt.Sprintf("%x", filesystem), func(t *testing.T) {
			config := leaseAnchorFixture(t)
			operations := leaseAnchorSystemOperations
			operations.fstatfs = func(_ int, stat *unix.Statfs_t) error {
				stat.Type = filesystem
				return nil
			}
			if a, err := openAnchor(config, operations); !errors.Is(err, syscall.EOPNOTSUPP) {
				if a != nil {
					_ = a.Close()
				}
				t.Fatalf("Open on remote filesystem = %v", err)
			}
			entries, err := os.ReadDir(config.Directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("remote rejection changed directory: %v, %v", entries, err)
			}
			if _, err := unix.Fgetxattr(config.BindingFD, leaseBindingAttribute, make([]byte, leaseAnchorMaxBytes)); !errors.Is(err, syscall.ENODATA) {
				t.Fatalf("remote rejection changed native binding: %v", err)
			}
		})
	}
}

func TestLeaseAnchorProbesCapabilitiesOnInitializeAndReopen(t *testing.T) {
	injected := errors.New("injected lease capability failure")
	for _, reopen := range []bool{false, true} {
		for name, change := range map[string]func(*leaseAnchorOperations){
			"filesystem": func(o *leaseAnchorOperations) {
				o.fstatfs = func(int, *unix.Statfs_t) error { return injected }
			},
			"xattr-write": func(o *leaseAnchorOperations) {
				o.setxattr = func(int, string, []byte, int) error { return injected }
			},
			"xattr-read": func(o *leaseAnchorOperations) {
				o.getxattr = func(int, string, []byte) (int, error) { return 0, injected }
			},
			"flock": func(o *leaseAnchorOperations) {
				o.flock = func(int, int) error { return injected }
			},
			"create-only-rename": func(o *leaseAnchorOperations) {
				o.renameat2 = func(int, string, int, string, uint) error { return injected }
			},
			"replacement-rename": func(o *leaseAnchorOperations) {
				o.renameat = func(int, string, int, string) error { return injected }
			},
			"file-sync": func(o *leaseAnchorOperations) {
				o.fsync = func(fd int) error {
					var stat unix.Stat_t
					if err := unix.Fstat(fd, &stat); err != nil {
						return err
					}
					if stat.Mode&unix.S_IFMT == unix.S_IFREG {
						return injected
					}
					return unix.Fsync(fd)
				}
			},
			"directory-sync": func(o *leaseAnchorOperations) {
				o.fsync = func(fd int) error {
					var stat unix.Stat_t
					if err := unix.Fstat(fd, &stat); err != nil {
						return err
					}
					if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
						return injected
					}
					return unix.Fsync(fd)
				}
			},
		} {
			t.Run(fmt.Sprintf("reopen=%v/%s", reopen, name), func(t *testing.T) {
				config := leaseAnchorFixture(t)
				var accepted Evidence
				if reopen {
					a := mustLeaseAnchor(t, config)
					accepted = anchorEvidence(a)
					if err := a.Advance(accepted); err != nil {
						t.Fatal(err)
					}
					if err := a.Complete(); err != nil {
						t.Fatal(err)
					}
					if err := a.Close(); err != nil {
						t.Fatal(err)
					}
					config.Initialize = false
				}
				operations := leaseAnchorSystemOperations
				change(&operations)
				if a, err := openAnchor(config, operations); !errors.Is(err, injected) {
					if a != nil {
						_ = a.Close()
					}
					t.Fatalf("Open without capability = %v", err)
				}
				assertLeaseProbesAbsent(t, config)
				if reopen {
					a := mustLeaseAnchor(t, config)
					got, exists, err := a.Load()
					if err != nil || !exists || got != accepted {
						t.Fatalf("failed probe changed witness: %+v, %v, %v", got, exists, err)
					}
				} else if entries, err := os.ReadDir(config.Directory); err != nil || len(entries) != 0 {
					t.Fatalf("failed capability probe initialized state: %v, %v", entries, err)
				}
			})
		}
	}
}

func assertLeaseProbesAbsent(t *testing.T, config Config) {
	t.Helper()
	for _, suffix := range []string{".probe-source", ".probe-target"} {
		if _, err := os.Lstat(filepath.Join(config.Directory, config.Name+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("capability probe survived: %s: %v", suffix, err)
		}
	}
}

func TestLeaseAnchorRejectsFalseCapabilitySuccess(t *testing.T) {
	for name, change := range map[string]func(*leaseAnchorOperations){
		"nonexclusive-flock": func(o *leaseAnchorOperations) {
			o.flock = func(int, int) error { return nil }
		},
		"replaceable-xattr": func(o *leaseAnchorOperations) {
			o.setxattr = func(fd int, name string, value []byte, _ int) error {
				return unix.Fsetxattr(fd, name, value, 0)
			}
		},
		"wrong-xattr-value": func(o *leaseAnchorOperations) {
			o.getxattr = func(_ int, _ string, value []byte) (int, error) {
				return copy(value, "wrong value"), nil
			}
		},
		"ignored-create-only-rename": func(o *leaseAnchorOperations) {
			o.renameat2 = func(int, string, int, string, uint) error { return nil }
		},
		"replacing-create-only-rename": func(o *leaseAnchorOperations) {
			o.renameat2 = func(fromFD int, from string, toFD int, to string, _ uint) error {
				return unix.Renameat(fromFD, from, toFD, to)
			}
		},
		"ignored-replacement-rename": func(o *leaseAnchorOperations) {
			o.renameat = func(int, string, int, string) error { return nil }
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := leaseAnchorFixture(t)
			operations := leaseAnchorSystemOperations
			change(&operations)
			if a, err := openAnchor(config, operations); !errors.Is(err, syscall.EOPNOTSUPP) {
				if a != nil {
					_ = a.Close()
				}
				t.Fatalf("Open with false capability success = %v", err)
			}
			assertLeaseProbesAbsent(t, config)
		})
	}
}

func TestLeaseAnchorCleansOnlyValidInterruptedProbes(t *testing.T) {
	for _, state := range []string{"empty", "partial", "complete", "symlink", "hardlink", "oversized", "public", "directory"} {
		t.Run(state, func(t *testing.T) {
			config := leaseAnchorFixture(t)
			source := filepath.Join(config.Directory, config.Name+".probe-source")
			target := filepath.Join(config.Directory, config.Name+".probe-target")
			body := []byte(leaseProbeContents)
			if state == "empty" {
				body = nil
			} else if state == "partial" {
				body = []byte("lease")
			} else if state == "oversized" {
				body = bytes.Repeat([]byte{0}, len(leaseProbeContents)+1)
			}
			if err := os.WriteFile(source, body, 0o600); err != nil {
				t.Fatal(err)
			}
			switch state {
			case "symlink":
				if err := os.Symlink(source, target); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(source, target); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(source, 0o644); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.WriteFile(target, []byte("lease"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if state == "empty" || state == "partial" || state == "complete" {
				mustLeaseAnchor(t, config)
				assertLeaseProbesAbsent(t, config)
			} else {
				if a, err := Open(config); !errors.Is(err, syscall.EIO) {
					if a != nil {
						_ = a.Close()
					}
					t.Fatalf("Open with invalid probe residue = %v", err)
				}
				if actual, err := os.ReadFile(source); err != nil || !bytes.Equal(actual, body) {
					t.Fatalf("invalid residue was changed: %q, %v", actual, err)
				}
			}
		})
	}
}
