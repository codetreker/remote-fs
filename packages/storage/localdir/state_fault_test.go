package localdir

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDirectoryStateRaiseFailureFencesAndRecovers(t *testing.T) {
	cases := []struct {
		kind         string
		nth          int
		expectRaised bool
	}{
		{"write", 1, false}, {"write", 2, true},
		{"fsync", 1, false}, {"fsync", 2, true}, {"fsync", 3, true}, {"fsync", 4, true}, {"fsync", 5, true},
		{"rename", 1, false}, {"rename", 2, true},
		{"witness before effect", 1, true}, {"witness after effect", 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.kind+"-"+string(rune('0'+tc.nth)), func(t *testing.T) {
			ctx := context.Background()
			cfg := stateTestConfig(t)
			if err := Init(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			state := requireStateOpen(t, cfg)
			if err := state.RaiseMaxLease(ctx, time.Second); err != nil {
				t.Fatal(err)
			}
			original := state.ops
			calls := 0
			switch tc.kind {
			case "write":
				state.ops.write = func(fd int, data []byte) (int, error) {
					calls++
					if calls == tc.nth {
						return 0, syscall.ENOSPC
					}
					return original.write(fd, data)
				}
			case "fsync":
				state.ops.fsync = func(fd int) error {
					calls++
					if calls == tc.nth {
						return syscall.ENOSPC
					}
					return original.fsync(fd)
				}
			case "rename":
				state.ops.rename = func(a int, b string, c int, d string) error {
					calls++
					if calls == tc.nth {
						return syscall.ENOSPC
					}
					return original.rename(a, b, c, d)
				}
			case "witness before effect":
				state.ops.setxattr = func(int, string, []byte, int) error { return syscall.ENOSPC }
			case "witness after effect":
				state.ops.setxattr = func(fd int, name string, data []byte, flags int) error {
					return errors.Join(original.setxattr(fd, name, data, flags), syscall.ENOSPC)
				}
			}
			if err := state.RaiseMaxLease(ctx, 2*time.Second); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("failed phase acknowledged: %v", err)
			}
			if _, err := state.MaxLease(ctx); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("uncertain watermark was exposed: %v", err)
			}
			if err := state.RaiseMaxLease(ctx, time.Millisecond); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("smaller raise bypassed uncertainty: %v", err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			recovered := requireStateOpen(t, cfg)
			want := time.Second
			if tc.expectRaised {
				want = 2 * time.Second
			}
			if got, err := recovered.MaxLease(ctx); err != nil || got != want {
				t.Fatalf("recovered %v want %v: %v", got, want, err)
			}
		})
	}
}

func TestDirectoryStateRecordCloseUncertaintyRetainsOwner(t *testing.T) {
	ctx := context.Background()
	cfg := stateTestConfig(t)
	if err := Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	state, err := openBoundState(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	owner := state.ownerFD
	defer unix.Close(owner)
	closeCalls := 0
	original := state.ops.close
	state.ops.close = func(fd int) error {
		closeCalls++
		err := original(fd)
		if closeCalls == 1 {
			return errors.Join(err, syscall.EIO)
		}
		return err
	}
	if err := state.RaiseMaxLease(ctx, time.Second); !errors.Is(err, syscall.EIO) {
		t.Fatalf("private close error: %v", err)
	}
	if err := state.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("close did not retain native uncertainty: %v", err)
	}
	if _, err := openBoundState(ctx, cfg); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("private close uncertainty released namespace: %v", err)
	}
}

func TestDirectoryStateInterruptedInitializationRequiresMatchingIntent(t *testing.T) {
	for _, stop := range []string{"intent durable", "binding visible", "witness visible", "ready not renamed"} {
		t.Run(stop, func(t *testing.T) {
			ctx := context.Background()
			cfg := stateTestConfig(t)
			state, err := openStateDirectories(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			original := state.ops
			switch stop {
			case "intent durable":
				state.ops.setxattr = func(fd int, name string, data []byte, flags int) error {
					if name == bindingAttribute {
						return syscall.EIO
					}
					return original.setxattr(fd, name, data, flags)
				}
			case "binding visible":
				state.ops.setxattr = func(fd int, name string, data []byte, flags int) error {
					if name == witnessAttribute {
						return syscall.EIO
					}
					return original.setxattr(fd, name, data, flags)
				}
			case "witness visible":
				state.ops.setxattr = func(fd int, name string, data []byte, flags int) error {
					err := original.setxattr(fd, name, data, flags)
					if name == witnessAttribute {
						return errors.Join(err, syscall.EIO)
					}
					return err
				}
			case "ready not renamed":
				state.ops.rename = func(from int, src string, to int, dst string) error {
					if src == stateFilename+".next" {
						if record, err := state.readRecord(); err == nil && record.Phase == "INIT" {
							return syscall.EIO
						}
					}
					return original.rename(from, src, to, dst)
				}
			}
			if err := state.initialize(ctx); !errors.Is(err, syscall.EIO) {
				t.Fatalf("partial Init: %v", err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			if opened, err := openBoundState(ctx, cfg); err == nil {
				opened.Close()
				t.Fatal("Open accepted incomplete Init")
			}
			if err := Init(ctx, cfg); err != nil {
				t.Fatalf("recorded Init did not finish: %v", err)
			}
			ready := requireStateOpen(t, cfg)
			if got, err := ready.MaxLease(ctx); err != nil || got != 0 {
				t.Fatalf("init horizon %v: %v", got, err)
			}
		})
	}
}

func TestDirectoryStateInitializationCannotReplaceEvidence(t *testing.T) {
	for _, kind := range []string{"lost intent", "wrong intent", "nonempty state", "unrecorded partial write", "missing staging"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			cfg := stateTestConfig(t)
			state, err := openStateDirectories(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			original := state.ops.setxattr
			state.ops.setxattr = func(fd int, name string, data []byte, flags int) error {
				if name == witnessAttribute {
					return syscall.EIO
				}
				return original(fd, name, data, flags)
			}
			if err := state.initialize(ctx); !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			switch kind {
			case "lost intent":
				if err := os.Remove(filepath.Join(cfg.StateRoot, stateFilename)); err != nil {
					t.Fatal(err)
				}
			case "wrong intent":
				record, err := state.readRecord()
				if err != nil {
					t.Fatal(err)
				}
				record.Binding.State, err = newStateID()
				if err != nil {
					t.Fatal(err)
				}
				if err := state.syncRecord(record); err != nil {
					t.Fatal(err)
				}
			case "nonempty state":
				if err := os.WriteFile(filepath.Join(cfg.StateRoot, "unrecognized"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "unrecorded partial write":
				if err := unix.Fremovexattr(state.rootFD, bindingAttribute); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(cfg.StateRoot, stateFilename), filepath.Join(cfg.StateRoot, stateFilename+".next")); err != nil {
					t.Fatal(err)
				}
			case "missing staging":
				if err := os.Remove(filepath.Join(cfg.StateRoot, stagingDirectory)); err != nil {
					t.Fatal(err)
				}
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			if err := Init(ctx, cfg); err == nil {
				t.Fatal("Init replaced unknown evidence")
			}
		})
	}
}

type stateClock struct{ now time.Time }

func (c stateClock) Now() time.Time                       { return c.now }
func (c stateClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func TestDirectoryStateRecoveryStartUsesOwnershipClock(t *testing.T) {
	cfg := stateTestConfig(t)
	cfg.Locks.Clock = stateClock{time.Unix(123, 456)}
	if err := Init(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	state := requireStateOpen(t, cfg)
	if state.RecoveryStart() != cfg.Locks.Clock.Now() {
		t.Fatalf("unexpected ownership clock: %v", state.RecoveryStart())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := state.RaiseMaxLease(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled raise: %v", err)
	}
	if _, err := state.MaxLease(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled max: %v", err)
	}
}

func TestDirectoryStateProbesRefuseUnsupportedCapabilities(t *testing.T) {
	for _, capability := range []string{"xattr", "rename", "file fsync", "root fsync", "write"} {
		t.Run(capability, func(t *testing.T) {
			ctx := context.Background()
			cfg := stateTestConfig(t)
			state, err := openStateDirectories(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			original := state.ops
			switch capability {
			case "xattr":
				state.ops.setxattr = func(int, string, []byte, int) error { return syscall.EOPNOTSUPP }
			case "rename":
				state.ops.rename = func(int, string, int, string) error { return syscall.EOPNOTSUPP }
			case "file fsync":
				state.ops.fsync = func(fd int) error {
					var stat unix.Stat_t
					if err := unix.Fstat(fd, &stat); err != nil {
						return err
					}
					if stat.Mode&unix.S_IFMT == unix.S_IFREG {
						return syscall.EOPNOTSUPP
					}
					return original.fsync(fd)
				}
			case "root fsync":
				state.ops.fsync = func(fd int) error {
					if fd == state.rootFD {
						return syscall.EOPNOTSUPP
					}
					return original.fsync(fd)
				}
			case "write":
				state.ops.write = func(int, []byte) (int, error) { return 0, syscall.EOPNOTSUPP }
			}
			if err := state.initialize(ctx); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unsupported capability: %v", err)
			}
			if _, err := readStateAttribute(state.rootFD, bindingAttribute); !errors.Is(err, unix.ENODATA) {
				t.Fatalf("unsupported filesystem was bound: %v", err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			if err := Init(ctx, cfg); err != nil {
				t.Fatalf("clean supported retry: %v", err)
			}
		})
	}
}

func TestDirectoryStateRecoversPrivateProbeArtifactsAtMinimumBounds(t *testing.T) {
	ctx := context.Background()
	cfg := stateTestConfig(t)
	cfg.Limits.MaxStagingBytes = 1
	cfg.Limits.MaxRecoveryEntries = 1
	if err := os.Mkdir(filepath.Join(cfg.StateRoot, stagingDirectory), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.StateRoot, stagingDirectory, ".remote-fs-probe-interrupted-moved"), []byte("l"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(cfg.Root, "user.remote-fs.probe", []byte("lease capability probe"), unix.XATTR_CREATE); err != nil {
		t.Fatal(err)
	}
	if err := Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	state := requireStateOpen(t, cfg)
	if _, err := readStateAttribute(state.rootFD, "user.remote-fs.probe"); !errors.Is(err, unix.ENODATA) {
		t.Fatalf("probe xattr remains: %v", err)
	}
	names, err := os.ReadDir(cfg.Root)
	if err != nil || len(names) != 0 {
		t.Fatalf("probe exposed namespace entries: %v, %v", names, err)
	}
	names, err = os.ReadDir(filepath.Join(cfg.StateRoot, stagingDirectory))
	if err != nil || len(names) != 0 {
		t.Fatalf("probe staging remains: %v, %v", names, err)
	}
}

func TestDirectoryStateReplacementCannotReplayAnOlderAcceptedRecord(t *testing.T) {
	ctx := context.Background()
	cfg := stateTestConfig(t)
	if err := Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	state := requireStateOpen(t, cfg)
	if err := state.RaiseMaxLease(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(cfg.StateRoot, stateFilename))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.RaiseMaxLease(ctx, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(cfg.StateRoot, cfg.StateRoot+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cfg.StateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(cfg.StateRoot, stagingDirectory), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.StateRoot, stateFilename), old, 0o600); err != nil {
		t.Fatal(err)
	}
	if next, err := openBoundState(ctx, cfg); err == nil {
		next.Close()
		t.Fatal("replacement reset the lease horizon")
	}
	if err := Init(ctx, cfg); err == nil {
		t.Fatal("Init reset replacement state")
	}
}
