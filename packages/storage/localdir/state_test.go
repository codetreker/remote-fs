package localdir

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
)

func stateTestConfig(t *testing.T) Config {
	t.Helper()
	base := t.TempDir()
	cfg := Config{Root: filepath.Join(base, "root"), StateRoot: filepath.Join(base, "state"), Locks: locking.DefaultOptions()}
	for _, path := range []string{cfg.Root, cfg.StateRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return cfg
}

func requireStateOpen(t *testing.T, cfg Config) *directoryState {
	t.Helper()
	state, err := openBoundState(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	return state
}

func TestDirectoryStateExplicitInitOwnershipAndReopen(t *testing.T) {
	ctx := context.Background()
	cfg := stateTestConfig(t)
	if _, err := openBoundState(ctx, cfg); err == nil {
		t.Fatal("Open accepted missing evidence")
	}
	raw, err := openRawState(cfg.Root, cfg.Limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openRawState(cfg.Root, cfg.Limits); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("second raw owner: %v", err)
	}
	if err := Init(ctx, cfg); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("Init bypassed raw owner: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := openRawState(cfg.Root, cfg.Limits); err == nil {
		t.Fatal("raw owner accepted bound root")
	}
	if err := Init(ctx, cfg); !errors.Is(err, unix.EEXIST) {
		t.Fatalf("repeated Init: %v", err)
	}
	state := requireStateOpen(t, cfg)
	if _, err := openBoundState(ctx, cfg); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("second bound owner: %v", err)
	}
	if got, err := state.MaxLease(ctx); err != nil || got != 0 {
		t.Fatalf("initial duration %v: %v", got, err)
	}
	if err := state.RaiseMaxLease(ctx, 9*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := state.RaiseMaxLease(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Locks.MaxLease = time.Second
	reopened := requireStateOpen(t, cfg)
	if got, err := reopened.MaxLease(ctx); err != nil || got != 9*time.Second {
		t.Fatalf("lower config changed horizon %v: %v", got, err)
	}
	entries, err := os.ReadDir(cfg.Root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("private entries leaked into namespace: %v, %v", entries, err)
	}
}

func TestDirectoryStatePairedRecovery(t *testing.T) {
	a := leasePoint{Generation: 2, Nanos: int64(time.Second)}
	p := leasePoint{Generation: 3, Nanos: int64(2 * time.Second)}
	lower := leasePoint{Generation: 1, Nanos: int64(time.Millisecond)}
	cases := []struct {
		name              string
		accepted, witness leasePoint
		prepared          *leasePoint
		want              time.Duration
		valid             bool
	}{
		{"stable", a, a, nil, time.Second, true},
		{"before witness", a, a, &p, 2 * time.Second, true},
		{"after witness", a, p, &p, 2 * time.Second, true},
		{"lost prepared", a, p, nil, 0, false},
		{"witness rollback", a, lower, nil, 0, false},
		{"record rollback", lower, a, nil, 0, false},
		{"unrelated witness", a, lower, &p, 0, false},
		{"skipped generation", lower, lower, &p, 0, false},
		{"decreasing prepared", p, p, &leasePoint{Generation: 4, Nanos: 1}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := stateTestConfig(t)
			if err := Init(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
			state := requireStateOpen(t, cfg)
			record := state.record
			record.Accepted, record.Prepared = tc.accepted, tc.prepared
			if err := state.syncRecord(record); err != nil {
				t.Fatal(err)
			}
			if err := state.syncWitness(tc.witness, unix.XATTR_REPLACE); err != nil {
				t.Fatal(err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openBoundState(context.Background(), cfg)
			if !tc.valid {
				if err == nil {
					reopened.Close()
					t.Fatal("invalid pair was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.MaxLease(context.Background())
			if err != nil || got != tc.want {
				t.Fatalf("horizon %v want %v: %v", got, tc.want, err)
			}
			persisted, err := reopened.readRecord()
			if err != nil || persisted.Prepared != nil || persisted.Accepted.Nanos != int64(tc.want) {
				t.Fatalf("recovery did not finalize: %+v, %v", persisted, err)
			}
		})
	}
}

func TestDirectoryStateConcurrentRaisesNeverDecrease(t *testing.T) {
	cfg := stateTestConfig(t)
	if err := Init(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	state := requireStateOpen(t, cfg)
	var workers sync.WaitGroup
	for i := 1; i <= 24; i++ {
		workers.Add(1)
		go func(d time.Duration) {
			defer workers.Done()
			if err := state.RaiseMaxLease(context.Background(), d); err != nil {
				t.Error(err)
			}
		}(time.Duration(i) * time.Second)
	}
	workers.Wait()
	if got, err := state.MaxLease(context.Background()); err != nil || got != 24*time.Second {
		t.Fatalf("horizon %v: %v", got, err)
	}
}

func TestDirectoryStateMissingCorruptOrCopiedEvidenceFailsClosed(t *testing.T) {
	for _, damage := range []string{"record missing", "record corrupt", "binding missing", "witness missing", "witness corrupt", "state moved", "root replaced"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			cfg := stateTestConfig(t)
			if err := Init(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			state := requireStateOpen(t, cfg)
			if err := state.RaiseMaxLease(ctx, time.Second); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "record missing":
				if err := os.Remove(filepath.Join(cfg.StateRoot, stateFilename)); err != nil {
					t.Fatal(err)
				}
			case "record corrupt":
				if err := os.WriteFile(filepath.Join(cfg.StateRoot, stateFilename), []byte("corrupt"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "binding missing":
				if err := unix.Fremovexattr(state.rootFD, bindingAttribute); err != nil {
					t.Fatal(err)
				}
			case "witness missing":
				if err := unix.Fremovexattr(state.rootFD, witnessAttribute); err != nil {
					t.Fatal(err)
				}
			case "witness corrupt":
				if err := unix.Fsetxattr(state.rootFD, witnessAttribute, []byte("corrupt"), unix.XATTR_REPLACE); err != nil {
					t.Fatal(err)
				}
			case "state moved":
				moved := cfg.StateRoot + "-moved"
				if err := os.Rename(cfg.StateRoot, moved); err != nil {
					t.Fatal(err)
				}
				cfg.StateRoot = moved
			case "root replaced":
				if err := os.Rename(cfg.Root, cfg.Root+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(cfg.Root, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := state.health(); err == nil {
					t.Fatal("live root replacement went undetected")
				}
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			if opened, err := openBoundState(ctx, cfg); err == nil {
				opened.Close()
				t.Fatal("Open accepted damaged evidence")
			}
			if err := Init(ctx, cfg); err == nil {
				t.Fatal("Init reset damaged evidence")
			}
		})
	}
}

func TestDirectoryStatePrivacyAndPathConfinement(t *testing.T) {
	for _, kind := range []string{"nested state", "nested root", "state writable", "root writable", "symlink ancestor", "path bound"} {
		t.Run(kind, func(t *testing.T) {
			cfg := stateTestConfig(t)
			switch kind {
			case "nested state":
				cfg.StateRoot = filepath.Join(cfg.Root, "state")
				if err := os.Mkdir(cfg.StateRoot, 0o700); err != nil {
					t.Fatal(err)
				}
			case "nested root":
				cfg.Root = filepath.Join(cfg.StateRoot, "root")
				if err := os.Mkdir(cfg.Root, 0o700); err != nil {
					t.Fatal(err)
				}
			case "state writable":
				if err := os.Chmod(cfg.StateRoot, 0o750); err != nil {
					t.Fatal(err)
				}
			case "root writable":
				if err := os.Chmod(cfg.Root, 0o777); err != nil {
					t.Fatal(err)
				}
			case "symlink ancestor":
				link := filepath.Join(filepath.Dir(cfg.Root), "alias")
				if err := os.Symlink(filepath.Dir(cfg.Root), link); err != nil {
					t.Fatal(err)
				}
				cfg.Root = filepath.Join(link, "root")
			case "path bound":
				cfg.Limits.MaxPathBytes = 8
			}
			if err := Init(context.Background(), cfg); err == nil {
				t.Fatal("unsafe configuration was accepted")
			}
		})
	}
}

func TestDirectoryStateRecoveryGarbageIsBoundedAndPrivate(t *testing.T) {
	for _, kind := range []string{"bounded cleanup", "too many", "too large", "symlink", "hardlink", "directory", "unknown state file"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			cfg := stateTestConfig(t)
			if err := Init(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			staging := filepath.Join(cfg.StateRoot, stagingDirectory)
			if err := os.WriteFile(filepath.Join(staging, "upload"), []byte("payload"), 0o600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "too many":
				cfg.Limits.MaxRecoveryEntries = 2
				for _, name := range []string{"second", "third"} {
					if err := os.WriteFile(filepath.Join(staging, name), nil, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			case "too large":
				cfg.Limits.MaxStagingBytes = 3
			case "symlink":
				if err := os.Symlink(cfg.Root, filepath.Join(staging, "link")); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(filepath.Join(staging, "upload"), filepath.Join(staging, "link")); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(filepath.Join(staging, "nested"), 0o700); err != nil {
					t.Fatal(err)
				}
			case "unknown state file":
				if err := os.WriteFile(filepath.Join(cfg.StateRoot, "extra"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			state, err := openBoundState(ctx, cfg)
			if kind != "bounded cleanup" {
				if err == nil {
					state.Close()
					t.Fatal("unsafe recovery was accepted")
				}
				if _, err := os.Stat(filepath.Join(staging, "upload")); err != nil {
					t.Fatalf("recovery deleted before validating all entries: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			names, err := os.ReadDir(staging)
			if err != nil || len(names) != 0 {
				t.Fatalf("staging garbage remains: %v, %v", names, err)
			}
		})
	}
}

func TestDirectoryStateCloseUncertaintyRetainsOwnerAndDoesNotRetry(t *testing.T) {
	cfg := stateTestConfig(t)
	if err := Init(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	state, err := openBoundState(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	owner := state.ownerFD
	defer unix.Close(owner)
	child := state.stagingFD
	calls := 0
	state.ops.close = func(fd int) error {
		if fd == child {
			calls++
			if err := unix.Close(fd); err != nil {
				t.Fatal(err)
			}
			return syscall.EIO
		}
		return unix.Close(fd)
	}
	if err := state.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("close uncertainty: %v", err)
	}
	if _, err := openBoundState(context.Background(), cfg); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("uncertain close released owner: %v", err)
	}
	if err := state.Close(); !errors.Is(err, syscall.EIO) || calls != 1 {
		t.Fatalf("close retried consumed descriptor: %v, calls=%d", err, calls)
	}
	if err := state.health(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed health: %v", err)
	}
}
