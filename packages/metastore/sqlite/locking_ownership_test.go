package sqlite

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
)

func TestLeaseNativeOwnershipCrossProcess(t *testing.T) {
	for _, owner := range []string{"raw", "locked"} {
		for _, exit := range []string{"close", "kill"} {
			t.Run(owner+"/"+exit, func(t *testing.T) {
				config := lockingTestConfig(t)
				child := startNativeOwnerProcess(t, config.Database, owner)
				if owner == "raw" {
					other, err := OpenWithOptions(t.Context(), config.Database, "other", 0, DefaultOptions())
					if err != nil {
						t.Fatalf("second shared owner: %v", err)
					}
					if err := other.Close(); err != nil {
						t.Fatal(err)
					}
					if opened, err := OpenLocking(t.Context(), config); !errors.Is(err, syscall.EBUSY) {
						if opened != nil {
							opened.Close()
						}
						t.Fatalf("exclusive opener beside cross-process shared owner = %v", err)
					}
				} else {
					assertNativeOwnerHeld(t, config.Database)
					assertRawLeaseOpenRejected(t, config.Database)
				}
				child.stop(t, exit == "kill")
				assertNativeOwnerReleased(t, config.Database)
				if owner == "locked" {
					assertRawLeaseOpenRejected(t, config.Database)
					config.Initialize = false
				}
				reopened, err := OpenLocking(t.Context(), config)
				if err != nil {
					t.Fatalf("open after previous owner exited: %v", err)
				}
				defer func() {
					if err := reopened.Close(); err != nil {
						t.Error(err)
					}
				}()
				if _, err := reopened.Stat(t.Context(), "committed"); err != nil {
					t.Fatalf("acknowledged namespace after owner exit: %v", err)
				}
			})
		}
	}
}

func TestLockingStoreCloseFailureRetainsNativeOwnership(t *testing.T) {
	config := lockingTestConfig(t)
	child := startNativeOwnerProcess(t, config.Database, "close-failure")
	assertNativeOwnerHeld(t, config.Database)
	assertRawLeaseOpenRejected(t, config.Database)
	config.Initialize = false
	if opened, err := OpenLocking(t.Context(), config); !errors.Is(err, syscall.EBUSY) {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("exclusive opener after uncertain pool close = %v", err)
	}
	child.stop(t, true)
	assertNativeOwnerReleased(t, config.Database)
	assertRawLeaseOpenRejected(t, config.Database)
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatalf("open after uncertain owner's process exited: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertNativeOwnerHeld(t *testing.T, database string) {
	t.Helper()
	fd, err := unix.Open(database, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("native shared flock beside exclusive owner = %v", err)
	}
}

func assertNativeOwnerReleased(t *testing.T, database string) {
	t.Helper()
	fd, err := unix.Open(database, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("native exclusive flock after owner exit = %v", err)
	}
}

func assertRawLeaseOpenRejected(t *testing.T, database string) {
	t.Helper()
	directory := t.TempDir()
	alias := filepath.Join(directory, "alias.sqlite")
	if err := os.Symlink(database, alias); err != nil {
		t.Fatal(err)
	}
	parentAlias := filepath.Join(directory, "parent")
	if err := os.Symlink(filepath.Dir(database), parentAlias); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{database, alias, filepath.Join(parentAlias, filepath.Base(database))} {
		for _, namespace := range []string{"workspace", "other"} {
			opened, err := OpenWithOptions(t.Context(), path, namespace, 0, DefaultOptions())
			if opened != nil {
				opened.Close()
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("raw open of bound database through %q in namespace %q = %v", path, namespace, err)
			}
		}
	}
}

type nativeOwnerProcess struct {
	command *exec.Cmd
	input   io.WriteCloser
	stderr  bytes.Buffer
	cancel  context.CancelFunc
}

func startNativeOwnerProcess(t *testing.T, database, mode string) *nativeOwnerProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	child := &nativeOwnerProcess{
		command: exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLeaseNativeOwnershipProcess$", "-test.timeout=15s"),
		cancel:  cancel,
	}
	child.command.Env = append(os.Environ(), "RFS_NATIVE_OWNER_DATABASE="+database, "RFS_NATIVE_OWNER_MODE="+mode)
	child.command.Stderr = &child.stderr
	stdout, err := child.command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	child.input, err = child.command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := child.command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if child.command.ProcessState == nil {
			child.command.Process.Kill()
			child.command.Wait()
		}
		child.input.Close()
		cancel()
	})
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		signaled := false
		for scanner.Scan() {
			if scanner.Text() == "native ownership ready" && !signaled {
				ready <- nil
				signaled = true
			}
		}
		if !signaled {
			ready <- fmt.Errorf("child ended before ownership confirmation: %w", scanner.Err())
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			child.command.Wait()
			t.Fatalf("%v; stderr: %s", err, child.stderr.String())
		}
	case <-ctx.Done():
		child.command.Wait()
		t.Fatalf("child ownership confirmation: %v; stderr: %s", ctx.Err(), child.stderr.String())
	}
	return child
}

func (child *nativeOwnerProcess) stop(t *testing.T, kill bool) {
	t.Helper()
	if kill {
		if err := child.command.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	} else if _, err := io.WriteString(child.input, "close\n"); err != nil {
		t.Fatal(err)
	}
	err := child.command.Wait()
	child.cancel()
	if kill {
		var exited *exec.ExitError
		if !errors.As(err, &exited) || !exited.ProcessState.Sys().(syscall.WaitStatus).Signaled() ||
			exited.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("owner killed: %v; stderr: %s", err, child.stderr.String())
		}
	} else if err != nil {
		t.Fatalf("owner closed: %v; stderr: %s", err, child.stderr.String())
	}
}

func TestLeaseNativeOwnershipProcess(t *testing.T) {
	database := os.Getenv("RFS_NATIVE_OWNER_DATABASE")
	if database == "" {
		return
	}
	var store *Store
	var closeStore func() error
	switch os.Getenv("RFS_NATIVE_OWNER_MODE") {
	case "raw":
		opened, err := OpenWithOptions(t.Context(), database, "workspace", 0, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		store, closeStore = opened, opened.Close
	case "locked", "close-failure":
		opened, err := OpenLocking(t.Context(), LockingConfig{
			Database: database, Namespace: "workspace", SQLite: DefaultOptions(),
			Locks: locking.DefaultOptions(), Initialize: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		store, closeStore = opened.Store, opened.Close
	default:
		t.Fatal("unknown native owner mode")
	}
	if err := store.Create(t.Context(), "committed"); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RFS_NATIVE_OWNER_MODE") == "close-failure" {
		fault := errors.Join(syscall.EIO, errors.New("injected uncertain pool closure"))
		closePool := store.closePool
		calls := 0
		store.closePool = func(db *sql.DB) error {
			calls++
			return errors.Join(closePool(db), fault)
		}
		if err := closeStore(); !errors.Is(err, fault) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("uncertain close = %v", err)
		}
		if calls != 3 {
			t.Fatalf("closed pools = %d, want 3", calls)
		}
		if err := closeStore(); !errors.Is(err, fault) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("repeated uncertain close = %v", err)
		}
		if calls != 3 {
			t.Fatalf("terminal pool closures repeated: %d", calls)
		}
		if err := store.Create(t.Context(), "after-close"); !errors.Is(err, syscall.EIO) {
			t.Fatalf("mutation after failed close = %v", err)
		}
	}
	fmt.Println("native ownership ready")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() || scanner.Text() != "close" {
		t.Fatalf("owner close command: %v", scanner.Err())
	}
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}

func TestRawStoreRejectsEnableLocks(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := store.EnableLocks(t.Context(), locking.DefaultOptions()); !errors.Is(err, syscall.EINVAL) ||
		locking.CodeOf(err) != locking.Invalid {
		t.Fatalf("raw store enabled lock authority: %v", err)
	}
	assertNoLeaseAuthority(t, store)
	if err := store.Create(t.Context(), "ordinary"); err != nil {
		t.Fatalf("rejected authority changed ordinary mutation behavior: %v", err)
	}
	if _, err := store.Stat(t.Context(), "ordinary"); err != nil {
		t.Fatal(err)
	}
}

func TestConfigureLeaseRecoveryRequiresNativeAnchor(t *testing.T) {
	t.Run("shared owner", func(t *testing.T) {
		config := lockingTestConfig(t)
		store, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}()
		err = store.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
			Witness: &testLeaseWitness{}, StateID: "0123456789abcdef0123456789abcdef",
			RecoveryStart: time.Now(), Initialize: true,
		})
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("shared owner configured lease recovery: %v", err)
		}
		assertNoLeaseAuthority(t, store)
	})
	for _, witness := range []struct {
		name  string
		value LeaseWitness
	}{
		{"generic witness", &testLeaseWitness{}},
		{"nil witness", nil},
		{"typed nil anchor", (*LeaseAnchor)(nil)},
	} {
		t.Run(witness.name, func(t *testing.T) {
			store, _ := openNativeExclusiveTestStore(t)
			err := store.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
				Witness: witness.value, StateID: "0123456789abcdef0123456789abcdef",
				RecoveryStart: time.Now(), Initialize: true,
			})
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("unanchored witness configured lease recovery: %v", err)
			}
			assertNoLeaseAuthority(t, store)
		})
	}
	t.Run("different binding inode", func(t *testing.T) {
		store, owner := openNativeExclusiveTestStore(t)
		foreign, err := acquireLeaseDatabase(filepath.Join(filepath.Dir(owner.path), "foreign.sqlite"), true, true)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := foreign.Close(); err != nil {
				t.Error(err)
			}
		}()
		anchor := openNativeTestAnchor(t, store, foreign)
		err = store.ConfigureLeaseRecovery(t.Context(), LeaseRecoveryConfig{
			Witness: anchor, StateID: anchor.StateID(), RecoveryStart: owner.acquired, Initialize: true,
		})
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("different binding inode configured lease recovery: %v", err)
		}
		assertNoLeaseAuthority(t, store)
	})
	for _, invalid := range []string{"state identity", "initialization intent"} {
		t.Run(invalid, func(t *testing.T) {
			store, owner := openNativeExclusiveTestStore(t)
			anchor := openNativeTestAnchor(t, store, owner)
			config := LeaseRecoveryConfig{
				Witness: anchor, StateID: anchor.StateID(), RecoveryStart: owner.acquired, Initialize: true,
			}
			if invalid == "state identity" {
				config.StateID = "invalid"
			} else {
				config.Initialize = false
			}
			if err := store.ConfigureLeaseRecovery(t.Context(), config); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid %s configured lease recovery: %v", invalid, err)
			}
			assertNoLeaseAuthority(t, store)
			config.StateID, config.Initialize = anchor.StateID(), true
			if err := store.ConfigureLeaseRecovery(t.Context(), config); err != nil {
				t.Fatalf("valid native recovery after rejected configuration: %v", err)
			}
			if err := anchor.Complete(); err != nil {
				t.Fatal(err)
			}
			if err := store.EnableLocks(t.Context(), locking.DefaultOptions()); err != nil {
				t.Fatalf("valid native authority: %v", err)
			}
			if store.LockService() == nil {
				t.Fatal("valid native recovery did not expose its authority")
			}
		})
	}
}

func openNativeExclusiveTestStore(t *testing.T) (*Store, *leaseDatabaseFile) {
	t.Helper()
	config := lockingTestConfig(t)
	owner, err := acquireLeaseDatabase(config.Database, true, true)
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions()
	options.leaseRecoveryOwner, options.leaseOwner = true, owner
	store, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, options)
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Abort(); err != nil {
			t.Error(err)
		}
	})
	return store, owner
}

func openNativeTestAnchor(t *testing.T, store *Store, owner *leaseDatabaseFile) *LeaseAnchor {
	t.Helper()
	anchor, err := OpenLeaseAnchor(LeaseAnchorConfig{
		Directory: filepath.Dir(owner.path), Name: "." + filepath.Base(owner.path) + ".leases",
		Identity: "sqlite-database-lease-recovery", BindingFD: owner.fd,
		RecoveryStart: owner.acquired, Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Abort(); err != nil {
			t.Error(err)
		}
		if err := anchor.Close(); err != nil {
			t.Error(err)
		}
	})
	return anchor
}

func assertNoLeaseAuthority(t *testing.T, store *Store) {
	t.Helper()
	if store.LockService() != nil || store.leaseRecovery != nil {
		t.Fatal("rejected configuration attached lease authority or recovery")
	}
	var rows int
	if err := store.read.QueryRow(`SELECT count(*) FROM lease_recovery`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("rejected configuration persisted %d lease recovery records", rows)
	}
}
