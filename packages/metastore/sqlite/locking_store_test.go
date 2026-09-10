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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"golang.org/x/sys/unix"
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
					t.Fatalf("acknowledged volume after owner exit: %v", err)
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
		for _, volume := range []string{"workspace", "other"} {
			opened, err := OpenWithOptions(t.Context(), path, volume, 0, DefaultOptions())
			if opened != nil {
				opened.Close()
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("raw open of bound database through %q in volume %q = %v", path, volume, err)
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
			Database: database, Volume: "workspace", SQLite: DefaultOptions(),
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

func lockingTestConfig(t *testing.T) LockingConfig {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return LockingConfig{
		Database: filepath.Join(directory, "volume.sqlite"), Volume: "workspace",
		SQLite: DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	}
}

func TestLockingStoreRetainsOldMaximumAndRejectsRawReopen(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := s.RaiseMaxLease(t.Context(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if raw, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, DefaultOptions()); !errors.Is(err, syscall.EIO) {
		if raw != nil {
			raw.Close()
		}
		t.Fatalf("raw reopen = %v", err)
	}
	config.Initialize = false
	config.Locks.MaxLease = time.Second
	s, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if start := s.anchor.RecoveryStart(); start.IsZero() ||
		!start.Equal(s.file.Acquired()) || !start.Equal(s.RecoveryStart()) {
		t.Fatalf("reopened anchor starts recovery at %v, native owner at %v, store at %v",
			start, s.file.Acquired(), s.RecoveryStart())
	}
	if got, err := s.MaxLease(t.Context()); err != nil || got != time.Minute {
		t.Fatalf("maximum = %v, %v", got, err)
	}
	if _, err := s.Stat(t.Context(), "file"); err != nil {
		t.Fatalf("recovery read = %v", err)
	}
	if err := s.Create(t.Context(), "conflict"); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("recovery mutation = %v", err)
	}
	status, err := s.LockService().(locking.StatusService).Status(t.Context())
	if err != nil || !status.Recovering || status.RecoveryRemainingMillis <= 0 {
		t.Fatalf("recovery status = %+v, %v", status, err)
	}
}

func TestLockingStoreExclusiveOwnerRejectsExistingRawAndLockedOpeners(t *testing.T) {
	config := lockingTestConfig(t)
	raw, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if leased, err := OpenLocking(t.Context(), config); !errors.Is(err, syscall.EBUSY) {
		if leased != nil {
			leased.Close()
		}
		t.Fatalf("lock initialization beside raw owner = %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if second, err := OpenLocking(t.Context(), config); !errors.Is(err, syscall.EBUSY) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("second native owner = %v", err)
	}
}

func TestLockingStoreMissingEvidenceDoesNotReinitialize(t *testing.T) {
	for _, missing := range []string{"row", "witness", "intent"} {
		t.Run(missing, func(t *testing.T) {
			config := lockingTestConfig(t)
			s, err := OpenLocking(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.RaiseMaxLease(t.Context(), time.Minute); err != nil {
				t.Fatal(err)
			}
			if missing == "row" {
				if _, err := s.write.Exec(`DELETE FROM lease_recovery`); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if missing != "row" {
				if err := os.Remove(filepath.Join(filepath.Dir(config.Database), ".volume.sqlite.leases."+missing)); err != nil {
					t.Fatal(err)
				}
			}
			for _, initialize := range []bool{false, true} {
				config.Initialize = initialize
				if opened, err := OpenLocking(t.Context(), config); !errors.Is(err, syscall.EIO) {
					if opened != nil {
						opened.Close()
					}
					t.Fatalf("missing %s initialize=%v returned %v", missing, initialize, err)
				}
			}
			if raw, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, DefaultOptions()); !errors.Is(err, syscall.EIO) {
				if raw != nil {
					raw.Close()
				}
				t.Fatalf("missing %s raw reopen = %v", missing, err)
			}
		})
	}
}

func TestLockingStoreAcknowledgedLeaseSurvivesSIGKILL(t *testing.T) {
	if database := os.Getenv("RFS_LEASE_CRASH_DATABASE"); database != "" {
		config := LockingConfig{
			Database: database, Volume: "workspace", SQLite: DefaultOptions(),
			Locks: locking.DefaultOptions(), Initialize: true,
		}
		s, err := OpenLocking(context.Background(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Create(context.Background(), "held"); err != nil {
			t.Fatal(err)
		}
		acquireLeaseTestGrant(t, s.LockService(), "held", 5*time.Second)
		fmt.Println("lease watermark acknowledged")
		select {}
	}
	config := lockingTestConfig(t)
	child := exec.Command(os.Args[0], "-test.run=^TestLockingStoreAcknowledgedLeaseSurvivesSIGKILL$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "RFS_LEASE_CRASH_DATABASE="+config.Database)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.ProcessState == nil {
			child.Process.Kill()
			child.Wait()
		}
	}()
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "lease watermark acknowledged" {
				ready <- nil
				return
			}
		}
		ready <- fmt.Errorf("child stopped before acknowledgment: %v", scanner.Err())
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("%v; child stderr: %s", err, stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("child did not acknowledge its durable watermark")
	}
	if err := child.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("SIGKILL child exited successfully")
	}
	config.Initialize = false
	config.Locks.MaxLease = time.Second
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got, err := s.MaxLease(t.Context()); err != nil || got != 5*time.Second {
		t.Fatalf("maximum after SIGKILL = %v, %v", got, err)
	}
	if _, err := s.Stat(t.Context(), "held"); err != nil {
		t.Fatalf("committed file after SIGKILL = %v", err)
	}
	if err := s.Remove(t.Context(), "held"); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("conflicting mutation during recovered interval = %v", err)
	}
}

func acquireLeaseTestGrant(t *testing.T, service locking.Service, path string, ttl time.Duration) {
	t.Helper()
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "owner")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := service.Resolve(t.Context(), owner.Ref, path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner.Ref, Request: "grant", Resource: resource, Mode: locking.Exclusive, TTL: ttl,
	})
	if err != nil || result.Receipt.Outcome != locking.Granted || result.Grant == nil {
		t.Fatalf("grant was not acknowledged: outcome=%v, err=%v", result.Receipt.Outcome, err)
	}
}

func TestLockingStoreInvalidConfigurationDoesNotCreateEvidence(t *testing.T) {
	for _, change := range []func(*LockingConfig){
		func(c *LockingConfig) { c.Allowance = -1 },
		func(c *LockingConfig) { c.SQLite.MaxReaderConnections = -1 },
		func(c *LockingConfig) { c.Locks.MaxLease = -1 },
	} {
		config := lockingTestConfig(t)
		change(&config)
		if s, err := OpenLocking(t.Context(), config); err == nil {
			s.Close()
			t.Fatal("invalid configuration opened")
		}
		entries, err := os.ReadDir(filepath.Dir(config.Database))
		if err != nil || len(entries) != 0 {
			t.Fatalf("invalid configuration created evidence: %v, %v", entries, err)
		}
	}
}

func TestLockingStoreAbortReleasesNativeOwnership(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	s, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLockingStoreRawAliasCannotBypassNativeBinding(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.write.Exec(`DELETE FROM lease_recovery`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias.sqlite")
	if err := os.Symlink(config.Database, alias); err != nil {
		t.Fatal(err)
	}
	if raw, err := OpenWithOptions(t.Context(), alias, config.Volume, 0, DefaultOptions()); !errors.Is(err, syscall.EIO) {
		if raw != nil {
			raw.Close()
		}
		t.Fatalf("raw alias bypassed missing SQL evidence: %v", err)
	}
	for _, suffix := range []string{"?mode=rw", "#fragment", "%00"} {
		if raw, err := OpenWithOptions(t.Context(), config.Database+suffix, config.Volume, 0, DefaultOptions()); !errors.Is(err, syscall.EINVAL) {
			if raw != nil {
				raw.Close()
			}
			t.Fatalf("URI-modified database path = %v", err)
		}
	}
}
