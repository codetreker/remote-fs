package sqlite

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/locking"
)

func lockingTestConfig(t *testing.T) LockingConfig {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return LockingConfig{
		Database: filepath.Join(directory, "namespace.sqlite"), Namespace: "workspace",
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
	if raw, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions()); !errors.Is(err, syscall.EIO) {
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
	raw, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions())
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
				if err := os.Remove(filepath.Join(filepath.Dir(config.Database), ".namespace.sqlite.leases."+missing)); err != nil {
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
			if raw, err := OpenWithOptions(t.Context(), config.Database, config.Namespace, 0, DefaultOptions()); !errors.Is(err, syscall.EIO) {
				if raw != nil {
					raw.Close()
				}
				t.Fatalf("missing %s raw reopen = %v", missing, err)
			}
		})
	}
}

func TestLeaseDatabaseCloseNeverRetriesAReusedDescriptor(t *testing.T) {
	fd, err := unix.Open(filepath.Join(t.TempDir(), "held"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("close reported failure after descriptor release")
	owner := &leaseDatabaseFile{fd: fd, closeFD: func(fd int) error {
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

func TestLockingStoreAcknowledgedLeaseSurvivesSIGKILL(t *testing.T) {
	if database := os.Getenv("RFS_LEASE_CRASH_DATABASE"); database != "" {
		config := LockingConfig{
			Database: database, Namespace: "workspace", SQLite: DefaultOptions(),
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
	if raw, err := OpenWithOptions(t.Context(), alias, config.Namespace, 0, DefaultOptions()); !errors.Is(err, syscall.EIO) {
		if raw != nil {
			raw.Close()
		}
		t.Fatalf("raw alias bypassed missing SQL evidence: %v", err)
	}
	for _, suffix := range []string{"?mode=rw", "#fragment", "%00"} {
		if raw, err := OpenWithOptions(t.Context(), config.Database+suffix, config.Namespace, 0, DefaultOptions()); !errors.Is(err, syscall.EINVAL) {
			if raw != nil {
				raw.Close()
			}
			t.Fatalf("URI-modified database path = %v", err)
		}
	}
}
