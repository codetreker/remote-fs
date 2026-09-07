package localstore_test

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
)

func acquireLocalstoreLease(t *testing.T, service locking.Service, path string) {
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
	grant, err := service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner.Ref, Request: "lease", Resource: resource, Mode: locking.Exclusive, TTL: time.Minute,
	})
	if err != nil || grant.Receipt.Outcome != locking.Granted {
		t.Fatalf("lease outcome=%v, err=%v", grant.Receipt.Outcome, err)
	}
}

func TestLocalstoreLeaseRecoveryRejectsDisabledAndLoweredProtection(t *testing.T) {
	config := testConfig(privateRoot(t))
	options := locking.DefaultOptions()
	config.Locks, config.InitializeLocks = &options, true
	s := open(t, config)
	if err := s.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(t.Context(), "held", []byte("preserved")); err != nil {
		t.Fatal(err)
	}
	acquireLocalstoreLease(t, s.LockService(), "held")
	closeStore(t, s)
	config.Locks, config.InitializeLocks = nil, false
	if unsafe, err := localstore.Open(t.Context(), config); !errors.Is(err, syscall.EIO) {
		if unsafe != nil {
			unsafe.Close()
		}
		t.Fatalf("reopen without protection = %v", err)
	}
	options.MaxLease = time.Second
	config.Locks = &options
	s = open(t, config)
	defer closeStore(t, s)
	body, err := s.Read(t.Context(), "held")
	if err != nil || string(body) != "preserved" {
		t.Fatalf("recovery read = %q, %v", body, err)
	}
	if err := s.Write(t.Context(), "held", []byte("unsafe")); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("recovery write = %v", err)
	}
	status, err := s.LockService().(locking.StatusService).Status(t.Context())
	if err != nil || !status.Recovering || status.RecoveryRemainingMillis <= time.Second.Milliseconds() {
		t.Fatalf("lowered configuration recovery = %+v, %v", status, err)
	}
}

func TestLocalstoreLeaseRecoveryRefusesMissingWitness(t *testing.T) {
	config := testConfig(privateRoot(t))
	options := locking.DefaultOptions()
	config.Locks, config.InitializeLocks = &options, true
	s := open(t, config)
	if err := s.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	acquireLocalstoreLease(t, s.LockService(), "held")
	closeStore(t, s)
	if err := os.Remove(filepath.Join(config.Root, ".leases.witness")); err != nil {
		t.Fatal(err)
	}
	for _, initialize := range []bool{false, true} {
		config.InitializeLocks = initialize
		if reopened, err := localstore.Open(t.Context(), config); !errors.Is(err, syscall.EIO) {
			if reopened != nil {
				reopened.Close()
			}
			t.Fatalf("missing witness initialize=%v: %v", initialize, err)
		}
	}
}

func TestLocalstoreLeaseRecoveryCleansInterruptedCapabilityProbe(t *testing.T) {
	config := testConfig(privateRoot(t))
	options := locking.DefaultOptions()
	config.Locks, config.InitializeLocks = &options, true
	closeStore(t, open(t, config))
	config.InitializeLocks = false
	for _, name := range []string{".leases.probe-source", ".leases.probe-target"} {
		if err := os.WriteFile(filepath.Join(config.Root, name), []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	closeStore(t, open(t, config))
	for _, name := range []string{".leases.probe-source", ".leases.probe-target"} {
		if _, err := os.Stat(filepath.Join(config.Root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("probe residue %s: %v", name, err)
		}
	}
}
