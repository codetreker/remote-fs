package localdir_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func leaseDirectoryConfig(t *testing.T, options locking.Options) localdir.Config {
	t.Helper()
	base := t.TempDir()
	cfg := localdir.Config{
		Root: filepath.Join(base, "namespace"), StateRoot: filepath.Join(base, "state"),
		Locks: options, Limits: localdir.DefaultLimits(),
	}
	for _, directory := range []string{cfg.Root, cfg.StateRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := localdir.Init(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func openLeaseDirectory(t *testing.T, cfg localdir.Config) *localdir.Storage {
	t.Helper()
	native, err := localdir.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := native.Close(); err != nil {
			t.Errorf("closing ordinary-directory authority: %v", err)
		}
	})
	return native
}

func TestNativeFileLockContract(t *testing.T) {
	for _, quota := range []int64{0, 1 << 20} {
		name := "directory"
		if quota != 0 {
			name = "quota directory"
		}
		t.Run(name, func(t *testing.T) {
			lockcontract.Run(t, func(t *testing.T, options locking.Options) lockcontract.Fixture {
				native := openLeaseDirectory(t, leaseDirectoryConfig(t, options))
				var backend locked.Backend = native
				if quota != 0 {
					wrapper, err := limited.New(t.Context(), native, quota)
					if err != nil {
						t.Fatal(err)
					}
					backend = wrapper
				}
				paired, err := locked.New(backend)
				if err != nil {
					t.Fatal(err)
				}
				return lockcontract.Fixture{Storage: paired, Locks: paired.LockService(), Scope: paired.Scope}
			})
		})
	}
}

func TestDirectoryConstructorsShareExclusiveOwnership(t *testing.T) {
	base := t.TempDir()
	root, state := filepath.Join(base, "namespace"), filepath.Join(base, "state")
	for _, directory := range []string{root, state} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := localdir.Config{Root: root, StateRoot: state, Locks: locking.DefaultOptions(), Limits: localdir.DefaultLimits()}
	raw, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := locked.New(raw); err == nil {
		t.Fatal("raw directory advertised an enforcing authority")
	}
	if duplicate, err := localdir.New(root); err == nil {
		duplicate.Close()
		t.Fatal("raw open bypassed the existing directory owner")
	}
	if err := localdir.Init(t.Context(), cfg); err == nil {
		t.Fatal("Init bypassed an active raw writer")
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := localdir.Init(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	native := openLeaseDirectory(t, cfg)
	if duplicate, err := localdir.Open(t.Context(), cfg); err == nil {
		duplicate.Close()
		t.Fatal("bound open bypassed the existing authority")
	}
	if err := native.Close(); err != nil {
		t.Fatal(err)
	}
	if bypass, err := localdir.New(root); err == nil {
		bypass.Close()
		t.Fatal("raw open bypassed durable binding after its authority closed")
	}
}

func TestDirectoryRestartPreservesTheAcknowledgedLeaseWindow(t *testing.T) {
	clock := &directoryLeaseClock{now: time.Unix(1_700_000_000, 0)}
	options := locking.DefaultOptions()
	options.Clock = clock
	options.MaxLease = 100 * time.Millisecond
	cfg := leaseDirectoryConfig(t, options)
	first := openLeaseDirectory(t, cfg)
	if err := first.Write(t.Context(), "file", []byte("unchanged")); err != nil {
		t.Fatal(err)
	}
	service := first.LockService()
	owner := directoryLeaseOwner(t, service)
	resource, err := service.Resolve(t.Context(), owner, "file")
	if err != nil {
		t.Fatal(err)
	}
	granted, err := service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner, Request: "shared", Resource: resource,
		Mode: locking.Shared, TTL: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if granted.Grant == nil || granted.Grant.State != locking.Active {
		t.Fatal("shared lease was not granted")
	}
	oldProof := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{granted.Grant.Ref}}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	cfg.Locks.MaxLease = 10 * time.Millisecond
	second := openLeaseDirectory(t, cfg)
	statusProvider, ok := second.LockService().(interface {
		Status(context.Context) (locking.Status, error)
	})
	if !ok {
		t.Fatal("native authority cannot report recovery status")
	}
	status, err := statusProvider.Status(t.Context())
	if err != nil || !status.Recovering || status.RecoveryRemainingMillis != 80 {
		t.Fatalf("recovery did not preserve the earlier acknowledged window: %+v, %v", status, err)
	}
	if got, err := second.Read(t.Context(), "file"); err != nil || string(got) != "unchanged" {
		t.Fatalf("ordinary snapshot read during recovery: %q, %v", got, err)
	}
	for _, advance := range []time.Duration{0, 79 * time.Millisecond} {
		clock.advance(advance)
		if err := second.Write(t.Context(), "file", []byte("too early")); !errors.Is(err, &locking.Error{Code: locking.Recovering}) {
			t.Fatalf("mutation before the old window elapsed: %v", err)
		}
	}
	clock.advance(time.Millisecond)
	if err := second.Write(t.Context(), "file", []byte("after recovery")); err != nil {
		t.Fatal(err)
	}
	if err := second.Write(locking.WithScope(t.Context(), oldProof), "file", []byte("old authority")); storage.ErrnoOf(err) != syscall.ESTALE {
		t.Fatalf("old authority proof was not rejected: %v", err)
	}
	if got, err := second.Read(t.Context(), "file"); err != nil || string(got) != "after recovery" {
		t.Fatalf("old proof changed the new authority's file: %q, %v", got, err)
	}
}

func directoryLeaseOwner(t *testing.T, service locking.Service) locking.OwnerRef {
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
	return owner.Ref
}

type directoryLeaseClock struct {
	mu     sync.Mutex
	now    time.Time
	alarms []directoryLeaseAlarm
}

type directoryLeaseAlarm struct {
	at     time.Time
	result chan time.Time
}

func (c *directoryLeaseClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *directoryLeaseClock) After(wait time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(chan time.Time, 1)
	if wait <= 0 {
		result <- c.now
	} else {
		c.alarms = append(c.alarms, directoryLeaseAlarm{at: c.now.Add(wait), result: result})
	}
	return result
}

func (c *directoryLeaseClock) advance(elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(elapsed)
	remaining := c.alarms[:0]
	for _, alarm := range c.alarms {
		if !c.now.Before(alarm.at) {
			alarm.result <- c.now
		} else {
			remaining = append(remaining, alarm)
		}
	}
	c.alarms = remaining
}
