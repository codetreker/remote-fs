package memoryfixture

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func TestPreparedVolumesKeepNativeChildrenIsolated(t *testing.T) {
	p := NewPrepared(t, 2, 0, locking.DefaultOptions())
	var first, second *preparedChild
	var firstIdentity, secondIdentity storage.BackendIdentityResult
	var firstState, secondState sqlite.DurableState
	var firstSession storage.FileSession
	var firstEpoch string
	var before storage.Attr
	var barrier metastore.LogBarrier
	var status sqlite.ObjectStatus
	for _, name := range []string{"populated", "empty successor"} {
		t.Run(name, func(t *testing.T) {
			meta, volume := p.New(t)
			child := p.active
			identity, state := checkPreparedNative(t, child)
			session, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			live, err := session.Status(t.Context())
			if err != nil || live.Epoch == "" {
				t.Fatalf("session has no live identity: %+v, %v", live, err)
			}
			if first == nil {
				first, firstIdentity, firstState, firstSession, firstEpoch = child, identity, state, session, live.Epoch
			} else {
				second, secondIdentity, secondState = child, identity, state
				if first.objects == second.objects || first.meta == second.meta || first.storage == second.storage || firstEpoch == live.Epoch {
					t.Fatal("successor reused child-owned objects, pools, worker or session")
				}
			}
			if err := volume.Write(t.Context(), "payload", []byte(name)); err != nil {
				t.Fatal(err)
			}
			if first == child {
				if before, err = volume.Stat(t.Context(), "payload"); err != nil {
					t.Fatal(err)
				}
				if barrier, err = meta.Barrier(t.Context(), 4096); err != nil {
					t.Fatal(err)
				}
				if status, err = meta.ObjectStatus(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
		if t.Failed() {
			t.Fatal("prepared child failed")
		}
	}
	if firstState.DatabaseID != secondState.DatabaseID || firstIdentity.Volume == secondIdentity.Volume ||
		firstIdentity.RootNodeID == secondIdentity.RootNodeID || firstIdentity.Authority == secondIdentity.Authority {
		t.Fatalf("wrong sibling identities: first=%+v/%+v, second=%+v/%+v", firstIdentity, firstState, secondIdentity, secondState)
	}
	if _, err := firstSession.Status(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed child's session remained usable: %v", err)
	}
	if _, err := p.acquire(t.Context()); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("exhausted factory allocated another volume: %v", err)
	}
	if err := p.release(first); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("repeated release returned %v", err)
	}
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: p.database, Volume: preparedVolume(0), SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(),
	})
	if err != nil {
		t.Fatalf("previous native ownership was not released: %v", err)
	}
	volume := objectstore.New(first.objects, meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("closing reopened first child: %v", err)
		}
	})
	if got, err := volume.Stat(t.Context(), "payload"); err != nil || !reflect.DeepEqual(got, before) {
		t.Fatalf("sibling changed previous attributes: %+v, %v; want %+v", got, err, before)
	}
	if got, err := meta.Barrier(t.Context(), 4096); err != nil || got != barrier {
		t.Fatalf("sibling changed previous log barrier: %+v, %v; want %+v", got, err, barrier)
	}
	if got, err := meta.ObjectStatus(t.Context()); err != nil || got != status {
		t.Fatalf("sibling changed previous object status: %+v, %v; want %+v", got, err, status)
	}
	if got, err := volume.Usage(t.Context()); err != nil || got != int64(len("populated")) {
		t.Fatalf("sibling changed previous usage: %d, %v", got, err)
	}
	if body, err := volume.Read(t.Context(), "payload"); err != nil || string(body) != "populated" {
		t.Fatalf("sibling swept or changed previous objects: %q, %v", body, err)
	}
}

func checkPreparedNative(t *testing.T, child *preparedChild) (storage.BackendIdentityResult, sqlite.DurableState) {
	t.Helper()
	for _, check := range []func() error{
		child.storage.CheckBounded, child.storage.CheckFileStorage, child.storage.CheckPublicationAccounting,
		child.meta.CheckBackendIdentity, child.meta.CheckStableReferenceIdentity, child.meta.CheckOpenMetadataAccess,
		child.meta.CheckInlineCloseSettlement,
	} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
	if child.storage.LockService() == nil {
		t.Fatal("prepared child lost its native lock authority")
	}
	identity, err := child.meta.BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	state, err := child.meta.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := zeroLease(t.Context(), child.meta); err != nil {
		t.Fatal(err)
	}
	if names, err := child.storage.List(t.Context(), ""); err != nil || len(names) != 0 {
		t.Fatalf("new child is not empty: %v, %v", names, err)
	}
	if usage, err := child.storage.Usage(t.Context()); err != nil || usage != 0 {
		t.Fatalf("new child has nonzero usage: %d, %v", usage, err)
	}
	return identity, state
}

func TestPreparedVolumesRejectInvalidPreparation(t *testing.T) {
	database := filepath.Join(t.TempDir(), "volumes.db")
	if _, err := prepare(t.Context(), database, 0, 0, locking.DefaultOptions()); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid count returned %v", err)
	}
	if _, err := prepare(t.Context(), database, 1, 0, locking.Options{}); err == nil {
		t.Fatal("invalid authority options were accepted")
	}
	if _, err := prepare(t.Context(), database, 1, -1, locking.DefaultOptions()); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid allowance returned %v", err)
	}
}

func TestPreparedVolumesRejectActiveAllocation(t *testing.T) {
	p := NewPrepared(t, 2, 0, locking.DefaultOptions())
	child, err := p.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.release(child); err != nil {
			t.Errorf("closing active child: %v", err)
		}
	})
	result := make(chan error, 1)
	go func() { _, err := p.acquire(t.Context()); result <- err }()
	if err := <-result; !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("overlapping allocation returned %v", err)
	}
	if p.next != 1 {
		t.Fatal("rejected allocation consumed a volume")
	}
	if err := p.release(&preparedChild{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("foreign release returned %v", err)
	}
}

func TestPreparedVolumesStopAfterOpenFailure(t *testing.T) {
	p := NewPrepared(t, 2, 0, locking.DefaultOptions())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.acquire(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled native open returned %v", err)
	}
	if _, err := p.acquire(t.Context()); !errors.Is(err, context.Canceled) || p.next != 0 {
		t.Fatalf("failed preparation allowed a successor: %v, allocated=%d", err, p.next)
	}
}

func TestPreparedVolumesStopAfterCloseFailure(t *testing.T) {
	p := NewPrepared(t, 2, 0, locking.DefaultOptions())
	child, err := p.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	failed := errors.New("injected complete close failure")
	child.close = func() error { return errors.Join(child.storage.Close(), failed) }
	if err := p.release(child); !errors.Is(err, failed) {
		t.Fatalf("close failure was lost: %v", err)
	}
	if _, err := p.acquire(t.Context()); !errors.Is(err, failed) || p.next != 1 {
		t.Fatalf("failed cleanup allowed a successor: %v, allocated=%d", err, p.next)
	}
}

func TestPreparedVolumesStopAfterStrongGrant(t *testing.T) {
	p := NewPrepared(t, 2, 0, locking.DefaultOptions())
	child, err := p.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := child.storage.Close(); err != nil {
			t.Errorf("closing granted child: %v", err)
		}
	})
	if err := child.storage.Write(t.Context(), "payload", []byte("protected")); err != nil {
		t.Fatal(err)
	}
	service := child.storage.LockService()
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
	resource, err := service.Resolve(t.Context(), owner.Ref, "payload")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner.Ref, Request: "strong", Resource: resource, Mode: locking.Shared, TTL: time.Second,
	})
	if err != nil || grant.Grant == nil {
		t.Fatalf("strong grant failed: %+v, %v", grant, err)
	}
	if err := service.CloseSession(t.Context(), session.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.release(child); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("strong lease watermark was accepted: %v", err)
	}
	if _, err := p.acquire(t.Context()); !errors.Is(err, syscall.EINVAL) || p.next != 1 {
		t.Fatalf("strong lease allowed a successor: %v, allocated=%d", err, p.next)
	}
	if _, err := child.meta.BackendIdentity(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("watermark rejection did not close native owner: %v", err)
	}
}

func TestPreparedVolumesStopAfterInvalidLeaseOwner(t *testing.T) {
	for _, closed := range []bool{true, false} {
		name := "closed owner"
		want := error(syscall.ESTALE)
		if !closed {
			name, want = "canceled watermark read", context.Canceled
		}
		t.Run(name, func(t *testing.T) {
			p := NewPrepared(t, 2, 0, locking.DefaultOptions())
			child, err := p.acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if closed {
				if err := child.storage.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancelled, cancel := context.WithCancel(t.Context())
				cancel()
				p.cleanup = cancelled
			}
			if err := p.release(child); !errors.Is(err, want) {
				t.Fatalf("invalid owner supplied a healthy lease watermark: %v", err)
			}
			if _, err := p.acquire(t.Context()); !errors.Is(err, want) || p.next != 1 {
				t.Fatalf("unreadable watermark allowed a successor: %v, allocated=%d", err, p.next)
			}
		})
	}
}

func TestPreparedVolumesRejectExistingLeaseWatermark(t *testing.T) {
	p := NewPrepared(t, 2, 0, locking.DefaultOptions())
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: p.database, Volume: preparedVolume(0), SQLite: sqlite.DefaultOptions(),
		Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.RaiseMaxLease(t.Context(), time.Millisecond); err != nil {
		t.Fatal(errors.Join(err, meta.Close()))
	}
	if err := meta.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.acquire(t.Context()); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("preexisting strong lease watermark was accepted: %v", err)
	}
	if _, err := p.acquire(t.Context()); !errors.Is(err, syscall.EINVAL) || p.next != 0 {
		t.Fatalf("rejected first open allowed a successor: %v, allocated=%d", err, p.next)
	}
}
