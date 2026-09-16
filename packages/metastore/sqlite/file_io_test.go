package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func nativeIOFixture(t *testing.T, options storage.FileSessionOptions) (*LockingStore, *fileSession, *fileReference) {
	t.Helper()
	opened, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !opened.Terminal() {
			if err := opened.Abort(); err != nil {
				t.Error(err)
			}
		}
	})
	key, err := opened.Reserve(t.Context(), "held", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Commit(t.Context(), "held", metastore.Object{Key: key, Size: 4, ModTime: time.Now()}); err != nil {
		t.Fatal(err)
	}
	node, err := opened.Stat(t.Context(), "held")
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := opened.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	session := native.(*fileSession)
	return opened, session, retainRangeFile(t, session, uint64(node.ID))
}

func TestNativeIOMembershipsKeepDetachedStorageUntilEveryOperationEnds(t *testing.T) {
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 2
	opened, session, file := nativeIOFixture(t, options)
	first, err := file.AcquireIO(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	second, err := file.AcquireIO(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := file.AcquireIO(t.Context()); !errors.Is(err, syscall.EAGAIN) || extra != nil {
		t.Fatalf("membership capacity=%v,%v", extra, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if extra, err := file.AcquireIO(canceled); !errors.Is(err, context.Canceled) || extra != nil {
		t.Fatalf("canceled membership=%v,%v", extra, err)
	}
	if err := opened.Remove(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := file.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if extra, err := file.AcquireIO(t.Context()); !errors.Is(err, syscall.EBADF) || extra != nil {
		t.Fatalf("retired reference admission=%v,%v", extra, err)
	}
	if err := session.Dispose(t.Context()); !errors.Is(err, errFileIOActive) {
		t.Fatalf("physical retirement ignored operations: %v", err)
	}
	if extra, err := file.AcquireIO(t.Context()); !errors.Is(err, syscall.ESTALE) || extra != nil {
		t.Fatalf("retired session admission=%v,%v", extra, err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("idempotent membership release=%v", err)
	}
	if usage, err := opened.Usage(t.Context()); err != nil || usage != 4 {
		t.Fatalf("early physical credit=%d,%v", usage, err)
	}
	if file.ioUsers != 1 || session.activeIO != 1 || opened.fileDomain.activeIO != 1 || len(opened.fileDomain.memberships) != 1 {
		t.Fatal("first release changed another operation's membership")
	}
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Dispose(t.Context()); err != nil {
		t.Fatal(err)
	}
	if usage, err := opened.Usage(t.Context()); err != nil || usage != 0 {
		t.Fatalf("completed drain usage=%d,%v", usage, err)
	}
	if file.ioUsers != 0 || session.activeIO != 0 || opened.fileDomain.activeIO != 0 || len(opened.fileDomain.memberships) != 0 {
		t.Fatal("drained operations retained capacity")
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeIOMembershipExpiryFencesAccessBeforePhysicalRelease(t *testing.T) {
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Second
	opened, session, file := nativeIOFixture(t, options)
	membership, err := file.AcquireIO(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	captured, err := file.Capture(t.Context(), storage.FileIO{Length: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Remove(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Until(session.expires))
	defer deadline.Stop()
	<-deadline.C
	session.expire()
	if _, err := file.Capture(t.Context(), storage.FileIO{Length: 4}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("membership extended permission: %v", err)
	}
	if usage, err := opened.Usage(t.Context()); err != nil || usage != 4 {
		t.Fatalf("expiry released admitted storage=%d,%v", usage, err)
	}
	if captured.Size != 4 || captured.Content == "" || file.closed {
		t.Fatal("expiry discarded captured identity before operation release")
	}
	if err := membership.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if usage, err := opened.Usage(t.Context()); err != nil || usage != 0 {
		t.Fatalf("last operation did not complete expired cleanup=%d,%v", usage, err)
	}
	if !file.closed || !session.closed || len(opened.fileDomain.memberships) != 0 {
		t.Fatal("expired physical ownership was not retired")
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeIOMembershipFailedReleaseRemainsOwnedAndRetryable(t *testing.T) {
	opened, session, file := nativeIOFixture(t, storage.DefaultFileSessionOptions())
	membership, err := file.AcquireIO(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	owned := membership.(*fileIOMembership)
	if err := opened.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	originalTimeout := session.cleanupTimeout
	session.cleanupTimeout = 20 * time.Millisecond
	released := make(chan error, 1)
	go func() { released <- membership.Close(t.Context()) }()
	err = <-released
	session.cleanupTimeout = originalTimeout
	if !errors.Is(err, context.DeadlineExceeded) {
		opened.coordinator.commit.release()
		t.Fatalf("blocked release=%v", err)
	}
	if !owned.finished.Load() || owned.closed || len(opened.fileDomain.memberships) != 1 || file.ioUsers != 1 {
		opened.coordinator.commit.release()
		t.Fatal("failed release lost native cleanup ownership")
	}
	opened.coordinator.commit.release()
	if err := opened.coordinator.healthy(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed bounded cleanup did not fence authority: %v", err)
	}
	if err := membership.Close(t.Context()); err != nil {
		t.Fatalf("finished membership retry=%v", err)
	}
	if !owned.closed || len(opened.fileDomain.memberships) != 0 || file.ioUsers != 0 || session.activeIO != 0 || opened.fileDomain.activeIO != 0 {
		t.Fatal("retry did not reap exactly one completed operation")
	}
	if err := membership.Close(t.Context()); err != nil {
		t.Fatalf("repeated release=%v", err)
	}
	if extra, err := file.AcquireIO(t.Context()); !errors.Is(err, syscall.EIO) || extra != nil {
		t.Fatalf("cleanup retry reactivated poisoned authority=%v,%v", extra, err)
	}
	if err := opened.Abort(); err != nil {
		t.Fatalf("finished operations prevented physical shutdown: %v", err)
	}
}
