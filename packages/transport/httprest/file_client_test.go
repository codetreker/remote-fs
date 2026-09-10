package httprest_test

import (
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRetainedHTTPCancelLockReconcilesPendingAttempt(t *testing.T) {
	ctx := t.Context()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	client, _ := filePair(t, backend)
	if err := client.CheckFileStorage(); err != nil {
		t.Fatal(err)
	}
	firstSession := fileSession(t, client)
	secondSession := fileSession(t, client)
	first := openHTTPFile(t, firstSession, "file", storage.FileOpenOptions{Read: true, Write: true})
	second := openHTTPFile(t, secondSession, "file", storage.FileOpenOptions{Read: true, Write: true})
	newID := func(session storage.FileSession) storage.LockRequestID {
		t.Helper()
		status, err := session.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id, err := storage.NewLockRequestID(status.ActionEpoch)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	held, err := first.SetLock(ctx, 1, lock, newID(firstSession))
	if err != nil || held.State != storage.LockGranted {
		t.Fatalf("held = %+v, %v", held, err)
	}
	lock.Wait = true
	id := newID(secondSession)
	pending, err := second.SetLock(ctx, 2, lock, id)
	if err != nil || pending.State != storage.LockPending {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	for i := 0; i < 2; i++ {
		cancelled, err := second.CancelLock(ctx, 2, id)
		if err != nil || cancelled.Request != id || cancelled.State != storage.LockCancelled {
			t.Fatalf("cancel %d = %+v, %v", i, cancelled, err)
		}
	}
	if err := first.DropLocks(ctx, 1, storage.Flock); err != nil {
		t.Fatal(err)
	}
	observed, err := second.QueryLock(ctx, 2, id)
	if err != nil || observed.State != storage.LockCancelled {
		t.Fatalf("cancelled request was granted after release: %+v, %v", observed, err)
	}
	conflict, err := first.GetLock(ctx, 1, lock)
	if err != nil || conflict.Found {
		t.Fatalf("cancel left a grant: %+v, %v", conflict, err)
	}
	if _, err := second.CancelLock(ctx, 2, "invalid"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid cancellation = %v", err)
	}
}
