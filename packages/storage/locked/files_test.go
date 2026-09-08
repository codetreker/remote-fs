package locked_test

import (
	"context"
	"errors"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestScopedRetainedFilesKeepStrongProofsOnlyForMutations(t *testing.T) {
	backend := pairedBackend(t)
	if err := backend.Write(t.Context(), "file", []byte("initial")); err != nil {
		t.Fatal(err)
	}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
	proof := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}
	view, err := facade.WithScope(proof)
	if err != nil {
		t.Fatal(err)
	}
	proof.Grants[0].Generation++
	session, err := view.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true, Write: true})
	if err != nil {
		t.Fatal(err)
	}
	attr, err := file.WriteAt(t.Context(), 0, []byte("updated"))
	if err != nil {
		t.Fatalf("retained write lost the frozen proof: %v", err)
	}
	if _, err := file.Truncate(t.Context(), 4); err != nil {
		t.Fatalf("retained truncate lost the frozen proof: %v", err)
	}
	mode := storage.SettableMode & 0o600
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if _, err := facade.LockService().Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if got, err := file.ReadAt(t.Context(), 0, 100); err != nil || string(got.Data) != "upda" {
		t.Fatalf("read inherited a stale strong proof: %q, %v", got.Data, err)
	}
	if got, err := file.Stat(t.Context()); err != nil || got.ID != attr.ID || got.Size != 4 || got.Mode.Perm() != 0o600 {
		t.Fatalf("retained stat through a released strong scope = %+v, %v", got, err)
	}
	if _, err := session.StatNode(t.Context(), attr.ID); err != nil {
		t.Fatalf("identity stat inherited a stale strong proof: %v", err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("bad")); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retained mutation ignored a stale strong proof: %v", err)
	}
	if _, err := session.SetNodeAttr(t.Context(), attr.ID, storage.AttrChange{Mode: &mode}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("identity mutation ignored a stale strong proof: %v", err)
	}
	if _, err := session.OpenNode(t.Context(), attr.ID, storage.FileOpenOptions{Read: true, Write: true, Truncate: true}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("truncating open ignored a stale strong proof: %v", err)
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := session.Renew(t.Context())
	if err != nil || renewed.Epoch != status.Epoch || renewed.Revision <= status.Revision || renewed.Remaining <= 0 {
		t.Fatalf("renewal through a released strong scope = %+v, %v", renewed, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := session.Renew(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled renewal lost its cause: %v", err)
	}
	status = renewed
	request, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	if attempt, err := file.SetLock(t.Context(), 1, lock, request); err != nil || attempt.State != storage.LockGranted {
		t.Fatalf("advisory acquisition inherited a stale strong proof: %+v, %v", attempt, err)
	}
	if conflict, err := file.GetLock(t.Context(), 2, lock); err != nil || !conflict.Found || conflict.Owner != 1 {
		t.Fatalf("retained advisory holder query = %+v, %v", conflict, err)
	}
	pending, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	waiting := lock
	waiting.Wait = true
	if attempt, err := file.SetLock(t.Context(), 2, waiting, pending); err != nil || attempt.State != storage.LockPending {
		t.Fatalf("conflicting retained request = %+v, %v", attempt, err)
	}
	if _, err := file.CancelLock(t.Context(), 3, pending); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("cancelling another owner's request = %v, want EINVAL", err)
	}
	if attempt, err := file.QueryLock(t.Context(), 2, pending); err != nil || attempt.State != storage.LockPending {
		t.Fatalf("wrong-owner cancellation changed the pending request: %+v, %v", attempt, err)
	}
	if attempt, err := file.CancelLock(t.Context(), 2, pending); err != nil || attempt.State != storage.LockCancelled || attempt.EverGranted {
		t.Fatalf("retained request cancellation = %+v, %v", attempt, err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatalf("sync inherited a stale strong proof: %v", err)
	}
	if err := file.DropLocks(t.Context(), 1, storage.Flock); err != nil {
		t.Fatal(err)
	}
	if attempt, err := file.QueryLock(t.Context(), 2, pending); err != nil || attempt.State != storage.LockCancelled {
		t.Fatalf("cancelled request acquired after the holder released: %+v, %v", attempt, err)
	}
	anonymous := locking.WithScope(t.Context(), locking.MutationScope{})
	if _, err := file.WriteAt(anonymous, 0, []byte("good")); err != nil {
		t.Fatalf("explicit anonymous file mutation inherited view proofs: %v", err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed retained stat = %v, want EBADF", err)
	}
}

func TestRetainedFacadeForwardsQuotaCapabilityAndDetachedUsage(t *testing.T) {
	facade, err := locked.New(pairedBackend(t))
	if err != nil {
		t.Fatal(err)
	}
	quota, err := limited.New(t.Context(), facade, limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	session, err := quota.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true, Write: true, Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("held")); err != nil {
		t.Fatal(err)
	}
	if err := quota.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := quota.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := facade.Usage(t.Context()); err != nil || used != 4 {
		t.Fatalf("scoped facade usage = %d, %v; want 4", used, err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if space, err := quota.Space(t.Context()); err != nil || space.Used != 0 {
		t.Fatalf("scoped facade cleanup usage = %+v, %v", space, err)
	}
}

func TestRetainedFacadeRejectsUnsupportedBackend(t *testing.T) {
	backend := &missingAuthority{service: pairedBackend(t).LockService()}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := facade.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); err != syscall.EOPNOTSUPP {
		t.Fatalf("unsupported retained capability returned %v", err)
	}
	if _, err := facade.Usage(t.Context()); err != syscall.ENOSYS {
		t.Fatalf("unsupported authoritative usage returned %v", err)
	}
}
