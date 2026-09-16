package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func retainRangeFile(t *testing.T, session *fileSession, nodeID uint64) *fileReference {
	t.Helper()
	result, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: nodeID, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := session.Reference(t.Context(), result.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return ref.(*fileReference)
}

func TestFileRangesKeepDistinctOwnersAndCancelWaitingAction(t *testing.T) {
	s, first := newFileAuthority(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := s.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	second := native.(*fileSession)
	t.Cleanup(func() {
		if err := second.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
	})
	a, b := retainRangeFile(t, first, uint64(node.ID)), retainRangeFile(t, second, uint64(node.ID))
	scope := storage.RangeScope{Enforced: true}
	if _, err := b.RangeSnapshot(ctx, 0, scope); err != nil {
		t.Fatal(err)
	}
	snapshot, err := a.RangeSnapshot(ctx, 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	set := []storage.RangeAcquisition{{ID: 1, Start: 10, End: 20, Exclusive: true}}
	held, err := a.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 0, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: set}, fileActionID(t, first))
	if err != nil {
		t.Fatal(err)
	}
	if held.Effects&storage.EffectRangesChanged == 0 {
		t.Fatal("range effect absent")
	}
	other, err := b.RangeSnapshot(ctx, 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Other) != 1 || other.Other[0].Owner.Session != first.epoch {
		t.Fatalf("other owner=%+v", other)
	}
	id := fileActionID(t, second)
	finished := make(chan error, 1)
	go func() {
		_, err := b.WaitRanges(ctx, storage.RangeWaitRequest{Owner: 0, Scope: scope, ExpectedRevision: other.Revision, Ranges: set, DetectDeadlock: true}, id)
		finished <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		receipt, err := second.QueryAction(ctx, id)
		if err == nil && receipt.State == storage.FileActionPending {
			break
		}
		if !errors.Is(err, syscall.ESTALE) || time.Now().After(deadline) {
			t.Fatalf("wait admission=%+v %v", receipt, err)
		}
		time.Sleep(time.Millisecond)
	}
	cancelled, err := second.CancelAction(ctx, id)
	if !errors.Is(err, syscall.EINTR) || cancelled.State != storage.FileActionNotApplied {
		t.Fatalf("cancel=%+v %v", cancelled, err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, syscall.EINTR) {
			t.Fatalf("wait=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not drain waiter")
	}
	if second.waiters != 0 {
		t.Fatal("waiter budget leaked")
	}
	snapshot, err = a.RangeSnapshot(ctx, 0, scope)
	if err != nil || len(snapshot.Own) != 1 {
		t.Fatalf("cancel changed holder=%+v %v", snapshot, err)
	}
}

func TestFileRangeReplacementRetainsAcquisitionIdentityAndScope(t *testing.T) {
	s, session := newFileAuthority(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	scope := storage.RangeScope{Enforced: true}
	snapshot, err := f.RangeSnapshot(ctx, 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.RangeReplaceRequest{Owner: 0, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 1, End: 9}, {ID: 2, Start: 1, End: 9}}}
	if _, err := f.ReplaceRanges(ctx, request, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = f.RangeSnapshot(ctx, 0, scope)
	if err != nil || len(snapshot.Own) != 2 {
		t.Fatalf("merged acquisitions=%+v %v", snapshot, err)
	}
	request.ExpectedRevision = snapshot.Revision
	request.Ranges = request.Ranges[1:]
	if _, err := f.ReplaceRanges(ctx, request, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	actor := fileaccess.Owner{Session: session.id, ID: 0}
	if err := s.fileDomain.access.CheckIO(uint64(node.ID), &actor, fileaccess.Span{Start: 1, End: 1}, true); !errors.Is(err, fileaccess.ErrConflict) {
		t.Fatalf("shared owner write bypass=%v", err)
	}
	if _, err := f.ReplaceRanges(ctx, request, fileActionID(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stale range revision=%v", err)
	}
	if _, err := f.RetireRangeOwner(ctx, 0, scope, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = f.RangeSnapshot(ctx, 0, scope)
	if err != nil || len(snapshot.Own) != 0 {
		t.Fatalf("scope retirement=%+v %v", snapshot, err)
	}
}

func TestFileRangeQueriesDoNotConsumeOwnersOrChangeRevision(t *testing.T) {
	s, session := newFileAuthority(t)
	ctx := t.Context()
	session.options.MaxRangeOwners = 1
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	scope := storage.RangeScope{Domain: 9}
	before, err := s.fileDomain.access.Revision()
	if err != nil {
		t.Fatal(err)
	}
	for id := storage.RangeOwnerID(0); id < 32; id++ {
		snapshot, err := f.RangeSnapshot(ctx, id, scope)
		if err != nil || snapshot.Revision != before {
			t.Fatalf("query %d changed authority: %+v %v", id, snapshot, err)
		}
	}
	if len(session.owners) != 0 {
		t.Fatal("queries consumed owner admission")
	}
	snapshot, err := f.RangeSnapshot(ctx, 99, scope)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.RangeReplaceRequest{Owner: 99, Scope: scope, ExpectedRevision: snapshot.Revision + 1, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 0, End: 3}}}
	if _, err := f.ReplaceRanges(ctx, request, fileActionID(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stale CAS=%v", err)
	}
	if len(session.owners) != 0 {
		t.Fatal("failed CAS consumed owner admission")
	}
	request.ExpectedRevision = snapshot.Revision
	if _, err := f.ReplaceRanges(ctx, request, fileActionID(t, session)); err != nil {
		t.Fatalf("fresh owner CAS spuriously changed revision: %v", err)
	}
	if len(session.owners) != 1 {
		t.Fatal("held owner missing")
	}
	unseen, err := f.RangeSnapshot(ctx, 100, scope)
	if err != nil || unseen.OwnerAvailable != 0 {
		t.Fatalf("full owner capacity snapshot=%+v %v", unseen, err)
	}
	if _, err := f.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 100, Scope: scope, ExpectedRevision: unseen.Revision}, fileActionID(t, session)); err != nil {
		t.Fatalf("empty unseen CAS consumed full owner capacity: %v", err)
	}
	if len(session.owners) != 1 {
		t.Fatal("empty unseen CAS changed held owners")
	}

	if _, err := f.RetireRangeOwner(ctx, 99, scope, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	if len(session.owners) != 0 {
		t.Fatal("empty scoped retirement retained owner")
	}
}

func TestFileRangeWaitDeadlineIsKnownCancellation(t *testing.T) {
	s, session := newFileAuthority(t)
	ctx := t.Context()
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	scope := storage.RangeScope{Domain: 7}
	snapshot, err := f.RangeSnapshot(ctx, 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	id := fileActionID(t, session)
	deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	receipt, err := f.WaitRanges(deadline, storage.RangeWaitRequest{Owner: 0, Scope: scope, ExpectedRevision: snapshot.Revision}, id)
	if !errors.Is(err, syscall.EINTR) || !errors.Is(err, context.DeadlineExceeded) || receipt.State != storage.FileActionNotApplied || receipt.Effects != 0 {
		t.Fatalf("deadline=%+v %v", receipt, err)
	}
	if session.waiters != 0 || len(session.owners) != 0 {
		t.Fatal("deadline retained wait-only ownership")
	}
	queried, err := session.QueryAction(ctx, id)
	if !errors.Is(err, syscall.EINTR) || queried.State != receipt.State {
		t.Fatalf("deadline receipt=%+v %v", queried, err)
	}
}

func waitForPendingFileAction(t *testing.T, session *fileSession, id storage.FileActionID) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		receipt, err := session.QueryAction(t.Context(), id)
		if err == nil && receipt.State == storage.FileActionPending {
			return
		}
		if !errors.Is(err, syscall.ESTALE) || time.Now().After(deadline) {
			t.Fatalf("pending action=%+v %v", receipt, err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFileBeginCloseRetiresOnlyItsWaitsBeforeAdapterDrain(t *testing.T) {
	s, session := newFileAuthority(t)
	ctx := t.Context()
	files := make([]*fileReference, 2)
	for i, name := range []string{"a", "b"} {
		if err := s.Create(ctx, name); err != nil {
			t.Fatal(err)
		}
		node, err := s.Stat(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = retainRangeFile(t, session, uint64(node.ID))
	}
	scope := storage.RangeScope{Domain: 8}
	ids := make([]storage.FileActionID, 2)
	done := []chan error{make(chan error, 1), make(chan error, 1)}
	for i, f := range files {
		snapshot, err := f.RangeSnapshot(ctx, 0, scope)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = fileActionID(t, session)
		go func(i int, f *fileReference, revision uint64) {
			_, err := f.WaitRanges(ctx, storage.RangeWaitRequest{Owner: 0, Scope: scope, ExpectedRevision: revision}, ids[i])
			done[i] <- err
		}(i, f, snapshot.Revision)
		waitForPendingFileAction(t, session, ids[i])
	}
	_, proceed, err := files[0].BeginClose(ctx, fileActionID(t, session))
	if err != nil || !proceed {
		t.Fatalf("begin close=%v %v", proceed, err)
	}
	select {
	case err := <-done[0]:
		if !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("retired wait=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("logical close did not drain its waiter")
	}
	other, err := session.QueryAction(ctx, ids[1])
	if err != nil || other.State != storage.FileActionPending {
		t.Fatalf("other reference wait changed=%+v %v", other, err)
	}
	if _, err := session.CancelAction(ctx, ids[1]); !errors.Is(err, syscall.EINTR) {
		t.Fatal(err)
	}
	select {
	case <-done[1]:
	case <-time.After(time.Second):
		t.Fatal("second waiter did not cancel")
	}
}

func TestFileAccessErrorsPreserveGenericFailures(t *testing.T) {
	for _, test := range []struct {
		cause error
		code  syscall.Errno
		kind  storage.FileConflictKind
	}{
		{fileaccess.ErrInvalid, syscall.EINVAL, 0}, {fileaccess.ErrRevision, syscall.EAGAIN, storage.ConflictRevision},
		{fileaccess.ErrConflict, syscall.EAGAIN, storage.ConflictRange}, {fileaccess.ErrAccess, syscall.EACCES, 0},
		{fileaccess.ErrCapacity, syscall.EAGAIN, storage.ConflictCapacity}, {fileaccess.ErrDeadlock, syscall.EDEADLK, storage.ConflictDeadlock},
		{fileaccess.ErrUnknownClaim, syscall.ESTALE, storage.ConflictRetired}, {fileaccess.ErrRetired, syscall.ESTALE, storage.ConflictRetired}, {fileaccess.ErrClosed, syscall.ESTALE, storage.ConflictRetired},
		{fileaccess.ErrExhausted, syscall.EOVERFLOW, 0}, {fileaccess.ErrCanceled, syscall.EINTR, 0}, {context.Canceled, syscall.EINTR, 0}, {context.DeadlineExceeded, syscall.EIO, 0},
	} {
		err := fileAccessError(test.cause)
		if !errors.Is(err, test.code) || !errors.Is(err, test.cause) {
			t.Fatalf("cause %v became %v", test.cause, err)
		}
		var typed *storage.FileError
		if !errors.As(err, &typed) {
			t.Fatal("lost generic classification")
		}
		if test.kind != 0 && (typed.Conflict == nil || typed.Conflict.Kind != test.kind) {
			t.Fatalf("conflict=%+v", typed.Conflict)
		}
	}
	if fileAccessError(nil) != nil {
		t.Fatal("success became error")
	}
}

func TestScopedRangeRetirementChecksGuardBeforeCancellingWaits(t *testing.T) {
	ctx := t.Context()
	config := lockingTestConfig(t)
	config.SQLite.Files = storage.DefaultFileServiceOptions()
	config.SQLite.Files.MaxWaits = 1
	s, err := OpenLocking(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	native, _, err := s.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	session := native.(*fileSession)
	t.Cleanup(func() {
		if err := session.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Create(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	scope := storage.RangeScope{Domain: 9}
	snapshot, err := f.RangeSnapshot(ctx, 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	waitingID := fileActionID(t, session)
	finished := make(chan error, 1)
	go func() {
		_, err := f.WaitRanges(ctx, storage.RangeWaitRequest{Owner: 1, Scope: scope, ExpectedRevision: snapshot.Revision}, waitingID)
		finished <- err
	}()
	waitForPendingFileAction(t, session, waitingID)
	rejected := metastore.WithFilePublicationGuard(ctx, func() error { return syscall.ESTALE })
	if _, err := session.RetireRangeOwner(rejected, 1, fileActionID(t, session)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("rejected owner retirement=%v", err)
	}
	if _, err := f.RetireRangeOwner(rejected, 1, scope, fileActionID(t, session)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("rejected retirement=%v", err)
	}
	probe, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = f.WaitRanges(probe, storage.RangeWaitRequest{Owner: 2, Scope: scope, ExpectedRevision: snapshot.Revision}, fileActionID(t, session))
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("rejected retirement freed the occupied waiter slot: %v", err)
	}
	if _, err := session.CancelAction(ctx, waitingID); !errors.Is(err, syscall.EINTR) {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("original waiter did not cancel")
	}
}
