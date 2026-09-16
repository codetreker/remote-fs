package fuse

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/storage"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type genericRangeFixture struct {
	storage.FileSession
	gate     sync.Mutex
	mu       sync.Mutex
	engine   *fileaccess.Coordinator
	session  uint64
	nextWait uint64
	receipts map[storage.FileActionID]storage.FileActionReceipt
	waits    map[storage.FileActionID]*fileaccess.Wait
	entered  chan struct{}
}

type genericRangeFile struct {
	storage.File
	authority *genericRangeFixture
	node      uint64
}

func newGenericRangeVolume(t *testing.T) (*volume, *genericRangeFixture) {
	t.Helper()
	engine, err := fileaccess.New(fileaccess.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	a := &genericRangeFixture{engine: engine, session: 1, receipts: make(map[storage.FileActionID]storage.FileActionReceipt), waits: make(map[storage.FileActionID]*fileaccess.Wait), entered: make(chan struct{}, 16)}
	v := &volume{files: a, flushTimeout: time.Second, sessionOptions: storage.DefaultFileSessionOptions(), stop: make(chan struct{})}
	status, _ := a.Status(t.Context())
	if err := v.confirm(time.Now(), status); err != nil {
		t.Fatal(err)
	}
	return v, a
}
func (a *genericRangeFixture) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{Epoch: strconv.FormatUint(a.session, 10), Revision: 1, ActionEpoch: 1, Remaining: time.Minute, HistoryRemaining: time.Minute}, nil
}
func (a *genericRangeFixture) QueryAction(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.receipts[id]
	if !ok {
		return storage.FileActionReceipt{}, syscall.ESTALE
	}
	if r.Errno != 0 {
		return r, r.Errno
	}
	return r, nil
}
func (a *genericRangeFixture) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	a.mu.Lock()
	wait := a.waits[id]
	a.mu.Unlock()
	if wait != nil {
		wait.Cancel()
		revision, err := wait.Await(ctx)
		a.finish(id, storage.OpFileWaitRanges, revision, err)
	}
	return a.QueryAction(ctx, id)
}
func (a *genericRangeFixture) finish(id storage.FileActionID, op storage.Operation, revision uint64, err error) (storage.FileActionReceipt, error) {
	r := storage.FileActionReceipt{Action: id, Operation: op, State: storage.FileActionCompleted, RangeRevision: revision}
	if err != nil {
		r.State = storage.FileActionNotApplied
		kind := storage.FileConflictKind(0)
		switch {
		case errors.Is(err, fileaccess.ErrRevision):
			r.Errno, kind = syscall.EAGAIN, storage.ConflictRevision
		case errors.Is(err, fileaccess.ErrConflict):
			r.Errno, kind = syscall.EAGAIN, storage.ConflictRange
		case errors.Is(err, fileaccess.ErrCapacity):
			r.Errno, kind = syscall.ENOLCK, storage.ConflictCapacity
		case errors.Is(err, fileaccess.ErrDeadlock):
			r.Errno, kind = syscall.EDEADLK, storage.ConflictDeadlock
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, fileaccess.ErrCanceled):
			r.Errno = syscall.EINTR
		default:
			r.Errno = syscall.EIO
		}
		if kind != 0 {
			r.Conflict = &storage.FileConflict{Kind: kind}
		}
	}
	a.mu.Lock()
	a.receipts[id] = r
	delete(a.waits, id)
	a.mu.Unlock()
	if r.Errno != 0 {
		return r, r.Errno
	}
	return r, nil
}
func (f genericRangeFile) scope(s storage.RangeScope) fileaccess.Scope {
	return fileaccess.Scope{Resource: f.node, Domain: uint64(s.Domain), Enforced: s.Enforced}
}
func (f genericRangeFile) owner(id storage.RangeOwnerID) fileaccess.Owner {
	return fileaccess.Owner{Session: f.authority.session, ID: uint64(id)}
}
func fixtureRanges(ranges []storage.RangeAcquisition) []fileaccess.Acquisition {
	out := make([]fileaccess.Acquisition, len(ranges))
	for i, r := range ranges {
		out[i] = fileaccess.Acquisition{ID: uint64(r.ID), Start: r.Start, End: r.End, Boundary: r.Boundary, Exclusive: r.Exclusive}
	}
	return out
}
func (f genericRangeFile) RangeSnapshot(_ context.Context, owner storage.RangeOwnerID, scope storage.RangeScope) (storage.RangeSnapshot, error) {
	s, err := f.authority.engine.Snapshot(f.owner(owner), f.scope(scope))
	if err != nil {
		return storage.RangeSnapshot{}, err
	}
	r := storage.RangeSnapshot{Revision: s.Revision, Available: s.Available, OwnerAvailable: s.OwnerAvailable}
	for _, a := range s.Own {
		r.Own = append(r.Own, storage.RangeAcquisition{ID: storage.RangeAcquisitionID(a.ID), Start: a.Start, End: a.End, Boundary: a.Boundary, Exclusive: a.Exclusive})
	}
	for _, h := range s.Other {
		r.Other = append(r.Other, storage.HeldRange{Owner: storage.RangeOwner{Session: strconv.FormatUint(h.Owner.Session, 10), ID: storage.RangeOwnerID(h.Owner.ID)}, Range: storage.RangeAcquisition{ID: storage.RangeAcquisitionID(h.Acquisition.ID), Start: h.Acquisition.Start, End: h.Acquisition.End, Boundary: h.Acquisition.Boundary, Exclusive: h.Acquisition.Exclusive}})
	}
	return r, nil
}

func (f genericRangeFile) ReplaceRanges(ctx context.Context, r storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := ctx.Err(); err != nil {
		return f.authority.finish(id, storage.OpFileReplaceRanges, 0, err)
	}
	f.authority.gate.Lock()
	defer f.authority.gate.Unlock()
	revision, err := f.authority.engine.ReplaceOwned(f.owner(r.Owner), f.scope(r.Scope), r.ExpectedRevision, fixtureRanges(r.Ranges), ctx.Err)
	return f.authority.finish(id, storage.OpFileReplaceRanges, revision, err)
}
func (f genericRangeFile) WaitRanges(ctx context.Context, r storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	a := f.authority
	a.gate.Lock()
	a.nextWait++
	wait, err := a.engine.RegisterWait(a.nextWait, f.owner(r.Owner), f.scope(r.Scope), r.ExpectedRevision, fixtureRanges(r.Ranges), r.DetectDeadlock, ctx.Err)
	if err != nil {
		a.gate.Unlock()
		return a.finish(id, storage.OpFileWaitRanges, 0, err)
	}
	a.mu.Lock()
	a.waits[id] = wait
	a.receipts[id] = storage.FileActionReceipt{Action: id, Operation: storage.OpFileWaitRanges, State: storage.FileActionPending}
	a.mu.Unlock()
	a.gate.Unlock()
	select {
	case a.entered <- struct{}{}:
	default:
	}
	revision, err := wait.Await(ctx)
	return a.finish(id, storage.OpFileWaitRanges, revision, err)
}
func (f genericRangeFile) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, id storage.FileActionID) (storage.FileActionReceipt, error) {
	a := f.authority
	a.gate.Lock()
	defer a.gate.Unlock()
	if err := a.engine.CancelOwnerWaits(f.owner(owner), f.scope(scope)); err != nil {
		return a.finish(id, storage.OpFileRetireRanges, 0, err)
	}
	s, err := a.engine.Snapshot(f.owner(owner), f.scope(scope))
	if err != nil {
		return a.finish(id, storage.OpFileRetireRanges, 0, err)
	}
	revision, err := a.engine.ReplaceOwned(f.owner(owner), f.scope(scope), s.Revision, nil, ctx.Err)
	return a.finish(id, storage.OpFileRetireRanges, revision, err)
}
func genericHandle(v *volume, a *genericRangeFixture, nodeID uint64) *handle {
	return newHandle(&node{volume: v, id: &identity{node: nodeID}}, genericRangeFile{authority: a, node: nodeID}, true, true)
}
func genericKernelLock(t *testing.T, h *handle, owner uint64, start, end uint64, kind uint32, flock bool, want syscall.Errno) {
	t.Helper()
	flags := uint32(0)
	if flock {
		flags = gofuse.FUSE_LK_FLOCK
	}
	lock := gofuse.FileLock{Start: start, End: end, Typ: kind, Pid: uint32(owner + 100)}
	if got := h.Setlk(t.Context(), owner, &lock, flags); got != want {
		t.Fatalf("owner%d lock%v [%d,%d]=%v,want %v", owner, kind, start, end, got, want)
	}
}

func TestGenericRangesKeepPOSIXAndFlockConversionRulesLocal(t *testing.T) {
	v, a := newGenericRangeVolume(t)
	first, second := genericHandle(v, a, 1), genericHandle(v, a, 1)
	genericKernelLock(t, first, 1, 0, 99, syscall.F_RDLCK, false, 0)
	genericKernelLock(t, first, 1, 20, 29, syscall.F_WRLCK, false, 0)
	s, err := first.file.RangeSnapshot(t.Context(), 1, advisoryScope(posixFamily))
	if err != nil || len(s.Own) != 3 {
		t.Fatalf("split=%+v/%v", s, err)
	}
	genericKernelLock(t, second, 2, 0, 9, syscall.F_RDLCK, false, 0)
	genericKernelLock(t, first, 1, 0, 99, syscall.F_WRLCK, false, syscall.EAGAIN)
	s, err = first.file.RangeSnapshot(t.Context(), 1, advisoryScope(posixFamily))
	if err != nil || len(s.Own) != 3 {
		t.Fatalf("failed conversion changed POSIX set=%+v/%v", s, err)
	}
	genericKernelLock(t, first, 1, 0, 0, syscall.F_RDLCK, true, 0)
	genericKernelLock(t, second, 2, 0, 0, syscall.F_RDLCK, true, 0)
	genericKernelLock(t, first, 1, 0, 0, syscall.F_WRLCK, true, syscall.EAGAIN)
	s, err = first.file.RangeSnapshot(t.Context(), 1, advisoryScope(flockFamily))
	if err != nil || len(s.Own) != 0 {
		t.Fatalf("failed flock conversion kept old grant=%+v/%v", s, err)
	}
	if err := first.dropAdvisory(t.Context(), 1, posixFamily); err != nil {
		t.Fatal(err)
	}
	s, err = second.file.RangeSnapshot(t.Context(), 2, advisoryScope(flockFamily))
	if err != nil || len(s.Own) != 1 {
		t.Fatalf("POSIX close changed other flock=%+v/%v", s, err)
	}
}

func TestGenericRevisionWaitContinuesAcrossBudgetsAndCloseCancels(t *testing.T) {
	v, a := newGenericRangeVolume(t)
	v.flushTimeout = 25 * time.Millisecond
	first, second := genericHandle(v, a, 1), genericHandle(v, a, 1)
	genericKernelLock(t, first, 1, 0, 9, syscall.F_WRLCK, false, 0)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan syscall.Errno, 1)
	lock := gofuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	go func() { done <- second.Setlkw(ctx, 2, &lock, 0) }()
	for range 3 {
		select {
		case <-a.entered:
		case errno := <-done:
			t.Fatalf("wait ended before its third budget: %v", errno)
		case <-ctx.Done():
			t.Fatal("wait did not renew")
		}
	}
	if err := second.dropAdvisory(t.Context(), 2, posixFamily); err != nil {
		t.Fatal(err)
	}
	select {
	case errno := <-done:
		if errno != syscall.EINTR {
			t.Fatalf("close cancellation=%v", errno)
		}
	case <-ctx.Done():
		t.Fatal("close did not stop waiter")
	}
	s, err := first.file.RangeSnapshot(t.Context(), 1, advisoryScope(posixFamily))
	if err != nil || len(s.Own) != 1 {
		t.Fatalf("cancel affected holder=%+v/%v", s, err)
	}
}

func TestGenericLocalCloseKeepsSameOwnerOnOtherFiles(t *testing.T) {
	v, a := newGenericRangeVolume(t)
	first, other := genericHandle(v, a, 1), genericHandle(v, a, 2)
	genericKernelLock(t, first, 0, 0, 9, syscall.F_WRLCK, false, 0)
	genericKernelLock(t, other, 0, 0, 9, syscall.F_WRLCK, false, 0)
	if err := first.dropAdvisory(t.Context(), 0, posixFamily); err != nil {
		t.Fatal(err)
	}
	s, err := other.file.RangeSnapshot(t.Context(), 0, advisoryScope(posixFamily))
	if err != nil || len(s.Own) != 1 {
		t.Fatalf("close crossed file identity=%+v/%v", s, err)
	}
	genericKernelLock(t, other, 1, 0, 9, syscall.F_WRLCK, false, syscall.EAGAIN)
}

type beforeRangeWaitFile struct {
	storage.File
	before func()
}

func (f beforeRangeWaitFile) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f.before()
	return f.File.WaitRanges(ctx, request, id)
}

func TestGenericBlockingWaitReplansWhenConflictEndsBeforeEnrollment(t *testing.T) {
	v, a := newGenericRangeVolume(t)
	holder, waiter := genericHandle(v, a, 1), genericHandle(v, a, 1)
	genericKernelLock(t, holder, 1, 0, 9, syscall.F_WRLCK, false, 0)
	var once sync.Once
	waiter.file = beforeRangeWaitFile{File: waiter.file, before: func() {
		once.Do(func() {
			if err := holder.dropAdvisory(t.Context(), 1, posixFamily); err != nil {
				t.Fatal(err)
			}
		})
	}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	lock := gofuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	if errno := waiter.Setlkw(ctx, 2, &lock, 0); errno != 0 {
		t.Fatalf("release before wait enrollment=%v,want acquisition", errno)
	}
	s, err := waiter.file.RangeSnapshot(t.Context(), 2, advisoryScope(posixFamily))
	if err != nil || len(s.Own) != 1 {
		t.Fatalf("replanned owner=%+v/%v", s, err)
	}
}

func TestGenericCapacityDecisionValidatesGuardAndPreservesRanges(t *testing.T) {
	v, a := newGenericRangeVolume(t)
	limits := fileaccess.DefaultLimits()
	limits.MaxSetRanges = 1
	limits.MaxOwnerRanges = 1
	engine, err := fileaccess.New(limits)
	if err != nil {
		t.Fatal(err)
	}
	a.engine.Close()
	a.engine = engine
	t.Cleanup(engine.Close)
	h := genericHandle(v, a, 1)
	genericKernelLock(t, h, 1, 0, 99, syscall.F_RDLCK, false, 0)
	before, err := h.file.RangeSnapshot(t.Context(), 1, advisoryScope(posixFamily))
	if err != nil {
		t.Fatal(err)
	}
	genericKernelLock(t, h, 1, 20, 29, syscall.F_WRLCK, false, syscall.ENOLCK)
	after, err := h.file.RangeSnapshot(t.Context(), 1, advisoryScope(posixFamily))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Own, after.Own) || before.Revision != after.Revision {
		t.Fatalf("capacity refusal changed held state: %+v -> %+v", before, after)
	}
}

func TestGenericConflictQueriesKeepProcessDiagnosticsLocal(t *testing.T) {
	left, authority := newGenericRangeVolume(t)
	right, remote := newGenericRangeVolume(t)
	remote.engine.Close()
	remote.engine = authority.engine
	remote.session = 2
	right.status = storage.FileSessionStatus{}
	status, _ := remote.Status(t.Context())
	if err := right.confirm(time.Now(), status); err != nil {
		t.Fatal(err)
	}
	holder, local, foreign := genericHandle(left, authority, 1), genericHandle(left, authority, 1), genericHandle(right, remote, 1)
	genericKernelLock(t, holder, 7, 2, 9, syscall.F_RDLCK, false, 0)
	query := gofuse.FileLock{Start: 0, End: 10, Typ: syscall.F_WRLCK}
	for _, test := range []struct {
		h   *handle
		pid uint32
	}{{local, 107}, {foreign, 0}} {
		var out gofuse.FileLock
		if errno := test.h.Getlk(t.Context(), 8, &query, 0, &out); errno != 0 || out.Typ != syscall.F_RDLCK || out.Start != 2 || out.End != 9 || out.Pid != test.pid {
			t.Fatalf("query=%+v/%v,want PID%d", out, errno, test.pid)
		}
	}
	query.Typ = syscall.F_RDLCK
	var out gofuse.FileLock
	if errno := local.Getlk(t.Context(), 8, &query, 0, &out); errno != 0 || out.Typ != syscall.F_UNLCK {
		t.Fatalf("compatible query=%+v/%v", out, errno)
	}
	if err := holder.dropAdvisory(t.Context(), 7, posixFamily); err != nil {
		t.Fatal(err)
	}
	scope := fileaccess.Scope{Resource: 1, Domain: uint64(advisoryScope(posixFamily).Domain)}
	owner := fileaccess.Owner{Session: 1, ID: 99}
	snapshot, err := authority.engine.Snapshot(owner, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.engine.ReplaceOwned(owner, scope, snapshot.Revision, []fileaccess.Acquisition{{ID: 1, Start: 5, End: 5, Boundary: true, Exclusive: true}}, t.Context().Err); err != nil {
		t.Fatal(err)
	}
	query.Typ = syscall.F_WRLCK
	if errno := local.Getlk(t.Context(), 8, &query, 0, &out); errno != syscall.EIO {
		t.Fatalf("unrepresentable boundary reported as ordinary POSIX lock: %v", errno)
	}
	query.Start, query.End = 5, 5
	if errno := local.Getlk(t.Context(), 8, &query, 0, &out); errno != 0 || out.Typ != syscall.F_UNLCK {
		t.Fatalf("boundary endpoint query=%+v/%v", out, errno)
	}
}
