package fuse

import (
	"context"
	"errors"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func advisoryVolume(t *testing.T) *volume {
	t.Helper()
	return advisoryVolumeWithLimits(t, storage.DefaultFileSessionOptions())
}

func advisoryVolumeWithLimits(t *testing.T, options storage.FileSessionOptions) *volume {
	t.Helper()
	_, backing := memoryfixture.New(t, "fuse-advisory", 0, locking.DefaultOptions())
	if err := backing.Write(t.Context(), "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	session, status, err := backing.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		id, err := storage.NewFileActionID(status.ActionEpoch)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := session.Close(ctx, id); err != nil {
			t.Errorf("close advisory session: %v", err)
		}
	})
	v := &volume{storage: backing, files: session, maxFileSize: 1 << 20,
		flushTimeout: time.Second, sessionOptions: options, stop: make(chan struct{}), done: make(chan struct{})}
	start := time.Now()
	if err := v.confirm(start, status); err != nil {
		t.Fatal(err)
	}
	return v
}

func advisoryHandle(t *testing.T, v *volume, read, write bool) *handle {
	t.Helper()
	var uses storage.AccessUse
	if read {
		uses |= storage.ReadContent
	}
	if write {
		uses |= storage.WriteContent
	}
	attr, err := v.storage.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	file, attr, err := v.retainNode(t.Context(), attr.ID, storage.AccessClaim{Uses: uses})
	if err != nil {
		t.Fatal(err)
	}
	return newHandle(&node{volume: v, id: &identity{node: attr.ID}}, file, read, write)
}

func TestAdvisoryBridgePreservesCloseOwnerAndFlockRelease(t *testing.T) {
	v := advisoryVolume(t)
	rootAttr, err := v.storage.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	root := &node{volume: v, id: rootIdentity(rootAttr.ID)}
	raw := newRawFilesystem(fs.NewNodeFS(root, &fs.Options{}), v)
	var entry gofuse.EntryOut
	if status := raw.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "file", &entry); status != 0 {
		t.Fatal(status)
	}
	open := func() uint64 {
		var out gofuse.OpenOut
		if status := raw.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Flags: syscall.O_RDWR}, &out); status != 0 {
			t.Fatal(status)
		}
		return out.Fh
	}
	first, second := open(), open()
	set := gofuse.LkIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: first, Owner: 41,
		Lk: gofuse.FileLock{Start: 10, End: 19, Typ: syscall.F_WRLCK, Pid: 123}}
	if status := raw.SetLk(nil, &set); status != 0 {
		t.Fatal(status)
	}
	query := set
	query.Fh, query.Owner = second, 42
	var conflict gofuse.LkOut
	if status := raw.GetLk(nil, &query, &conflict); status != 0 || conflict.Lk.Typ != syscall.F_WRLCK || conflict.Lk.Start != 10 || conflict.Lk.End != 19 || conflict.Lk.Pid != 123 {
		t.Fatalf("range conflict = %+v, %v", conflict.Lk, status)
	}
	if status := raw.Flush(nil, &gofuse.FlushIn{InHeader: set.InHeader, Fh: second, LockOwner: set.Owner}); status != 0 {
		t.Fatal(status)
	}
	if status := raw.GetLk(nil, &query, &conflict); status != 0 || conflict.Lk.Typ != syscall.F_UNLCK {
		t.Fatalf("closing another descriptor left POSIX owner locked: %+v, %v", conflict.Lk, status)
	}
	set.LkFlags, set.Lk.Start, set.Lk.End = gofuse.FUSE_LK_FLOCK, 0, math.MaxInt64
	if status := raw.SetLk(nil, &set); status != 0 {
		t.Fatal(status)
	}
	query.LkFlags, query.Lk.Start, query.Lk.End = set.LkFlags, 0, math.MaxInt64
	if status := raw.Flush(nil, &gofuse.FlushIn{InHeader: set.InHeader, Fh: second, LockOwner: set.Owner}); status != 0 {
		t.Fatal(status)
	}
	if status := raw.GetLk(nil, &query, &conflict); status != 0 || conflict.Lk.Typ != syscall.F_WRLCK {
		t.Fatalf("POSIX close cleanup changed flock: %+v, %v", conflict.Lk, status)
	}
	raw.Release(nil, &gofuse.ReleaseIn{InHeader: set.InHeader, Fh: first,
		LockOwner: set.Owner, ReleaseFlags: gofuse.FUSE_RELEASE_FLOCK_UNLOCK})
	if status := raw.GetLk(nil, &query, &conflict); status != 0 || conflict.Lk.Typ != syscall.F_UNLCK {
		t.Fatalf("final release retained flock: %+v, %v", conflict.Lk, status)
	}
	raw.Release(nil, &gofuse.ReleaseIn{InHeader: set.InHeader, Fh: second})
	if len(v.raw.requests) != 0 {
		t.Fatal("raw bridge retained completed owner metadata")
	}
}

func TestAdvisoryCallbackAccessAndRanges(t *testing.T) {
	v := advisoryVolume(t)
	reader, writer := advisoryHandle(t, v, true, false), advisoryHandle(t, v, false, true)
	lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
	if errno := reader.Setlk(t.Context(), 1, &lk, 0); errno != syscall.EBADF {
		t.Fatalf("read-only POSIX exclusive = %v", errno)
	}
	lk.Typ = syscall.F_RDLCK
	if errno := writer.Setlk(t.Context(), 2, &lk, 0); errno != syscall.EBADF {
		t.Fatalf("write-only POSIX shared = %v", errno)
	}
	lk.Typ = syscall.F_WRLCK
	if errno := reader.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
		t.Fatalf("read-only exclusive flock = %v", errno)
	}
	for _, invalid := range []gofuse.FileLock{
		{Start: 2, End: 1, Typ: syscall.F_WRLCK},
		{Start: 0, End: math.MaxInt64 + 1, Typ: syscall.F_WRLCK},
		{Start: 0, End: 1, Typ: 99},
	} {
		if errno := writer.Setlk(t.Context(), 2, &invalid, 0); errno != syscall.EINVAL {
			t.Fatalf("invalid range %+v = %v", invalid, errno)
		}
	}
	if errno := writer.Setlk(t.Context(), 2, &lk, 2); errno != syscall.EINVAL {
		t.Fatalf("unknown flags = %v", errno)
	}
}

type interruptedAdvisoryFile struct {
	storage.File
	session       storage.FileSession
	afterSet      func()
	afterWait     func()
	beforeQuery   func()
	beforeCancel  func()
	loseSetReply  bool
	cancelFailure error
	request       storage.FileActionID
	queries       int
	cleanContext  bool
}

func (f *interruptedAdvisoryFile) ReplaceRanges(ctx context.Context, request storage.RangeReplaceRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f.request = id
	receipt, err := f.File.ReplaceRanges(ctx, request, id)
	if err == nil && f.afterSet != nil {
		f.afterSet()
	}
	if err == nil && f.loseSetReply {
		return storage.FileActionReceipt{}, context.Canceled
	}
	return receipt, err
}

func (f *interruptedAdvisoryFile) WaitRanges(ctx context.Context, request storage.RangeWaitRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f.request = id
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		receipt storage.FileActionReceipt
		err     error
	}
	done := make(chan outcome, 1)
	go func() { receipt, err := f.File.WaitRanges(call, request, id); done <- outcome{receipt, err} }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	admitted := false
	for !admitted {
		select {
		case result := <-done:
			return result.receipt, result.err
		case <-ticker.C:
			receipt, err := f.session.QueryAction(call, id)
			if err == nil && receipt.State == storage.FileActionPending {
				admitted = true
			}
		case <-call.Done():
			result := <-done
			return result.receipt, result.err
		}
	}
	if f.afterWait != nil {
		f.afterWait()
	}
	result := <-done
	if f.loseSetReply {
		return storage.FileActionReceipt{}, context.Canceled
	}
	return result.receipt, result.err
}

type interruptedAdvisorySession struct {
	storage.FileSession
	file *interruptedAdvisoryFile
}

func (s interruptedAdvisorySession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f := s.file
	if id != f.request {
		return s.FileSession.QueryAction(ctx, id)
	}
	f.queries++
	_, finite := ctx.Deadline()
	f.cleanContext = ctx.Err() == nil && finite
	if f.beforeQuery != nil {
		f.beforeQuery()
	}
	if f.cancelFailure != nil {
		return storage.FileActionReceipt{}, f.cancelFailure
	}
	return s.FileSession.QueryAction(ctx, id)
}
func (s interruptedAdvisorySession) CancelAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f := s.file
	_, finite := ctx.Deadline()
	f.cleanContext = ctx.Err() == nil && finite
	if f.beforeCancel != nil {
		f.beforeCancel()
	}
	if f.cancelFailure != nil {
		return storage.FileActionReceipt{}, f.cancelFailure
	}
	return s.FileSession.CancelAction(ctx, id)
}

func injectAdvisory(h *handle, probe *interruptedAdvisoryFile) {
	probe.File = h.file
	probe.session = h.node.volume.files
	h.file = probe
	h.node.volume.files = interruptedAdvisorySession{FileSession: probe.session, file: probe}
}

func TestAdvisoryCallbackReconcilesCancellationBeforeReturning(t *testing.T) {
	for _, test := range []struct {
		name       string
		conflict   bool
		lostReply  bool
		cancelFail bool
		want       syscall.Errno
	}{
		{"pending cancellation", true, false, false, syscall.EINTR},
		{"lost pending reply", true, true, false, syscall.EINTR},
		{"grant won cancellation", false, true, false, 0},
		{"unknown cancellation", true, true, true, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := advisoryVolume(t)
			holder, waiter := advisoryHandle(t, v, true, true), advisoryHandle(t, v, true, true)
			lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
			if test.conflict {
				if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
					t.Fatal(errno)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := &interruptedAdvisoryFile{File: waiter.file, loseSetReply: test.lostReply,
				afterSet: cancel, afterWait: cancel}
			if test.cancelFail {
				injected.cancelFailure = errors.New("lost cancellation reply")
			}
			injectAdvisory(waiter, injected)
			if errno := waiter.Setlkw(ctx, 2, &lk, gofuse.FUSE_LK_FLOCK); errno != test.want {
				t.Fatalf("cancelled lock returned %v, want %v", errno, test.want)
			}
			if test.lostReply && !injected.cleanContext {
				t.Fatal("cancellation reconciliation lacked a live finite cleanup context")
			}
			if test.cancelFail {
				if errnoOf(v.check()) != syscall.EIO {
					t.Fatal("unknown acquisition did not fence volume I/O")
				}
				return
			}
			attempt, err := injected.session.QueryAction(t.Context(), injected.request)
			wantState, wantErrno := storage.FileActionNotApplied, syscall.EINTR
			if !test.conflict {
				wantState, wantErrno = storage.FileActionCompleted, 0
			}
			if attempt.State != wantState || errnoOf(err) != wantErrno {
				t.Fatalf("server action state/error = %v/%v, want %v/%v", attempt.State, err, wantState, wantErrno)
			}
		})
	}
}

func TestAdvisoryCallbackContinuesPendingRequest(t *testing.T) {
	v := advisoryVolume(t)
	holder, waiter := advisoryHandle(t, v, true, true), advisoryHandle(t, v, true, true)
	lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
	if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
		t.Fatal(errno)
	}
	injected := &interruptedAdvisoryFile{File: waiter.file, afterWait: func() {
		if err := holder.dropAdvisory(t.Context(), 1, flockFamily); err != nil {
			t.Fatal(err)
		}
	}}
	injectAdvisory(waiter, injected)
	if errno := waiter.Setlkw(t.Context(), 2, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
		t.Fatal(errno)
	}
	if injected.queries != 0 {
		t.Fatalf("blocking revision wait was polled %d times", injected.queries)
	}
}

func TestRawOwnerMetadataBoundsCancellationAndCleanup(t *testing.T) {
	m := &rawMetadata{limit: 2, requests: make(map[<-chan struct{}]rawRequest)}
	cancel := make(chan struct{})
	first, finishFirst, ok := m.begin(cancel, rawRequest{kind: rawFlush, owner: 0})
	if !ok {
		t.Fatal("first owner rejected")
	}
	second, finishSecond, ok := m.begin(cancel, rawRequest{kind: rawRelease, owner: 2})
	if !ok || second == first {
		t.Fatal("second owner did not receive an independent metadata identity")
	}
	if _, _, ok := m.begin(nil, rawRequest{owner: 3}); ok {
		t.Fatal("metadata exceeded its configured capacity")
	}
	for channel, owner := range map[<-chan struct{}]lockOwner{first: 0, second: 2} {
		request, ok := m.lookup(channel)
		if !ok || request.owner != owner {
			t.Fatalf("metadata owner = %d, %v; want %d", request.owner, ok, owner)
		}
	}
	close(cancel)
	for _, channel := range []<-chan struct{}{first, second} {
		select {
		case <-channel:
		case <-time.After(time.Second):
			t.Fatal("raw cancellation was not forwarded")
		}
	}
	finishFirst()
	finishSecond()
	if len(m.requests) != 0 {
		t.Fatal("completed calls retained owner metadata")
	}
	_, finish, ok := m.begin(nil, rawRequest{})
	if !ok {
		t.Fatal("metadata capacity did not recover")
	}
	finish()
}

type failedOwnerCleanup struct {
	storage.File
	dropErr       error
	closed        bool
	finiteCleanup bool
}

func (f *failedOwnerCleanup) RetireRangeOwner(ctx context.Context, owner storage.RangeOwnerID, scope storage.RangeScope, id storage.FileActionID) (storage.FileActionReceipt, error) {
	_, hasDeadline := ctx.Deadline()
	f.finiteCleanup = ctx.Err() == nil && hasDeadline
	return storage.FileActionReceipt{Action: id, Operation: storage.OpFileRetireRanges, State: storage.FileActionNotApplied, Errno: syscall.EIO}, f.dropErr
}

func (f *failedOwnerCleanup) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f.closed = true
	return f.File.Close(ctx, id)
}

func TestAdvisoryReleaseClosesReferenceAfterOwnerCleanupFailure(t *testing.T) {
	v := advisoryVolume(t)
	h := advisoryHandle(t, v, true, true)
	injected := &failedOwnerCleanup{File: h.file, dropErr: errors.New("owner cleanup failed")}
	h.file = injected
	lk := gofuse.FileLock{Typ: syscall.F_WRLCK}
	if errno := h.Setlk(t.Context(), 7, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
		t.Fatal(errno)
	}
	v.raw = &rawMetadata{limit: 1, requests: make(map[<-chan struct{}]rawRequest)}
	cancelled := make(chan struct{})
	close(cancelled)
	forwarded, done, ok := v.raw.begin(cancelled, rawRequest{kind: rawRelease, owner: 7, flockUnlock: true})
	if !ok {
		t.Fatal("release metadata rejected")
	}
	defer done()
	if errno := h.Release(&gofuse.Context{Cancel: forwarded}); errno != syscall.EIO {
		t.Fatalf("failed owner cleanup returned %v", errno)
	}
	if !injected.closed || !injected.finiteCleanup {
		t.Fatalf("release cleanup: closed=%v finite=%v", injected.closed, injected.finiteCleanup)
	}
	if _, err := injected.File.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("released reference remained usable: %v", err)
	}
	if errnoOf(v.check()) != syscall.EIO {
		t.Fatal("unobservable release failure did not fence volume I/O")
	}
}

func TestAdvisoryCallbackNormalRetirementPreservesShutdownOutcome(t *testing.T) {
	for _, cancelRace := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "cancel race"}[cancelRace], func(t *testing.T) {
			v := advisoryVolume(t)
			holder, waiter := advisoryHandle(t, v, true, true), advisoryHandle(t, v, true, true)
			lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
			if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
				t.Fatal(errno)
			}
			stop := func() {
				v.mu.Lock()
				v.stopping = true
				close(v.stop)
				v.mu.Unlock()
			}
			injected := &interruptedAdvisoryFile{File: waiter.file}
			if cancelRace {
				injected.loseSetReply, injected.cancelFailure, injected.beforeCancel = true, syscall.ESTALE, stop
				injected.afterWait = func() {
					stop()
					id, idErr := storage.NewFileActionID(v.status.ActionEpoch)
					if idErr != nil {
						t.Error(idErr)
						return
					}
					if _, err := injected.session.Close(t.Context(), id); err != nil {
						t.Error(err)
					}
				}
			} else {
				injected.afterWait = func() {
					stop()
					id, idErr := storage.NewFileActionID(v.status.ActionEpoch)
					if idErr != nil {
						t.Error(idErr)
						return
					}
					if _, err := injected.session.Close(t.Context(), id); err != nil {
						t.Error(err)
					}
				}
			}
			injectAdvisory(waiter, injected)
			if errno := waiter.Setlkw(t.Context(), 2, &lk, gofuse.FUSE_LK_FLOCK); errno != syscall.ESTALE {
				t.Fatalf("normal retirement returned %v", errno)
			}
			if v.fault != nil {
				t.Fatalf("normal retirement introduced a fault: %v", v.fault)
			}
		})
	}
}

type saturatedRawProbe struct {
	gofuse.RawFileSystem
	flushes, releases int
}

func (p *saturatedRawProbe) Flush(<-chan struct{}, *gofuse.FlushIn) gofuse.Status {
	p.flushes++
	return 0
}

func (p *saturatedRawProbe) Release(<-chan struct{}, *gofuse.ReleaseIn) { p.releases++ }

func TestRawOwnerMetadataSaturationFencesAndStillDelegatesRelease(t *testing.T) {
	v := advisoryVolume(t)
	v.sessionOptions.MaxOperations = 1
	probe := &saturatedRawProbe{RawFileSystem: gofuse.NewDefaultRawFileSystem()}
	raw := newRawFilesystem(probe, v)
	_, done, ok := v.raw.begin(nil, rawRequest{kind: rawFlush})
	if !ok {
		t.Fatal("first metadata reservation failed")
	}
	defer done()
	if status := raw.Flush(nil, &gofuse.FlushIn{}); status != gofuse.EIO || probe.flushes != 0 {
		t.Fatalf("saturated flush returned %v and delegated %d calls", status, probe.flushes)
	}
	raw.Release(nil, &gofuse.ReleaseIn{})
	if probe.releases != 1 || errnoOf(v.check()) != syscall.EIO {
		t.Fatalf("saturated release: delegates=%d health=%v", probe.releases, v.check())
	}
}

func TestAdvisoryFailedSplitKeepsTheOriginalRange(t *testing.T) {
	v := advisoryVolume(t)
	v.sessionOptions.MaxRanges = 1
	holder, observer := advisoryHandle(t, v, true, true), advisoryHandle(t, v, true, true)
	whole := gofuse.FileLock{Start: 0, End: 99, Typ: syscall.F_RDLCK, Pid: 19}
	if errno := holder.Setlk(t.Context(), 1, &whole, 0); errno != 0 {
		t.Fatal(errno)
	}
	middle := gofuse.FileLock{Start: 20, End: 29, Typ: syscall.F_WRLCK, Pid: 19}
	if errno := holder.Setlk(t.Context(), 1, &middle, 0); errno != syscall.ENOLCK {
		t.Fatalf("split=%v,want ENOLCK", errno)
	}
	query := gofuse.FileLock{Start: 0, End: 99, Typ: syscall.F_WRLCK}
	var conflict gofuse.FileLock
	if errno := observer.Getlk(t.Context(), 2, &query, 0, &conflict); errno != 0 || conflict.Start != 0 || conflict.End != 99 || conflict.Typ != syscall.F_RDLCK {
		t.Fatalf("original range=%+v/%v", conflict, errno)
	}
}

func TestAdvisoryCrossFileDeadlockUsesLocalProcessIdentity(t *testing.T) {
	v := advisoryVolume(t)
	if err := v.storage.Write(t.Context(), "other", []byte("other")); err != nil {
		t.Fatal(err)
	}
	first := advisoryHandle(t, v, true, true)
	attr, err := v.storage.Stat(t.Context(), "other")
	if err != nil {
		t.Fatal(err)
	}
	file, attr, err := v.retainNode(t.Context(), attr.ID, storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent})
	if err != nil {
		t.Fatal(err)
	}
	second := newHandle(&node{volume: v, id: &identity{node: attr.ID}}, file, true, true)
	lock := gofuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	if errno := first.Setlk(t.Context(), 1, &lock, 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno := second.Setlk(t.Context(), 2, &lock, 0); errno != 0 {
		t.Fatal(errno)
	}
	admitted := make(chan struct{})
	probe := &interruptedAdvisoryFile{afterWait: func() { close(admitted) }}
	injectAdvisory(second, probe)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	waiting := make(chan syscall.Errno, 1)
	go func() { waiting <- second.Setlkw(ctx, 1, &lock, 0) }()
	select {
	case <-admitted:
	case <-ctx.Done():
		t.Fatal("first dependency was not admitted")
	}
	if errno := first.Setlkw(ctx, 2, &lock, 0); errno != syscall.EDEADLK {
		t.Fatalf("cross-file cycle=%v,want EDEADLK", errno)
	}
	if err := second.dropAdvisory(t.Context(), 2, posixFamily); err != nil {
		t.Fatal(err)
	}
	select {
	case errno := <-waiting:
		if errno != 0 {
			t.Fatalf("wait after cycle release=%v", errno)
		}
	case <-ctx.Done():
		t.Fatal("wait remained after dependency release")
	}
}

func TestAdvisoryOwnerCapacityRecoversAfterQueriesAndUnlocks(t *testing.T) {
	options := storage.DefaultFileSessionOptions()
	options.MaxRangeOwners = 1
	v := advisoryVolumeWithLimits(t, options)
	h := advisoryHandle(t, v, true, true)
	lock := gofuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	for owner := uint64(0); owner < 16; owner++ {
		var out gofuse.FileLock
		if errno := h.Getlk(t.Context(), owner, &lock, 0, &out); errno != 0 || out.Typ != syscall.F_UNLCK {
			t.Fatalf("query owner%d=%+v/%v", owner, out, errno)
		}
		if errno := h.Setlk(t.Context(), owner, &lock, 0); errno != 0 {
			t.Fatalf("acquire owner%d=%v", owner, errno)
		}
		if err := h.dropAdvisory(t.Context(), lockOwner(owner), posixFamily); err != nil {
			t.Fatalf("close owner%d=%v", owner, err)
		}
	}
	if len(v.localOwners().entries) != 0 {
		t.Fatal("retired owners remain locally reachable")
	}
}

func TestAdvisoryNativeWaitOutlivesItsInitialLease(t *testing.T) {
	options := storage.DefaultFileSessionOptions()
	options.Lease = 400 * time.Millisecond
	v := advisoryVolumeWithLimits(t, options)
	v.flushTimeout = 100 * time.Millisecond
	v.mu.Lock()
	initialDeadline := v.deadline
	v.mu.Unlock()
	holder, waiter := advisoryHandle(t, v, true, true), advisoryHandle(t, v, true, true)
	lock := gofuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	if errno := holder.Setlk(t.Context(), 1, &lock, 0); errno != 0 {
		t.Fatal(errno)
	}
	admitted := make(chan struct{}, 16)
	injectAdvisory(waiter, &interruptedAdvisoryFile{afterWait: func() { admitted <- struct{}{} }})
	v.renewContext, v.cancelRenew = context.WithCancel(t.Context())
	go v.maintain()
	t.Cleanup(func() {
		if err := v.stopSession(); err != nil {
			t.Errorf("stop renewed session: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan syscall.Errno, 1)
	go func() { done <- waiter.Setlkw(ctx, 2, &lock, 0) }()
	for range 6 {
		select {
		case <-admitted:
		case errno := <-done:
			t.Fatalf("valid blocking wait ended early: %v", errno)
		case <-ctx.Done():
			t.Fatal("native wait did not span its budgets")
		}
	}
	if !time.Now().After(initialDeadline) {
		t.Fatal("wait did not cross the first confirmed lease deadline")
	}
	if err := holder.dropAdvisory(t.Context(), 1, posixFamily); err != nil {
		t.Fatal(err)
	}
	select {
	case errno := <-done:
		if errno != 0 {
			t.Fatalf("grant after renewed wait: %v", errno)
		}
	case <-ctx.Done():
		t.Fatal("wait did not acquire after release")
	}
}
