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

func advisoryNamespace(t *testing.T) *namespace {
	t.Helper()
	_, backing := memoryfixture.New(t, "fuse-advisory", 0, locking.DefaultOptions())
	if err := backing.Write(t.Context(), "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	session, err := backing.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close advisory session: %v", err)
		}
	})
	ns := &namespace{storage: backing, files: session, maxFileSize: 1 << 20,
		flushTimeout: time.Second, sessionOptions: options, stop: make(chan struct{}), done: make(chan struct{})}
	start := time.Now()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.confirm(start, status); err != nil {
		t.Fatal(err)
	}
	return ns
}

func advisoryHandle(t *testing.T, ns *namespace, read, write bool) *handle {
	t.Helper()
	file, err := ns.files.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: read, Write: write})
	if err != nil {
		t.Fatal(err)
	}
	attr, err := file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return newHandle(&node{ns: ns, id: &identity{node: attr.ID}}, file, read, write)
}

func TestAdvisoryBridgePreservesCloseOwnerAndFlockRelease(t *testing.T) {
	ns := advisoryNamespace(t)
	rootAttr, err := ns.storage.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	root := &node{ns: ns, id: rootIdentity(rootAttr.ID)}
	raw := newRawFilesystem(fs.NewNodeFS(root, &fs.Options{}), ns)
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
	if len(ns.raw.requests) != 0 {
		t.Fatal("raw bridge retained completed owner metadata")
	}
}

func TestAdvisoryCallbackAccessAndRanges(t *testing.T) {
	ns := advisoryNamespace(t)
	reader, writer := advisoryHandle(t, ns, true, false), advisoryHandle(t, ns, false, true)
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
	afterSet      func(storage.LockAttempt)
	beforeQuery   func()
	beforeCancel  func()
	loseSetReply  bool
	cancelFailure error
	request       storage.LockRequestID
	queries       int
	cleanContext  bool
}

func (f *interruptedAdvisoryFile) SetLock(ctx context.Context, owner storage.LockOwner, lock storage.FileLock, request storage.LockRequestID) (storage.LockAttempt, error) {
	f.request = request
	attempt, err := f.File.SetLock(ctx, owner, lock, request)
	if err != nil {
		return attempt, err
	}
	if f.afterSet != nil {
		f.afterSet(attempt)
	}
	if f.loseSetReply {
		return storage.LockAttempt{}, context.Canceled
	}
	return attempt, nil
}

func (f *interruptedAdvisoryFile) QueryLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	f.queries++
	if request != f.request {
		return storage.LockAttempt{}, errors.New("query changed its request identity")
	}
	if f.beforeQuery != nil {
		f.beforeQuery()
	}
	return f.File.QueryLock(ctx, owner, request)
}

func (f *interruptedAdvisoryFile) CancelLock(ctx context.Context, owner storage.LockOwner, request storage.LockRequestID) (storage.LockAttempt, error) {
	_, hasDeadline := ctx.Deadline()
	f.cleanContext = ctx.Err() == nil && hasDeadline
	if f.beforeCancel != nil {
		f.beforeCancel()
	}
	if f.cancelFailure != nil {
		return storage.LockAttempt{}, f.cancelFailure
	}
	return f.File.CancelLock(ctx, owner, request)
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
			ns := advisoryNamespace(t)
			holder, waiter := advisoryHandle(t, ns, true, true), advisoryHandle(t, ns, true, true)
			lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
			if test.conflict {
				if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
					t.Fatal(errno)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := &interruptedAdvisoryFile{File: waiter.file, loseSetReply: test.lostReply,
				afterSet: func(storage.LockAttempt) { cancel() }}
			if test.cancelFail {
				injected.cancelFailure = errors.New("lost cancellation reply")
			}
			waiter.file = injected
			if errno := waiter.Setlkw(ctx, 2, &lk, gofuse.FUSE_LK_FLOCK); errno != test.want {
				t.Fatalf("cancelled lock returned %v, want %v", errno, test.want)
			}
			if !injected.cleanContext {
				t.Fatal("cancellation reconciliation lacked a live finite cleanup context")
			}
			if test.cancelFail {
				if errnoOf(ns.check()) != syscall.EIO {
					t.Fatal("unknown acquisition did not fence namespace I/O")
				}
				return
			}
			attempt, err := injected.File.QueryLock(t.Context(), 2, injected.request)
			if err != nil {
				t.Fatal(err)
			}
			wantState := storage.LockCancelled
			if !test.conflict {
				wantState = storage.LockGranted
			}
			if attempt.State != wantState {
				t.Fatalf("server action state = %v, want %v", attempt.State, wantState)
			}
		})
	}
}

func TestAdvisoryCallbackContinuesPendingRequest(t *testing.T) {
	ns := advisoryNamespace(t)
	holder, waiter := advisoryHandle(t, ns, true, true), advisoryHandle(t, ns, true, true)
	lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
	if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
		t.Fatal(errno)
	}
	injected := &interruptedAdvisoryFile{File: waiter.file, beforeQuery: func() {
		if err := holder.file.DropLocks(t.Context(), 1, storage.Flock); err != nil {
			t.Fatal(err)
		}
	}}
	waiter.file = injected
	if errno := waiter.Setlkw(t.Context(), 2, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
		t.Fatal(errno)
	}
	if injected.queries != 1 {
		t.Fatalf("pending action queried %d times, want one continuation", injected.queries)
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
	for channel, owner := range map[<-chan struct{}]storage.LockOwner{first: 0, second: 2} {
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

func (f *failedOwnerCleanup) DropLocks(ctx context.Context, owner storage.LockOwner, family storage.LockFamily) error {
	_, hasDeadline := ctx.Deadline()
	f.finiteCleanup = ctx.Err() == nil && hasDeadline
	return f.dropErr
}

func (f *failedOwnerCleanup) Close(ctx context.Context) error {
	f.closed = true
	return f.File.Close(ctx)
}

func TestAdvisoryReleaseClosesReferenceAfterOwnerCleanupFailure(t *testing.T) {
	ns := advisoryNamespace(t)
	h := advisoryHandle(t, ns, true, true)
	injected := &failedOwnerCleanup{File: h.file, dropErr: errors.New("owner cleanup failed")}
	h.file = injected
	ns.raw = &rawMetadata{limit: 1, requests: make(map[<-chan struct{}]rawRequest)}
	cancelled := make(chan struct{})
	close(cancelled)
	forwarded, done, ok := ns.raw.begin(cancelled, rawRequest{kind: rawRelease, owner: 7, flockUnlock: true})
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
	if _, err := injected.File.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("released reference remained usable: %v", err)
	}
	if errnoOf(ns.check()) != syscall.EIO {
		t.Fatal("unobservable release failure did not fence namespace I/O")
	}
}

func TestAdvisoryCallbackNormalRetirementPreservesShutdownOutcome(t *testing.T) {
	for _, cancelRace := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "cancel race"}[cancelRace], func(t *testing.T) {
			ns := advisoryNamespace(t)
			holder, waiter := advisoryHandle(t, ns, true, true), advisoryHandle(t, ns, true, true)
			lk := gofuse.FileLock{Start: 0, End: math.MaxInt64, Typ: syscall.F_WRLCK}
			if errno := holder.Setlk(t.Context(), 1, &lk, gofuse.FUSE_LK_FLOCK); errno != 0 {
				t.Fatal(errno)
			}
			stop := func() {
				ns.mu.Lock()
				ns.stopping = true
				close(ns.stop)
				ns.mu.Unlock()
			}
			injected := &interruptedAdvisoryFile{File: waiter.file}
			if cancelRace {
				injected.loseSetReply, injected.cancelFailure, injected.beforeCancel = true, syscall.ESTALE, stop
			} else {
				injected.afterSet = func(storage.LockAttempt) { stop() }
			}
			waiter.file = injected
			if errno := waiter.Setlkw(t.Context(), 2, &lk, gofuse.FUSE_LK_FLOCK); errno != syscall.ESTALE {
				t.Fatalf("normal retirement returned %v", errno)
			}
			if ns.fault != nil {
				t.Fatalf("normal retirement introduced a fault: %v", ns.fault)
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
	ns := advisoryNamespace(t)
	ns.sessionOptions.MaxOperations = 1
	probe := &saturatedRawProbe{RawFileSystem: gofuse.NewDefaultRawFileSystem()}
	raw := newRawFilesystem(probe, ns)
	_, done, ok := ns.raw.begin(nil, rawRequest{kind: rawFlush})
	if !ok {
		t.Fatal("first metadata reservation failed")
	}
	defer done()
	if status := raw.Flush(nil, &gofuse.FlushIn{}); status != gofuse.EIO || probe.flushes != 0 {
		t.Fatalf("saturated flush returned %v and delegated %d calls", status, probe.flushes)
	}
	raw.Release(nil, &gofuse.ReleaseIn{})
	if probe.releases != 1 || errnoOf(ns.check()) != syscall.EIO {
		t.Fatalf("saturated release: delegates=%d health=%v", probe.releases, ns.check())
	}
}
