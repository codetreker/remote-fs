package smb

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type handleFileStub struct {
	result storage.ReferenceCloseResult
	err    error
	closes atomic.Int32
}

func (*handleFileStub) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, syscall.ENOSYS
}
func (*handleFileStub) ReadAt(context.Context, int64, int) (storage.FileRead, error) {
	return storage.FileRead{}, syscall.ENOSYS
}
func (*handleFileStub) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	return storage.Attr{}, syscall.ENOSYS
}
func (*handleFileStub) Truncate(context.Context, int64) (storage.Attr, error) {
	return storage.Attr{}, syscall.ENOSYS
}
func (*handleFileStub) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.ENOSYS
}
func (*handleFileStub) Sync(context.Context) error { return syscall.ENOSYS }
func (f *handleFileStub) Close(ctx context.Context) error {
	_, err := f.CloseWithResult(ctx)
	return err
}
func (f *handleFileStub) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	f.closes.Add(1)
	return f.result, f.err
}

func TestFileHandleSlotsAreReservedAndNeverReused(t *testing.T) {
	s := &session{id: 41}
	left, right := &tree{}, &tree{}
	if !left.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	first, err := left.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.reserveFileHandle(s, 1); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("full tree admission: %v", err)
	}
	if !right.beginFileWork(s) {
		t.Fatal("second tree admission refused")
	}
	second, err := right.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.id == second.id {
		t.Fatal("session reused a FileId across trees")
	}
	left.releaseFileHandle(first)
	third, err := left.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	if third.id == first.id || third.id == second.id {
		t.Fatal("FileId was reused")
	}
	left.endFileWork()
	right.endFileWork()
}

func TestFileWorkFenceDrainsAcceptedOperations(t *testing.T) {
	s := &session{id: 5}
	tree := &tree{}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- waitFileWork(ctx, tree.fenceFileWork()) }()
	for {
		tree.fileMu.Lock()
		stopping := tree.fileStopping
		tree.fileMu.Unlock()
		if stopping {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
	}
	if tree.beginFileWork(s) {
		t.Fatal("work admitted after tree fence")
	}
	select {
	case <-finished:
		t.Fatal("tree drained before accepted work completed")
	default:
	}
	tree.endFileWork()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestTreeCleanupPublishesFenceBeforeDraining(t *testing.T) {
	s := &session{id: 25}
	tree := &tree{kind: controlTree, id: 1, sessionID: s.id, done: make(chan struct{})}
	if !tree.beginFileWork(s) {
		t.Fatal("initial admission refused")
	}
	c := &connection{server: &Server{config: Config{Limits: DefaultLimits()}}, pending: make(map[uint64]*pendingRequest)}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- c.closeTreeContext(ctx, tree) }()
	select {
	case <-tree.done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if tree.beginFileWork(s) {
		t.Fatal("work admitted after tree cleanup started")
	}
	select {
	case <-finished:
		t.Fatal("tree closed before admitted work drained")
	default:
	}
	tree.endFileWork()
	if err := <-finished; err != nil || !tree.closed {
		t.Fatalf("tree cleanup: closed=%v err=%v", tree.closed, err)
	}
}

func TestRetiredSessionCannotAdmitFileWork(t *testing.T) {
	s := &session{id: 6}
	tree := &tree{}
	if !tree.beginFileWork(s) {
		t.Fatal("initial admission refused")
	}
	s.mu.Lock()
	s.retired = true
	s.mu.Unlock()
	if tree.beginFileWork(s) {
		t.Fatal("retired session admitted file work")
	}
	tree.endFileWork()
}

func TestStatusCountsPendingAndRetainedFileSlots(t *testing.T) {
	s := &session{id: 7, trees: make(map[uint32]*tree)}
	tree := &tree{id: 3}
	s.trees[tree.id] = tree
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	tree.endFileWork()
	c := &connection{sessions: map[uint64]*session{s.id: s}}
	var status Status
	c.addStatus(&status)
	if status.Handles != 1 || status.Trees != 1 {
		t.Fatalf("owned resource status: %+v", status)
	}
	tree.releaseFileHandle(handle)
	status = Status{}
	c.addStatus(&status)
	if status.Handles != 0 {
		t.Fatalf("released slot remained in status: %+v", status)
	}
}

func TestFileHandleCloseKeepsOnlyUnreleasedOwnership(t *testing.T) {
	s := &session{id: 8}
	tree := &tree{}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	retained, err := tree.reserveFileHandle(s, 2)
	if err != nil {
		t.Fatal(err)
	}
	released, err := tree.reserveFileHandle(s, 2)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("close outcome unavailable")
	retainedFile := &handleFileStub{err: failure}
	releasedFile := &handleFileStub{result: storage.ReferenceCloseResult{Released: true}, err: syscall.ENOTEMPTY}
	retained.file, released.file = retainedFile, releasedFile
	tree.endFileWork()
	if err := tree.closeFileHandles(t.Context()); !errors.Is(err, failure) || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("joined cleanup errors: %v", err)
	}
	if tree.findFileHandle(retained.id) != retained || tree.findFileHandle(released.id) != nil {
		t.Fatal("close result did not determine ownership")
	}
	retainedFile.result = storage.ReferenceCloseResult{Released: true}
	retainedFile.err = nil
	if err := tree.closeFileHandles(t.Context()); err != nil {
		t.Fatal(err)
	}
	if tree.findFileHandle(retained.id) != nil || retainedFile.closes.Load() != 2 || releasedFile.closes.Load() != 1 {
		t.Fatal("cleanup retry did not retire exactly the retained handle")
	}
}

func TestPendingOpenRetainsSlotUntilSameActionRecovery(t *testing.T) {
	s := &session{id: 9}
	tree := &tree{}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("open result unknown")
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true}}
	attempts := 0
	handle.pendingOpen = func(context.Context) (bool, error) {
		attempts++
		if attempts == 1 {
			return false, failure
		}
		handle.file = file
		return false, nil
	}
	tree.endFileWork()
	if err := tree.closeFileHandles(t.Context()); !errors.Is(err, failure) || tree.findFileHandle(handle.id) == nil {
		t.Fatalf("unknown open released its slot: %v", err)
	}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	if _, err := tree.reserveFileHandle(s, 1); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("pending open lost capacity: %v", err)
	}
	tree.endFileWork()
	if err := tree.closeFileHandles(t.Context()); err != nil {
		t.Fatal(err)
	}
	if tree.findFileHandle(handle.id) != nil || file.closes.Load() != 1 || attempts != 2 {
		t.Fatal("recovered reference was not closed exactly once")
	}
}

type resultSessionStub struct {
	*endpointFileSession
	result storage.ReferenceCloseResult
	err    error
}

func (s *resultSessionStub) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	return s.result, s.err
}

func TestLastTreeSessionCloseDischargesUnresolvedOpen(t *testing.T) {
	for _, released := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "released"}[released], func(t *testing.T) {
			failure := errors.New("open outcome unknown")
			server := &Server{config: Config{Limits: DefaultLimits()}}
			export := &Export{server: server, refs: 1, trees: 1}
			done := make(chan struct{})
			close(done)
			authority := &authoritySession{raw: &resultSessionStub{endpointFileSession: newEndpointFileSession(), result: storage.ReferenceCloseResult{Released: released}, err: func() error {
				if released {
					return nil
				}
				return failure
			}()},
				export: export, refs: 1, done: done}
			tree := &tree{kind: volumeTree, id: 1, sessionID: 2, export: export, authority: authority, done: make(chan struct{})}
			s := &session{id: 2}
			if !tree.beginFileWork(s) {
				t.Fatal("file admission refused")
			}
			handle, err := tree.reserveFileHandle(s, 1)
			if err != nil {
				t.Fatal(err)
			}
			handle.pendingOpen = func(context.Context) (bool, error) { return false, failure }
			tree.endFileWork()
			c := &connection{server: server, pending: make(map[uint64]*pendingRequest)}
			err = c.closeTreeContext(t.Context(), tree)
			if !errors.Is(err, failure) {
				t.Fatalf("cleanup error: %v", err)
			}
			if tree.closed != released || (tree.findFileHandle(handle.id) == nil) != released {
				t.Fatalf("owner state after session close: released=%v tree.closed=%v", released, tree.closed)
			}
		})
	}
}
