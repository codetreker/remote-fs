package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func fileIOGateFixture() (*tree, *session, *fileHandle) {
	s := &session{id: 41}
	a := &authoritySession{deadline: time.Now().Add(time.Minute)}
	a.identity = storage.FileSessionIdentityResult{Backend: endpointBackendIdentity(), SessionEpoch: "gate-session"}
	t := &tree{id: 7, sessionID: s.id, authority: a, handles: make(map[wire.FileID]*fileHandle)}
	h := &fileHandle{id: wire.FileID{1}, treeID: t.id, sessionID: s.id, ownerSession: s, identity: a.identity, nodeID: 9}
	h.state.Store(uint32(handleLive))
	t.handles[h.id] = h
	return t, s, h
}

func admitGateTicket(t *testing.T, tree *tree, s *session, h *fileHandle) *fileIOTicket {
	t.Helper()
	ticket, err := tree.admitFileIO(t.Context(), s, h.id, 8)
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}

func TestFileIOGateRunsFIFOAndDifferentHandlesConcurrently(t *testing.T) {
	tree, session, firstHandle := fileIOGateFixture()
	secondHandle := &fileHandle{id: wire.FileID{2}, treeID: tree.id, sessionID: session.id, ownerSession: session, identity: tree.authority.identity, nodeID: 10}
	secondHandle.state.Store(uint32(handleLive))
	tree.handles[secondHandle.id] = secondHandle
	first := admitGateTicket(t, tree, session, firstHandle)
	second := admitGateTicket(t, tree, session, firstHandle)
	third := admitGateTicket(t, tree, session, firstHandle)
	parallel := admitGateTicket(t, tree, session, secondHandle)
	if first.sequence != 1 || second.sequence != 2 || third.sequence != 3 || parallel.sequence != 1 {
		t.Fatal("admission sequences are not per handle")
	}
	if err := first.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := parallel.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	parallel.finish()
	for _, queued := range []*fileIOTicket{second, third} {
		select {
		case <-queued.ready:
			t.Fatal("same-handle I/O overtook running operation")
		default:
		}
	}
	first.finish()
	if err := second.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-third.ready:
		t.Fatal("third ticket overtook second ticket")
	default:
	}
	second.finish()
	if err := third.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	third.finish()
	first.finish()
	if tree.fileActive != 0 || tree.fileIdle != nil {
		t.Fatal("finished tickets retained tree work")
	}
}

func TestFileIOGateCancellationDropsQueuedWorkWithoutDispatch(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	first := admitGateTicket(t, tree, session, handle)
	second := admitGateTicket(t, tree, session, handle)
	third := admitGateTicket(t, tree, session, handle)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := second.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled queued ticket: %v", err)
	}
	if tree.fileActive != 2 || len(handle.ioQueue) != 1 || handle.ioQueue[0] != third {
		t.Fatal("canceled ticket retained capacity or changed FIFO order")
	}
	first.finish()
	if err := third.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	third.finish()
	second.finish()
	if tree.fileActive != 0 {
		t.Fatal("duplicate completion released another ticket's work")
	}
}

func TestFileIOGateFenceDropsQueueAndWaitsForRunningWork(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	first := admitGateTicket(t, tree, session, handle)
	second := admitGateTicket(t, tree, session, handle)
	if err := first.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	idle := tree.fenceHandleFileIO(handle)
	if err := second.wait(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("fenced queued ticket: %v", err)
	}
	if tree.fileActive != 1 {
		t.Fatal("fence did not release queued tree admission")
	}
	if _, err := tree.admitFileIO(t.Context(), session, handle.id, 8); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("I/O after handle fence: %v", err)
	}
	select {
	case <-idle:
		t.Fatal("handle drained while running work remained")
	default:
	}
	first.finish()
	if err := waitFileWork(t.Context(), idle); err != nil {
		t.Fatal(err)
	}
	second.finish()
	if tree.fileActive != 0 || handle.ioRunning != nil || len(handle.ioQueue) != 0 {
		t.Fatal("fenced tickets retained work")
	}
}

func TestFileIOGateTreeFenceReleasesEveryQueuedAdmission(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	first := admitGateTicket(t, tree, session, handle)
	second := admitGateTicket(t, tree, session, handle)
	if err := first.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	idle := tree.fenceFileWork()
	if err := second.wait(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("tree-fenced queued ticket: %v", err)
	}
	if !handle.ioFenced || tree.fileActive != 1 {
		t.Fatal("tree fence did not fence handle queue before draining")
	}
	if _, err := tree.admitFileIO(t.Context(), session, handle.id, 8); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("I/O after tree fence: %v", err)
	}
	first.finish()
	if err := waitFileWork(t.Context(), idle); err != nil {
		t.Fatal(err)
	}
}

func TestFileIOGateFenceDiscardsGrantedUndispatchedTurn(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	first := admitGateTicket(t, tree, session, handle)
	second := admitGateTicket(t, tree, session, handle)
	if idle := tree.fenceHandleFileIO(handle); idle != nil {
		t.Fatal("undispatched granted ticket retained a running wait")
	}
	for _, ticket := range []*fileIOTicket{first, second} {
		if err := ticket.wait(t.Context()); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("undispatched ticket survived fence: %v", err)
		}
		ticket.finish()
	}
	if tree.fileActive != 0 {
		t.Fatal("undispatched tickets retained tree capacity")
	}
}

func TestFileIOGateAdmissionValidatesBoundedLiveOwnership(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*tree, *session, *fileHandle)
		err    error
	}{
		{"session retired", func(_ *tree, s *session, _ *fileHandle) { s.retired = true }, syscall.ESTALE},
		{"authority stopped", func(tree *tree, _ *session, _ *fileHandle) { tree.authority.stopping = true }, syscall.ESTALE},
		{"authority closed", func(tree *tree, _ *session, _ *fileHandle) { tree.authority.closed = true }, syscall.ESTALE},
		{"authority expired", func(tree *tree, _ *session, _ *fileHandle) { tree.authority.deadline = time.Now().Add(-time.Second) }, syscall.ESTALE},
		{"tree mismatch", func(_ *tree, _ *session, h *fileHandle) { h.treeID++ }, syscall.EBADF},
		{"session mismatch", func(_ *tree, _ *session, h *fileHandle) { h.sessionID++ }, syscall.EBADF},
		{"owner mismatch", func(_ *tree, _ *session, h *fileHandle) { h.ownerSession = &session{} }, syscall.EBADF},
		{"identity mismatch", func(_ *tree, _ *session, h *fileHandle) { h.identity.SessionEpoch = "other" }, syscall.EBADF},
		{"zero object identity", func(_ *tree, _ *session, h *fileHandle) { h.nodeID = 0 }, syscall.EBADF},
		{"not live", func(_ *tree, _ *session, h *fileHandle) { h.state.Store(uint32(handleCleanupOnly)) }, syscall.EBADF},
		{"sequence exhausted", func(_ *tree, _ *session, h *fileHandle) { h.ioSequence = math.MaxUint64 }, syscall.EOVERFLOW},
	} {
		t.Run(test.name, func(t *testing.T) {
			tree, session, handle := fileIOGateFixture()
			test.change(tree, session, handle)
			if _, err := tree.admitFileIO(t.Context(), session, handle.id, 1); !errors.Is(err, test.err) {
				t.Fatalf("admission error %v, want %v", err, test.err)
			}
			if tree.fileActive != 0 || len(handle.ioQueue) != 0 || handle.ioRunning != nil {
				t.Fatal("failed admission retained work")
			}
		})
	}
	tree, session, handle := fileIOGateFixture()
	if _, err := tree.admitFileIO(t.Context(), session, handle.id, 0); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unbounded queue limit: %v", err)
	}
	first, err := tree.admitFileIO(t.Context(), session, handle.id, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.admitFileIO(t.Context(), session, handle.id, 1); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("full queue admission: %v", err)
	}
	first.finish()
}

func TestFileIOGateCanceledGrantedTurnDoesNotHoldCloseLifetime(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	if err := handle.closeLifetime.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer handle.closeLifetime.unlock()
	first := admitGateTicket(t, tree, session, handle)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := first.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled granted turn: %v", err)
	}
	if tree.fileActive != 0 || handle.ioRunning != nil {
		t.Fatal("canceled granted turn retained work")
	}
}

type gateCloseFile struct {
	handleFileStub
	closed chan struct{}
}

func (f *gateCloseFile) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	close(f.closed)
	return f.handleFileStub.CloseWithResult(ctx)
}

func TestFileIOGateCloseCannotReachBackendUntilRunningIODrains(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	file := &gateCloseFile{closed: make(chan struct{}), handleFileStub: handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}}
	handle.file = file
	first := admitGateTicket(t, tree, session, handle)
	second := admitGateTicket(t, tree, session, handle)
	if err := first.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- tree.closeFileHandle(ctx, handle) }()
	if err := second.wait(ctx); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("CLOSE did not discard queued I/O: %v", err)
	}
	select {
	case <-file.closed:
		t.Fatal("backend closed before running I/O returned")
	default:
	}
	first.finish()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if tree.findFileHandle(handle.id) != nil {
		t.Fatal("settled CLOSE retained handle")
	}
}

type postqueryGateFile struct {
	gateCloseFile
	statCalled chan struct{}
	attr       storage.Attr
}

func (f *postqueryGateFile) Stat(context.Context) (storage.Attr, error) {
	close(f.statCalled)
	return f.attr, nil
}

func TestFileIOGateClosePostqueryCapturesAfterRunningIO(t *testing.T) {
	tree, session, handle := fileIOGateFixture()
	tree.export = &Export{share: Share{Volume: "gate-volume"}}
	stamp := time.Date(2026, 9, 23, 1, 2, 3, 0, time.UTC)
	file := &postqueryGateFile{
		gateCloseFile: gateCloseFile{closed: make(chan struct{}), handleFileStub: handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}},
		statCalled:    make(chan struct{}),
		attr:          storage.Attr{ID: handle.nodeID, Kind: storage.NodeRegular, Size: 1, AllocationKnown: true, AllocationSize: 4096, AccessTime: stamp, ModTime: stamp},
	}
	handle.file = file
	first := admitGateTicket(t, tree, session, handle)
	second := admitGateTicket(t, tree, session, handle)
	if err := first.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	server := &Server{config: Config{Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}}
	c := &connection{server: server}
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	binary.LittleEndian.PutUint16(body[2:], 1)
	copy(body[8:], handle.id[:])
	type result struct {
		body   []byte
		status uint32
	}
	finished := make(chan result, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		response, status := c.closeFile(ctx, session, tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{})
		finished <- result{response, status}
	}()
	if err := second.wait(ctx); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("CLOSE did not fence queued I/O: %v", err)
	}
	select {
	case <-file.statCalled:
		t.Fatal("postquery read attributes before running I/O drained")
	default:
	}
	file.attr.Size = 19
	first.finish()
	got := <-finished
	if got.status != statusOK || len(got.body) != 60 || binary.LittleEndian.Uint16(got.body[2:]) != 1 || binary.LittleEndian.Uint64(got.body[48:]) != 19 {
		t.Fatalf("CLOSE did not capture completed I/O size: status=%#x body=%v", got.status, got.body)
	}
}

func TestFileIOGateConcurrentCancellationCompletionAndFence(t *testing.T) {
	for iteration := 0; iteration < 64; iteration++ {
		tree, session, handle := fileIOGateFixture()
		first := admitGateTicket(t, tree, session, handle)
		second := admitGateTicket(t, tree, session, handle)
		third := admitGateTicket(t, tree, session, handle)
		if err := first.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var completed sync.WaitGroup
		completed.Add(3)
		for _, finish := range []func(){first.finish, second.finish, func() { tree.fenceFileWork() }} {
			go func() {
				defer completed.Done()
				<-start
				finish()
			}()
		}
		close(start)
		completed.Wait()
		third.finish()
		if tree.fileActive != 0 || handle.ioRunning != nil || len(handle.ioQueue) != 0 || !handle.ioFenced {
			t.Fatalf("concurrent fence/completion retained or double-released work at iteration %d", iteration)
		}
	}
}
