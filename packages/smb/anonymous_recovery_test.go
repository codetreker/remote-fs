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

type anonymousParentSession struct {
	*endpointFileSession
	closeFn   func(context.Context) (storage.ReferenceCloseResult, error)
	closes    atomic.Int32
	queries   atomic.Int32
	queryErr  error
	certified atomic.Bool
}

func (s *anonymousParentSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.closes.Add(1)
	result, err := s.closeFn(ctx)
	var settlement *storage.CloseSettlementError
	if result.Released && result.Check(err) == nil && !errors.As(err, &settlement) {
		s.certified.Store(true)
	}
	return result, err
}

func (s *anonymousParentSession) QueryFileAction(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
	s.queries.Add(1)
	return storage.FileActionReceipt{}, s.queryErr
}

type anonymousRecoveryFixture struct {
	server     *Server
	export     *Export
	connection *connection
	session    *session
	authority  *authoritySession
	raw        *anonymousParentSession
	trees      []*tree
}

func newAnonymousRecoveryFixture(t *testing.T, treeCount int) *anonymousRecoveryFixture {
	t.Helper()
	server := &Server{config: Config{Limits: DefaultLimits()}}
	export := &Export{server: server, refs: treeCount + 1, trees: treeCount}
	s := &session{id: 52, trees: make(map[uint32]*tree), authorities: make(map[*Export]*authoritySession)}
	c := &connection{server: server, sessions: map[uint64]*session{s.id: s}, pending: make(map[uint64]*pendingRequest)}
	raw := &anonymousParentSession{
		endpointFileSession: newEndpointFileSession(),
		queryErr:            errors.Join(syscall.EIO, syscall.ESTALE),
		closeFn: func(context.Context) (storage.ReferenceCloseResult, error) {
			return storage.ReferenceCloseResult{Released: true}, nil
		},
	}
	done, ready := make(chan struct{}), make(chan struct{})
	close(done)
	close(ready)
	a := &authoritySession{raw: raw, refs: treeCount, orphan: true, export: export, connection: c, smbSession: s, done: done, ready: ready}
	s.authorities[export] = a
	f := &anonymousRecoveryFixture{server: server, export: export, connection: c, session: s, authority: a, raw: raw}
	for i := range treeCount {
		tree := &tree{id: uint32(i + 1), sessionID: s.id, kind: volumeTree, export: export, authority: a, done: make(chan struct{})}
		s.trees[tree.id] = tree
		f.trees = append(f.trees, tree)
	}
	return f
}

func (f *anonymousRecoveryFixture) unknownOpen(t *testing.T, tree *tree) *fileHandle {
	t.Helper()
	if !tree.beginFileWork(f.session) {
		t.Fatal("CREATE admission failed")
	}
	defer tree.endFileWork()
	handle, err := tree.reserveFileHandle(f.session, 1)
	if err != nil {
		t.Fatal(err)
	}
	handle.action, err = storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	handle.pendingOpen = f.connection.pendingOpenRecovery(tree, handle, storage.OpFileOpenAt, func(context.Context) (storage.Attr, storage.OpenOutcome, error) {
		t.Error("expired CREATE action was replayed")
		return storage.Attr{}, 0, syscall.EIO
	})
	return handle
}

func (f *anonymousRecoveryFixture) assertRetired(t *testing.T, wantCalls int32, handles ...*fileHandle) {
	t.Helper()
	if !f.authority.isClosed() || f.raw.closes.Load() != wantCalls {
		t.Fatalf("parent close certificate: closed=%v calls=%d", f.authority.isClosed(), f.raw.closes.Load())
	}
	f.authority.mu.Lock()
	refs := f.authority.refs
	f.authority.mu.Unlock()
	f.server.mu.Lock()
	exportRefs, exportTrees := f.export.refs, f.export.trees
	f.server.mu.Unlock()
	f.server.handleMu.Lock()
	owners := len(f.server.handleOwners)
	f.server.handleMu.Unlock()
	f.session.mu.Lock()
	trees, authorities := len(f.session.trees), len(f.session.authorities)
	f.session.mu.Unlock()
	if refs != 0 || exportRefs != 0 || exportTrees != 0 || owners != 0 || trees != 0 || authorities != 0 {
		t.Fatalf("retained charges: authority=%d export=%d trees=%d owners=%d sessionTrees=%d authorities=%d", refs, exportRefs, exportTrees, owners, trees, authorities)
	}
	for _, handle := range handles {
		if handle.pendingOpen != nil || !handle.released {
			t.Fatal("anonymous CREATE retained its recovery callback after parent release")
		}
	}
}

func TestAnonymousCreateExpiryRequiresParentReleaseCertificate(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 1)
	handle := f.unknownOpen(t, f.trees[0])
	if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.EIO) || !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired action error was lost: %v", err)
	}
	if f.raw.queries.Load() < 1 {
		t.Fatalf("expired CREATE queries = %d", f.raw.queries.Load())
	}
	f.assertRetired(t, 1, handle)
}

func TestAnonymousCreateLiveSiblingBlocksParentRetirement(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 2)
	left, right := f.trees[0], f.trees[1]
	handle := f.unknownOpen(t, left)
	if err := f.connection.closeTreeContext(t.Context(), left); !errors.Is(err, syscall.EIO) {
		t.Fatalf("TREE_DISCONNECT unknown result: %v", err)
	}
	if f.raw.closes.Load() != 0 || left.closed || left.findFileHandle(handle.id) != handle {
		t.Fatal("TREE_DISCONNECT surrendered an unknown owner while a sibling remained live")
	}
	if !right.beginFileWork(f.session) {
		t.Fatal("live sibling was fenced by another tree's unknown CREATE")
	}
	right.endFileWork()
	f.connection.fenceTree(right)
	if err := f.connection.closeTreeContext(t.Context(), left); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("all-stopping shared authority result: %v", err)
	}
	if f.raw.closes.Load() != 1 || !left.closed || right.closed || f.authority.refs != 1 || left.findFileHandle(handle.id) != nil {
		t.Fatal("all-stopping shared authority did not certify its unknown owner before decrementing tree references")
	}
	if err := f.connection.closeOwnedExport(t.Context(), f.export); err != nil {
		t.Fatalf("remaining export retirement: %v", err)
	}
	f.assertRetired(t, 1, handle)
}

func TestAnonymousCreateIncompleteMembershipCannotCertifyParent(t *testing.T) {
	for _, pendingEnrollment := range []bool{false, true} {
		name := "missing tree membership"
		if pendingEnrollment {
			name = "pending tree enrollment"
		}
		t.Run(name, func(t *testing.T) {
			f := newAnonymousRecoveryFixture(t, 1)
			tree := f.trees[0]
			handle := f.unknownOpen(t, tree)
			if pendingEnrollment {
				f.session.openingTrees = 1
			} else {
				delete(f.session.trees, tree.id)
			}
			if err := f.connection.closeTreeContext(t.Context(), tree); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("incomplete membership cleanup: %v", err)
			}
			if f.raw.closes.Load() != 0 || tree.closed || tree.findFileHandle(handle.id) != handle || len(f.server.handleOwners) != 1 {
				t.Fatal("incomplete membership surrendered an unknown CREATE through parent closure")
			}
			f.session.openingTrees = 0
			f.session.trees[tree.id] = tree
			if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("complete membership retry: %v", err)
			}
			f.assertRetired(t, 1, handle)
		})
	}
}

func TestAnonymousCreateRetainsChargesWithoutSettledParentRelease(t *testing.T) {
	cases := []struct {
		name   string
		result storage.ReferenceCloseResult
		err    error
	}{
		{name: "unreleased", result: storage.ReferenceCloseResult{Determined: true}, err: syscall.EBUSY},
		{name: "unknown", err: syscall.EIO},
		{name: "pending", result: storage.ReferenceCloseResult{Released: true}, err: &storage.CloseSettlementError{State: storage.CloseSettlementPending, Cause: syscall.EAGAIN}},
		{name: "settlement unknown", result: storage.ReferenceCloseResult{Released: true}, err: &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, Cause: syscall.EIO}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAnonymousRecoveryFixture(t, 1)
			tree := f.trees[0]
			handle := f.unknownOpen(t, tree)
			f.raw.closeFn = func(context.Context) (storage.ReferenceCloseResult, error) { return tc.result, tc.err }
			if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ESTALE) || !errors.Is(err, tc.err) {
				t.Fatalf("unsettled parent result lost: %v", err)
			}
			if f.authority.isClosed() || tree.closed || tree.findFileHandle(handle.id) != handle || handle.pendingOpen == nil || handle.released {
				t.Fatal("unsettled parent result discharged the anonymous owner")
			}
			if f.raw.closes.Load() != 1 || f.authority.refs != 1 || f.export.refs != 2 || f.export.trees != 1 || len(f.server.handleOwners) != 1 || len(f.session.authorities) != 1 || len(f.session.trees) != 1 {
				t.Fatal("unsettled parent result lost retry capacity or charged a second tree")
			}
			f.raw.closeFn = func(context.Context) (storage.ReferenceCloseResult, error) {
				return storage.ReferenceCloseResult{Released: true}, nil
			}
			if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("successful parent retry hid the original unknown action: %v", err)
			}
			f.assertRetired(t, 2, handle)
		})
	}
}

func TestAnonymousCreateSettledParentSemanticErrorCompletesRetirement(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 1)
	handle := f.unknownOpen(t, f.trees[0])
	f.raw.closeFn = func(context.Context) (storage.ReferenceCloseResult, error) {
		return storage.ReferenceCloseResult{Released: true}, syscall.ENOTEMPTY
	}
	if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ESTALE) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("semantic or unknown error lost after confirmed release: %v", err)
	}
	f.assertRetired(t, 1, handle)
	if err := f.connection.closeOwnedExport(t.Context(), f.export); err != nil || f.raw.closes.Load() != 1 {
		t.Fatalf("settled parent was retried after bookkeeping retirement: %v", err)
	}
}

func TestAnonymousCreateParentSettlementPreservesKnownSemanticError(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 1)
	handle := f.unknownOpen(t, f.trees[0])
	f.raw.closeFn = func(context.Context) (storage.ReferenceCloseResult, error) {
		return storage.ReferenceCloseResult{Released: true}, &storage.CloseSettlementError{
			State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EAGAIN,
		}
	}
	if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ESTALE) || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("pending parent settlement: %v", err)
	}
	if f.authority.isClosed() || f.trees[0].findFileHandle(handle.id) != handle {
		t.Fatal("pending parent settlement surrendered its anonymous owner")
	}
	f.raw.closeFn = func(context.Context) (storage.ReferenceCloseResult, error) {
		return storage.ReferenceCloseResult{Released: true}, nil
	}
	if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ESTALE) || !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("confirmed settlement did not preserve semantic and unknown outcomes: %v", err)
	}
	f.assertRetired(t, 2, handle)
}

func TestAnonymousCreateParentCertificateWaitsForSiblingFileWork(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 2)
	handle := f.unknownOpen(t, f.trees[0])
	sibling := f.trees[1]
	if !sibling.beginFileWork(f.session) {
		t.Fatal("sibling operation admission failed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- f.connection.closeOwnedExport(ctx, f.export) }()
	select {
	case <-sibling.done:
	case <-ctx.Done():
		sibling.endFileWork()
		t.Fatal(ctx.Err())
	}
	if f.raw.closes.Load() != 0 {
		sibling.endFileWork()
		t.Fatal("parent release overtook admitted sibling work")
	}
	select {
	case err := <-finished:
		sibling.endFileWork()
		t.Fatalf("export cleanup completed while sibling work remained: %v", err)
	default:
	}
	sibling.endFileWork()
	if err := <-finished; !errors.Is(err, syscall.EIO) {
		t.Fatalf("drained cleanup result: %v", err)
	}
	f.assertRetired(t, 1, handle)
}

func TestAnonymousCreateParentCertificateWaitsForRecoveryCallback(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 1)
	tree := f.trees[0]
	handle := f.unknownOpen(t, tree)
	original := handle.pendingOpen
	entered, release := make(chan struct{}), make(chan struct{})
	var callbacks atomic.Int32
	handle.pendingOpen = func(ctx context.Context) (bool, error) {
		if callbacks.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		if f.raw.certified.Load() {
			t.Error("CREATE recovery callback ran after full parent release")
		}
		return original(ctx)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	directDone := make(chan error, 1)
	go func() { directDone <- tree.closeFileHandle(ctx, handle) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	exportDone := make(chan error, 1)
	go func() { exportDone <- f.connection.closeOwnedExport(ctx, f.export) }()
	select {
	case <-tree.done:
	case <-ctx.Done():
		close(release)
		t.Fatal(ctx.Err())
	}
	if f.raw.closes.Load() != 0 {
		close(release)
		t.Fatal("parent certificate overtook an active CREATE recovery callback")
	}
	close(release)
	if err := <-directDone; !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("direct recovery result: %v", err)
	}
	if err := <-exportDone; !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("export recovery result: %v", err)
	}
	f.assertRetired(t, 1, handle)
}

func TestAnonymousCreateConcurrentCloseCannotReplayAfterParentCertificate(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 1)
	tree := f.trees[0]
	handle := f.unknownOpen(t, tree)
	parentEntered, parentRelease := make(chan struct{}), make(chan struct{})
	f.raw.closeFn = func(ctx context.Context) (storage.ReferenceCloseResult, error) {
		close(parentEntered)
		select {
		case <-parentRelease:
			return storage.ReferenceCloseResult{Released: true}, nil
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	exportDone := make(chan error, 1)
	go func() { exportDone <- f.connection.closeOwnedExport(ctx, f.export) }()
	select {
	case <-parentEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queriesBefore := f.raw.queries.Load()
	directStarted, directDone := make(chan struct{}), make(chan error, 1)
	go func() {
		close(directStarted)
		directDone <- tree.closeFileHandle(ctx, handle)
	}()
	<-directStarted
	select {
	case err := <-directDone:
		close(parentRelease)
		t.Fatalf("concurrent handle cleanup crossed in-progress parent closure: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if f.raw.queries.Load() != queriesBefore {
		close(parentRelease)
		t.Fatal("concurrent handle cleanup queried a parent whose closure was in progress")
	}
	close(parentRelease)
	if err := <-exportDone; !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("export retirement result: %v", err)
	}
	if err := <-directDone; !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("concurrent cleanup lost the terminal unknown result: %v", err)
	}
	if f.raw.queries.Load() != queriesBefore {
		t.Fatal("concurrent handle cleanup replayed after parent closure")
	}
	f.assertRetired(t, 1, handle)
}

func TestRetainedCloseParentCertificatePreservesBarrierSemanticError(t *testing.T) {
	f := newAnonymousRecoveryFixture(t, 1)
	tree := f.trees[0]
	if !tree.beginFileWork(f.session) {
		t.Fatal("file admission failed")
	}
	handle, err := tree.reserveFileHandle(f.session, 1)
	tree.endFileWork()
	if err != nil {
		t.Fatal(err)
	}
	closes := 0
	var original storage.CloseAttempt
	file := &ownerActionFile{
		status: func() (storage.CloseOwnerStatus, error) {
			return storage.CloseOwnerStatus{Ready: true, NextGeneration: 1, CurrentEpoch: 1}, nil
		},
		query: func(storage.CloseAttempt) (storage.FileActionReceipt, error) {
			return storage.FileActionReceipt{}, syscall.ESTALE
		},
		close: func(attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
			closes++
			if closes == 1 {
				original = attempt
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &storage.CloseSettlementError{
					State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EAGAIN,
				}
			}
			if attempt != original {
				t.Error("barrier recovery changed the original close attempt")
			}
			return storage.ReferenceCloseResult{}, syscall.ESTALE
		},
	}
	handle.file, handle.published = file, true
	handle.state.Store(uint32(handleLive))
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("initial barrier close: %v", err)
	}
	if tree.findFileHandle(handle.id) != handle || handleState(handle.state.Load()) != handleBarrierOnly {
		t.Fatal("pending barrier surrendered its owner")
	}
	if err := f.connection.closeOwnedExport(t.Context(), f.export); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("parent certificate discarded the barrier semantic error: %v", err)
	}
	f.assertRetired(t, 1, handle)
	if closes < 2 {
		t.Fatal("parent certificate skipped exact retained close recovery")
	}
	if err := tree.closeFileHandle(t.Context(), handle); !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("late cleanup lost the terminal semantic error: %v", err)
	}
}
