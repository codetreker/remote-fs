package httprest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type stagedCloseBackend struct {
	*objectstore.Storage
	alwaysFail bool
}

type uncertainSessionCloseBackend struct {
	*objectstore.Storage
	calls atomic.Int32
}

type racedEpochBackend struct {
	*objectstore.Storage
	epoch atomic.Uint64
}

type hiddenReleaseBackend struct{ *objectstore.Storage }

type mixedCloseBackend struct {
	*objectstore.Storage
	unknownNative bool
}

type rejectedCloseBackend struct{ *objectstore.Storage }

type rejectedCloseSession struct{ storage.FileSession }

type rejectedCloseFile struct {
	storage.File
	mu             sync.Mutex
	rejectedAction storage.FileActionID
	proofQueries   int
}

func (b rejectedCloseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &rejectedCloseSession{FileSession: native}, nil
}

func (*rejectedCloseSession) CheckRecoverableReferenceClose() error { return nil }

func (s *rejectedCloseSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	native, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &rejectedCloseFile{File: native}, nil
}

func (f *rejectedCloseFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	if f.rejectedAction == "" {
		f.rejectedAction = attempt.Action
	}
	rejected := f.rejectedAction == attempt.Action
	f.mu.Unlock()
	if rejected {
		return storage.ReferenceCloseResult{}, syscall.EINVAL
	}
	return f.File.(storage.ReferenceCloseActions).CloseWithAction(ctx, attempt)
}

func (f *rejectedCloseFile) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	f.mu.Lock()
	if f.rejectedAction != "" && f.rejectedAction == attempt.Action {
		f.proofQueries++
		if f.proofQueries == 1 {
			f.mu.Unlock()
			return storage.FileActionReceipt{}, syscall.EIO
		}
	}
	f.mu.Unlock()
	return f.File.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, attempt)
}

func (f *rejectedCloseFile) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	return f.File.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
}

type mixedCloseSession struct {
	storage.FileSession
	unknownNative bool
}

type mixedCloseFile struct {
	storage.File
	mu            sync.Mutex
	nativeAttempt storage.CloseAttempt
	explicitCalls int
	unknownNative bool
	released      bool
}

func (b mixedCloseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &mixedCloseSession{FileSession: native, unknownNative: b.unknownNative}, nil
}

func (*mixedCloseSession) CheckRecoverableReferenceClose() error { return nil }

func (s *mixedCloseSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	native, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &mixedCloseFile{File: native, unknownNative: s.unknownNative}, nil
}

func (f *mixedCloseFile) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	if f.released {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	if f.nativeAttempt.Action == "" {
		id, err := storage.NewFileActionID(1)
		if err != nil {
			f.mu.Unlock()
			return storage.ReferenceCloseResult{}, err
		}
		f.nativeAttempt = storage.CloseAttempt{Action: id, Generation: 1}
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	f.mu.Unlock()
	result, err := f.File.CloseWithResult(ctx)
	if result.Released {
		f.mu.Lock()
		f.released = true
		f.mu.Unlock()
	}
	return result, err
}

func (f *mixedCloseFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	if attempt != f.nativeAttempt {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, syscall.EINVAL
	}
	f.explicitCalls++
	if f.unknownNative && f.explicitCalls == 1 {
		f.mu.Unlock()
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	f.mu.Unlock()
	result, err := f.File.CloseWithResult(ctx)
	if result.Released {
		f.mu.Lock()
		f.released = true
		f.mu.Unlock()
	}
	return result, err
}

func (f *mixedCloseFile) QueryCloseAttempt(context.Context, storage.CloseAttempt) (storage.FileActionReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return storage.FileActionReceipt{Action: f.nativeAttempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionUnknown}, nil
}

func (f *mixedCloseFile) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	status := storage.CloseOwnerStatus{Released: f.released, NextGeneration: 1, CurrentEpoch: 1}
	if !f.released {
		if f.nativeAttempt.Action == "" {
			status.Ready = true
		} else {
			attempt := f.nativeAttempt
			status.Current = &attempt
			status.CurrentOutcome = storage.FileActionUnknown
		}
	}
	return status, status.Check()
}

type lyingRecoveryBackend struct{ *objectstore.Storage }

type lyingRecoverySession struct{ storage.FileSession }

type legacyReferenceView struct{ storage.File }

func (b lyingRecoveryBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &lyingRecoverySession{FileSession: native}, nil
}

func (*lyingRecoverySession) CheckRecoverableReferenceClose() error { return nil }

func (s *lyingRecoverySession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	native, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &legacyReferenceView{File: native}, nil
}

type hiddenReleaseSession struct{ storage.FileSession }

type hiddenReleaseFile struct {
	storage.File
	first atomic.Bool
}

func (b hiddenReleaseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &hiddenReleaseSession{FileSession: native}, nil
}

func (*hiddenReleaseSession) CheckRecoverableReferenceClose() error { return nil }

func (s *hiddenReleaseSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	native, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &hiddenReleaseFile{File: native}, nil
}

func (f *hiddenReleaseFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	result, err := f.File.(storage.ReferenceCloseActions).CloseWithAction(ctx, attempt)
	if f.first.CompareAndSwap(false, true) && result.Released {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	return result, err
}

func (f *hiddenReleaseFile) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	return f.File.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, attempt)
}

func (f *hiddenReleaseFile) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	return f.File.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
}

type racedEpochSession struct {
	storage.FileSession
	backend *racedEpochBackend
}

type racedEpochFile struct {
	storage.File
	backend *racedEpochBackend
}

func (b *racedEpochBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	b.epoch.Store(1)
	return &racedEpochSession{FileSession: native, backend: b}, nil
}

func (s *racedEpochSession) CheckRecoverableReferenceClose() error { return nil }

func (s *racedEpochSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := s.FileSession.Status(ctx)
	status.ActionEpoch = s.backend.epoch.Load()
	return status, err
}

func (s *racedEpochSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	native, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &racedEpochFile{File: native, backend: s.backend}, nil
}

func (f *racedEpochFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	if f.backend.epoch.CompareAndSwap(1, 2) {
		return storage.ReferenceCloseResult{}, &storage.CloseActionNotExecutedError{CurrentEpoch: 2}
	}
	return f.File.CloseWithResult(ctx)
}

func (f *racedEpochFile) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	epoch, err := attempt.Action.Epoch()
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	if epoch == f.backend.epoch.Load() {
		return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionNotExecuted}, nil
	}
	return f.File.(storage.ReferenceCloseActions).QueryCloseAttempt(ctx, attempt)
}

func (f *racedEpochFile) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	status, err := f.File.(storage.ReferenceCloseActions).CloseOwnerStatus(ctx)
	status.CurrentEpoch = f.backend.epoch.Load()
	return status, err
}

type uncertainSessionClose struct {
	storage.FileSession
	backend *uncertainSessionCloseBackend
}

func (b *uncertainSessionCloseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &uncertainSessionClose{FileSession: native, backend: b}, nil
}

func (s *uncertainSessionClose) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	if s.backend.calls.Add(1) == 1 {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	return s.FileSession.CloseWithResult(ctx)
}

func (b stagedCloseBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	native, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &stagedCloseSession{FileSession: native, alwaysFail: b.alwaysFail}, nil
}

type stagedCloseSession struct {
	storage.FileSession
	alwaysFail bool
}

func (*stagedCloseSession) CheckRecoverableReferenceClose() error { return nil }

func (s *stagedCloseSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	native, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return &stagedCloseFile{File: native, session: s.FileSession, calls: make(map[uint64]int), attempts: make(map[uint64]storage.CloseAttempt), alwaysFail: s.alwaysFail}, nil
}

type stagedCloseFile struct {
	storage.File
	session    storage.FileSession
	mu         sync.Mutex
	calls      map[uint64]int
	attempts   map[uint64]storage.CloseAttempt
	released   bool
	alwaysFail bool
}

func (f *stagedCloseFile) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	f.calls[attempt.Generation]++
	f.attempts[attempt.Generation] = attempt
	count := f.calls[attempt.Generation]
	f.mu.Unlock()
	if f.alwaysFail {
		return storage.ReferenceCloseResult{Determined: true}, syscall.EIO
	}
	if attempt.Generation == 1 {
		return storage.ReferenceCloseResult{Determined: true}, syscall.EIO
	}
	if attempt.Generation == 2 && count == 1 {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	result, err := f.File.CloseWithResult(ctx)
	if result.Released {
		f.mu.Lock()
		f.released = true
		f.mu.Unlock()
	}
	return result, err
}

func (f *stagedCloseFile) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	f.mu.Lock()
	recorded, found := f.attempts[attempt.Generation]
	count := f.calls[attempt.Generation]
	f.mu.Unlock()
	outcome := storage.FileActionNotExecuted
	if found && recorded == attempt {
		outcome = storage.FileActionCompleted
		if !f.alwaysFail && attempt.Generation == 2 && count == 1 {
			outcome = storage.FileActionUnknown
		}
	}
	return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: outcome}, nil
}

func (f *stagedCloseFile) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	f.mu.Lock()
	var highest uint64
	for generation := range f.calls {
		if generation > highest {
			highest = generation
		}
	}
	count := f.calls[highest]
	released := f.released
	f.mu.Unlock()
	nativeStatus, err := f.session.Status(ctx)
	if err != nil {
		return storage.CloseOwnerStatus{}, err
	}
	status := storage.CloseOwnerStatus{Released: released, Ready: !released, NextGeneration: highest + 1, CurrentEpoch: nativeStatus.ActionEpoch}
	if !f.alwaysFail && highest == 2 && count == 1 {
		status.NextGeneration = 2
	}
	return status, status.Check()
}

func TestHTTPHistoricalCloseReplayKeepsNewUnknownAttempt(t *testing.T) {
	_, native := memoryfixture.New(t, "http-close-generations", 1<<20, locking.DefaultOptions())
	if err := native.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(stagedCloseBackend{Storage: native}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := session.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose(); err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	firstID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	first := storage.CloseAttempt{Action: firstID, Generation: 1}
	second := storage.CloseAttempt{Action: secondID, Generation: 2}
	tooFar := storage.CloseAttempt{Action: secondID, Generation: 100}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), tooFar); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAction != "" || file.closeGeneration != 0 {
		t.Fatalf("first generation gap changed owner: result=%+v err=%v action=%s generation=%d", result, err, file.closeAction, file.closeGeneration)
	}
	result, _, err := file.CloseWithActionAndBarrier(t.Context(), first)
	if !result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("first close=%+v err=%v", result, err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), tooFar); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAction != storage.LockRequestID(firstID) || file.closeGeneration != 1 {
		t.Fatalf("post-failure generation gap changed owner: result=%+v err=%v action=%s generation=%d", result, err, file.closeAction, file.closeGeneration)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: firstID, Generation: 2}); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAction != storage.LockRequestID(firstID) || file.closeGeneration != 1 {
		t.Fatalf("reused action ID changed owner generation: result=%+v err=%v action=%s generation=%d", result, err, file.closeAction, file.closeGeneration)
	}
	result, _, err = file.CloseWithActionAndBarrier(t.Context(), second)
	if result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown second close=%+v err=%v", result, err)
	}
	owner, err := file.CloseOwnerStatus(t.Context())
	if err != nil || owner.Ready || owner.Current != nil {
		t.Fatalf("HTTP pending close was hidden by native readiness: owner=%+v err=%v", owner, err)
	}
	result, _, err = file.CloseWithActionAndBarrier(t.Context(), first)
	if !result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("historical replay=%+v err=%v", result, err)
	}
	if file.closeAction != storage.LockRequestID(secondID) || file.closeGeneration != 2 {
		t.Fatalf("historical replay replaced current close: action=%s generation=%d", file.closeAction, file.closeGeneration)
	}
	result, _, err = file.CloseWithActionAndBarrier(t.Context(), second)
	if !result.Released || !result.Determined || err != nil {
		t.Fatalf("same-ID resolution=%+v err=%v", result, err)
	}
}

func TestHTTPPriorCloseIDReuseKeepsCurrentGeneration(t *testing.T) {
	_, native := memoryfixture.New(t, "http-prior-close-id-reuse", 1<<20, locking.DefaultOptions())
	backend := stagedCloseBackend{Storage: native, alwaysFail: true}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	firstID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	first := storage.CloseAttempt{Action: firstID, Generation: 1}
	second := storage.CloseAttempt{Action: secondID, Generation: 2}
	for _, attempt := range []storage.CloseAttempt{first, second} {
		result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), attempt)
		if !result.Determined || result.Released || !errors.Is(closeErr, syscall.EIO) {
			t.Fatalf("determined close %d=%+v err=%v", attempt.Generation, result, closeErr)
		}
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	owner := served.files[file.id]
	owner.closeReserve++
	served.cleanupReserved++
	served.mu.Unlock()
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: firstID, Generation: 3}); result.Released || !errors.Is(err, syscall.EINVAL) || file.closeAction != storage.LockRequestID(secondID) || file.closeGeneration != 2 {
		t.Fatalf("prior ID reuse poisoned client cursor: result=%+v err=%v action=%s generation=%d", result, err, file.closeAction, file.closeGeneration)
	}
	thirdID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	third := storage.CloseAttempt{Action: thirdID, Generation: 3}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), third); !result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("valid third close was blocked: result=%+v err=%v", result, err)
	}
}

func TestHTTPFutureCloseIDsDoNotSaturateCleanupHistory(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-future-close-ids", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	options.MaxCloseActions = 3
	session, err := client.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	for range options.MaxCloseActions + 1 {
		future, idErr := storage.NewFileActionID(^uint64(0))
		if idErr != nil {
			t.Fatal(idErr)
		}
		response, closeErr := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileClose, Session: file.session.id, File: file.id, Action: storage.LockRequestID(future), CloseGeneration: 1})
		if response.CloseResult != nil || !errors.Is(closeErr, syscall.EINVAL) {
			t.Fatalf("future action result=%+v err=%v", response.CloseResult, closeErr)
		}
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	retainedIDs := len(served.closeIDs)
	owner := served.files[file.id]
	reserved := owner.closeReserve
	served.mu.Unlock()
	if retainedIDs != 0 || reserved != 2 {
		t.Fatalf("future IDs consumed cleanup history: ids=%d reserve=%d", retainedIDs, reserved)
	}
	status, err := file.CloseOwnerStatus(t.Context())
	if err != nil || !status.Ready {
		t.Fatalf("legitimate close became unavailable: status=%+v err=%v", status, err)
	}
	valid, err := storage.NewFileActionID(status.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: valid, Generation: status.NextGeneration}); err != nil || !result.Released {
		t.Fatalf("legitimate close was blocked: result=%+v err=%v", result, err)
	}
}

func TestHTTPNodeAndFileReferenceCloseActionsRoundTrip(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	openID, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.(storage.NodeReferences).OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID},
		Action: openID, Use: storage.UseClaim{Uses: storage.ReadData},
	})
	if err != nil || opened.Reference == nil {
		t.Fatalf("node reference open=%+v err=%v", opened, err)
	}
	reference := opened.Reference.(*remoteNodeReference)
	owner, err := reference.CloseOwnerStatus(t.Context())
	if err != nil || !owner.Ready || owner.NextGeneration != 1 {
		t.Fatalf("node close owner=%+v err=%v", owner, err)
	}
	closeID, err := storage.NewFileActionID(owner.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: closeID, Generation: owner.NextGeneration}
	receipt, err := reference.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Action != closeID || receipt.Operation != storage.OpFileClose || receipt.Outcome != storage.FileActionNotExecuted {
		t.Fatalf("fresh node close receipt=%+v err=%v", receipt, err)
	}
	result, barrier, err := reference.CloseWithActionAndBarrier(t.Context(), attempt)
	if err != nil || !result.Released || !result.Determined || barrier != nil {
		t.Fatalf("node reference close=%+v barrier=%+v err=%v", result, barrier, err)
	}
	receipt, err = reference.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Action != closeID || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("completed node close receipt=%+v err=%v", receipt, err)
	}
	result, err = reference.CloseWithAction(t.Context(), attempt)
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("same-ID node close replay=%+v err=%v", result, err)
	}
	fileValue, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := fileValue.(storage.ReferenceCloseActions)
	fileOwner, err := file.CloseOwnerStatus(t.Context())
	if err != nil || !fileOwner.Ready {
		t.Fatalf("file close owner=%+v err=%v", fileOwner, err)
	}
	fileID, err := storage.NewFileActionID(fileOwner.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	fileAttempt := storage.CloseAttempt{Action: fileID, Generation: fileOwner.NextGeneration}
	result, err = file.CloseWithAction(t.Context(), fileAttempt)
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("file reference close=%+v err=%v", result, err)
	}
}

func TestHTTPExpiredCloseProofsDoNotBlockValidCleanup(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-expired-close-proofs", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	options.MaxCloseActions = 3
	options.History = 750 * time.Millisecond
	session, err := client.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	initial, err := file.CloseOwnerStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, statusErr := file.CloseOwnerStatus(t.Context())
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if status.CurrentEpoch > initial.CurrentEpoch {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("native close epoch did not advance")
		}
	}
	for index := range options.MaxCloseActions {
		stale, idErr := storage.NewFileActionID(initial.CurrentEpoch)
		if idErr != nil {
			t.Fatal(idErr)
		}
		result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: stale, Generation: 1})
		var proof *storage.CloseActionNotExecutedError
		if result.Released || !errors.As(closeErr, &proof) {
			owner, ownerErr := file.CloseOwnerStatus(t.Context())
			receipt, queryErr := file.QueryCloseAttempt(t.Context(), storage.CloseAttempt{Action: stale, Generation: 1})
			t.Fatalf("stale action %d proof=%+v err=%v owner=%+v ownerErr=%v receipt=%+v queryErr=%v", index, result, closeErr, owner, ownerErr, receipt, queryErr)
		}
	}
	file.mu.Lock()
	proofCount := len(file.closeNotExecuted)
	if proofCount != options.MaxCloseActions {
		file.mu.Unlock()
		t.Fatalf("proof history did not reach configured cap: %d", proofCount)
	}
	for attempt, proof := range file.closeNotExecuted {
		proof.expires = time.Now().Add(-time.Second)
		file.closeNotExecuted[attempt] = proof
	}
	file.mu.Unlock()
	stale, err := storage.NewFileActionID(initial.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: stale, Generation: 1})
	var proof *storage.CloseActionNotExecutedError
	if result.Released || !errors.As(closeErr, &proof) {
		t.Fatalf("expired proof capacity blocked a stale miss: result=%+v err=%v", result, closeErr)
	}
	file.mu.Lock()
	retained := len(file.closeNotExecuted)
	file.mu.Unlock()
	if retained != 1 {
		t.Fatalf("expired proof history retained %d entries", retained)
	}
	status, err := file.CloseOwnerStatus(t.Context())
	if err != nil || !status.Ready {
		t.Fatalf("valid close not ready after proof reclamation: status=%+v err=%v", status, err)
	}
	valid, err := storage.NewFileActionID(status.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: valid, Generation: status.NextGeneration}); err != nil || !result.Released {
		t.Fatalf("valid close blocked after stale proofs: result=%+v err=%v", result, err)
	}
}

func TestHTTPRejectedCloseRecoversAfterTransientProofQueryFailure(t *testing.T) {
	_, native := memoryfixture.New(t, "http-transient-close-proof-query", 1<<20, locking.DefaultOptions())
	backend := rejectedCloseBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	firstID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	first := storage.CloseAttempt{Action: firstID, Generation: 1}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), first); result.Released || result.Determined || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("native pre-effect rejection=%+v err=%v", result, err)
	}
	if file.closeAction != storage.LockRequestID(firstID) || file.closePendingProof == nil {
		t.Fatalf("unknown rejection owner was lost: action=%s pending=%v", file.closeAction, file.closePendingProof != nil)
	}
	result, _, replayErr := file.CloseWithActionAndBarrier(t.Context(), first)
	var proof *storage.CloseActionNotExecutedError
	if result.Released || result.Determined || !errors.As(replayErr, &proof) || file.closeAction != "" || file.closePendingProof != nil {
		t.Fatalf("same-ID proof retry did not restore owner: result=%+v err=%v action=%s pending=%v", result, replayErr, file.closeAction, file.closePendingProof != nil)
	}
	secondID, err := storage.NewFileActionID(proof.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: secondID, Generation: 1}); err != nil || !result.Released || !result.Determined {
		t.Fatalf("new caller ID could not release owner: result=%+v err=%v", result, err)
	}
	_, replayErr = client.fileCall(t.Context(), fileRequest{Op: storage.OpFileClose, Session: file.session.id, File: file.id, Action: storage.LockRequestID(firstID), CloseGeneration: 1})
	if !errors.As(replayErr, &proof) {
		t.Fatalf("retained rejected action lost its typed replay after release: %v", replayErr)
	}
}

func TestHTTPUnacknowledgedAutoCloseRefundsReservedHistory(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-auto-close-reservation", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	limits := DefaultFileLimits()
	limits.MaxCleanupActions = 4
	options := DefaultHandlerOptions()
	options.Files = limits
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	enrolled, err := handler.files.enroll(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	handler.files.mu.Lock()
	session := handler.files.sessions[enrolled.Session]
	handler.files.mu.Unlock()
	for range 3 {
		id, err := storage.NewLockRequestID(session.epoch(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
		opened, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileOpen, Session: enrolled.Session, Action: id, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}}, sha256.Sum256([]byte(id)))
		if err != nil || opened.File == "" {
			t.Fatalf("open with reserved close history=%+v err=%v", opened, err)
		}
		session.mu.Lock()
		entry := session.files[opened.File]
		entry.pending = time.Now().Add(-time.Second)
		session.mu.Unlock()
		select {
		case handler.files.wake <- struct{}{}:
		default:
		}
		deadline := time.After(2 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		for {
			session.mu.Lock()
			_, present := session.files[opened.File]
			reserved := session.cleanupReserved
			session.mu.Unlock()
			if !present {
				if reserved != 2 {
					t.Fatalf("auto close left unused reservation: %d", reserved)
				}
				break
			}
			select {
			case <-ticker.C:
			case <-deadline:
				t.Fatal("unacknowledged reference was not closed")
			}
		}
		ticker.Stop()
	}
}

func TestHTTPCloseUsesCallerIDAcrossIndependentEpochs(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-close-epoch", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	oldID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	served.started = time.Now().Add(-served.options.History - time.Second)
	currentEpoch := served.epoch(time.Now())
	served.mu.Unlock()
	old := storage.CloseAttempt{Action: oldID, Generation: 1}
	if currentEpoch != file.session.epoch+1 {
		t.Fatalf("test did not create one-window HTTP skew: HTTP=%d client=%d", currentEpoch, file.session.epoch)
	}
	result, _, err := file.CloseWithActionAndBarrier(t.Context(), old)
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("caller ID changed or close failed=%+v err=%v", result, err)
	}
	receipt, err := file.QueryCloseAttempt(t.Context(), old)
	if err != nil || receipt.Action != old.Action || receipt.Operation != storage.OpFileClose || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("retained caller action=%+v err=%v", receipt, err)
	}
}

func TestHTTPFileCloseReceiptSurvivesSessionRetirement(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-file-close-terminal", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	id, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: id, Generation: 1}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), attempt); err != nil || !result.Released {
		t.Fatalf("file close=%+v err=%v", result, err)
	}
	if result, err := session.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("session close=%+v err=%v", result, err)
	}
	replayed, _, err := file.CloseWithActionAndBarrier(t.Context(), attempt)
	if err != nil || !replayed.Released || !replayed.Determined {
		t.Fatalf("terminal file close replay=%+v err=%v", replayed, err)
	}
	receipt, err := file.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Action != id || receipt.Operation != storage.OpFileClose || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("terminal file close receipt=%+v err=%v", receipt, err)
	}
	owner, err := file.CloseOwnerStatus(t.Context())
	if err != nil || !owner.Released || owner.Ready || owner.Current != nil {
		t.Fatalf("terminal owner release status=%+v err=%v", owner, err)
	}
}

func TestHTTPSessionCloseUnknownReplaysSameAction(t *testing.T) {
	_, native := memoryfixture.New(t, "http-unknown-session-close", 1<<20, locking.DefaultOptions())
	backend := &uncertainSessionCloseBackend{Storage: native}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	first, err := session.CloseWithResult(t.Context())
	if first.Determined || first.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown session close=%+v err=%v", first, err)
	}
	originalID := remote.closeAction
	second, err := session.CloseWithResult(t.Context())
	if err != nil || !second.Released || !second.Determined || remote.closeAction != originalID || backend.calls.Load() != 2 {
		t.Fatalf("same-ID session resolution=%+v err=%v action=%s calls=%d", second, err, remote.closeAction, backend.calls.Load())
	}
}

func TestHTTPUnclaimedCloseOwnerBoundsNewOpenAdmission(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-unclaimed-bound", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, err := client.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	remote.mu.Lock()
	remote.unclaimed = make(map[string]*remoteFile)
	remote.unclaimed["retained"] = &remoteFile{session: remote, id: "retained"}
	remote.mu.Unlock()
	if _, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("new open admitted with retained unclaimed owner: %v", err)
	}
	remote.mu.Lock()
	delete(remote.unclaimed, "retained")
	remote.mu.Unlock()
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatalf("open remained fenced after owner reconciliation: %v", err)
	}
	if result, err := opened.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("admitted reference close=%+v err=%v", result, err)
	}
}

func TestHTTPUnclaimedCloseRemintsOnlyBeforeAdmission(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-unclaimed-rollover", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	opened, err := remote.call(t.Context(), fileRequest{Op: storage.OpFileOpen, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}})
	if err != nil || opened.File == "" {
		t.Fatalf("unacknowledged open=%+v err=%v", opened, err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	served.started = time.Now().Add(-served.options.History - time.Second)
	served.mu.Unlock()
	if err := remote.closeUnclaimedFile(t.Context(), opened.File, opened.Epoch); err != nil {
		t.Fatalf("unclaimed close after HTTP epoch rollover: %v", err)
	}
	served.mu.Lock()
	_, retained := served.files[opened.File]
	closeActions := 0
	for id, action := range served.actions {
		if action.op == storage.OpFileClose {
			closeActions++
			idEpoch, idErr := id.Epoch()
			if idErr != nil || idEpoch != served.epoch(time.Now()) {
				served.mu.Unlock()
				t.Fatalf("admitted cleanup used stale HTTP epoch: id=%s epoch=%d err=%v", id, idEpoch, idErr)
			}
		}
	}
	served.mu.Unlock()
	remote.mu.Lock()
	unclaimed := len(remote.unclaimed)
	remote.mu.Unlock()
	if retained || closeActions != 1 || unclaimed != 0 {
		t.Fatalf("unclaimed cleanup state: retained=%v actions=%d clientOwners=%d", retained, closeActions, unclaimed)
	}
}

func TestHTTPNativeEpochMissReturnsBoundNonexecutionProof(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-native-epoch-proof", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.History = 750 * time.Millisecond
	session, err := client.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	oldID, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	initialEpoch := status.ActionEpoch
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(3 * time.Second)
	for {
		status, err = session.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if status.ActionEpoch > initialEpoch {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("native action epoch did not advance")
		}
	}
	old := storage.CloseAttempt{Action: oldID, Generation: 1}
	result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), old)
	var proof *storage.CloseActionNotExecutedError
	if result.Released || result.Determined || !errors.As(closeErr, &proof) || proof.CurrentEpoch != status.ActionEpoch {
		t.Fatalf("stale native close=%+v err=%v proof=%+v status=%+v", result, closeErr, proof, status)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	_, retained := served.files[file.id]
	_, admitted := served.actions[storage.LockRequestID(oldID)]
	served.mu.Unlock()
	if !retained || admitted {
		t.Fatalf("pre-effect epoch rejection changed owner: retained=%v admitted=%v", retained, admitted)
	}
	for try := 0; try < 4; try++ {
		newID, idErr := storage.NewFileActionID(proof.CurrentEpoch)
		if idErr != nil {
			t.Fatal(idErr)
		}
		result, _, err = file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: newID, Generation: 1})
		if err == nil && result.Released && result.Determined {
			break
		}
		if !errors.As(err, &proof) {
			t.Fatalf("caller-chosen replacement=%+v err=%v", result, err)
		}
	}
	if !result.Released || err != nil {
		t.Fatalf("caller-chosen replacement did not settle: %+v err=%v", result, err)
	}
	replayed, _, replayErr := file.CloseWithActionAndBarrier(t.Context(), old)
	if replayed.Released || replayed.Determined || !errors.As(replayErr, &proof) {
		t.Fatalf("old rejected ID changed after release: %+v err=%v", replayed, replayErr)
	}
}

func TestHTTPNativeRolloverDuringAdmissionRestoresCloseOwner(t *testing.T) {
	_, native := memoryfixture.New(t, "http-close-admission-rollover", 1<<20, locking.DefaultOptions())
	backend := &racedEpochBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	oldID, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	old := storage.CloseAttempt{Action: oldID, Generation: 1}
	result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), old)
	var proof *storage.CloseActionNotExecutedError
	if result.Released || result.Determined || !errors.As(closeErr, &proof) || proof.CurrentEpoch != 2 {
		t.Fatalf("rollover response=%+v err=%v", result, closeErr)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	owner := served.files[file.id]
	ownerAction := storage.LockRequestID("")
	var ownerGeneration uint64
	if owner != nil {
		ownerAction, ownerGeneration = owner.closeAction, owner.closeGeneration
	}
	_, oldJournal := served.actions[storage.LockRequestID(oldID)]
	use := served.closeIDs[storage.LockRequestID(oldID)]
	reserved := served.cleanupReserved
	served.mu.Unlock()
	if oldJournal || !use.notExecuted || owner == nil || ownerAction != "" || ownerGeneration != 0 || reserved != 4 {
		t.Fatalf("pre-effect rollback failed: journal=%v use=%+v ownerAction=%q ownerGeneration=%d reserved=%d", oldJournal, use, ownerAction, ownerGeneration, reserved)
	}
	receipt, err := file.QueryCloseAttempt(t.Context(), old)
	if err != nil || receipt.Outcome != storage.FileActionNotExecuted || receipt.Action != oldID {
		t.Fatalf("old action receipt=%+v err=%v", receipt, err)
	}
	newID, err := storage.NewFileActionID(proof.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	result, _, err = file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: newID, Generation: 1})
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("replacement close=%+v err=%v", result, err)
	}
}

func TestHTTPLocalCloseProofCapacityDoesNotLeakTypedProof(t *testing.T) {
	_, native := memoryfixture.New(t, "http-close-proof-cap", 1<<20, locking.DefaultOptions())
	backend := &racedEpochBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	remote.closeActionLimit = 0
	oldID, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: oldID, Generation: 1})
	var proof *storage.CloseActionNotExecutedError
	if result.Released || result.Determined || !errors.Is(closeErr, syscall.EAGAIN) || errors.As(closeErr, &proof) {
		t.Fatalf("full local proof set exposed unretained proof: result=%+v err=%v", result, closeErr)
	}
	newID, err := storage.NewFileActionID(2)
	if err != nil {
		t.Fatal(err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: newID, Generation: 1}); result.Released || !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("unproven replacement was admitted: result=%+v err=%v", result, err)
	}
}

func TestHTTPOldCloseIDCannotRebindAfterReceiptPrunes(t *testing.T) {
	_, native := memoryfixture.New(t, "http-close-id-reuse", 1<<20, locking.DefaultOptions())
	backend := stagedCloseBackend{Storage: native, alwaysFail: true}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.History = 750 * time.Millisecond
	session, err := client.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	oldID, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	initialEpoch := status.ActionEpoch
	first := storage.CloseAttempt{Action: oldID, Generation: 1}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), first); !result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("first retained close=%+v err=%v", result, err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(3 * time.Second)
	for status.ActionEpoch <= initialEpoch {
		status, err = session.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if status.ActionEpoch > initialEpoch {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("native epoch did not advance")
		}
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	served.actions[storage.LockRequestID(oldID)].expires = time.Now().Add(-time.Second)
	served.mu.Unlock()
	select {
	case handler.files.wake <- struct{}{}:
	default:
	}
	for {
		served.mu.Lock()
		_, retained := served.actions[storage.LockRequestID(oldID)]
		served.mu.Unlock()
		if !retained {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("old HTTP close receipt did not prune")
		}
	}
	secondID, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: secondID, Generation: 2}); !result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("second retained close=%+v err=%v", result, err)
	}
	third := storage.CloseAttempt{Action: oldID, Generation: 3}
	result, _, err := file.CloseWithActionAndBarrier(t.Context(), third)
	var proof *storage.CloseActionNotExecutedError
	if result.Released || result.Determined || !errors.As(err, &proof) {
		t.Fatalf("old ID reuse was accepted: result=%+v err=%v", result, err)
	}
	served.mu.Lock()
	_, rebound := served.actions[storage.LockRequestID(oldID)]
	served.mu.Unlock()
	if rebound {
		t.Fatal("old action ID rebound to a newer generation")
	}
}

func TestHTTPSessionRetirementReconcilesUnknownFileClose(t *testing.T) {
	_, native := memoryfixture.New(t, "http-terminal-unknown-file", 1<<20, locking.DefaultOptions())
	backend := hiddenReleaseBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	id, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: id, Generation: 1}
	result, _, err := file.CloseWithActionAndBarrier(t.Context(), attempt)
	if result.Released || result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("suppressed native release=%+v err=%v", result, err)
	}
	receipt, err := file.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Action != id || receipt.Operation != storage.OpFileClose || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("bound native close receipt=%+v err=%v", receipt, err)
	}
	unbound, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: file.session.id, FileAction: id})
	if err != nil || unbound.ActionReceipt == nil || unbound.ActionReceipt.Outcome != storage.FileActionUnknown {
		t.Fatalf("bound query changed the HTTP close result: receipt=%+v err=%v", unbound.ActionReceipt, err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	handler.files.closeRetiringSession(file.session.id, served, false)
	handler.files.mu.Lock()
	_, active := handler.files.sessions[file.session.id]
	terminal := handler.files.terminalCloses[file.session.id]
	handler.files.mu.Unlock()
	if active || terminal == nil {
		t.Fatalf("session retirement did not preserve terminal history: active=%v terminal=%v", active, terminal != nil)
	}
	result, _, err = file.CloseWithActionAndBarrier(t.Context(), attempt)
	if !result.Released || !result.Determined || err != nil {
		t.Fatalf("same-ID terminal reconciliation=%+v err=%v", result, err)
	}
	receipt, err = file.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Outcome != storage.FileActionCompleted || receipt.Action != id {
		t.Fatalf("terminal close receipt=%+v err=%v", receipt, err)
	}
}

func TestHTTPExplicitSessionCloseReconcilesUnknownFileClose(t *testing.T) {
	_, native := memoryfixture.New(t, "http-explicit-session-unknown-file", 1<<20, locking.DefaultOptions())
	backend := hiddenReleaseBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	id, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: id, Generation: 1}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), attempt); result.Released || result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown file close=%+v err=%v", result, err)
	}
	if result, err := session.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("explicit session close=%+v err=%v", result, err)
	}
	result, _, err := file.CloseWithActionAndBarrier(t.Context(), attempt)
	if !result.Released || !result.Determined || err != nil {
		t.Fatalf("file close remained unknown after explicit session release: %+v err=%v", result, err)
	}
	receipt, err := file.QueryCloseAttempt(t.Context(), attempt)
	if err != nil || receipt.Action != id || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("terminal action receipt=%+v err=%v", receipt, err)
	}
}

func TestHTTPSessionRetirementKeepsUnboundCloseUnknown(t *testing.T) {
	_, native := memoryfixture.New(t, "http-terminal-unbound-file", 1<<20, locking.DefaultOptions())
	backend := stagedCloseBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	firstID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), storage.CloseAttempt{Action: firstID, Generation: 1}); !result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("first retained close=%+v err=%v", result, err)
	}
	secondID, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	second := storage.CloseAttempt{Action: secondID, Generation: 2}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), second); result.Determined || result.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unbound close=%+v err=%v", result, err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[file.session.id]
	handler.files.mu.Unlock()
	handler.files.closeRetiringSession(file.session.id, served, false)
	replayed, _, err := file.CloseWithActionAndBarrier(t.Context(), second)
	if replayed.Determined || replayed.Released || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unbound action falsely settled after session release: %+v err=%v", replayed, err)
	}
}

func TestHTTPExplicitCloseRequiresRecoverableBackend(t *testing.T) {
	_, native := memoryfixture.New(t, "http-legacy-explicit-close", 1<<20, locking.DefaultOptions())
	backend := &failedFirstCloseBackend{Storage: native}
	backend.failed.Store(true)
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	fileValue, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := fileValue.(*remoteFile)
	id, err := storage.NewFileActionID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.CloseAttempt{Action: id, Generation: 1}
	if result, _, err := file.CloseWithActionAndBarrier(t.Context(), attempt); result.Released || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("client explicit close result=%+v err=%v", result, err)
	}
	if response, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileClose, Session: file.session.id, File: file.id, Action: storage.LockRequestID(id), CloseGeneration: 1}); response.CloseResult != nil || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("server explicit close result=%+v err=%v", response.CloseResult, err)
	}
	if result, err := file.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("legacy implicit close result=%+v err=%v", result, err)
	}
}

func TestHTTPImplicitUnknownAdoptsNativeCloseAttempt(t *testing.T) {
	for _, unknownNative := range []bool{false, true} {
		name := "released"
		if unknownNative {
			name = "unknown-then-released"
		}
		t.Run(name, func(t *testing.T) {
			_, native := memoryfixture.New(t, "http-mixed-close-"+name, 1<<20, locking.DefaultOptions())
			backend := mixedCloseBackend{Storage: native, unknownNative: unknownNative}
			if err := backend.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			handler, err := NewHandler(backend, nil)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			file := opened.(*remoteFile)
			first, err := file.CloseWithResult(t.Context())
			if first.Released || first.Determined || !errors.Is(err, syscall.EIO) {
				t.Fatalf("implicit transport close=%+v err=%v", first, err)
			}
			transportID := file.closeAction
			owner, err := file.CloseOwnerStatus(t.Context())
			if err != nil || owner.Current == nil || owner.Current.Action == storage.FileActionID(transportID) || owner.Ready {
				t.Fatalf("native owner was not exposed separately: %+v err=%v", owner, err)
			}
			result, _, closeErr := file.CloseWithActionAndBarrier(t.Context(), *owner.Current)
			if unknownNative {
				if result.Determined || result.Released || !errors.Is(closeErr, syscall.EIO) {
					t.Fatalf("first native adoption=%+v err=%v", result, closeErr)
				}
				result, _, closeErr = file.CloseWithActionAndBarrier(t.Context(), *owner.Current)
			}
			if closeErr != nil || !result.Released || !result.Determined {
				t.Fatalf("native adoption did not release: %+v err=%v", result, closeErr)
			}
			receipt, err := file.QueryCloseAttempt(t.Context(), storage.CloseAttempt{Action: storage.FileActionID(transportID), Generation: 1})
			if err != nil || receipt.Action != storage.FileActionID(transportID) || receipt.Outcome != storage.FileActionCompleted {
				t.Fatalf("implicit transport receipt did not reconcile: %+v err=%v", receipt, err)
			}
		})
	}
}

func TestHTTPImplicitUnknownReconcilesAtSessionClose(t *testing.T) {
	_, native := memoryfixture.New(t, "http-implicit-terminal-close", 1<<20, locking.DefaultOptions())
	backend := mixedCloseBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	if result, err := file.CloseWithResult(t.Context()); result.Released || result.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("implicit close=%+v err=%v", result, err)
	}
	transportID := file.closeAction
	if result, err := session.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("session release=%+v err=%v", result, err)
	}
	result, err := file.CloseWithResult(t.Context())
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("terminal implicit replay=%+v err=%v", result, err)
	}
	receipt, err := client.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: file.session.id, FileAction: storage.FileActionID(transportID)})
	if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("terminal implicit receipt=%+v err=%v", receipt.ActionReceipt, err)
	}
}

func TestHTTPLostImplicitCloseResponseReplaysAfterSessionRetirement(t *testing.T) {
	_, native := memoryfixture.New(t, "http-lost-implicit-terminal", 1<<20, locking.DefaultOptions())
	backend := mixedCloseBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	file := opened.(*remoteFile)
	id, err := storage.NewLockRequestID(file.session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	request := fileRequest{Op: storage.OpFileClose, Session: file.session.id, File: file.id, Action: id, CloseGeneration: 1, CloseImplicit: true}
	original := client.http.Transport
	var dropped atomic.Bool
	client.http.Transport = fileRoundTripFunc(func(httpRequest *http.Request) (*http.Response, error) {
		var command struct {
			Op storage.Operation `json:"op"`
		}
		if httpRequest.GetBody != nil {
			body, bodyErr := httpRequest.GetBody()
			if bodyErr != nil {
				return nil, bodyErr
			}
			decodeErr := json.NewDecoder(body).Decode(&command)
			_ = body.Close()
			if decodeErr != nil {
				return nil, decodeErr
			}
		}
		response, callErr := original.RoundTrip(httpRequest)
		if callErr != nil || command.Op != storage.OpFileClose || !dropped.CompareAndSwap(false, true) {
			return response, callErr
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, errors.New("lost implicit close response")
	})
	if response, err := client.fileCall(t.Context(), request); response.CloseResult != nil || err == nil || !dropped.Load() {
		t.Fatalf("implicit response loss was not observed: result=%+v err=%v dropped=%v", response.CloseResult, err, dropped.Load())
	}
	owner, err := file.CloseOwnerStatus(t.Context())
	if err != nil || owner.Current == nil || owner.Current.Action == storage.FileActionID(id) {
		t.Fatalf("native action was not distinct from lost transport action: owner=%+v err=%v", owner, err)
	}
	if result, err := session.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("session release=%+v err=%v", result, err)
	}
	replayed, err := client.fileCall(t.Context(), request)
	if err != nil || replayed.CloseResult == nil || !replayed.CloseResult.Released || !replayed.CloseResult.Determined {
		t.Fatalf("lost transport action did not settle at terminal: result=%+v err=%v", replayed.CloseResult, err)
	}
	receipt, err := file.QueryCloseAttempt(t.Context(), storage.CloseAttempt{Action: storage.FileActionID(id), Generation: 1})
	if err != nil || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("lost transport action receipt=%+v err=%v", receipt, err)
	}
}

func TestHTTPRecoverableSessionRejectsReferenceWithoutCloseActions(t *testing.T) {
	_, native := memoryfixture.New(t, "http-lying-close-capability", 1<<20, locking.DefaultOptions())
	backend := lyingRecoveryBackend{Storage: native}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := session.(storage.RecoverableReferenceClose).CheckRecoverableReferenceClose(); err != nil {
		t.Fatalf("session preflight did not advertise its claim: %v", err)
	}
	opened, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if opened != nil || !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("lying reference was published: file=%v err=%v", opened != nil, err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[session.(*remoteFileSession).id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	retained := len(served.files)
	served.mu.Unlock()
	if retained != 0 {
		t.Fatalf("failed open left %d server cleanup owners", retained)
	}
}

func TestHTTPCloseOwnerStatusWireRejectsMalformedResults(t *testing.T) {
	req := fileRequest{Op: storage.OpFileCloseOwnerStatus}
	valid := []byte(`{"epoch":1,"data":"","closeOwnerStatus":{"released":false,"ready":true,"currentOutcome":0,"nextGeneration":1,"currentEpoch":1}}`)
	var response fileResponse
	if err := decodeFileJSON(valid, &response); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(req, response); err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string]string{
		"missing generation":     `{"epoch":1,"data":"","closeOwnerStatus":{"released":false,"ready":true,"currentEpoch":1}}`,
		"current while ready":    `{"epoch":1,"data":"","closeOwnerStatus":{"released":false,"ready":true,"current":{"action":"1:0123456789abcdef0123456789abcdef","generation":1},"currentOutcome":3,"nextGeneration":1,"currentEpoch":1}}`,
		"unknown nested field":   `{"epoch":1,"data":"","closeOwnerStatus":{"released":false,"ready":true,"nextGeneration":1,"currentEpoch":1,"surprise":1}}`,
		"duplicate nested field": `{"epoch":1,"data":"","closeOwnerStatus":{"released":false,"ready":true,"ready":false,"nextGeneration":1,"currentEpoch":1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var decoded fileResponse
			if err := decodeFileJSON([]byte(payload), &decoded); err == nil {
				if err := validateFileResponse(req, decoded); err == nil {
					t.Fatal("accepted malformed close owner status")
				}
			}
		})
	}
}

func TestHTTPRetiredReferenceAllowsBoundCloseAndProjectsReady(t *testing.T) {
	_, native := memoryfixture.New(t, "http-retired-bound-close", 1<<20, locking.DefaultOptions())
	backend := stagedCloseBackend{Storage: native, alwaysFail: true}
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	options.MaxCloseActions = 5
	session, err := backend.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	capability := fileCapability()
	owner := &servedFile{native: file, closeReserve: 2}
	served := &servedFileSession{
		native: session, files: map[string]*servedFile{capability: owner}, actions: make(map[storage.LockRequestID]*servedFileAction),
		options: options, started: time.Now(), expires: time.Now().Add(options.Lease), retired: true,
		recoverable: true, sessionCloseReserve: 2, cleanupReserved: 4,
	}
	limits := DefaultFileLimits()
	handler := &Handler{files: &fileRegistry{limits: limits, sessions: map[string]*servedFileSession{"session": served}, terminalCloses: make(map[string]*terminalFileClose)}}
	statusRequest := fileRequest{Op: storage.OpFileCloseOwnerStatus, Session: "session", File: capability}
	statusResponse, err := handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil || statusResponse.CloseOwnerStatus == nil {
		t.Fatalf("retired owner status=%+v err=%v", statusResponse.CloseOwnerStatus, err)
	}
	status, err := statusResponse.CloseOwnerStatus.storage()
	if err != nil || !status.Ready || status.NextGeneration != 1 {
		t.Fatalf("retired owner not ready=%+v err=%v", status, err)
	}
	if _, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileStat, Session: "session", File: capability}, [32]byte{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired ordinary stat remained available: %v", err)
	}
	firstID, err := storage.NewLockRequestID(status.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	first := fileRequest{Op: storage.OpFileClose, Session: "session", File: capability, Action: firstID, CloseGeneration: 1}
	firstResult, firstErr := handler.fileCall(t.Context(), first, [32]byte{1})
	if firstResult.CloseResult == nil || !firstResult.CloseResult.Determined || firstResult.CloseResult.Released || !errors.Is(firstErr, syscall.EIO) {
		t.Fatalf("retired first close=%+v err=%v", firstResult.CloseResult, firstErr)
	}
	statusResponse, err = handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	status, err = statusResponse.CloseOwnerStatus.storage()
	if err != nil || !status.Ready || status.NextGeneration != 2 {
		t.Fatalf("retired second attempt not ready=%+v err=%v", status, err)
	}
	secondID, err := storage.NewLockRequestID(status.CurrentEpoch)
	if err != nil {
		t.Fatal(err)
	}
	second := fileRequest{Op: storage.OpFileClose, Session: "session", File: capability, Action: secondID, CloseGeneration: 2}
	secondResult, secondErr := handler.fileCall(t.Context(), second, [32]byte{2})
	if secondResult.CloseResult == nil || !secondResult.CloseResult.Determined || secondResult.CloseResult.Released || !errors.Is(secondErr, syscall.EIO) {
		t.Fatalf("retired second close=%+v err=%v", secondResult.CloseResult, secondErr)
	}
	statusResponse, err = handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	status, err = statusResponse.CloseOwnerStatus.storage()
	if err != nil || status.Ready || status.NextGeneration != 3 {
		t.Fatalf("HTTP receipt fullness was hidden=%+v err=%v", status, err)
	}
	served.mu.Lock()
	delete(served.actions, firstID)
	served.cleanupActions--
	served.cleanupReserved++
	owner.closeReserve++
	served.mu.Unlock()
	statusResponse, err = handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	status, err = statusResponse.CloseOwnerStatus.storage()
	if err != nil || !status.Ready {
		t.Fatalf("released HTTP receipt slot not observable=%+v err=%v", status, err)
	}
	served.mu.Lock()
	filledIDs := make([]storage.LockRequestID, 0, options.MaxCloseActions)
	for len(served.closeIDs) < options.MaxCloseActions {
		id, idErr := storage.NewLockRequestID(status.CurrentEpoch)
		if idErr != nil {
			served.mu.Unlock()
			t.Fatal(idErr)
		}
		served.closeIDs[id] = closeIDUse{owner: capability, generation: 3}
		filledIDs = append(filledIDs, id)
	}
	served.mu.Unlock()
	statusResponse, err = handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	status, err = statusResponse.CloseOwnerStatus.storage()
	if err != nil || status.Ready {
		t.Fatalf("global close ID saturation claimed admission=%+v err=%v", status, err)
	}
	served.mu.Lock()
	for _, id := range filledIDs {
		delete(served.closeIDs, id)
	}
	served.mu.Unlock()
	served.mu.Lock()
	served.autoClose = true
	served.mu.Unlock()
	statusResponse, err = handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	status, err = statusResponse.CloseOwnerStatus.storage()
	if err != nil || status.Ready {
		t.Fatalf("session close transition claimed new admission=%+v err=%v", status, err)
	}
	served.mu.Lock()
	served.autoClose = false
	served.explicitClose = true
	served.mu.Unlock()
	statusResponse, err = handler.fileCall(t.Context(), statusRequest, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	status, err = statusResponse.CloseOwnerStatus.storage()
	if err != nil || status.Ready {
		t.Fatalf("explicit session close transition claimed new admission=%+v err=%v", status, err)
	}
}
