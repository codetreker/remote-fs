package httprest

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type concurrentCloseReference struct {
	storage.File
	attempt  storage.CloseAttempt
	implicit func(context.Context) (storage.ReferenceCloseResult, error)
	explicit func(context.Context) (storage.ReferenceCloseResult, error)
	released atomic.Bool
	effects  atomic.Int32
}

func (f *concurrentCloseReference) release() (storage.ReferenceCloseResult, error) {
	if f.released.CompareAndSwap(false, true) {
		f.effects.Add(1)
	}
	return storage.ReferenceCloseResult{Released: true, Determined: true}, syscall.ENOTEMPTY
}

func (f *concurrentCloseReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	return f.implicit(ctx)
}

func (f *concurrentCloseReference) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	if attempt != f.attempt {
		return storage.ReferenceCloseResult{}, syscall.EINVAL
	}
	return f.explicit(ctx)
}

func (f *concurrentCloseReference) QueryCloseAttempt(_ context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if attempt != f.attempt {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	outcome := storage.FileActionUnknown
	if f.released.Load() {
		outcome = storage.FileActionCompleted
	}
	return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: outcome}, nil
}

func (f *concurrentCloseReference) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	status := storage.CloseOwnerStatus{Released: f.released.Load(), NextGeneration: 1, CurrentEpoch: 1}
	if !status.Released {
		attempt := f.attempt
		status.Current, status.CurrentOutcome = &attempt, storage.FileActionUnknown
	}
	return status, status.Check()
}

type concurrentCloseSession struct {
	storage.FileSession
	file    *concurrentCloseReference
	closing func(context.Context)
	err     error
}

func (s concurrentCloseSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	if s.closing != nil {
		s.closing(ctx)
	}
	s.file.release()
	return storage.ReferenceCloseResult{Released: true, Determined: true}, s.err
}

type concurrentCloseLog struct {
	metastore.Log
	fail atomic.Bool
}

func (l *concurrentCloseLog) Barrier(context.Context, int64) (metastore.LogBarrier, error) {
	if l.fail.Load() {
		return metastore.LogBarrier{}, syscall.EIO
	}
	return metastore.LogBarrier{Incarnation: "close-history", Position: 1}, nil
}

type concurrentCloseCall struct {
	response fileResponse
	err      error
}

type transientCloseQueryReference struct {
	*concurrentCloseReference
	queries atomic.Int32
}

type recordedCloseResult struct {
	result      storage.ReferenceCloseResult
	err         error
	lostReplies int32
	calls       atomic.Int32
}

type recordedCloseReference struct {
	*concurrentCloseReference
	results map[storage.CloseAttempt]*recordedCloseResult
}

func (f *recordedCloseReference) CloseWithAction(_ context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	recorded := f.results[attempt]
	if recorded == nil {
		return storage.ReferenceCloseResult{}, syscall.EINVAL
	}
	calls := recorded.calls.Add(1)
	if calls == 1 && recorded.result.Released {
		f.release()
	}
	if calls <= recorded.lostReplies {
		return storage.ReferenceCloseResult{}, syscall.EIO
	}
	return recorded.result, recorded.err
}

func (f *recordedCloseReference) QueryCloseAttempt(_ context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	recorded := f.results[attempt]
	if recorded == nil {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	outcome := storage.FileActionNotExecuted
	if recorded.calls.Load() > 0 {
		outcome = storage.FileActionCompleted
	}
	return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: outcome}, nil
}

func (f *recordedCloseReference) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	status := storage.CloseOwnerStatus{Released: f.released.Load(), NextGeneration: 1, CurrentEpoch: 1}
	status.Ready = !status.Released
	for attempt, recorded := range f.results {
		if recorded.calls.Load() > 0 {
			status.NextGeneration = max(status.NextGeneration, attempt.Generation+1)
		}
	}
	return status, status.Check()
}

type recordedCloseSession struct {
	storage.FileSession
	file    *recordedCloseReference
	cleanup storage.CloseAttempt
	calls   atomic.Int32
}

func (s *recordedCloseSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.calls.Add(1)
	if s.file.released.Load() {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	return s.file.CloseWithAction(ctx, s.cleanup)
}

func (f *transientCloseQueryReference) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	if f.queries.Add(1) == 1 {
		return storage.FileActionReceipt{}, syscall.EIO
	}
	return f.concurrentCloseReference.QueryCloseAttempt(ctx, attempt)
}

func concurrentCloseFixture(t *testing.T) (*Handler, *servedFileSession, *concurrentCloseReference, *concurrentCloseLog, fileRequest, fileRequest) {
	t.Helper()
	implicitID, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	nativeID, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	file := &concurrentCloseReference{attempt: storage.CloseAttempt{Action: nativeID, Generation: 1}}
	options := storage.DefaultFileSessionOptions()
	session := &servedFileSession{
		native: concurrentCloseSession{file: file}, recoverable: true,
		files:    map[string]*servedFile{"file": {native: file, closeReserve: 2}},
		actions:  make(map[storage.LockRequestID]*servedFileAction),
		closeIDs: make(map[storage.LockRequestID]closeIDUse),
		options:  options, started: time.Now(), expires: time.Now().Add(options.Lease),
		cleanupReserved: 4, sessionCloseReserve: 2,
	}
	log := &concurrentCloseLog{}
	handler := &Handler{
		log: log, maxIncarnationBytes: 1024, stopping: make(chan struct{}),
		files: &fileRegistry{
			limits: DefaultFileLimits(), sessions: map[string]*servedFileSession{"session": session},
			terminalCloses: make(map[string]*terminalFileClose), wake: make(chan struct{}, 1),
		},
	}
	implicit := fileRequest{Op: storage.OpFileClose, Session: "session", File: "file", Action: implicitID, CloseGeneration: 1, CloseImplicit: true}
	explicit := fileRequest{Op: storage.OpFileClose, Session: "session", File: "file", Action: storage.LockRequestID(nativeID), CloseGeneration: 1}
	return handler, session, file, log, implicit, explicit
}

func awaitConcurrentClose(t *testing.T, calls <-chan concurrentCloseCall) concurrentCloseCall {
	t.Helper()
	select {
	case result := <-calls:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("close request did not finish")
		return concurrentCloseCall{}
	}
}

func assertConcurrentCloseRecovered(t *testing.T, handler *Handler, request fileRequest, digest [32]byte) {
	t.Helper()
	response, err := handler.fileCall(t.Context(), request, digest)
	if response.CloseResult == nil || !response.CloseResult.Released || !response.CloseResult.Determined || response.CloseResult.BarrierPending || response.Barrier == nil || !errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EIO) {
		t.Fatalf("same-ID recovery=%+v barrier=%+v err=%v", response.CloseResult, response.Barrier, err)
	}
	receipt, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: request.Session, File: request.File, FileAction: storage.FileActionID(request.Action), CloseGeneration: request.CloseGeneration}, [32]byte{})
	if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("recovered receipt=%+v err=%v", receipt.ActionReceipt, err)
	}
}

func TestHTTPConcurrentImplicitAndAdoptedCloseReplays(t *testing.T) {
	handler, session, file, log, implicit, explicit := concurrentCloseFixture(t)
	implicitEntered, explicitEntered := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var implicitCalls, explicitCalls atomic.Int32
	file.implicit = func(ctx context.Context) (storage.ReferenceCloseResult, error) {
		if implicitCalls.Add(1) == 1 {
			return storage.ReferenceCloseResult{}, syscall.EIO
		}
		close(implicitEntered)
		select {
		case <-release:
			return file.release()
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
	}
	file.explicit = func(ctx context.Context) (storage.ReferenceCloseResult, error) {
		if explicitCalls.Add(1) == 1 {
			return storage.ReferenceCloseResult{}, syscall.EIO
		}
		close(explicitEntered)
		select {
		case <-release:
			return file.release()
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
	}
	for index, request := range []fileRequest{implicit, explicit} {
		response, err := handler.fileCall(t.Context(), request, [32]byte{byte(index + 1)})
		if response.CloseResult == nil || response.CloseResult.Determined || !errors.Is(err, syscall.EIO) {
			t.Fatalf("initial unknown receipt=%+v err=%v", response.CloseResult, err)
		}
	}
	log.fail.Store(true)
	calls := make(chan concurrentCloseCall, 2)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for index, request := range []fileRequest{implicit, explicit} {
		go func() {
			response, err := handler.fileCall(ctx, request, [32]byte{byte(index + 1)})
			calls <- concurrentCloseCall{response, err}
		}()
	}
	for _, entered := range []<-chan struct{}{implicitEntered, explicitEntered} {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("both retries did not enter native close")
		}
	}
	close(release)
	for range 2 {
		call := awaitConcurrentClose(t, calls)
		if call.response.CloseResult == nil || !call.response.CloseResult.Released || !call.response.CloseResult.BarrierPending || !errors.Is(call.err, syscall.ENOTEMPTY) || !errors.Is(call.err, syscall.EIO) {
			t.Fatalf("concurrent close=%+v err=%v", call.response.CloseResult, call.err)
		}
	}
	log.fail.Store(false)
	handler.files.closeRetiringSession("session", session, false)
	for index, request := range []fileRequest{implicit, explicit} {
		assertConcurrentCloseRecovered(t, handler, request, [32]byte{byte(index + 1)})
	}
	if file.effects.Load() != 1 || implicitCalls.Load() != 2 || explicitCalls.Load() != 2 {
		t.Fatalf("native close repeated: effects=%d implicit=%d explicit=%d", file.effects.Load(), implicitCalls.Load(), explicitCalls.Load())
	}
}

func TestHTTPDelayedInitialClosePublicationRetainsRecoveryOwner(t *testing.T) {
	for _, retirement := range []string{"explicit", "background"} {
		t.Run(retirement, func(t *testing.T) {
			handler, session, file, log, implicit, explicit := concurrentCloseFixture(t)
			initialEntered, initialReturn := make(chan struct{}), make(chan struct{})
			var implicitCalls, explicitCalls atomic.Int32
			file.implicit = func(ctx context.Context) (storage.ReferenceCloseResult, error) {
				if implicitCalls.Add(1) != 1 {
					return file.release()
				}
				close(initialEntered)
				select {
				case <-initialReturn:
					return storage.ReferenceCloseResult{}, syscall.EIO
				case <-ctx.Done():
					return storage.ReferenceCloseResult{}, ctx.Err()
				}
			}
			file.explicit = func(context.Context) (storage.ReferenceCloseResult, error) {
				explicitCalls.Add(1)
				return file.release()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			initial := make(chan concurrentCloseCall, 1)
			go func() {
				response, err := handler.fileCall(ctx, implicit, [32]byte{1})
				initial <- concurrentCloseCall{response, err}
			}()
			select {
			case <-initialEntered:
			case <-ctx.Done():
				t.Fatal("initial close was not admitted")
			}
			log.fail.Store(true)
			released, err := handler.fileCall(t.Context(), explicit, [32]byte{2})
			if released.CloseResult == nil || !released.CloseResult.Released || !released.CloseResult.BarrierPending || !errors.Is(err, syscall.ENOTEMPTY) {
				t.Fatalf("adopted close did not retain its release fact: result=%+v err=%v", released.CloseResult, err)
			}
			session.mu.Lock()
			retained, retired := session.files["file"] != nil, session.retired
			action := session.actions[implicit.Action]
			select {
			case <-action.done:
				t.Fatal("initial publication was not delayed")
			default:
			}
			if action.response.CloseResult != nil || !retained || !retired {
				t.Fatalf("unpublished owner changed: result=%+v retained=%v retired=%v", action.response.CloseResult, retained, retired)
			}
			session.mu.Unlock()
			if response, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileStat, Session: "session", File: "file"}, [32]byte{}); response.Attr != nil || !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("retired reference admitted data: response=%+v err=%v", response, err)
			}
			if retirement == "explicit" {
				closeID, err := storage.NewLockRequestID(1)
				if err != nil {
					t.Fatal(err)
				}
				response, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileSessionClose, Session: "session", Action: closeID, CloseGeneration: 1}, [32]byte{3})
				if response.CloseResult == nil || !response.CloseResult.Released || !response.CloseResult.BarrierPending || !errors.Is(err, syscall.EIO) {
					t.Fatalf("session release with pending publication=%+v err=%v", response.CloseResult, err)
				}
			} else {
				handler.files.closeRetiringSession("session", session, false)
			}
			handler.files.mu.Lock()
			active, terminal := handler.files.sessions["session"], handler.files.terminalCloses["session"]
			handler.files.mu.Unlock()
			if active != session || terminal != nil {
				t.Fatal("session discarded the unpublished close owner")
			}
			cancelled, stop := context.WithCancel(t.Context())
			stop()
			if _, err := handler.fileCall(cancelled, implicit, [32]byte{1}); !errors.Is(err, context.Canceled) {
				t.Fatalf("pending same-ID replay ignored cancellation: %v", err)
			}
			close(initialReturn)
			late := awaitConcurrentClose(t, initial)
			if late.response.CloseResult == nil || late.response.CloseResult.Determined || !errors.Is(late.err, syscall.EIO) {
				t.Fatalf("late initial result=%+v err=%v", late.response.CloseResult, late.err)
			}
			log.fail.Store(false)
			handler.files.closeRetiringSession("session", session, false)
			handler.files.mu.Lock()
			active, terminal = handler.files.sessions["session"], handler.files.terminalCloses["session"]
			handler.files.mu.Unlock()
			if active != nil || terminal == nil {
				t.Fatal("settled close history did not terminalize")
			}
			for index, request := range []fileRequest{implicit, explicit} {
				assertConcurrentCloseRecovered(t, handler, request, [32]byte{byte(index + 1)})
			}
			if file.effects.Load() != 1 || implicitCalls.Load() != 2 || explicitCalls.Load() != 1 {
				t.Fatalf("native close repeated: effects=%d implicit=%d explicit=%d", file.effects.Load(), implicitCalls.Load(), explicitCalls.Load())
			}
		})
	}
}

func TestHTTPHandlerCloseDrainsDelayedClosePublication(t *testing.T) {
	handler, session, file, _, implicit, _ := concurrentCloseFixture(t)
	initialEntered, initialReturn := make(chan struct{}), make(chan struct{})
	sessionReleased := make(chan struct{})
	var implicitCalls, sessionCalls atomic.Int32
	file.implicit = func(ctx context.Context) (storage.ReferenceCloseResult, error) {
		if implicitCalls.Add(1) != 1 {
			return file.release()
		}
		close(initialEntered)
		select {
		case <-initialReturn:
			return storage.ReferenceCloseResult{}, syscall.EIO
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
	}
	native := session.native.(concurrentCloseSession)
	native.closing = func(context.Context) {
		if sessionCalls.Add(1) == 1 {
			close(sessionReleased)
		}
	}
	session.native = native
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	initial := make(chan concurrentCloseCall, 1)
	go func() {
		response, err := handler.fileCall(ctx, implicit, [32]byte{1})
		initial <- concurrentCloseCall{response, err}
	}()
	select {
	case <-initialEntered:
	case <-ctx.Done():
		t.Fatal("initial close was not admitted")
	}
	closed := make(chan error, 1)
	closeCtx, stopClose := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopClose()
	go func() { closed <- handler.Close(closeCtx) }()
	select {
	case <-sessionReleased:
	case <-ctx.Done():
		t.Fatal("handler did not release the native session")
	}
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler close did not retain delayed publication: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("handler close ignored its deadline")
	}
	close(initialReturn)
	awaitConcurrentClose(t, initial)
	if err := handler.Close(ctx); err != nil {
		t.Fatalf("handler did not finish after receipt publication: %v", err)
	}
	handler.files.mu.Lock()
	active, terminal := handler.files.sessions["session"], handler.files.terminalCloses["session"]
	handler.files.mu.Unlock()
	if active != nil || terminal == nil || file.effects.Load() != 1 || sessionCalls.Load() != 1 {
		t.Fatalf("handler retained incomplete cleanup: active=%v terminal=%v effects=%d nativeSessionCloses=%d", active, terminal, file.effects.Load(), sessionCalls.Load())
	}
	response, err := handler.replayTerminalFileClose(t.Context(), implicit, [32]byte{1}, terminal)
	if response.CloseResult == nil || !response.CloseResult.Released || response.CloseResult.BarrierPending || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("drained terminal receipt=%+v err=%v", response.CloseResult, err)
	}
}

func TestHTTPPendingACKCloseDefersConcurrentInitialReceipt(t *testing.T) {
	handler, session, file, _, implicit, _ := concurrentCloseFixture(t)
	pendingEntered, releasePending := make(chan struct{}), make(chan struct{})
	initialEntered, initialReturn := make(chan struct{}), make(chan struct{})
	sessionCleanup := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var implicitCalls atomic.Int32
	file.implicit = func(context.Context) (storage.ReferenceCloseResult, error) {
		switch implicitCalls.Add(1) {
		case 1:
			close(pendingEntered)
			select {
			case <-releasePending:
				result, _ := file.release()
				return result, nil
			case <-ctx.Done():
				return storage.ReferenceCloseResult{}, ctx.Err()
			}
		case 2:
			close(initialEntered)
			select {
			case <-initialReturn:
				return storage.ReferenceCloseResult{}, syscall.EIO
			case <-ctx.Done():
				return storage.ReferenceCloseResult{}, ctx.Err()
			}
		default:
			return file.release()
		}
	}
	native := session.native.(concurrentCloseSession)
	native.closing = func(context.Context) {
		select {
		case sessionCleanup <- struct{}{}:
		default:
		}
	}
	session.native = native
	session.files["file"].pending = time.Now().Add(-time.Second)
	handler.files.mu.Lock()
	handler.files.startLocked()
	handler.files.mu.Unlock()
	handler.files.wakeCleanup()
	defer func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-pendingEntered:
	case <-ctx.Done():
		t.Fatal("pending-ACK cleanup was not admitted")
	}
	initial := make(chan concurrentCloseCall, 1)
	go func() {
		response, err := handler.fileCall(ctx, implicit, [32]byte{1})
		initial <- concurrentCloseCall{response, err}
	}()
	select {
	case <-initialEntered:
	case <-ctx.Done():
		t.Fatal("concurrent initial close was not admitted")
	}
	close(releasePending)
	select {
	case <-sessionCleanup:
	case <-ctx.Done():
		t.Fatal("pending-ACK release did not schedule deferred receipt cleanup")
	}
	session.mu.Lock()
	retired, retained := session.retired, session.files["file"] != nil
	session.mu.Unlock()
	if !retired || !retained {
		t.Fatalf("pending-ACK owner discarded: retired=%v retained=%v", retired, retained)
	}
	close(initialReturn)
	awaitConcurrentClose(t, initial)
	if err := handler.Close(ctx); err != nil {
		t.Fatal(err)
	}
	handler.files.mu.Lock()
	active, terminal := handler.files.sessions["session"], handler.files.terminalCloses["session"]
	handler.files.mu.Unlock()
	if active != nil || terminal == nil || file.effects.Load() != 1 {
		t.Fatalf("pending-ACK cleanup stranded owner: active=%v terminal=%v effects=%d", active, terminal, file.effects.Load())
	}
	response, err := handler.replayTerminalFileClose(t.Context(), implicit, [32]byte{1}, terminal)
	if response.CloseResult == nil || !response.CloseResult.Released || response.CloseResult.BarrierPending || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("pending-ACK terminal receipt=%+v err=%v", response.CloseResult, err)
	}
}

func TestHTTPCancelledReconciliationRetainsCloseOwner(t *testing.T) {
	handler, session, file, _, implicit, explicit := concurrentCloseFixture(t)
	var implicitCalls atomic.Int32
	file.implicit = func(context.Context) (storage.ReferenceCloseResult, error) {
		if implicitCalls.Add(1) == 1 {
			return storage.ReferenceCloseResult{}, syscall.EIO
		}
		return file.release()
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	file.explicit = func(context.Context) (storage.ReferenceCloseResult, error) {
		cancel()
		return file.release()
	}
	if response, err := handler.fileCall(t.Context(), implicit, [32]byte{1}); response.CloseResult == nil || response.CloseResult.Determined || !errors.Is(err, syscall.EIO) {
		t.Fatalf("initial unknown close=%+v err=%v", response.CloseResult, err)
	}
	response, err := handler.fileCall(ctx, explicit, [32]byte{2})
	if response.CloseResult == nil || !response.CloseResult.Released || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("release fact lost on cancellation: result=%+v err=%v", response.CloseResult, err)
	}
	session.mu.Lock()
	retired, retained := session.retired, session.files["file"] != nil
	session.mu.Unlock()
	if !retired || !retained || implicitCalls.Load() != 1 {
		t.Fatalf("cancelled reconciliation lost recovery owner: retired=%v retained=%v nativeCalls=%d", retired, retained, implicitCalls.Load())
	}
	handler.files.closeRetiringSession("session", session, false)
	for index, request := range []fileRequest{implicit, explicit} {
		assertConcurrentCloseRecovered(t, handler, request, [32]byte{byte(index + 1)})
	}
	if implicitCalls.Load() != 2 || file.effects.Load() != 1 {
		t.Fatalf("cancelled recovery repeated native effects: calls=%d effects=%d", implicitCalls.Load(), file.effects.Load())
	}
}

func TestHTTPDeferredSessionPublicationRetainsOneNativeReleaseError(t *testing.T) {
	handler, session, file, _, implicit, _ := concurrentCloseFixture(t)
	file.implicit = func(context.Context) (storage.ReferenceCloseResult, error) { return file.release() }
	var sessionCalls atomic.Int32
	native := session.native.(concurrentCloseSession)
	native.err = syscall.ENOTEMPTY
	native.closing = func(context.Context) { sessionCalls.Add(1) }
	session.native = native
	action := &servedFileAction{
		id: implicit.Action, op: storage.OpFileClose, closeOwner: "file",
		closeImplicit: true, closeGeneration: 1, done: make(chan struct{}),
		expires: time.Now().Add(time.Minute), digest: [32]byte{1},
	}
	session.actions[implicit.Action] = action
	for range 32 {
		handler.files.closeRetiringSession("session", session, true)
	}
	if sessionCalls.Load() != 1 || handler.files.terminalErr.count != 1 || !errors.Is(handler.files.err, syscall.ENOTEMPTY) {
		t.Fatalf("deferred publication repeated native cleanup/error: nativeCalls=%d failures=%d err=%v", sessionCalls.Load(), handler.files.terminalErr.count, handler.files.err)
	}
	session.mu.Lock()
	action.response = fileResponse{Epoch: 1, CloseResult: &referenceCloseResult{}}
	action.err = syscall.EIO
	action.uncertain = true
	close(action.done)
	session.mu.Unlock()
	handler.files.closeRetiringSession("session", session, true)
	if sessionCalls.Load() != 1 || handler.files.terminalErr.count != 1 || handler.files.sessions["session"] != nil {
		t.Fatalf("settled publication repeated native cleanup/error: nativeCalls=%d failures=%d active=%v", sessionCalls.Load(), handler.files.terminalErr.count, handler.files.sessions["session"])
	}
	terminal := handler.files.terminalCloses["session"]
	if terminal == nil || !errors.Is(terminal.releaseErr, syscall.ENOTEMPTY) {
		t.Fatalf("native release error lost at terminal: %v", terminal)
	}
	assertConcurrentCloseRecovered(t, handler, implicit, [32]byte{1})
}

func TestHTTPTransientCloseQueryFailureRetainsRecoveryOwner(t *testing.T) {
	for _, retirement := range []string{"explicit", "background"} {
		t.Run(retirement, func(t *testing.T) {
			handler, session, file, _, _, request := concurrentCloseFixture(t)
			query := &transientCloseQueryReference{concurrentCloseReference: file}
			session.files["file"].native = query
			var closeCalls, sessionCalls atomic.Int32
			file.explicit = func(context.Context) (storage.ReferenceCloseResult, error) {
				result, err := file.release()
				if closeCalls.Add(1) == 1 {
					return storage.ReferenceCloseResult{}, syscall.EIO
				}
				return result, err
			}
			native := session.native.(concurrentCloseSession)
			native.closing = func(context.Context) { sessionCalls.Add(1) }
			session.native = native
			response, err := handler.fileCall(t.Context(), request, [32]byte{2})
			if response.CloseResult == nil || response.CloseResult.Determined || !errors.Is(err, syscall.EIO) || file.effects.Load() != 1 {
				t.Fatalf("hidden native release=%+v err=%v effects=%d", response.CloseResult, err, file.effects.Load())
			}
			if retirement == "explicit" {
				closeID, err := storage.NewLockRequestID(1)
				if err != nil {
					t.Fatal(err)
				}
				response, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileSessionClose, Session: "session", Action: closeID, CloseGeneration: 1}, [32]byte{3})
				if response.CloseResult == nil || !response.CloseResult.Released || err != nil {
					t.Fatalf("session release=%+v err=%v", response.CloseResult, err)
				}
			} else {
				session.mu.Lock()
				session.retired = true
				session.mu.Unlock()
				handler.files.closeRetiringSession("session", session, false)
			}
			if t.Context().Err() != nil || query.queries.Load() != 1 {
				t.Fatalf("query failure did not use a live context: err=%v queries=%d", t.Context().Err(), query.queries.Load())
			}
			handler.files.mu.Lock()
			active, terminal := handler.files.sessions["session"], handler.files.terminalCloses["session"]
			handler.files.mu.Unlock()
			session.mu.Lock()
			retained := session.files["file"] != nil && session.closeReconciling
			session.mu.Unlock()
			if active != session || terminal != nil || !retained {
				t.Fatal("failed query discarded the exact native recovery owner")
			}
			receipt, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: "session", FileAction: storage.FileActionID(request.Action)}, [32]byte{})
			if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionUnknown {
				t.Fatalf("failed query changed the unresolved receipt=%+v err=%v", receipt.ActionReceipt, err)
			}
			handler.files.closeRetiringSession("session", session, false)
			handler.files.mu.Lock()
			active, terminal = handler.files.sessions["session"], handler.files.terminalCloses["session"]
			handler.files.mu.Unlock()
			if active != nil || terminal == nil {
				t.Fatal("successful native query did not complete deferred retirement")
			}
			assertConcurrentCloseRecovered(t, handler, request, [32]byte{2})
			if query.queries.Load() != 2 || closeCalls.Load() != 2 || sessionCalls.Load() != 1 || file.effects.Load() != 1 {
				t.Fatalf("query recovery repeated native cleanup: queries=%d closeCalls=%d sessionCloses=%d effects=%d", query.queries.Load(), closeCalls.Load(), sessionCalls.Load(), file.effects.Load())
			}
		})
	}
}

func TestHTTPCompletedCloseQueryRequiresExactResult(t *testing.T) {
	for _, fact := range []struct {
		name        string
		result      storage.ReferenceCloseResult
		err         error
		lostReplies int32
	}{
		{"undetermined-replay", storage.ReferenceCloseResult{Released: true, Determined: true}, syscall.ENOTEMPTY, 2},
		{"historical-retained", storage.ReferenceCloseResult{Determined: true}, syscall.ENOSPC, 1},
	} {
		t.Run(fact.name, func(t *testing.T) {
			for _, retirement := range []string{"explicit", "background"} {
				t.Run(retirement, func(t *testing.T) {
					handler, session, file, _, _, request := concurrentCloseFixture(t)
					laterID, err := storage.NewFileActionID(1)
					if err != nil {
						t.Fatal(err)
					}
					first := storage.CloseAttempt{Action: storage.FileActionID(request.Action), Generation: request.CloseGeneration}
					later := storage.CloseAttempt{Action: laterID, Generation: 2}
					reference := &recordedCloseReference{concurrentCloseReference: file, results: map[storage.CloseAttempt]*recordedCloseResult{
						first: {result: fact.result, err: fact.err, lostReplies: fact.lostReplies},
						later: {result: storage.ReferenceCloseResult{Released: true, Determined: true}},
					}}
					native := &recordedCloseSession{file: reference, cleanup: later}
					session.native = native
					session.files["file"].native = reference
					response, err := handler.fileCall(t.Context(), request, [32]byte{2})
					if response.CloseResult == nil || response.CloseResult.Determined || !errors.Is(err, syscall.EIO) {
						t.Fatalf("lost initial result=%+v err=%v", response.CloseResult, err)
					}
					if retirement == "explicit" {
						closeID, err := storage.NewLockRequestID(1)
						if err != nil {
							t.Fatal(err)
						}
						response, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileSessionClose, Session: "session", Action: closeID, CloseGeneration: 1}, [32]byte{3})
						if response.CloseResult == nil || !response.CloseResult.Released || err != nil {
							t.Fatalf("session release=%+v err=%v", response.CloseResult, err)
						}
					} else {
						session.mu.Lock()
						session.retired = true
						session.mu.Unlock()
						handler.files.closeRetiringSession("session", session, false)
					}
					if fact.lostReplies > 1 {
						handler.files.mu.Lock()
						active, terminal := handler.files.sessions["session"], handler.files.terminalCloses["session"]
						handler.files.mu.Unlock()
						session.mu.Lock()
						retained := session.closeReconciling && session.files["file"] != nil
						session.mu.Unlock()
						if active != session || terminal != nil || !retained {
							t.Fatal("Completed query discarded ownership before the exact result was known")
						}
						handler.files.closeRetiringSession("session", session, false)
					}
					handler.files.mu.Lock()
					active, terminal := handler.files.sessions["session"], handler.files.terminalCloses["session"]
					handler.files.mu.Unlock()
					if active != nil || terminal == nil {
						t.Fatal("determined exact result did not complete retirement")
					}
					response, err = handler.fileCall(t.Context(), request, [32]byte{2})
					if response.CloseResult == nil || !response.CloseResult.Determined || response.CloseResult.Released != fact.result.Released || response.CloseResult.BarrierPending || !errors.Is(err, fact.err) || errors.Is(err, syscall.EIO) {
						t.Fatalf("exact action result changed after retirement: result=%+v err=%v", response.CloseResult, err)
					}
					if (response.Barrier != nil) != fact.result.Released {
						t.Fatalf("release barrier does not match original result: result=%+v barrier=%+v", response.CloseResult, response.Barrier)
					}
					receipt, err := handler.fileCall(t.Context(), fileRequest{Op: storage.OpFileQueryAction, Session: "session", File: "file", FileAction: first.Action, CloseGeneration: first.Generation}, [32]byte{})
					if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionCompleted {
						t.Fatalf("exact action receipt=%+v err=%v", receipt.ActionReceipt, err)
					}
					laterCalls := int32(0)
					if !fact.result.Released {
						laterCalls = 1
					}
					if reference.results[first].calls.Load() != fact.lostReplies+1 || reference.results[later].calls.Load() != laterCalls || native.calls.Load() != 1 || file.effects.Load() != 1 {
						t.Fatalf("exact result retrieval repeated effects: original=%d later=%d session=%d effects=%d", reference.results[first].calls.Load(), reference.results[later].calls.Load(), native.calls.Load(), file.effects.Load())
					}
				})
			}
		})
	}
}
