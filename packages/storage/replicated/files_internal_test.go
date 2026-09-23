package replicated

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type fileAuthorityStub struct {
	storage.File
	write            func(context.Context) (storage.Attr, *httprest.MutationBarrier, error)
	read             func(context.Context) (storage.FileRead, error)
	close            func(context.Context) error
	releasedOnError  bool
	closeBarrier     *httprest.MutationBarrier
	omitCloseBarrier bool
}

type terminalCloseFileStub struct {
	*fileAuthorityStub
	closes int
}

func (f *terminalCloseFileStub) CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	f.closes++
	return storage.ReferenceCloseResult{Released: true}, &httprest.MutationBarrier{Incarnation: "log"}, syscall.ENOTEMPTY
}

func TestRetainedFileTerminalCloseKeepsReleasedResult(t *testing.T) {
	session := retainedTestSession(t, &fileSessionStub{close: func(context.Context) error { return nil }})
	remote := &terminalCloseFileStub{fileAuthorityStub: &fileAuthorityStub{}}
	file := &retainedFile{session: session, remote: remote}
	for range 2 {
		result, err := file.CloseWithResult(t.Context())
		if !result.Released || !errors.Is(err, syscall.ENOTEMPTY) {
			t.Fatalf("terminal close=%+v %v", result, err)
		}
	}
	if remote.closes != 1 {
		t.Fatalf("terminal close repeated remote operation %d times", remote.closes)
	}
}

func TestReleasedFileCloseRetriesLocalBarrierConfirmation(t *testing.T) {
	session := retainedTestSession(t, &fileSessionStub{close: func(context.Context) error { return nil }})
	closes := 0
	remote := &fileAuthorityStub{
		close:        func(context.Context) error { closes++; return nil },
		closeBarrier: &httprest.MutationBarrier{Incarnation: "log", Position: 1},
	}
	file := &retainedFile{session: session, remote: remote}
	cut, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := file.CloseWithResult(cut); !result.Released || !errors.Is(err, syscall.EIO) || file.closed {
		t.Fatalf("unconfirmed released close=%+v %v closed=%t", result, err, file.closed)
	}
	session.base.mu.Lock()
	session.base.at = 1
	session.base.wake()
	session.base.mu.Unlock()
	if result, err := file.CloseWithResult(t.Context()); !result.Released || err != nil || !file.closed || closes != 1 {
		t.Fatalf("reconciled close=%+v %v closed=%t remote calls=%d", result, err, file.closed, closes)
	}
}

type pendingBarrierFileStub struct {
	*fileAuthorityStub
	closes int
}

func (f *pendingBarrierFileStub) CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	f.closes++
	if f.closes == 1 {
		return storage.ReferenceCloseResult{Released: true}, nil, &httprest.CloseBarrierPendingError{Cause: syscall.EIO}
	}
	if f.closes == 2 {
		return storage.ReferenceCloseResult{}, nil, syscall.EIO
	}
	return storage.ReferenceCloseResult{Released: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
}

func TestReleasedFileCloseReplaysMissingBarrier(t *testing.T) {
	session := retainedTestSession(t, &fileSessionStub{close: func(context.Context) error { return nil }})
	remote := &pendingBarrierFileStub{fileAuthorityStub: &fileAuthorityStub{}}
	file := &retainedFile{session: session, remote: remote}
	if result, err := file.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.EIO) || file.closed {
		t.Fatalf("pending close=%+v %v closed=%t", result, err, file.closed)
	}
	if result, err := file.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.EIO) || file.closed || remote.closes != 2 {
		t.Fatalf("transient replay=%+v %v closed=%t remote calls=%d", result, err, file.closed, remote.closes)
	}
	if result, err := file.CloseWithResult(t.Context()); !result.Released || err != nil || !file.closed || remote.closes != 3 {
		t.Fatalf("replayed close=%+v %v closed=%t remote calls=%d", result, err, file.closed, remote.closes)
	}
}

func TestReleasedCloseWithoutBarrierNeverReportsConfirmedSuccess(t *testing.T) {
	fileSession := retainedTestSession(t, nil)
	file := &retainedFile{session: fileSession, remote: &fileAuthorityStub{
		close: func(context.Context) error { return nil }, omitCloseBarrier: true,
	}}
	if result, err := file.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.EIO) || file.closed {
		t.Fatalf("file close without barrier=%+v %v closed=%t", result, err, file.closed)
	}
	referenceSession := retainedTestSession(t, nil)
	reference := &nodeReference{session: referenceSession, remote: &barrierReferenceStub{omitCloseBarrier: true}}
	if result, err := reference.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.EIO) || reference.closed {
		t.Fatalf("node close without barrier=%+v %v closed=%t", result, err, reference.closed)
	}
	session := retainedTestSession(t, &fileSessionStub{
		close: func(context.Context) error { return nil }, omitCloseBarrier: true,
	})
	if result, err := session.CloseWithResult(t.Context()); !result.Released || !errors.Is(err, syscall.EIO) || session.closed || len(session.base.fileSessions) != 1 {
		t.Fatalf("session close without barrier=%+v %v closed=%t owned=%d", result, err, session.closed, len(session.base.fileSessions))
	}
}

func (f *fileAuthorityStub) WriteAtWithBarrier(ctx context.Context, _ int64, _ []byte) (storage.Attr, *httprest.MutationBarrier, error) {
	return f.write(ctx)
}
func (f *fileAuthorityStub) TruncateWithBarrier(context.Context, int64) (storage.Attr, *httprest.MutationBarrier, error) {
	panic("unexpected truncate")
}
func (f *fileAuthorityStub) SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *httprest.MutationBarrier, error) {
	panic("unexpected setattr")
}
func (f *fileAuthorityStub) ReadAt(ctx context.Context, _ int64, _ int) (storage.FileRead, error) {
	return f.read(ctx)
}
func (f *fileAuthorityStub) Close(ctx context.Context) error { return f.close(ctx) }
func (f *fileAuthorityStub) CloseWithBarrier(ctx context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	err := f.close(ctx)
	barrier := f.closeBarrier
	if barrier == nil && !f.omitCloseBarrier {
		barrier = &httprest.MutationBarrier{Incarnation: "log"}
	}
	return storage.ReferenceCloseResult{Released: err == nil || f.releasedOnError}, barrier, err
}

type fileSessionStub struct {
	httprest.FileSessionWithBarrier
	close            func(context.Context) error
	open             func(context.Context) (storage.File, *httprest.MutationBarrier, error)
	releasedOnError  bool
	closeBarrier     *httprest.MutationBarrier
	omitCloseBarrier bool
}

func (s *fileSessionStub) Close(ctx context.Context) error { return s.close(ctx) }
func (s *fileSessionStub) CloseWithBarrier(ctx context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	err := s.close(ctx)
	barrier := s.closeBarrier
	if barrier == nil && !s.omitCloseBarrier {
		barrier = &httprest.MutationBarrier{Incarnation: "log"}
	}
	return storage.ReferenceCloseResult{Released: err == nil || s.releasedOnError}, barrier, err
}
func (s *fileSessionStub) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	err := s.close(ctx)
	return storage.ReferenceCloseResult{Released: err == nil || s.releasedOnError}, err
}
func (s *fileSessionStub) OpenFileWithBarrier(ctx context.Context, _ string, _ storage.FileOpenOptions) (storage.File, *httprest.MutationBarrier, error) {
	return s.open(ctx)
}

func retainedTestSession(t *testing.T, remote httprest.FileSessionWithBarrier) *fileSession {
	t.Helper()
	base := confirmationTestStorage(DefaultOptions())
	lifetime, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	t.Cleanup(base.stop)
	session := &fileSession{base: base, remote: remote, lifetime: lifetime, stop: stop, changed: make(chan struct{})}
	base.fileSessions = map[*fileSession]struct{}{session: {}}
	return session
}

func TestRetainedFileRequiresAnAtomicReplicationBarrier(t *testing.T) {
	for _, test := range []struct {
		name    string
		barrier *httprest.MutationBarrier
		failure error
	}{
		{name: "missing"},
		{name: "wrong log", barrier: &httprest.MutationBarrier{Incarnation: "elsewhere"}},
		{name: "negative position", barrier: &httprest.MutationBarrier{Incarnation: "log", Position: -1}},
		{name: "unknown publication", failure: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			calls := 0
			file := &retainedFile{session: session, remote: &fileAuthorityStub{write: func(context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
				calls++
				return storage.Attr{ID: 5, Size: 1}, test.barrier, test.failure
			}}}
			attr, err := file.WriteAt(t.Context(), 0, []byte{'x'})
			if !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(attr, storage.Attr{}) || calls != 1 {
				t.Fatalf("unconfirmed publication returned %+v, %v after %d dispatches", attr, err, calls)
			}
			if session.base.activeConfirmations != 0 {
				t.Fatal("failed file mutation retained confirmation capacity")
			}
		})
	}
}

func TestRetainedFileCancellationDistinguishesAdmissionFromPublication(t *testing.T) {
	session := retainedTestSession(t, nil)
	var calls atomic.Int64
	committed := make(chan struct{})
	file := &retainedFile{session: session, remote: &fileAuthorityStub{write: func(context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
		calls.Add(1)
		close(committed)
		return storage.Attr{ID: 5, Size: 1}, &httprest.MutationBarrier{Incarnation: "log", Position: 1}, nil
	}}}
	before, cancelBefore := context.WithCancelCause(t.Context())
	cause := errors.New("caller withdrew the file operation")
	cancelBefore(cause)
	if _, err := file.WriteAt(before, 0, nil); storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, cause) || calls.Load() != 0 {
		t.Fatalf("pre-send cancellation lost its cause or dispatched: %v; calls %d", err, calls.Load())
	}
	after, cancelAfter := context.WithCancel(t.Context())
	defer cancelAfter()
	done := make(chan error, 1)
	go func() { _, err := file.WriteAt(after, 0, nil); done <- err }()
	<-committed
	cancelAfter()
	if err := awaitConfirmationCancellation(t, done); storage.ErrnoOf(err) != syscall.EIO || calls.Load() != 1 {
		t.Fatalf("post-publication cancellation was retried or reported as unapplied: %v; calls %d", err, calls.Load())
	}
}

func TestRetainedSessionCloseCancelsAndDrainsBeforeRemoteCleanup(t *testing.T) {
	var remoteCloses atomic.Int64
	session := retainedTestSession(t, &fileSessionStub{close: func(context.Context) error {
		remoteCloses.Add(1)
		return nil
	}})
	entered, cancelled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	file := &retainedFile{session: session, remote: &fileAuthorityStub{read: func(ctx context.Context) (storage.FileRead, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-resume
		return storage.FileRead{}, ctx.Err()
	}}}
	reading := make(chan error, 1)
	go func() { _, err := file.ReadAt(t.Context(), 0, 1); reading <- err }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- session.Close(t.Context()) }()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("close did not cancel the admitted read")
	}
	if remoteCloses.Load() != 0 {
		t.Fatal("session cleanup ran before admitted calls drained")
	}
	close(resume)
	if err := awaitConfirmationCancellation(t, reading); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := awaitConfirmationCancellation(t, closed); err != nil || remoteCloses.Load() != 1 {
		t.Fatalf("session cleanup returned %v after %d calls", err, remoteCloses.Load())
	}
	if len(session.base.fileSessions) != 0 {
		t.Fatal("confirmed cleanup retained the session ownership record")
	}
	if err := session.Close(t.Context()); err != nil || remoteCloses.Load() != 1 {
		t.Fatal("repeated close repeated authority cleanup:", err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal("session cleanup did not close its files:", err)
	}
}

func TestRetainedSessionKeepsOwnershipWhenCleanupIsUnknown(t *testing.T) {
	cause := errors.New("close response was lost")
	calls := 0
	session := retainedTestSession(t, &fileSessionStub{close: func(context.Context) error {
		calls++
		if calls == 1 {
			return cause
		}
		return nil
	}})
	if err := session.Close(t.Context()); !errors.Is(err, cause) {
		t.Fatal("session cleanup lost its error:", err)
	}
	if len(session.base.fileSessions) != 1 {
		t.Fatal("unknown cleanup reported reclaimed session capacity")
	}
	if _, _, err := session.begin(t.Context(), false); !errors.Is(err, syscall.ESTALE) {
		t.Fatal("unknown cleanup admitted another operation:", err)
	}
	if err := session.Close(t.Context()); err != nil || len(session.base.fileSessions) != 0 {
		t.Fatalf("explicit cleanup reconciliation failed: %v", err)
	}
}

func TestReleasedSessionCloseRetriesLocalBarrierConfirmation(t *testing.T) {
	closes := 0
	remote := &fileSessionStub{
		close:        func(context.Context) error { closes++; return nil },
		closeBarrier: &httprest.MutationBarrier{Incarnation: "log", Position: 1},
	}
	session := retainedTestSession(t, remote)
	cut, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := session.CloseWithResult(cut); !result.Released || !errors.Is(err, syscall.EIO) || session.closed || len(session.base.fileSessions) != 1 {
		t.Fatalf("unconfirmed released session=%+v %v closed=%t owned=%d", result, err, session.closed, len(session.base.fileSessions))
	}
	session.base.mu.Lock()
	session.base.at = 1
	session.base.wake()
	session.base.mu.Unlock()
	if result, err := session.CloseWithResult(t.Context()); !result.Released || err != nil || !session.closed || len(session.base.fileSessions) != 0 || closes != 1 {
		t.Fatalf("reconciled session=%+v %v closed=%t owned=%d remote calls=%d", result, err, session.closed, len(session.base.fileSessions), closes)
	}
}

func TestRetainedOpenClosesTheReferenceWhenConfirmationFails(t *testing.T) {
	closeCause := errors.New("file close outcome is unknown")
	closes := 0
	remoteFile := &fileAuthorityStub{close: func(context.Context) error {
		closes++
		if closes == 1 {
			return closeCause
		}
		return nil
	}}
	session := retainedTestSession(t, &fileSessionStub{open: func(context.Context) (storage.File, *httprest.MutationBarrier, error) {
		return remoteFile, nil, nil
	}})
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if file == nil || !errors.Is(err, syscall.EIO) || !errors.Is(err, closeCause) || closes != 1 {
		t.Fatalf("unconfirmed open lost reference cleanup: %v, %v, closes %d", file, err, closes)
	}
	if len(session.base.fileSessions) != 1 {
		t.Fatal("unknown reference cleanup lost its owning session")
	}
	if result, err := file.CloseWithResult(t.Context()); !result.Released || err != nil || closes != 2 {
		t.Fatalf("reconciled cleanup=%+v %v, closes %d", result, err, closes)
	}
}

func TestFailedOpenCleanupRunsWhileSessionIsClosing(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.mu.Lock()
	session.closing = true
	session.mu.Unlock()
	closes := 0
	remote := &fileAuthorityStub{
		close:           func(context.Context) error { closes++; return syscall.ENOTEMPTY },
		releasedOnError: true,
	}
	file, err := session.cleanupFailedOpenFile(remote, syscall.EIO)
	if file != nil || !errors.Is(err, syscall.ENOTEMPTY) || closes != 1 {
		t.Fatalf("closing session cleanup=%T %v, closes %d", file, err, closes)
	}
}

func TestFailedSessionOpenPreservesUnconfirmedCleanupOwnership(t *testing.T) {
	base := confirmationTestStorage(DefaultOptions())
	t.Cleanup(base.stop)
	calls := 0
	remote := &fileSessionStub{close: func(context.Context) error {
		calls++
		if calls == 1 {
			return syscall.EIO
		}
		return nil
	}}
	session, err := base.cleanupFailedOpenSession(remote, syscall.EOPNOTSUPP)
	if session == nil || !errors.Is(err, syscall.EOPNOTSUPP) || !errors.Is(err, syscall.EIO) || calls != 1 {
		t.Fatalf("unconfirmed session cleanup=%T %v, calls %d", session, err, calls)
	}
	for _, check := range []struct {
		name string
		call func() error
	}{
		{name: "open file", call: func() error {
			_, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			return err
		}},
		{name: "open node", call: func() error {
			_, err := session.OpenNode(t.Context(), 1, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			return err
		}},
		{name: "stat node", call: func() error {
			_, err := session.StatNode(t.Context(), 1)
			return err
		}},
		{name: "set node attr", call: func() error {
			_, err := session.SetNodeAttr(t.Context(), 1, storage.AttrChange{})
			return err
		}},
		{name: "renew", call: func() error {
			_, err := session.Renew(t.Context())
			return err
		}},
		{name: "status", call: func() error {
			_, err := session.Status(t.Context())
			return err
		}},
	} {
		if err := check.call(); !errors.Is(err, syscall.EOPNOTSUPP) || !errors.Is(err, syscall.EIO) || calls != 1 || len(base.fileCleanupSessions) != 1 {
			t.Fatalf("failed session %s=%v, native calls %d, owned %d", check.name, err, calls, len(base.fileCleanupSessions))
		}
	}
	if result, err := session.CloseWithResult(t.Context()); !result.Released || err != nil || calls != 2 {
		t.Fatalf("reconciled session cleanup=%+v %v, calls %d", result, err, calls)
	}
	terminal := &fileSessionStub{close: func(context.Context) error { return syscall.ENOTEMPTY }, releasedOnError: true}
	session, err = base.cleanupFailedOpenSession(terminal, syscall.EOPNOTSUPP)
	if session != nil || !errors.Is(err, syscall.EOPNOTSUPP) || !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("terminal session cleanup=%T %v", session, err)
	}
}

func TestStorageCloseRetriesPartialSessionOpenCleanup(t *testing.T) {
	base := confirmationTestStorage(DefaultOptions())
	replica, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	base.local = replica
	base.stopped = make(chan struct{})
	close(base.stopped)
	calls := 0
	remote := &fileSessionStub{close: func(context.Context) error {
		calls++
		if calls == 1 {
			return syscall.EIO
		}
		return nil
	}}
	session, err := base.cleanupFailedOpenSession(remote, syscall.EOPNOTSUPP)
	if session == nil || !errors.Is(err, syscall.EIO) || len(base.fileCleanupSessions) != 1 {
		t.Fatalf("partial session=%T %v, owned %d", session, err, len(base.fileCleanupSessions))
	}
	if err := base.Close(); err != nil || calls != 2 || len(base.fileCleanupSessions) != 0 {
		t.Fatalf("storage close=%v, calls %d, owned %d", err, calls, len(base.fileCleanupSessions))
	}
}

type pendingFailedOpenSessionStub struct {
	*fileSessionStub
	closes int
}

func (s *pendingFailedOpenSessionStub) CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	s.closes++
	if s.closes == 1 {
		return storage.ReferenceCloseResult{Released: true}, nil, &httprest.CloseBarrierPendingError{Cause: syscall.EIO}
	}
	return storage.ReferenceCloseResult{Released: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
}

func TestPartialSessionOpenRetainsPendingBarrierReconciliation(t *testing.T) {
	base := confirmationTestStorage(DefaultOptions())
	t.Cleanup(base.stop)
	remote := &pendingFailedOpenSessionStub{fileSessionStub: &fileSessionStub{}}
	session, err := base.cleanupFailedOpenSession(remote, syscall.EOPNOTSUPP)
	if session == nil || !errors.Is(err, syscall.EIO) || len(base.fileCleanupSessions) != 1 || remote.closes != 1 {
		t.Fatalf("pending session=%T %v owned=%d remote calls=%d", session, err, len(base.fileCleanupSessions), remote.closes)
	}
	if err := base.closeFileSessions(); err != nil || len(base.fileCleanupSessions) != 0 || remote.closes != 2 {
		t.Fatalf("session barrier replay=%v owned=%d remote calls=%d", err, len(base.fileCleanupSessions), remote.closes)
	}
}

func TestRetainedFileUsesTheBoundedConfirmationPool(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.base.options.MaxActiveConfirmations = 1
	session.base.options.MaxWaitingConfirmations = 0
	active, err := session.base.expect(t.Context(), "write", "named")
	if err != nil {
		t.Fatal(err)
	}
	defer session.base.forget(active)
	file := &retainedFile{session: session, remote: &fileAuthorityStub{write: func(context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
		t.Fatal("saturated confirmation pool dispatched the retained mutation")
		return storage.Attr{}, nil, nil
	}}}
	if _, err := file.WriteAt(t.Context(), 0, nil); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal("retained mutation bypassed confirmation admission:", err)
	}
}

func TestCleanupConfirmationFailurePreservesItsCauseAndEIOClassification(t *testing.T) {
	cause := errors.New("replica follower stopped before cleanup became visible")
	err := &cleanupConfirmationFailure{cause: cause}
	if !errors.Is(err, cause) || !errors.Is(err, syscall.EIO) || storage.ErrnoOf(err) != syscall.EIO {
		t.Fatalf("cleanup confirmation error lost cause or classification: %v", err)
	}
	if err.Classification() != syscall.EIO {
		t.Fatalf("classification=%v", err.Classification())
	}
	if got := err.Error(); got == "" {
		t.Fatal("cleanup confirmation error has no diagnostic")
	}
}
