package replicated

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type fileAuthorityStub struct {
	storage.File
	write func(context.Context) (storage.Attr, *httprest.MutationBarrier, error)
	read  func(context.Context) (storage.FileRead, error)
	close func(context.Context) error
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

type fileSessionStub struct {
	httprest.FileSessionWithBarrier
	close func(context.Context) error
	open  func(context.Context) (storage.File, *httprest.MutationBarrier, error)
}

func (s *fileSessionStub) Close(ctx context.Context) error { return s.close(ctx) }
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
			if !errors.Is(err, syscall.EIO) || attr != (storage.Attr{}) || calls != 1 {
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

func TestRetainedOpenClosesTheReferenceWhenConfirmationFails(t *testing.T) {
	closeCause := errors.New("file close outcome is unknown")
	closes := 0
	remoteFile := &fileAuthorityStub{close: func(context.Context) error { closes++; return closeCause }}
	session := retainedTestSession(t, &fileSessionStub{open: func(context.Context) (storage.File, *httprest.MutationBarrier, error) {
		return remoteFile, nil, nil
	}})
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true})
	if file != nil || !errors.Is(err, syscall.EIO) || !errors.Is(err, closeCause) || closes != 1 {
		t.Fatalf("unconfirmed open lost reference cleanup: %v, %v, closes %d", file, err, closes)
	}
	if len(session.base.fileSessions) != 1 {
		t.Fatal("unknown reference cleanup lost its owning session")
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
