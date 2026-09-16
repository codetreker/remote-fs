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
	httprest.FileWithBarrier
	write func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error)
	read  func(context.Context) (storage.FileRead, error)
	close func(context.Context) error
}

func (f *fileAuthorityStub) WriteAtWithBarrier(ctx context.Context, _ storage.FileWriteRequest, _ storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return f.write(ctx)
}
func (f *fileAuthorityStub) ReadAt(ctx context.Context, _ storage.FileReadRequest) (storage.FileRead, error) {
	return f.read(ctx)
}
func (f *fileAuthorityStub) CloseWithBarrier(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return storage.FileActionReceipt{Action: action, State: storage.FileActionCompleted}, nil, f.close(ctx)
}

type fileSessionStub struct {
	httprest.FileSessionWithBarrier
	closeAction func(context.Context, storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error)
	close       func(context.Context) error
	retain      func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error)
}

func (s *fileSessionStub) CloseWithBarrier(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	if s.closeAction != nil {
		return s.closeAction(ctx, action)
	}
	return storage.FileActionReceipt{Action: action, State: storage.FileActionCompleted}, nil, s.close(ctx)
}
func (s *fileSessionStub) RetainWithBarrier(ctx context.Context, _ storage.RetainRequest, _ storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
	return s.retain(ctx)
}

const testFileAction storage.FileActionID = "1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func retainedTestSession(t *testing.T, remote httprest.FileSessionWithBarrier) *fileSession {
	t.Helper()
	base := confirmationTestStorage(DefaultOptions())
	lifetime, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	t.Cleanup(base.stop)
	session := &fileSession{actionEpoch: 1, base: base, remote: remote, lifetime: lifetime, stop: stop, changed: make(chan struct{})}
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
			file := &retainedFile{session: session, remote: &fileAuthorityStub{write: func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
				calls++
				return storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Observation: storage.FileObservation{Attr: storage.Attr{ID: 5, Kind: storage.NodeRegular, Size: 1}}}, test.barrier, test.failure
			}}}
			attr, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte{'x'}}, testFileAction)
			if !errors.Is(err, syscall.EIO) || attr.Effects != storage.EffectContentChanged || attr.Observation.Attr.ID != 5 || calls != 1 {
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
	file := &retainedFile{session: session, remote: &fileAuthorityStub{write: func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		calls.Add(1)
		close(committed)
		return storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Observation: storage.FileObservation{Attr: storage.Attr{ID: 5, Kind: storage.NodeRegular, Size: 1}}}, &httprest.MutationBarrier{Incarnation: "log", Position: 1}, nil
	}}}
	before, cancelBefore := context.WithCancelCause(t.Context())
	cause := errors.New("caller withdrew the file operation")
	cancelBefore(cause)
	if _, err := file.WriteAt(before, storage.FileWriteRequest{}, testFileAction); storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, cause) || calls.Load() != 0 {
		t.Fatalf("pre-send cancellation lost its cause or dispatched: %v; calls %d", err, calls.Load())
	}
	after, cancelAfter := context.WithCancel(t.Context())
	defer cancelAfter()
	done := make(chan error, 1)
	go func() { _, err := file.WriteAt(after, storage.FileWriteRequest{}, testFileAction); done <- err }()
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
	file := &retainedFile{session: session, remote: &fileAuthorityStub{close: func(context.Context) error { return nil }, read: func(ctx context.Context) (storage.FileRead, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-resume
		return storage.FileRead{}, ctx.Err()
	}}}
	reading := make(chan error, 1)
	go func() { _, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); reading <- err }()
	<-entered
	closed := make(chan error, 1)
	go func() { _, err := session.Close(t.Context(), testFileAction); closed <- err }()
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
	if _, err := session.Close(t.Context(), testFileAction); err != nil || remoteCloses.Load() != 1 {
		t.Fatal("repeated close repeated authority cleanup:", err)
	}
	if _, err := file.Close(t.Context(), testFileAction); err != nil {
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
	if _, err := session.Close(t.Context(), testFileAction); !errors.Is(err, cause) {
		t.Fatal("session cleanup lost its error:", err)
	}
	if len(session.base.fileSessions) != 1 {
		t.Fatal("unknown cleanup reported reclaimed session capacity")
	}
	if _, _, err := session.begin(t.Context(), false); !errors.Is(err, syscall.ESTALE) {
		t.Fatal("unknown cleanup admitted another operation:", err)
	}
	if _, err := session.Close(t.Context(), testFileAction); err != nil || len(session.base.fileSessions) != 0 {
		t.Fatalf("explicit cleanup reconciliation failed: %v", err)
	}
}

func TestRetainedOpenPreservesTheReferenceWhenConfirmationFails(t *testing.T) {
	receipt := storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: storage.EffectRetained | storage.EffectCreated, Reference: 9}
	session := retainedTestSession(t, &fileSessionStub{retain: func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return receipt, nil, nil
	}})
	got, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: 5, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, testFileAction)
	if !errors.Is(err, syscall.EIO) || got.Reference != 9 || got.Effects != receipt.Effects {
		t.Fatalf("unconfirmed retain lost ownership facts: %+v, %v", got, err)
	}
	if len(session.base.fileSessions) != 1 {
		t.Fatal("unconfirmed retain lost its owning session")
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
	file := &retainedFile{session: session, remote: &fileAuthorityStub{write: func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		t.Fatal("saturated confirmation pool dispatched the retained mutation")
		return storage.FileActionReceipt{}, nil, nil
	}}}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{}, testFileAction); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal("retained mutation bypassed confirmation admission:", err)
	}
}

func TestRetainedActionConfirmsPartialEffectsWithoutDiscardingOriginalError(t *testing.T) {
	session := retainedTestSession(t, nil)
	cause := errors.New("finalization failed after metadata commit")
	want := storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: storage.EffectMetadataChanged, Reference: 17}
	got, err := session.perform(t.Context(), "partial", true, func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return want, nil, cause
	})
	if !errors.Is(err, cause) || !errors.Is(err, syscall.EIO) || got.Reference != 17 || got.Effects != want.Effects {
		t.Fatalf("partial receipt or causal error lost: %+v, %v", got, err)
	}
	if session.base.activeConfirmations != 0 {
		t.Fatal("partial effect leaked confirmation admission")
	}
}

func TestRetainedControlCleanupBypassesFailedReplicaAndSaturatedMutationPool(t *testing.T) {
	session := retainedTestSession(t, nil)
	session.base.options.MaxActiveConfirmations = 1
	session.base.activeConfirmations = 1
	session.base.failure = errors.New("replica stopped")
	called := false
	receipt, err := session.perform(t.Context(), "cleanup", false, func(context.Context) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		called = true
		return storage.FileActionReceipt{Action: testFileAction, State: storage.FileActionCompleted, Effects: storage.EffectReferenceRetired}, nil, nil
	})
	if err != nil || !called || receipt.Effects != storage.EffectReferenceRetired {
		t.Fatalf("authority cleanup depended on replica: %+v, %v", receipt, err)
	}
	if session.base.activeConfirmations != 1 {
		t.Fatal("cleanup changed ordinary confirmation capacity")
	}
	session.base.activeConfirmations = 0
}

func TestRetainedAutomaticCleanupReusesUnknownCloseAction(t *testing.T) {
	var actions []storage.FileActionID
	remote := &fileSessionStub{closeAction: func(_ context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		actions = append(actions, action)
		if len(actions) == 1 {
			return storage.FileActionReceipt{Action: action, State: storage.FileActionUnknown}, nil, syscall.EIO
		}
		return storage.FileActionReceipt{Action: action, State: storage.FileActionCompleted}, nil, nil
	}}
	session := retainedTestSession(t, remote)
	if _, err := session.Close(t.Context(), testFileAction); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if err := session.cleanup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(actions) != 2 || actions[0] != testFileAction || actions[1] != testFileAction {
		t.Fatalf("cleanup replaced an unresolved action: %v", actions)
	}
}

func TestRetainedCloseReceiptDoesNotExposeCachedPayload(t *testing.T) {
	remote := &fileSessionStub{closeAction: func(_ context.Context, action storage.FileActionID) (storage.FileActionReceipt, *httprest.MutationBarrier, error) {
		return storage.FileActionReceipt{Action: action, State: storage.FileActionCompleted, Observation: storage.FileObservation{Attr: storage.Attr{Metadata: storage.Metadata{{Key: "test.value", Version: 1, Data: []byte("kept")}}}}}, nil, nil
	}}
	session := retainedTestSession(t, remote)
	first, err := session.Close(t.Context(), testFileAction)
	if err != nil {
		t.Fatal(err)
	}
	first.Observation.Attr.Metadata[0].Data[0] = 'x'
	replay, err := session.Close(t.Context(), testFileAction)
	if err != nil || string(replay.Observation.Attr.Metadata[0].Data) != "kept" {
		t.Fatalf("close replay borrowed caller payload: %+v, %v", replay, err)
	}
	replay.Observation.Attr.Metadata[0].Data[0] = 'y'
	again, err := session.Close(t.Context(), testFileAction)
	if err != nil || string(again.Observation.Attr.Metadata[0].Data) != "kept" {
		t.Fatalf("replayed receipt mutated retained history: %+v, %v", again, err)
	}
}
