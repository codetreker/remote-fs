package httprest

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type genericHistoryBackend struct {
	storage.FileStorage
	mu      sync.Mutex
	session *genericHistorySession
}

func (b *genericHistoryBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	native, status, err := b.FileStorage.NewFileSession(ctx, o)
	if err != nil {
		return nil, status, err
	}
	session := &genericHistorySession{FileSession: native}
	b.mu.Lock()
	b.session = session
	b.mu.Unlock()
	return session, status, nil
}

type genericHistorySession struct {
	storage.FileSession
	mu     sync.Mutex
	result storage.FileActionReceipt
	err    error
	calls  int
}

func (s *genericHistorySession) QueryAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result.Action != id {
		return s.FileSession.QueryAction(ctx, id)
	}
	s.calls++
	return s.result, s.err
}

func TestGenericFileHistoryQueriesPreserveNativePendingUnknownAndFailure(t *testing.T) {
	client, handler, backend := genericServerFixture(t, DefaultFileLimits())
	wrapped := &genericHistoryBackend{FileStorage: backend}
	handler.files.backend = wrapped
	session := genericServerSession(t, client, storage.DefaultFileSessionOptions())
	id := genericServerAction(t, session)
	wrapped.mu.Lock()
	native := wrapped.session
	wrapped.mu.Unlock()
	for _, state := range []storage.FileActionState{storage.FileActionPending, storage.FileActionUnknown, storage.FileActionNotApplied} {
		expected := storage.FileActionReceipt{Action: id, Operation: storage.OpFileWrite, State: state}
		var expectedErr error
		if state == storage.FileActionUnknown {
			expected.Errno = syscall.EIO
			expectedErr = syscall.EIO
		}
		if state == storage.FileActionNotApplied {
			expected.Errno = syscall.EACCES
			expectedErr = syscall.EACCES
		}
		native.mu.Lock()
		native.result = expected
		native.err = expectedErr
		native.mu.Unlock()
		received, err := session.QueryAction(t.Context(), id)
		if received.State != state || received.Effects != 0 || received.Action != id || received.Errno != expected.Errno || !errors.Is(err, expectedErr) {
			t.Fatalf("native state %v became %+v, %v", state, received, err)
		}
	}
	native.mu.Lock()
	calls := native.calls
	native.mu.Unlock()
	if calls != 3 {
		t.Fatalf("queries reached native history %d times", calls)
	}
}

func TestGenericFileHistoryRetainsClosedReceiptAndActionCapacity(t *testing.T) {
	client, _, backend := genericServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	options.MaxActions = 1
	session := genericServerSession(t, client, options)
	file, retained := genericServerRetain(t, session, attr.ID, storage.AccessClaim{Uses: storage.AllAccessUses})
	if _, err := file.Close(t.Context(), genericServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	queried, err := session.QueryAction(t.Context(), retained.Action)
	if err != nil || queried.Reference != retained.Reference || queried.Observation.Attr.ID != attr.ID || queried.HistoryRemaining <= 0 {
		t.Fatalf("closed retain history = %+v, %v", queried, err)
	}
	if _, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: attr.ID}, genericServerAction(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("retained history capacity was released: %v", err)
	}
	if _, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("history query resurrected a reference: %v", err)
	}
}
