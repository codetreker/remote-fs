package smb

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type spaceClientSource struct {
	storage.FileStorage
	calls int
	err   error
}

func (s *spaceClientSource) Space(context.Context) (storage.Space, error) {
	s.calls++
	return storage.Space{Total: 100, Avail: 80, Used: 20}, s.err
}

type lifecycleClientSession struct {
	storage.FileSession
	renew  storage.FileSessionStatus
	err    error
	cancel func(storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *lifecycleClientSession) Renew(context.Context) (storage.FileSessionStatus, error) {
	return s.renew, s.err
}
func (s *lifecycleClientSession) CancelAction(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.cancel(id)
}

type lifecycleClientFile struct {
	storage.File
	syncCalls   int
	syncErr     error
	closeResult storage.FileActionReceipt
	closeErr    error
	closedID    storage.FileActionID
}

func (f *lifecycleClientFile) Sync(context.Context) error { f.syncCalls++; return f.syncErr }
func (f *lifecycleClientFile) Close(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f.closedID = id
	return f.closeResult, f.closeErr
}

func TestClientBackendSpaceAndRenewPreserveErrors(t *testing.T) {
	s, raw, _, _, _ := newClientTestSession(t)
	source := &spaceClientSource{FileStorage: s.backend.source}
	s.backend.source = source
	space, err := s.backend.Space(t.Context())
	if err != nil || space.Total != 100 || source.calls != 1 {
		t.Fatal(space, err, source.calls)
	}
	source.err = syscall.EIO
	if _, err = s.backend.Space(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	s.backend.authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
	if _, err = s.backend.Space(t.Context()); !errors.Is(err, authz.ErrDenied) || source.calls != 2 {
		t.Fatal(err, source.calls)
	}
	life := &lifecycleClientSession{FileSession: raw, renew: storage.FileSessionStatus{ActionEpoch: 18, Remaining: time.Minute}}
	s.raw = life
	status, err := s.Renew(t.Context())
	if err != nil || status.ActionEpoch != 18 || s.status.ActionEpoch != 18 {
		t.Fatal(status, err)
	}
	life.err = syscall.EIO
	if _, err = s.Renew(t.Context()); !errors.Is(err, syscall.EIO) || s.status.ActionEpoch != 18 {
		t.Fatal(err)
	}
}
func TestClientCancellationUsesCurrentAliasAndRejectsWrongReceipts(t *testing.T) {
	s, raw, _, _, _ := newClientTestSession(t)
	original := clientTestAction(t)
	actual := clientTestAction(t)
	if err := s.remember(original, clientAction{}); err != nil {
		t.Fatal(err)
	}
	s.actions[original].actual = actual
	life := &lifecycleClientSession{FileSession: raw}
	s.raw = life
	life.cancel = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		if id != actual {
			t.Fatal(id)
		}
		return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EINTR, HistoryRemaining: time.Minute}, syscall.EINTR
	}
	r, err := s.CancelAction(t.Context(), original)
	if r.Action != original || r.Receipt.Action != actual || !errors.Is(err, syscall.EINTR) || !s.actions[original].terminal {
		t.Fatal(r, err)
	}
	life.cancel = func(storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: original, State: storage.FileActionCompleted}, nil
	}
	if _, err = s.CancelAction(t.Context(), original); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	life.cancel = func(storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: actual, State: storage.FileActionUnknown}, nil
	}
	if _, err = s.CancelAction(t.Context(), original); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
}
func TestClientReferenceCleanupAndIdentityAuthorization(t *testing.T) {
	s, _, _, _, native := newClientTestSession(t)
	life := &lifecycleClientFile{File: native}
	f := &clientFile{session: s, raw: life}
	if err := f.Sync(t.Context()); err != nil || life.syncCalls != 1 {
		t.Fatal(err)
	}
	life.syncErr = syscall.EIO
	if err := f.Sync(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	s.principal = Principal{SID: "S-1-5-21-1"}
	s.backend.authorize = authz.AuthorizerFunc(func(ctx context.Context, r authz.AccessRequest) error {
		p, ok := PrincipalFromContext(ctx)
		if !ok || p.SID != s.principal.SID || r.Reference != 33 || r.Node != 3 {
			t.Fatal(p, r)
		}
		return authz.ErrDenied
	})
	if err := f.Sync(context.Background()); !errors.Is(err, authz.ErrDenied) || life.syncCalls != 2 {
		t.Fatal(err)
	}
	if err := f.Sync(WithPrincipal(t.Context(), Principal{SID: "S-1-5-21-2"})); !errors.Is(err, authz.ErrDenied) {
		t.Fatal(err)
	}
	id := clientTestAction(t)
	life.closeResult = storage.FileActionReceipt{State: storage.FileActionRetired, Reference: 33, Operation: storage.OpFileClose}
	r, err := f.Close(t.Context(), id)
	if err != nil || r.Action != id || r.Receipt.State != storage.FileActionRetired || life.closedID != id {
		t.Fatal(r, err)
	}
	life.closeErr = syscall.EIO
	if _, err = f.Close(t.Context(), id); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
}
func TestClientCleanupErrorsPreserveCauses(t *testing.T) {
	cause := errors.New("cleanup failed")
	err := cleanupFailed(cause)
	if err.Error() == "" || !errors.Is(err, cause) || !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	link := &windowsSymlinkError{windowsSymlinkInfo: windowsSymlinkInfo{Target: "target"}, Err: syscall.ELOOP}
	if link.Error() == "" || !errors.Is(link, syscall.ELOOP) {
		t.Fatal(link)
	}
}

func TestClientBackendPinsAuthorityIdentityAndCapabilities(t *testing.T) {
	for _, field := range []string{"volume", "root", "events"} {
		t.Run(field, func(t *testing.T) {
			source := &clientTestSource{state: storage.FileVolumeState{VolumeIdentity: "authority:1", RootID: 1, MaxEventBytes: 1024}}
			backend := &clientBackend{source: source}
			original, err := backend.State(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			switch field {
			case "volume":
				source.state.VolumeIdentity = "other:1"
			case "root":
				source.state.RootID = 2
			case "events":
				source.state.MaxEventBytes = 2048
			}
			if _, err = backend.State(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			source.state = storage.FileVolumeState{VolumeIdentity: original.VolumeIdentity, RootID: original.RootID, MaxEventBytes: original.MaxEventBytes}
			if _, err = backend.State(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatal("identity continuity resumed", err)
			}
		})
	}
}
