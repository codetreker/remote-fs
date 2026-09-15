package httprest

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func windowsServerFixture(t *testing.T, limits FileLimits) (*Storage, *Handler, *objectstore.Storage) {
	t.Helper()
	meta, backend := memoryfixture.New(t, "windows-http", 1<<20, locking.DefaultOptions())
	options := DefaultHandlerOptions()
	options.Files = limits
	handler, err := NewHandlerWithOptions(backend, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	state, err := client.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewLockRequestID(state.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	activation, err := client.EnableWindows(t.Context(), action)
	if err != nil || !activation.Enabled || activation.State != storage.WindowsActionCompleted {
		t.Fatalf("enabling Windows semantics = %+v, %v", activation, err)
	}
	return client, handler, backend
}

func windowsServerSession(t *testing.T, client *Storage, options storage.FileSessionOptions) storage.WindowsSession {
	t.Helper()
	session, err := client.NewWindowsSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func windowsServerAction(t *testing.T, session storage.WindowsSession) storage.WindowsActionID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func windowsServerOpen(t *testing.T, session storage.WindowsSession, request storage.WindowsOpenRequest) storage.WindowsOpenResult {
	t.Helper()
	result, err := session.Open(t.Context(), request, windowsServerAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func windowsRootRequest() storage.WindowsOpenRequest {
	return storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsDirectory}}
}

func TestWindowsServerRetainsIdentityAndReconcilesNativeActions(t *testing.T) {
	client, _, backend := windowsServerFixture(t, DefaultFileLimits())
	session := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	root := windowsServerOpen(t, session, windowsRootRequest())
	request := storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate, Kind: storage.WindowsRegularFile},
		Lookup:            storage.WindowsLookup{ParentID: root.Attr.ID, ParentReference: root.File.Reference(), Name: "file"}, Mode: 0o640,
	}
	openID := windowsServerAction(t, session)
	opened, err := session.Open(t.Context(), request, openID)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := session.Open(t.Context(), request, openID)
	if err != nil || replayed.File.Reference() != opened.File.Reference() || replayed.Attr.ID != opened.Attr.ID {
		t.Fatalf("open replay = %+v, %v", replayed, err)
	}
	changed := request
	changed.Mode = 0o600
	if _, err := session.Open(t.Context(), changed, openID); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("reused open action accepted changed operands: %v", err)
	}
	writeID := windowsServerAction(t, session)
	first, err := opened.File.WriteAt(t.Context(), 0, []byte("first"), writeID)
	if err != nil || first.Attr.Size != 5 {
		t.Fatalf("first write = %+v, %v", first, err)
	}
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("replacement"), windowsServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	old, err := opened.File.WriteAt(t.Context(), 0, []byte("first"), writeID)
	if err != nil || old.Attr.Size != first.Attr.Size {
		t.Fatalf("write replay changed its original result: %+v, %v", old, err)
	}
	read, err := opened.File.ReadAt(t.Context(), 0, 32)
	if err != nil || string(read.Data) != "replacement" || read.Attr.ID != opened.Attr.ID {
		t.Fatalf("live read after replay = %+v, %v", read, err)
	}
	result, err := session.QueryAction(t.Context(), openID)
	if err != nil || result.File.Reference() != opened.File.Reference() {
		t.Fatalf("query open = %+v, %v", result, err)
	}
	rename := storage.WindowsRenameRequest{Source: request.Lookup, Destination: storage.WindowsLookup{ParentID: root.Attr.ID, ParentReference: root.File.Reference(), Name: "renamed"}}
	rename.Source.ExpectedID = opened.Attr.ID
	if _, err := opened.File.Rename(t.Context(), rename, windowsServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	attr, err := opened.File.Stat(t.Context())
	if err != nil || attr.NameInfo.Path != "renamed" || attr.ID != opened.Attr.ID {
		t.Fatalf("retained renamed metadata = %+v, %v", attr, err)
	}
	if body, err := backend.Read(t.Context(), "renamed"); err != nil || string(body) != "replacement" {
		t.Fatalf("authoritative renamed bytes = %q, %v", body, err)
	}
	if _, err := opened.File.Close(t.Context(), windowsServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	old, err = opened.File.WriteAt(t.Context(), 0, []byte("first"), writeID)
	if err != nil || old.Attr.Size != first.Attr.Size {
		t.Fatalf("closed-reference action replay = %+v, %v", old, err)
	}
	if _, err := opened.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed reference read = %v", err)
	}
}

func TestWindowsServerPendingCancellationAndPartialLockReceipt(t *testing.T) {
	client, _, backend := windowsServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	a := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	b := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: root.ID, Name: "file"}}
	fa, fb := windowsServerOpen(t, a, request).File, windowsServerOpen(t, b, request).File
	locked := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 0, Length: 2, Type: storage.Exclusive}}}
	if result, err := fa.LockBatch(t.Context(), locked, windowsServerAction(t, a)); err != nil || result.State != storage.WindowsActionCompleted {
		t.Fatalf("initial lock = %+v, %v", result, err)
	}
	pendingID := windowsServerAction(t, b)
	pending, err := fb.LockBatch(t.Context(), locked, pendingID)
	if err != nil || pending.State != storage.WindowsActionPending {
		t.Fatalf("waiting lock = %+v, %v", pending, err)
	}
	cancelled, err := b.CancelAction(t.Context(), pendingID)
	if err != nil || cancelled.State != storage.WindowsActionCancelled {
		t.Fatalf("cancel lock = %+v, %v", cancelled, err)
	}
	partial := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{
		{Offset: 0, Length: 2, Type: storage.Unlock},
		{Offset: 3, Length: 1, Type: storage.LockType(99)},
	}}
	partialID := windowsServerAction(t, a)
	result, err := fa.LockBatch(t.Context(), partial, partialID)
	if !errors.Is(err, syscall.EINVAL) || result.Applied != 1 || result.Errno != syscall.EINVAL {
		t.Fatalf("partial lock failure lost known effects: %+v, %v", result, err)
	}
	queried, err := a.QueryAction(t.Context(), partialID)
	if !errors.Is(err, syscall.EINVAL) || queried.Applied != result.Applied || queried.Errno != result.Errno {
		t.Fatalf("query partial failure = %+v, %v", queried, err)
	}
	replayed, err := fb.LockBatch(t.Context(), locked, pendingID)
	if err != nil || replayed.State != storage.WindowsActionCancelled {
		t.Fatalf("cancelled action was reissued as a new acquisition: %+v, %v", replayed, err)
	}
	if read, err := fb.ReadAt(t.Context(), 0, 2); err != nil || !bytes.Equal(read.Data, []byte("co")) {
		t.Fatalf("partial unlock did not release its completed range: %+v, %v", read, err)
	}
}

func TestWindowsServerSharingFailureReleasesTransportOpenReservation(t *testing.T) {
	client, handler, backend := windowsServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	a := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	b := windowsServerSession(t, client, options)
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsReadData | storage.WindowsWriteData | storage.WindowsReadAttributes, Share: storage.WindowsShareRead, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: root.ID, Name: "file"}}
	fa := windowsServerOpen(t, a, request).File
	request.Share = storage.WindowsShareAll
	for range 3 {
		id := windowsServerAction(t, b)
		_, err := b.Open(t.Context(), request, id)
		if !errors.Is(err, syscall.EACCES) || storage.WindowsFailureOf(err) != storage.WindowsSharingViolation {
			t.Fatalf("sharing refusal = %v", err)
		}
		result, err := b.QueryAction(t.Context(), id)
		if !errors.Is(err, syscall.EACCES) || result.State != storage.WindowsActionRejected || result.Failure != storage.WindowsSharingViolation {
			t.Fatalf("sharing receipt = %+v, %v", result, err)
		}
	}
	remote := b.(*remoteWindowsSession)
	handler.windows.mu.Lock()
	served := handler.windows.sessions[remote.id]
	handler.windows.mu.Unlock()
	served.mu.Lock()
	retained := len(served.files)
	served.mu.Unlock()
	if retained != 0 {
		t.Fatalf("rejected opens retained %d transport references", retained)
	}
	if _, err := fa.Close(t.Context(), windowsServerAction(t, a)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Open(t.Context(), request, windowsServerAction(t, b)); err != nil {
		t.Fatalf("released open capacity was not reusable: %v", err)
	}
}

func TestWindowsServerBoundsActionsAndRetainsCleanupCapacity(t *testing.T) {
	limits := DefaultFileLimits()
	limits.MaxSessions, limits.MaxActions, limits.MaxCleanupActions = 1, 1, 1
	client, handler, backend := windowsServerFixture(t, limits)
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	if _, err := client.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("session admission above its cap = %v", err)
	}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: root.ID, Name: "file"}}
	opened := windowsServerOpen(t, session, request)
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("refused"), windowsServerAction(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("data action above its cap = %v", err)
	}
	if body, err := backend.Read(t.Context(), "file"); err != nil || len(body) != 0 {
		t.Fatalf("refused action changed bytes: %q, %v", body, err)
	}
	closeID := windowsServerAction(t, session)
	if _, err := opened.File.Close(t.Context(), closeID); err != nil {
		t.Fatalf("full data history blocked cleanup: %v", err)
	}
	if _, err := opened.File.Close(t.Context(), closeID); err != nil {
		t.Fatalf("cleanup replay failed: %v", err)
	}
	remote := session.(*remoteWindowsSession)
	handler.windows.mu.Lock()
	served := handler.windows.sessions[remote.id]
	handler.windows.mu.Unlock()
	served.mu.Lock()
	if served.dataActions != 1 || served.cleanupActions != 1 || len(served.files) != 1 {
		t.Errorf("registry counts: data=%d cleanup=%d refs=%d", served.dataActions, served.cleanupActions, len(served.files))
	}
	served.mu.Unlock()
	if !validFileCapability(opened.File.Reference()) {
		t.Fatal("file reference is not a bounded opaque capability")
	}
}

type windowsLostOpenBackend struct {
	storage.WindowsStorage
	opened chan struct{}
}

func (b *windowsLostOpenBackend) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	session, err := b.WindowsStorage.NewWindowsSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &windowsLostOpenSession{WindowsSession: session, opened: b.opened}, nil
}

type windowsLostOpenSession struct {
	storage.WindowsSession
	opened chan struct{}
}

func (s *windowsLostOpenSession) Open(ctx context.Context, request storage.WindowsOpenRequest, action storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	result, err := s.WindowsSession.Open(ctx, request, action)
	if err != nil {
		return result, err
	}
	close(s.opened)
	<-ctx.Done()
	return storage.WindowsOpenResult{}, ctx.Err()
}

func TestWindowsServerCancelledOpenIsResolvedUnderItsOriginalAction(t *testing.T) {
	client, handler, backend := windowsServerFixture(t, DefaultFileLimits())
	opened := make(chan struct{})
	handler.windows.backend = &windowsLostOpenBackend{WindowsStorage: backend, opened: opened}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := windowsServerSession(t, client, options)
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate}, Lookup: storage.WindowsLookup{ParentID: root.ID, Name: "file"}, Mode: 0o600}
	action := windowsServerAction(t, session)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.Open(ctx, request, action)
		done <- err
	}()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("native open did not finish")
	}
	cancel()
	select {
	case err := <-done:
		if storage.ErrnoOf(err) != syscall.EIO {
			t.Fatalf("open response cancellation claimed a known no-effect outcome: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled open did not return")
	}
	result, err := session.QueryAction(t.Context(), action)
	if err != nil || result.State != storage.WindowsActionCompleted || result.File == nil {
		t.Fatalf("original open receipt = %+v, %v", result, err)
	}
	replayed, err := session.Open(t.Context(), request, action)
	if err != nil || replayed.File.Reference() != result.File.Reference() {
		t.Fatalf("resolved open replay = %+v, %v", replayed, err)
	}
	if _, err := result.File.WriteAt(t.Context(), 0, []byte("recovered"), windowsServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if body, err := backend.Read(t.Context(), "file"); err != nil || string(body) != "recovered" {
		t.Fatalf("reconciled file bytes = %q, %v", body, err)
	}
}

type windowsCleanupIdentity struct{}

type windowsCleanupProbe struct {
	storage.WindowsSession
	mu       sync.Mutex
	err      error
	identity any
	ended    error
	calls    int
}

func (s *windowsCleanupProbe) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.identity, s.ended = ctx.Value(windowsCleanupIdentity{}), ctx.Err()
	return s.err
}

func TestWindowsRegistryClosePreservesCleanupOwnershipAndIdentity(t *testing.T) {
	failure := errors.New("native session cleanup failed")
	for _, cause := range []error{nil, failure} {
		name := "success"
		if cause != nil {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			registry := newWindowsRegistry(nil, DefaultFileLimits())
			cleanup := context.WithValue(context.Background(), windowsCleanupIdentity{}, "authenticated member")
			lifetime, cancel := context.WithCancelCause(cleanup)
			native := &windowsCleanupProbe{err: cause}
			session := &servedWindowsSession{native: native, options: storage.DefaultFileSessionOptions(), lifetime: lifetime, cancel: cancel, cleanup: cleanup, expires: time.Now().Add(time.Minute), files: make(map[string]*servedWindowsFile), references: make(map[string]string), actions: make(map[storage.WindowsActionID]*servedWindowsAction)}
			registry.sessions["owned"] = session
			registry.stop()
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			err := registry.wait(ctx)
			if !errors.Is(err, cause) {
				t.Fatalf("cleanup result = %v, want %v", err, cause)
			}
			native.mu.Lock()
			if native.calls != 1 || native.identity != "authenticated member" || native.ended != nil {
				t.Errorf("cleanup calls=%d identity=%v context=%v", native.calls, native.identity, native.ended)
			}
			native.mu.Unlock()
			registry.mu.Lock()
			retained := registry.sessions["owned"] != nil
			registry.mu.Unlock()
			if retained != (cause != nil) {
				t.Fatalf("cleanup ownership retained=%v, error=%v", retained, cause)
			}
			if lifetime.Err() == nil || !session.retired {
				t.Fatal("cleanup did not fence the session lifetime")
			}
		})
	}
}

func TestWindowsServerRejectsUnobservableVolumeBeforeActivation(t *testing.T) {
	for _, test := range []struct {
		name        string
		log         bool
		body, frame int64
		want        syscall.Errno
	}{
		{name: "missing log", want: syscall.EOPNOTSUPP},
		{name: "frame too small", log: true, frame: 1024, want: syscall.EFBIG},
		{name: "body too small for reconciliation", log: true, body: windowsControlResponseLimit() - 1, want: syscall.EFBIG},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta, backend := memoryfixture.New(t, "windows-observation", 1<<20, locking.DefaultOptions())
			options := DefaultHandlerOptions()
			if test.body != 0 {
				options.MaxBodyBytes = test.body
			}
			if test.frame != 0 {
				options.MaxFrameBytes = test.frame
			}
			var handler *Handler
			var err error
			if test.log {
				handler, err = NewHandlerWithOptions(backend, meta, options)
			} else {
				handler, err = NewHandlerWithOptions(backend, nil, options)
			}
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				handler.Stop()
				server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := handler.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			if err := client.CheckWindowsStorage(); !errors.Is(err, test.want) {
				t.Fatalf("unobservable Windows capability = %v, want %v", err, test.want)
			}
			state, err := backend.WindowsState(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			id, err := storage.NewLockRequestID(state.ActionEpoch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.EnableWindows(t.Context(), id); !errors.Is(err, test.want) {
				t.Fatalf("unobservable Windows activation = %v, want %v", err, test.want)
			}
			if _, err := client.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, test.want) {
				t.Fatalf("unobservable Windows enrollment = %v, want %v", err, test.want)
			}
			state, err = backend.WindowsState(t.Context())
			if err != nil || state.Enabled {
				t.Fatalf("refused activation changed naming policy: %+v, %v", state, err)
			}
		})
	}
}

func TestWindowsServerSessionLimitRefusalPreservesEINVAL(t *testing.T) {
	limits := DefaultFileLimits()
	limits.Session.MaxFiles = 1
	client, handler, _ := windowsServerFixture(t, limits)
	requested := storage.DefaultFileSessionOptions()
	requested.MaxFiles = 2
	if _, err := client.NewWindowsSession(t.Context(), requested); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("server session limit was not an argument refusal: %v", err)
	}
	handler.windows.mu.Lock()
	defer handler.windows.mu.Unlock()
	if handler.windows.enrolling != 0 || len(handler.windows.sessions) != 0 {
		t.Fatal("refused session options reached native enrollment")
	}
}

func TestWindowsServerReconcilesAnOpenWithLargeAuthoritativeName(t *testing.T) {
	client, _, backend := windowsServerFixture(t, DefaultFileLimits())
	segment := strings.Repeat("&", 240)
	parent := ""
	for range DefaultMaxLockControlBytes/(6*int64(len(segment))) + 2 {
		parent = path.Join(parent, segment)
		if err := backend.Mkdir(t.Context(), parent); err != nil {
			t.Fatal(err)
		}
	}
	attr, err := backend.Stat(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	session := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate}, Lookup: storage.WindowsLookup{ParentID: attr.ID, Name: "file"}, Mode: 0o600}
	id := windowsServerAction(t, session)
	opened, err := session.Open(t.Context(), request, id)
	if err != nil {
		t.Fatal(err)
	}
	queried, err := session.QueryAction(t.Context(), id)
	if err != nil || queried.File == nil || queried.File.Reference() != opened.File.Reference() || queried.Attr.NameInfo != opened.Attr.NameInfo {
		t.Fatalf("a completed open cannot be reconciled with its full authoritative name: state=%v file=%v name_bytes=%d error=%v", queried.State, queried.File, len(opened.Attr.NameInfo.Path), err)
	}
}

func TestWindowsServerControlsRemainAvailableUnderDataPressure(t *testing.T) {
	client, handler, _ := windowsServerFixture(t, DefaultFileLimits())
	session := windowsServerSession(t, client, storage.DefaultFileSessionOptions())
	for _, admission := range []*bodyAdmission{client.responses, handler.responses} {
		release, err := admission.acquire(t.Context(), admission.maxBytes)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if status, err := session.Status(ctx); err != nil || status.Retired || status.Remaining <= 0 {
		t.Fatalf("data pressure blocked independent Windows control: %+v, %v", status, err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatalf("data pressure blocked session cleanup: %v", err)
	}
}

func TestWindowsServerFileCapacityReusesOnlyClosedReferences(t *testing.T) {
	client, _, backend := windowsServerFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := windowsServerSession(t, client, options)
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: root.ID, Name: "file"}}
	first := windowsServerOpen(t, session, request)
	if _, err := session.Open(t.Context(), request, windowsServerAction(t, session)); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("open above the reference bound = %v", err)
	}
	if _, err := first.File.Close(t.Context(), windowsServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	second := windowsServerOpen(t, session, request)
	if second.File.Reference() == first.File.Reference() || second.Attr.ID != first.Attr.ID {
		t.Fatal("a new open reused a closed capability or changed the retained object")
	}
	if _, err := first.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("a closed capability reached the replacement reference: %v", err)
	}
}

func TestWindowsServerDoesNotInventAnOmittedNativeActionError(t *testing.T) {
	lifetime, cancel := context.WithCancelCause(t.Context())
	defer cancel(context.Canceled)
	session := &servedWindowsSession{lifetime: lifetime, cancel: cancel}
	id, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	result := storage.WindowsActionResult{Action: id, State: storage.WindowsActionRejected, Errno: syscall.EACCES, Failure: storage.WindowsSharingViolation}
	response, err := windowsResultResponse(session, emptyWindowsResponse(), result, nil)
	if storage.ErrnoOf(err) != syscall.EIO || errors.Is(err, syscall.EACCES) || response.Action != nil {
		t.Fatalf("inconsistent native result became a fabricated action failure: %+v, %v", response, err)
	}
	if lifetime.Err() == nil || !session.retired {
		t.Fatal("an inconsistent native authority remained usable")
	}
}
