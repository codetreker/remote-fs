package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func windowsAuthorizationFixture(t *testing.T, policy *fileAuthorizationPolicy) (*Handler, *objectstore.Storage) {
	t.Helper()
	meta, backend := memoryfixture.New(t, "backend-name-is-not-authority", 1<<20, locking.DefaultOptions())
	options := DefaultHandlerOptions()
	options.Volume, options.Authorizer = "trusted-volume", policy
	h, err := NewHandlerWithOptions(backend, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h, backend
}

func windowsAuthorizationID(t *testing.T, epoch uint64) storage.WindowsActionID {
	t.Helper()
	id, err := storage.NewLockRequestID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func windowsAuthorizationRequest(t *testing.T, h *Handler, ctx context.Context, request windowsRequest) *httptest.ResponseRecorder {
	t.Helper()
	if request.Data == nil {
		request.Data = []byte{}
	}
	if request.Ranges == nil {
		request.Ranges = []storage.WindowsLockRange{}
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	op := OpWindows
	if windowsControl(request.Op) {
		op = OpWindowsControl
	}
	r := httptest.NewRequest(http.MethodPost, Prefix+string(op), bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", contentJSON)
	answer := httptest.NewRecorder()
	h.ServeHTTP(answer, r)
	return answer
}

func windowsAuthorizationSuccess(t *testing.T, h *Handler, ctx context.Context, request windowsRequest) windowsResponse {
	t.Helper()
	answer := windowsAuthorizationRequest(t, h, ctx, request)
	if answer.Code != http.StatusOK {
		t.Fatalf("Windows %s returned %d: %s", request.Op, answer.Code, answer.Body)
	}
	var response windowsResponse
	if err := json.Unmarshal(answer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func windowsAuthorizationRequests(t *testing.T) []windowsRequest {
	t.Helper()
	session, file := strings.Repeat("a", 64), strings.Repeat("b", 64)
	action := windowsAuthorizationID(t, 1)
	options := storage.DefaultFileSessionOptions()
	return []windowsRequest{
		{Op: storage.OpWindowsState},
		{Op: storage.OpWindowsEnable, Action: action},
		{Op: storage.OpWindowsQueryActivation, Action: action},
		{Op: storage.OpWindowsSessionOpen, Options: &options},
		{Op: storage.OpWindowsSessionClose, Session: session},
		{Op: storage.OpWindowsRenew, Session: session},
		{Op: storage.OpWindowsStatus, Session: session},
		{Op: storage.OpWindowsOpen, Session: session, Action: action, Open: windowsOpenOf(storage.WindowsOpenRequest{
			WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareRead | storage.WindowsShareDelete, Disposition: storage.WindowsOverwriteIf, Kind: storage.WindowsRegularFile, DeleteOnClose: true, OpenReparsePoint: true},
			Lookup:            storage.WindowsLookup{ParentID: 1, Name: "file", ExpectedID: 7},
			Mode:              0o640,
			DOSAttributes:     storage.WindowsDOSArchive,
		})},
		{Op: storage.OpWindowsStat, Session: session, File: file},
		{Op: storage.OpWindowsRead, Session: session, File: file, Offset: 2, Length: 8},
		{Op: storage.OpWindowsWrite, Session: session, File: file, Action: action, Offset: 2, Data: []byte("patch")},
		{Op: storage.OpWindowsTruncate, Session: session, File: file, Action: action, Offset: 2},
		{Op: storage.OpWindowsSetAttr, Session: session, File: file, Action: action, Change: windowsAttrChangeOf(storage.WindowsAttrChange{})},
		{Op: storage.OpWindowsList, Session: session, File: file},
		{Op: storage.OpWindowsReadLink, Session: session, File: file},
		{Op: storage.OpWindowsSetLink, Session: session, File: file, Action: action, Target: "target"},
		{Op: storage.OpWindowsRename, Session: session, File: file, Action: action, Rename: &storage.WindowsRenameRequest{
			Source:      storage.WindowsLookup{ParentID: 1, Name: "file", ExpectedID: 7},
			Destination: storage.WindowsLookup{ParentID: 1, Name: "renamed", ExpectedID: 8},
			Replace:     true,
		}},
		{Op: storage.OpWindowsSetDeletePending, Session: session, File: file, Action: action, DeletePending: true},
		{Op: storage.OpWindowsLockBatch, Session: session, File: file, Action: action, Ranges: []storage.WindowsLockRange{{Offset: 1, Length: 2, Type: storage.Exclusive, FailImmediately: true}}},
		{Op: storage.OpWindowsSync, Session: session, File: file},
		{Op: storage.OpWindowsClose, Session: session, File: file, Action: action},
		{Op: storage.OpWindowsQueryAction, Session: session, Action: action},
		{Op: storage.OpWindowsCancelAction, Session: session, Action: action},
	}
}

type windowsAuthorizationBackend struct {
	storage.WindowsStorage
	calls int
}

func (b *windowsAuthorizationBackend) CheckWindowsStorage() error {
	b.calls++
	return b.WindowsStorage.CheckWindowsStorage()
}

func (b *windowsAuthorizationBackend) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	b.calls++
	return b.WindowsStorage.WindowsState(ctx)
}

func (b *windowsAuthorizationBackend) EnableWindows(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	b.calls++
	return b.WindowsStorage.EnableWindows(ctx, id)
}

func (b *windowsAuthorizationBackend) QueryWindowsActivation(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	b.calls++
	return b.WindowsStorage.QueryWindowsActivation(ctx, id)
}

func (b *windowsAuthorizationBackend) NewWindowsSession(ctx context.Context, options storage.FileSessionOptions) (storage.WindowsSession, error) {
	b.calls++
	return b.WindowsStorage.NewWindowsSession(ctx, options)
}

func TestEveryWindowsOperationAuthorizesBeforeNativeStateOrCapabilityLookup(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	h, backend := windowsAuthorizationFixture(t, policy)
	probe := &windowsAuthorizationBackend{WindowsStorage: backend}
	h.windows.backend = probe
	identity := &struct{ name string }{name: "member"}
	ctx := context.WithValue(t.Context(), fileAuthorizationIdentity{}, identity)
	for _, request := range windowsAuthorizationRequests(t) {
		t.Run(string(request.Op), func(t *testing.T) {
			policy.reset(authz.ErrDenied)
			fileAuthorizationDenied(t, windowsAuthorizationRequest(t, h, ctx, request), "EACCES", "access denied")
			want := authz.AccessRequest{Volume: "trusted-volume", Operation: request.Op}
			if request.Op == storage.OpWindowsOpen {
				want.WindowsOpen = request.Open.Intent
			}
			policy.mu.Lock()
			defer policy.mu.Unlock()
			if len(policy.requests) != 1 || policy.requests[0] != want || len(policy.identities) != 1 || policy.identities[0] != identity {
				t.Fatalf("authorization=%+v identities=%v; want %+v and original identity", policy.requests, policy.identities, want)
			}
		})
	}
	if probe.calls != 0 {
		t.Fatalf("denied requests made %d native calls", probe.calls)
	}
	h.windows.mu.Lock()
	defer h.windows.mu.Unlock()
	if len(h.windows.sessions) != 0 || h.windows.enrolling != 0 || h.windows.running {
		t.Fatal("denied Windows requests allocated or started the registry")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("request completion cancelled host context: %v", err)
	}
}

func TestWindowsAuthorizationRevocationPreservesReferencesReceiptsAndContent(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := windowsAuthorizationFixture(t, policy)
	ctx := context.WithValue(t.Context(), fileAuthorizationIdentity{}, "member")
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	state := windowsAuthorizationSuccess(t, h, ctx, windowsRequest{Op: storage.OpWindowsState})
	activation := windowsAuthorizationID(t, state.State.ActionEpoch)
	windowsAuthorizationSuccess(t, h, ctx, windowsRequest{Op: storage.OpWindowsEnable, Action: activation})
	options := storage.DefaultFileSessionOptions()
	session := windowsAuthorizationSuccess(t, h, ctx, windowsRequest{Op: storage.OpWindowsSessionOpen, Options: &options})
	openAction := windowsAuthorizationID(t, session.Status.ActionEpoch)
	open := windowsRequest{Op: storage.OpWindowsOpen, Session: session.Session, Action: openAction, Open: windowsOpenOf(storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsRegularFile},
		Lookup:            storage.WindowsLookup{ParentID: root.ID, Name: "file"},
	})}
	opened := windowsAuthorizationSuccess(t, h, ctx, open)
	h.windows.mu.Lock()
	served := h.windows.sessions[session.Session]
	h.windows.mu.Unlock()
	served.mu.Lock()
	receipt, reference := served.actions[openAction], served.files[opened.File]
	expires, revision := served.expires, served.revision
	served.mu.Unlock()
	if receipt == nil || reference == nil {
		t.Fatal("successful open retained neither its action nor its reference")
	}
	writeAction := windowsAuthorizationID(t, session.Status.ActionEpoch)
	write := windowsRequest{Op: storage.OpWindowsWrite, Session: session.Session, File: opened.File, Action: writeAction, Data: []byte("altered!")}
	requests := windowsAuthorizationRequests(t)
	for i := range requests {
		request := &requests[i]
		if request.Session != "" {
			request.Session = session.Session
		}
		if request.File != "" {
			request.File = opened.File
		}
		switch request.Op {
		case storage.OpWindowsOpen:
			*request = open
		case storage.OpWindowsWrite:
			*request = write
		case storage.OpWindowsEnable, storage.OpWindowsQueryActivation:
			request.Action = activation
		case storage.OpWindowsQueryAction, storage.OpWindowsCancelAction:
			request.Action = openAction
		default:
			if request.Action != "" {
				request.Action = windowsAuthorizationID(t, session.Status.ActionEpoch)
			}
		}
	}
	policy.reset(authz.ErrDenied)
	for _, request := range requests {
		fileAuthorizationDenied(t, windowsAuthorizationRequest(t, h, ctx, request), "EACCES", "access denied")
	}
	policy.mu.Lock()
	calls := len(policy.requests)
	policy.mu.Unlock()
	if calls != len(requests) {
		t.Fatalf("revoked requests consulted current policy %d times, want %d", calls, len(requests))
	}
	served.mu.Lock()
	unchanged := served.actions[openAction] == receipt && len(served.actions) == 1 && served.files[opened.File] == reference && len(served.files) == 1 && !reference.closed && !served.retired && served.expires.Equal(expires) && served.revision == revision
	served.mu.Unlock()
	if !unchanged {
		t.Fatal("denial changed the retained reference, action history, or lease")
	}
	content, err := backend.Read(t.Context(), "file")
	if err != nil || string(content) != "original" {
		t.Fatalf("denied Windows mutations changed content: %q, %v", content, err)
	}
	policy.reset(nil)
	replayed := windowsAuthorizationSuccess(t, h, ctx, open)
	if replayed.File != opened.File {
		t.Fatalf("allowed replay returned reference %q, want %q", replayed.File, opened.File)
	}
	queried := windowsAuthorizationSuccess(t, h, ctx, windowsRequest{Op: storage.OpWindowsQueryAction, Session: session.Session, Action: openAction})
	if queried.Action == nil || queried.Action.State != storage.WindowsActionCompleted || queried.Action.File != opened.File {
		t.Fatalf("denial changed the native open receipt: %+v", queried.Action)
	}
	written := windowsAuthorizationSuccess(t, h, ctx, write)
	if written.Action == nil || written.Action.State != storage.WindowsActionCompleted || written.Action.Action != writeAction {
		t.Fatalf("allowed retry did not complete the original write action: %+v", written.Action)
	}
	content, err = backend.Read(t.Context(), "file")
	if err != nil || string(content) != "altered!" {
		t.Fatalf("allowed Windows write did not publish content: %q, %v", content, err)
	}
}

func TestWindowsAuthorizationSanitizesNativeLookingPolicyFailures(t *testing.T) {
	private := "private policy details with a retained file capability"
	symlink := &storage.WindowsSymlinkError{
		WindowsSymlinkInfo: storage.WindowsSymlinkInfo{Target: "private/target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "private/link"}, Unparsed: "/secret"},
		Err:                syscall.ELOOP,
	}
	native := &storage.WindowsError{Failure: storage.WindowsSharingViolation, Err: fmt.Errorf("%s: %w", private, syscall.EACCES)}
	for _, test := range []struct {
		name, errno, message string
		cause                error
	}{
		{"denied symlink", "EACCES", "access denied", errors.Join(authz.ErrDenied, symlink)},
		{"failed symlink", "EIO", "authorization failed", symlink},
		{"denied native failure", "EACCES", "access denied", errors.Join(authz.ErrDenied, native)},
		{"failed native failure", "EIO", "authorization failed", native},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := &fileAuthorizationPolicy{err: test.cause}
			h, backend := windowsAuthorizationFixture(t, policy)
			probe := &windowsAuthorizationBackend{WindowsStorage: backend}
			h.windows.backend = probe
			for _, request := range windowsAuthorizationRequests(t) {
				t.Run(string(request.Op), func(t *testing.T) {
					fileAuthorizationDenied(t, windowsAuthorizationRequest(t, h, t.Context(), request), test.errno, test.message)
				})
			}
			if probe.calls != 0 {
				t.Fatalf("policy failure made %d native calls", probe.calls)
			}
		})
	}
}

func TestWindowsAuthorizationCancellationPreventsNativeEffects(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"callback allows", nil},
		{"callback denies", authz.ErrDenied},
		{"callback returns symlink", &storage.WindowsSymlinkError{WindowsSymlinkInfo: storage.WindowsSymlinkInfo{Target: "private/target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "private/link"}}, Err: syscall.ELOOP}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h, backend := windowsAuthorizationFixture(t, &fileAuthorizationPolicy{})
			probe := &windowsAuthorizationBackend{WindowsStorage: backend}
			h.windows.backend = probe
			identity := &struct{ name string }{name: "member"}
			host := context.WithValue(t.Context(), fileAuthorizationIdentity{}, identity)
			for _, request := range windowsAuthorizationRequests(t) {
				t.Run(string(request.Op), func(t *testing.T) {
					ctx, cancel := context.WithCancel(host)
					defer cancel()
					called := 0
					h.authorizer = authz.AuthorizerFunc(func(observed context.Context, access authz.AccessRequest) error {
						called++
						if observed.Value(fileAuthorizationIdentity{}) != identity || access.Volume != "trusted-volume" || access.Operation != request.Op {
							t.Fatalf("authorization changed caller or operation: %+v", access)
						}
						cancel()
						return test.err
					})
					fileAuthorizationDenied(t, windowsAuthorizationRequest(t, h, ctx, request), "EINTR", context.Canceled.Error())
					if called != 1 {
						t.Fatalf("cancellation callback ran %d times, want 1", called)
					}
				})
			}
			if probe.calls != 0 {
				t.Fatalf("cancelled authorization made %d native calls", probe.calls)
			}
			if err := host.Err(); err != nil {
				t.Fatalf("request cancellation ended host identity context: %v", err)
			}
		})
	}
}
