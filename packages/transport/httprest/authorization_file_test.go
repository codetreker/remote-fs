package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type fileAuthorizationIdentity struct{}

type fileAuthorizationPolicy struct {
	mu         sync.Mutex
	err        error
	requests   []authz.AccessRequest
	identities []any
}

func (p *fileAuthorizationPolicy) Authorize(ctx context.Context, request authz.AccessRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request)
	p.identities = append(p.identities, ctx.Value(fileAuthorizationIdentity{}))
	return p.err
}

func (p *fileAuthorizationPolicy) reset(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
	p.requests = nil
	p.identities = nil
}

func fileAuthorizationFixture(t *testing.T, policy *fileAuthorizationPolicy, limits FileLimits) (*Handler, *objectstore.Storage) {
	t.Helper()
	_, backend := memoryfixture.New(t, "backend-name-is-not-authority", 1<<20, locking.DefaultOptions())
	options := DefaultHandlerOptions()
	options.Volume = "trusted-volume"
	options.Authorizer = policy
	options.Files = limits
	h, err := NewHandlerWithOptions(backend, nil, options)
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

func fileAuthorizationRequest(t *testing.T, h *Handler, request fileRequest) *httptest.ResponseRecorder {
	t.Helper()
	if request.Path == nil {
		request.Path = []byte{}
	}
	if request.Data == nil {
		request.Data = []byte{}
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	op := OpFile
	if fileControl(request.Op) {
		op = OpFileControl
	}
	r := httptest.NewRequest(http.MethodPost, Prefix+string(op), bytes.NewReader(body))
	r.Header.Set("Content-Type", contentJSON)
	r = r.WithContext(context.WithValue(r.Context(), fileAuthorizationIdentity{}, "member"))
	answer := httptest.NewRecorder()
	h.ServeHTTP(answer, r)
	return answer
}

func fileAuthorizationSuccess(t *testing.T, h *Handler, request fileRequest) fileResponse {
	t.Helper()
	answer := fileAuthorizationRequest(t, h, request)
	if answer.Code != http.StatusOK {
		t.Fatalf("file %s returned %d: %s", request.Op, answer.Code, answer.Body.String())
	}
	var response fileResponse
	if err := json.Unmarshal(answer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func fileAuthorizationDenied(t *testing.T, answer *httptest.ResponseRecorder, errno, message string) {
	t.Helper()
	if answer.Code != StatusStorageError {
		t.Fatalf("authorization status=%d body=%s", answer.Code, answer.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(answer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response) != 2 || response["errno"] != errno || response["message"] != message {
		t.Fatalf("authorization exposed a capability or native receipt: %+v", response)
	}
}

func TestEveryFileOperationAuthorizesBeforeCapabilityLookup(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	h, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	action, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	fullOpen := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Truncate: true, Exclusive: true}, Mode: 0o600}
	for _, req := range []fileRequest{
		{Op: authz.FileSessionOpen, Options: storage.DefaultFileSessionOptions()},
		{Op: authz.FileStatus},
		{Op: authz.FileRenew},
		{Op: authz.FileSessionClose},
		{Op: authz.FileStatNode, Node: 71},
		{Op: authz.FileSetNodeAttr, Node: 71, Change: &AttrChange{}},
		{Op: authz.FileOpen, Path: []byte("file"), Open: fullOpen},
		{Op: authz.FileOpenNode, Node: 71, Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Truncate: true}}},
		{Op: authz.FileAck},
		{Op: authz.FileStat},
		{Op: authz.FileRead, Length: 8},
		{Op: authz.FileWrite, Data: []byte("patch")},
		{Op: authz.FileTruncate, Offset: 2},
		{Op: authz.FileSetAttr, Change: &AttrChange{}},
		{Op: authz.FileSync},
		{Op: authz.FileGetLock, Owner: 13, Lock: lock},
		{Op: authz.FileSetLock, Owner: 13, Lock: lock, LockID: action},
		{Op: authz.FileUnlock, Owner: 13, Lock: storage.FileLock{Family: storage.Flock, Type: storage.Unlock, End: math.MaxInt64}, LockID: action},
		{Op: authz.FileQueryLock, Owner: 13, LockID: action},
		{Op: authz.FileCancelLock, Owner: 13, LockID: action},
		{Op: authz.FileDropLocks, Owner: 13, Family: storage.Flock},
		{Op: authz.FileClose},
	} {
		t.Run(string(req.Op), func(t *testing.T) {
			policy.reset(authz.ErrDenied)
			if req.Op != authz.FileSessionOpen {
				req.Session = strings.Repeat("a", 64)
			}
			if fileActionRequired(req.Op) {
				req.Action = action
			}
			switch req.Op {
			case authz.FileStat, authz.FileRead, authz.FileWrite, authz.FileTruncate, authz.FileSetAttr, authz.FileSync, authz.FileAck, authz.FileClose, authz.FileGetLock, authz.FileSetLock, authz.FileUnlock, authz.FileQueryLock, authz.FileCancelLock, authz.FileDropLocks:
				req.File = strings.Repeat("b", 64)
			}
			fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, req), "EACCES", "access denied")
			policy.mu.Lock()
			defer policy.mu.Unlock()
			want := authz.AccessRequest{Volume: "trusted-volume", Operation: req.Op}
			if req.Op == authz.FileOpen || req.Op == authz.FileOpenNode {
				want.Open = req.Open.OpenAccess
			}
			if len(policy.requests) != 1 || policy.requests[0] != want || policy.identities[0] != "member" {
				t.Fatalf("authorization=%+v identities=%v; want %+v", policy.requests, policy.identities, want)
			}
		})
	}
	h.files.mu.Lock()
	defer h.files.mu.Unlock()
	if len(h.files.sessions) != 0 || h.files.enrolling != 0 || h.files.running {
		t.Fatal("denied file requests allocated or started the registry")
	}
}

func TestInvalidFileArgumentsDoNotReachAuthorization(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	h, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	action, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	mode := uint32(fs.ModeDir | 0o600)
	tooLarge := storage.DefaultFileSessionOptions()
	tooLarge.MaxFiles++
	for _, req := range []fileRequest{
		{Op: authz.FileSessionOpen},
		{Op: authz.FileSessionOpen, Options: tooLarge},
		{Op: authz.FileOpen, Path: []byte("file")},
		{Op: authz.FileOpen, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Truncate: true}}},
		{Op: authz.FileOpen, Path: []byte("../outside"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}},
		{Op: authz.FileOpenNode, Node: 1, Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Write: true, Create: true}}},
		{Op: authz.FileOpenNode, Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}},
		{Op: authz.FileRead, Offset: -1, Length: 1},
		{Op: authz.FileWrite, Offset: math.MaxInt64, Data: []byte("x")},
		{Op: authz.FileTruncate, Offset: -1},
		{Op: authz.FileSetAttr, Change: &AttrChange{Mode: &mode}},
		{Op: authz.FileGetLock, Lock: storage.FileLock{Family: storage.Flock, Type: storage.Unlock, End: math.MaxInt64}},
		{Op: authz.FileSetLock, Lock: storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}, LockID: "invalid"},
		{Op: authz.FileSetLock, Lock: storage.FileLock{Family: storage.Flock, Type: storage.Unlock, End: math.MaxInt64}, LockID: action},
		{Op: authz.FileUnlock, Lock: storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}, LockID: action},
		{Op: authz.FileQueryLock, LockID: "invalid"},
		{Op: authz.FileDropLocks, Family: 99},
		{Op: "unknown"},
		{Op: "open", Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}},
	} {
		if req.Op != authz.FileSessionOpen {
			req.Session = strings.Repeat("a", 64)
		}
		if fileActionRequired(req.Op) {
			req.Action = action
		}
		switch req.Op {
		case authz.FileRead, authz.FileWrite, authz.FileTruncate, authz.FileSetAttr, authz.FileGetLock, authz.FileSetLock, authz.FileUnlock, authz.FileQueryLock, authz.FileDropLocks:
			req.File = strings.Repeat("b", 64)
		}
		answer := fileAuthorizationRequest(t, h, req)
		if answer.Code == http.StatusOK || strings.Contains(answer.Body.String(), "access denied") {
			t.Fatalf("invalid %s bypassed parameter validation: %d %s", req.Op, answer.Code, answer.Body.String())
		}
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	if len(policy.requests) != 0 {
		t.Fatalf("invalid requests reached policy: %+v", policy.requests)
	}
}

func TestFileAuthorizationUsesTrustedFailureFieldsWithoutNativeReceipts(t *testing.T) {
	secret := "private policy details"
	for _, test := range []struct {
		cause          error
		errno, message string
	}{
		{errors.Join(authz.ErrDenied, &locking.Error{Code: locking.Conflict, Recorded: true, Message: secret}), "EACCES", "access denied"},
		{fmt.Errorf("%s: %w", secret, syscall.ENOSPC), "EIO", "authorization failed"},
	} {
		policy := &fileAuthorizationPolicy{err: test.cause}
		h, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
		for _, op := range []authz.Operation{authz.FileSessionOpen, authz.FileStatus} {
			req := fileRequest{Op: op}
			if op == authz.FileSessionOpen {
				req.Options = storage.DefaultFileSessionOptions()
			} else {
				req.Session = strings.Repeat("a", 64)
			}
			fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, req), test.errno, test.message)
		}
	}
}

func TestDeniedFileActionsDoNotMutateOrExposeRetainedReceipts(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	session := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileSessionOpen, Options: storage.DefaultFileSessionOptions()})
	action, err := storage.NewLockRequestID(session.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	req := fileRequest{Op: authz.FileOpen, Session: session.Session, Action: action, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}, Mode: 0o777}}
	opened := fileAuthorizationSuccess(t, h, req)
	h.files.mu.Lock()
	served := h.files.sessions[session.Session]
	h.files.mu.Unlock()
	served.mu.Lock()
	receipt := served.actions[action]
	pending := served.files[opened.File].pending
	expires := served.expires
	served.mu.Unlock()
	policy.reset(authz.ErrDenied)
	for _, denied := range []fileRequest{
		req,
		{Op: authz.FileAck, Session: session.Session, File: opened.File},
		{Op: authz.FileRenew, Session: session.Session},
		{Op: authz.FileStatus, Session: session.Session},
		{Op: authz.FileClose, Session: session.Session, File: opened.File},
		{Op: authz.FileSessionClose, Session: session.Session},
	} {
		fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, denied), "EACCES", "access denied")
	}
	served.mu.Lock()
	if served.actions[action] != receipt || len(served.actions) != 1 || len(served.files) != 1 || served.retired ||
		served.files[opened.File].closing || !served.files[opened.File].pending.Equal(pending) || !served.expires.Equal(expires) {
		served.mu.Unlock()
		t.Fatal("denial touched a reference, lease, or previously recorded outcome")
	}
	served.mu.Unlock()
	policy.reset(nil)
	replayed := fileAuthorizationSuccess(t, h, req)
	if replayed.File != opened.File {
		t.Fatalf("allowed replay allocated a replacement: %+v; want %+v", replayed, opened)
	}
	fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileAck, Session: session.Session, File: opened.File})
	writeAction, err := storage.NewLockRequestID(session.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	write := fileRequest{Op: authz.FileWrite, Session: session.Session, File: opened.File, Action: writeAction, Data: []byte("altered!")}
	policy.reset(authz.ErrDenied)
	fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, write), "EACCES", "access denied")
	served.mu.Lock()
	_, recorded := served.actions[writeAction]
	served.mu.Unlock()
	if recorded {
		t.Fatal("denied write reserved an action receipt")
	}
	content, err := backend.Read(t.Context(), "file")
	if err != nil || string(content) != "original" {
		t.Fatalf("denied write changed backing content: %q, %v", content, err)
	}
	policy.reset(nil)
	fileAuthorizationSuccess(t, h, write)
	content, err = backend.Read(t.Context(), "file")
	if err != nil || string(content) != "altered!" {
		t.Fatalf("allowed retry did not execute its original action: %q, %v", content, err)
	}
}

func TestDeniedAdvisoryCleanupPreservesTheActualGrantAndActionHistory(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	h, backend := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	session := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileSessionOpen, Options: storage.DefaultFileSessionOptions()})
	openAction, err := storage.NewLockRequestID(session.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	opened := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileOpen, Session: session.Session, Action: openAction, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}})
	fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileAck, Session: session.Session, File: opened.File})
	grantID, err := storage.NewLockRequestID(session.Status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	acquire := fileRequest{Op: authz.FileSetLock, Session: session.Session, File: opened.File, Owner: 17, Lock: lock, LockID: grantID}
	granted := fileAuthorizationSuccess(t, h, acquire)
	if granted.Attempt == nil || granted.Attempt.State != storage.LockGranted {
		t.Fatalf("readonly descriptor failed exclusive flock: %+v", granted)
	}
	unlockID, err := storage.NewLockRequestID(session.Status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	dropAction, err := storage.NewLockRequestID(session.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	policy.reset(authz.ErrDenied)
	for _, request := range []fileRequest{
		acquire,
		{Op: authz.FileQueryLock, Session: session.Session, File: opened.File, Owner: 17, LockID: grantID},
		{Op: authz.FileCancelLock, Session: session.Session, File: opened.File, Owner: 17, LockID: grantID},
		{Op: authz.FileUnlock, Session: session.Session, File: opened.File, Owner: 17, LockID: unlockID, Lock: storage.FileLock{Family: storage.Flock, Type: storage.Unlock, End: math.MaxInt64}},
		{Op: authz.FileDropLocks, Session: session.Session, File: opened.File, Owner: 17, Family: storage.Flock, Action: dropAction},
	} {
		fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, request), "EACCES", "access denied")
	}
	policy.reset(nil)
	conflict := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileGetLock, Session: session.Session, File: opened.File, Owner: 18, Lock: lock})
	if conflict.Conflict == nil || !conflict.Conflict.Found {
		t.Fatalf("denied cleanup released the native grant: %+v", conflict)
	}
	query := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileQueryLock, Session: session.Session, File: opened.File, Owner: 17, LockID: grantID})
	if query.Attempt == nil || query.Attempt.State != storage.LockGranted || !query.Attempt.EverGranted {
		t.Fatalf("denial rewrote original grant history: %+v", query)
	}
	unknown := fileAuthorizationRequest(t, h, fileRequest{Op: authz.FileQueryLock, Session: session.Session, File: opened.File, Owner: 17, LockID: unlockID})
	var failure ErrorResponse
	if err := json.Unmarshal(unknown.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if unknown.Code != StatusStorageError || failure.Errno != "ESTALE" {
		t.Fatalf("denied unlock gained a native receipt: %d %+v", unknown.Code, failure)
	}
	h.files.mu.Lock()
	served := h.files.sessions[session.Session]
	h.files.mu.Unlock()
	served.mu.Lock()
	_, recorded := served.actions[dropAction]
	served.mu.Unlock()
	if recorded {
		t.Fatal("denied drop-locks reserved cleanup history")
	}
}

func TestDeniedFileCloseStillAllowsInternalLeaseCleanup(t *testing.T) {
	policy := &fileAuthorizationPolicy{}
	limits := DefaultFileLimits()
	limits.Session.Lease = 500 * time.Millisecond
	limits.PendingAck = 250 * time.Millisecond
	h, backend := fileAuthorizationFixture(t, policy, limits)
	if err := backend.Write(t.Context(), "file", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	session := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileSessionOpen, Options: limits.Session})
	action, err := storage.NewLockRequestID(session.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	opened := fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileOpen, Session: session.Session, Action: action, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}})
	fileAuthorizationSuccess(t, h, fileRequest{Op: authz.FileAck, Session: session.Session, File: opened.File})
	if err := backend.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	policy.reset(authz.ErrDenied)
	fileAuthorizationDenied(t, fileAuthorizationRequest(t, h, fileRequest{Op: authz.FileClose, Session: session.Session, File: opened.File}), "EACCES", "access denied")
	if used, err := backend.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("denied close released retained bytes: %d, %v", used, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		used, err := backend.Usage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if used == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease expiry retained %d bytes after close authorization was revoked", used)
		}
		time.Sleep(10 * time.Millisecond)
	}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	if len(policy.requests) != 1 || policy.requests[0].Operation != authz.FileClose {
		t.Fatalf("internal cleanup asked for caller authorization: %+v", policy.requests)
	}
}
