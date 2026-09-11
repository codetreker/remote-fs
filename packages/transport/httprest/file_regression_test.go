package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func openRetainedFixture(t *testing.T, client *Storage) (*remoteFileSession, *remoteFile) {
	t.Helper()
	s, err := client.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	f, err := s.OpenFile(context.Background(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	return s.(*remoteFileSession), f.(*remoteFile)
}

func TestRetainedHTTPRejectsMissingZeroValuedRequestMembers(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	action, err := storage.NewLockRequestID(session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	complete := fileRequest{Op: storage.OpFileTruncate, Session: session.id, File: file.id, Action: action, Offset: 3, Path: []byte{}, Data: []byte{}}
	encoded, err := json.Marshal(complete)
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range []string{"offset", "owner", "length", "data", "options", "open", "lock"} {
		t.Run(missing, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, missing)
			body, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			u, err := (Request{Op: OpFile}).URL(client.base)
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", contentJSON)
			response, err := client.http.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("missing %s status = %s", missing, response.Status)
			}
			contents, err := backend.Read(ctx, "file")
			if err != nil || string(contents) != "preserve" {
				t.Fatalf("malformed truncate changed data: %q, %v", contents, err)
			}
		})
	}
}

type fileRoundTripFunc func(*http.Request) (*http.Response, error)

func (f fileRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRetainedHTTPRejectsIncompleteAdvisoryReceipts(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	original := client.http.Transport
	for _, field := range []string{"Found", "Owner", "Lock"} {
		t.Run("conflict-"+field, func(t *testing.T) {
			client.http.Transport = fileRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := original.RoundTrip(r)
				if err != nil {
					return nil, err
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(body, &fields); err != nil {
					return nil, err
				}
				if raw, ok := fields["conflict"]; ok {
					var conflict map[string]json.RawMessage
					if err := json.Unmarshal(raw, &conflict); err != nil {
						return nil, err
					}
					delete(conflict, field)
					fields["conflict"], err = json.Marshal(conflict)
					if err != nil {
						return nil, err
					}
					body, err = json.Marshal(fields)
					if err != nil {
						return nil, err
					}
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			_, err := file.GetLock(ctx, 0, storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("incomplete conflict = %v", err)
			}
		})
	}
	client.http.Transport = original
	_ = session
}

func TestRetainedHTTPCleanupAndAcknowledgementSurviveDataHistoryCapacity(t *testing.T) {
	ctx := context.Background()
	limits := DefaultFileLimits()
	limits.MaxActions = 1
	client, _, backend := retainedHTTPFixture(t, limits)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	if _, err := file.WriteAt(ctx, 0, []byte("x")); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("data history capacity = %v", err)
	}
	status, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, End: math.MaxInt64}
	if result, err := file.SetLock(ctx, 0, lock, id); err != nil || result.State != storage.LockGranted {
		t.Fatalf("grant = %+v, %v", result, err)
	}
	other, err := backend.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(ctx)
	observer, err := other.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.DropLocks(ctx, 0, storage.POSIX); err != nil {
		t.Fatalf("close-owner cleanup blocked by data history: %v", err)
	}
	if conflict, err := observer.GetLock(ctx, 0, lock); err != nil || conflict.Found {
		t.Fatalf("close-owner cleanup left a lock: %+v, %v", conflict, err)
	}
}

type renewOrderBackend struct {
	*objectstore.Storage
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

type renewOrderSession struct {
	storage.FileSession
	backend *renewOrderBackend
}

func (b *renewOrderBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, error) {
	s, err := b.Storage.NewFileSession(ctx, o)
	if err != nil {
		return nil, err
	}
	return &renewOrderSession{FileSession: s, backend: b}, nil
}
func (s *renewOrderSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	status, err := s.FileSession.Renew(ctx)
	if err != nil {
		return status, err
	}
	if s.backend.calls.Add(1) == 1 {
		close(s.backend.entered)
		select {
		case <-s.backend.release:
		case <-ctx.Done():
			return storage.FileSessionStatus{}, ctx.Err()
		}
	}
	return status, nil
}

func TestRetainedHTTPOutOfOrderRenewalCannotShortenConfirmedLifetime(t *testing.T) {
	ctx := context.Background()
	_, native := memoryfixture.New(t, "http-renew-order", 1<<20, locking.DefaultOptions())
	backend := &renewOrderBackend{Storage: native, entered: make(chan struct{}), release: make(chan struct{}, 1)}
	defer close(backend.release)
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer func() {
		server.Close()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	defer session.Close(ctx)
	first := make(chan error, 1)
	go func() { _, err := remote.Renew(ctx); first <- err }()
	<-backend.entered
	time.Sleep(20 * time.Millisecond)
	second, err := remote.Renew(ctx)
	if err != nil {
		t.Fatal(err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	laterExpiry := served.expires
	served.mu.Unlock()
	backend.release <- struct{}{}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	served.mu.Lock()
	finalExpiry := served.expires
	finalRevision := served.revision
	served.mu.Unlock()
	if !finalExpiry.Equal(laterExpiry) || finalRevision != second.Revision {
		t.Fatalf("delayed renewal replaced deadline/revision: %v/%d -> %v/%d", laterExpiry, second.Revision, finalExpiry, finalRevision)
	}
	served.mu.Lock()
	served.expires = time.Now().Add(100 * time.Millisecond)
	served.mu.Unlock()
	status, err := remote.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Remaining > 100*time.Millisecond {
		t.Fatalf("status advertised beyond the registry fence: %v", status.Remaining)
	}
	served.mu.Lock()
	served.expires = laterExpiry
	served.mu.Unlock()
}

func TestRetainedHTTPCancellationAfterOpenEffectIsEIOAndCleansReference(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	original := client.http.Transport
	client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		var operation struct {
			Op storage.Operation `json:"op"`
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		if err := json.Unmarshal(body, &operation); err != nil {
			return nil, err
		}
		response, err := original.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		if operation.Op == storage.OpFileOpen {
			body, err = io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				return nil, err
			}
			response.Body = io.NopCloser(bytes.NewReader(body))
			cancel()
		}
		return response, nil
	})
	_, err = session.OpenFile(ctx, "created", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Exclusive: true}, Mode: 0600})
	client.http.Transport = original
	if !errors.Is(err, syscall.EIO) || storage.ErrnoOf(err) == syscall.EINTR || !errors.Is(err, context.Canceled) {
		t.Fatalf("post-create interruption = %v", err)
	}
	if _, err := backend.Stat(context.Background(), "created"); err != nil {
		t.Fatalf("create did not take effect before cancellation: %v", err)
	}
	if err := backend.Write(context.Background(), "created", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(context.Background(), "created"); err != nil {
		t.Fatal(err)
	}
	usage, err := backend.Usage(context.Background())
	if err != nil || usage != 0 {
		t.Fatalf("post-open cancellation left a native reference: %d, %v", usage, err)
	}
}

func TestRetainedHTTPPureReadCancellationAfterDispatchIsEINTR(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(context.Background(), "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	attr, err := file.Stat(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	cases := map[string]func(context.Context) error{
		"read":       func(ctx context.Context) error { _, e := file.ReadAt(ctx, 0, 4); return e },
		"stat":       func(ctx context.Context) error { _, e := file.Stat(ctx); return e },
		"stat-node":  func(ctx context.Context) error { _, e := session.StatNode(ctx, attr.ID); return e },
		"get-lock":   func(ctx context.Context) error { _, e := file.GetLock(ctx, 0, lock); return e },
		"query-lock": func(ctx context.Context) error { _, e := file.QueryLock(ctx, 0, id); return e },
		"status":     func(ctx context.Context) error { _, e := session.Status(ctx); return e },
	}
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			client.http.Transport = fileRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				close(entered)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- call(ctx) }()
			<-entered
			cancel()
			if err := <-result; !errors.Is(err, syscall.EINTR) || !errors.Is(err, context.Canceled) {
				t.Fatalf("post-dispatch read cancellation = %v", err)
			}
		})
	}
}

func TestRetainedHTTPCleanupHistoryExhaustionRetiresOwnedLocks(t *testing.T) {
	ctx := context.Background()
	limits := DefaultFileLimits()
	limits.MaxCleanupActions = 1
	client, _, backend := retainedHTTPFixture(t, limits)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	if err := file.DropLocks(ctx, 0, storage.POSIX); err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.POSIX, Type: storage.Exclusive, End: math.MaxInt64}
	if result, err := file.SetLock(ctx, 1, lock, id); err != nil || result.State != storage.LockGranted {
		t.Fatalf("grant = %+v, %v", result, err)
	}
	if err := file.DropLocks(ctx, 1, storage.POSIX); !errors.Is(err, syscall.EIO) {
		t.Fatalf("exhausted cleanup history = %v", err)
	}
	observerSession, err := backend.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer observerSession.Close(ctx)
	observer, err := observerSession.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	if conflict, err := observer.GetLock(ctx, 0, lock); err != nil || conflict.Found {
		t.Fatalf("failed cleanup retained a lock: %+v, %v", conflict, err)
	}
	if _, err := file.ReadAt(ctx, 0, 4); err == nil {
		t.Fatal("retired holder still allowed ordinary I/O")
	}
}

func TestRetainedHTTPAcceptedSmallBodyLimitSupportsSessionAndFileCalls(t *testing.T) {
	ctx := context.Background()
	_, backend := memoryfixture.New(t, "small-http-file-body", 1<<20, locking.DefaultOptions())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.MaxBodyBytes = 1024
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer func() {
		server.Close()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	dial := DefaultDialOptions()
	dial.MaxBodyBytes = 1024
	client, err := DialWithOptions(server.URL, server.Client(), dial)
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx)
	if _, err := session.Status(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	if read, err := file.ReadAt(ctx, 0, 4); err != nil || string(read.Data) != "data" {
		t.Fatalf("small-body read = %+v, %v", read, err)
	}
	if _, err := file.WriteAt(ctx, 0, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedHTTPFileScopeIsFrozenAndReadDoesNotCarryProof(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	owner := lockServerOwner(t, client)
	grant := lockServerGrant(t, client, owner, "file")
	originalScope := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}
	scoped, err := client.WithScope(originalScope)
	if err != nil {
		t.Fatal(err)
	}
	originalScope.Owner = locking.OwnerRef{}
	originalScope.Grants[0].Generation++
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		content, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(content))
		var operation fileRequest
		if err := json.Unmarshal(content, &operation); err != nil {
			return nil, err
		}
		got, present, err := requestMutationScope(request, OpWrite)
		if err != nil {
			return nil, err
		}
		expected := fileMutation(operation.Op)
		if present != expected {
			t.Errorf("%s proof presence = %v, want %v", operation.Op, present, expected)
		}
		if present && (got.Owner != owner || len(got.Grants) != 1 || got.Grants[0] != grant) {
			t.Errorf("mutated retained proof: %+v", got)
		}
		return original.RoundTrip(request)
	})
	session, err := scoped.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx)
	file, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, 0, []byte("live")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetAttr(ctx, storage.AttrChange{}); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(locking.WithScope(ctx, locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}), 0, 3); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedHTTPEnrollmentEnforcesTheServerFileSizeCap(t *testing.T) {
	ctx := context.Background()
	limits := DefaultFileLimits()
	limits.Session.MaxFileSize = 4
	client, _, backend := retainedHTTPFixture(t, limits)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFileSize = 5
	if _, err := client.NewFileSession(ctx, options); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("enrollment above server file cap = %v", err)
	}
	options.MaxFileSize = 4
	session, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx)
	file, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(ctx, 0, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, 4, []byte("x")); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("session file cap lost across the wire: %v", err)
	}
}

func TestRetainedHTTPAdvisoryActionErrorsUseSymbolicErrnos(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	status, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	if result, err := file.SetLock(ctx, 1, lock, first); err != nil || result.State != storage.LockGranted {
		t.Fatalf("first grant = %+v, %v", result, err)
	}
	original := client.http.Transport
	wireErrno := ""
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
		var result struct{ Attempt struct{ Errno string } }
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		wireErrno = result.Attempt.Errno
		response.Body = io.NopCloser(bytes.NewReader(body))
		return response, nil
	})
	second, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	result, err := file.SetLock(ctx, 2, lock, second)
	client.http.Transport = original
	if err != nil || result.State != storage.LockRejected || result.Errno != syscall.EAGAIN || wireErrno != "EAGAIN" {
		t.Fatalf("rejected lock result = %+v, wire errno %q, %v", result, wireErrno, err)
	}
}

type closeOrderBackend struct {
	*objectstore.Storage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type closeOrderSession struct {
	storage.FileSession
	backend *closeOrderBackend
}

func (b *closeOrderBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, error) {
	s, err := b.Storage.NewFileSession(ctx, o)
	if err != nil {
		return nil, err
	}
	return &closeOrderSession{FileSession: s, backend: b}, nil
}
func (s *closeOrderSession) Close(ctx context.Context) error {
	s.backend.once.Do(func() { close(s.backend.entered) })
	select {
	case <-s.backend.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.FileSession.Close(ctx)
}

func TestRetainedHTTPQueuedCloseDuringStopCannotAcknowledgeUndrainedCleanup(t *testing.T) {
	for _, operation := range []string{"file", "session"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			_, native := memoryfixture.New(t, "queued-http-close", 1<<20, locking.DefaultOptions())
			if err := native.Write(ctx, "file", []byte("data")); err != nil {
				t.Fatal(err)
			}
			backend := &closeOrderBackend{Storage: native, entered: make(chan struct{}), release: make(chan struct{})}
			handler, err := NewHandler(backend, nil)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer func() {
				close(backend.release)
				server.Close()
				if err := handler.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			client, err := Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			file, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			release, err := handler.lockControls.acquire(ctx, handler.lockControls.maxBytes)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			outcome := make(chan error, 1)
			go func() {
				if operation == "file" {
					outcome <- file.Close(ctx)
				} else {
					outcome <- session.Close(ctx)
				}
			}()
			deadline := time.Now().Add(time.Second)
			for {
				handler.lockControls.mu.Lock()
				waiting := handler.lockControls.waiters
				handler.lockControls.mu.Unlock()
				if waiting > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("close did not enter the control queue")
				}
				time.Sleep(time.Millisecond)
			}
			handler.Stop()
			<-backend.entered
			release()
			if err := <-outcome; !errors.Is(err, syscall.EIO) {
				t.Fatalf("queued %s Close acknowledged undrained cleanup: %v", operation, err)
			}
			handler.files.mu.Lock()
			retained := len(handler.files.sessions)
			handler.files.mu.Unlock()
			if retained != 1 {
				t.Fatalf("blocked native cleanup lost ownership: %d sessions", retained)
			}
		})
	}
}

func TestRetainedHTTPReadAllowsProgressWithoutInventingEOF(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(context.Background(), "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	_, file := openRetainedFixture(t, client)
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	for _, test := range []struct {
		name      string
		data      []byte
		wantError bool
	}{
		{name: "positive short read", data: []byte("da")},
		{name: "empty before EOF", data: []byte{}, wantError: true},
		{name: "beyond captured range", data: []byte("datax"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				response, err := original.RoundTrip(request)
				if err != nil {
					return nil, err
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
				var result fileResponse
				if err := json.Unmarshal(body, &result); err != nil {
					return nil, err
				}
				result.Data = test.data
				body, err = json.Marshal(result)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			read, err := file.ReadAt(context.Background(), 0, 4)
			if test.wantError {
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("inconsistent captured read = %+v, %v", read, err)
				}
			} else if err != nil || string(read.Data) != "da" || read.Attr.Size != 4 {
				t.Fatalf("short current revision read = %+v, %v", read, err)
			}
		})
	}
}

func TestRetainedHTTPUnlockReceiptDistinguishesReleaseFromAcquisition(t *testing.T) {
	for _, family := range []storage.LockFamily{storage.Flock, storage.POSIX} {
		t.Run(strconv.Itoa(int(family)), func(t *testing.T) {
			ctx := context.Background()
			client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
			if err := backend.Write(ctx, "file", []byte("data")); err != nil {
				t.Fatal(err)
			}
			session, file := openRetainedFixture(t, client)
			status, err := session.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			nextID := func() storage.LockRequestID {
				id, err := storage.NewLockRequestID(status.ActionEpoch)
				if err != nil {
					t.Fatal(err)
				}
				return id
			}
			original := client.http.Transport
			defer func() { client.http.Transport = original }()
			var sent atomic.Value
			client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				request.Body = io.NopCloser(bytes.NewReader(body))
				var wire fileRequest
				if err := json.Unmarshal(body, &wire); err != nil {
					return nil, err
				}
				sent.Store(wire.Op)
				return original.RoundTrip(request)
			})
			lock := storage.FileLock{Family: family, Type: storage.Exclusive, End: math.MaxInt64}
			acquisition := nextID()
			if result, err := file.SetLock(ctx, 0, lock, acquisition); err != nil || result.State != storage.LockGranted || !result.EverGranted {
				t.Fatalf("acquisition = %+v, %v", result, err)
			}
			if op := sent.Load(); op != storage.OpFileSetLock {
				t.Fatalf("acquisition wire operation = %v; want %s", op, storage.OpFileSetLock)
			}
			unlock := lock
			unlock.Type = storage.Unlock
			release := nextID()
			result, err := file.SetLock(ctx, 0, unlock, release)
			if err != nil || result.State != storage.LockReleased || result.EverGranted || result.Lock != unlock {
				t.Fatalf("explicit unlock receipt = %+v, %v", result, err)
			}
			if op := sent.Load(); op != storage.OpFileUnlock {
				t.Fatalf("unlock wire operation = %v; want %s", op, storage.OpFileUnlock)
			}
			result, err = file.QueryLock(ctx, 0, release)
			if err != nil || result.State != storage.LockReleased || result.EverGranted || result.Lock != unlock {
				t.Fatalf("queried unlock receipt = %+v, %v", result, err)
			}
			if err := file.DropLocks(ctx, 0, family); err != nil {
				t.Fatal(err)
			}
			result, err = file.QueryLock(ctx, 0, acquisition)
			if err != nil || result.State != storage.LockReleased || !result.EverGranted || result.Lock != lock {
				t.Fatalf("released acquisition receipt = %+v, %v", result, err)
			}
		})
	}
}

type pendingCloseBackend struct {
	*objectstore.Storage
	calls   atomic.Int32
	first   chan struct{}
	second  chan struct{}
	release chan struct{}
}

type pendingCloseSession struct {
	storage.FileSession
	backend *pendingCloseBackend
}
type pendingCloseFile struct {
	storage.File
	backend *pendingCloseBackend
}

func (b *pendingCloseBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, error) {
	s, err := b.Storage.NewFileSession(ctx, o)
	if err != nil {
		return nil, err
	}
	return &pendingCloseSession{FileSession: s, backend: b}, nil
}
func (s *pendingCloseSession) OpenFile(ctx context.Context, path string, o storage.FileOpenOptions) (storage.File, error) {
	f, err := s.FileSession.OpenFile(ctx, path, o)
	if err != nil {
		return nil, err
	}
	return &pendingCloseFile{File: f, backend: s.backend}, nil
}
func (f *pendingCloseFile) Close(ctx context.Context) error {
	switch f.backend.calls.Add(1) {
	case 1:
		close(f.backend.first)
	case 2:
		close(f.backend.second)
	}
	select {
	case <-f.backend.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return f.File.Close(ctx)
}

func TestRetainedHTTPPendingExpiryRetainsCapabilityAndChargeUntilNativeClose(t *testing.T) {
	ctx := context.Background()
	_, native := memoryfixture.New(t, "pending-close-ownership", 1<<20, locking.DefaultOptions())
	if err := native.Write(ctx, "file", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	backend := &pendingCloseBackend{Storage: native, first: make(chan struct{}), second: make(chan struct{}), release: make(chan struct{})}
	options := DefaultHandlerOptions()
	options.Files = DefaultFileLimits()
	options.Files.PendingAck = 20 * time.Millisecond
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(backend.release) }) }
	defer func() {
		release()
		server.Close()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	sessionOptions := storage.DefaultFileSessionOptions()
	sessionOptions.MaxFiles = 1
	session, err := client.NewFileSession(ctx, sessionOptions)
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	opened, err := remote.call(ctx, fileRequest{Op: storage.OpFileOpen, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := native.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.first:
	case <-time.After(2 * time.Second):
		t.Fatal("pending expiry did not enter native Close")
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	entry := served.files[opened.File]
	count := len(served.files)
	closing := entry != nil && entry.closing
	served.mu.Unlock()
	if entry == nil || !closing || count != 1 {
		t.Fatalf("pending close lost capability or reference admission charge: entry=%v closing=%v count=%d", entry != nil, closing, count)
	}
	if usage, err := native.Usage(ctx); err != nil || usage != 8 {
		t.Fatalf("pending close refunded native bytes: %d, %v", usage, err)
	}
	outcome := make(chan error, 1)
	go func() {
		_, err := client.fileCall(ctx, fileRequest{Op: storage.OpFileClose, Session: remote.id, File: opened.File})
		outcome <- err
	}()
	select {
	case err := <-outcome:
		t.Fatalf("explicit Close answered before native drain: %v", err)
	case <-backend.second:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit Close did not reach the retained native reference")
	}
	release()
	if err := <-outcome; err != nil {
		t.Fatal(err)
	}
	if usage, err := native.Usage(ctx); err != nil || usage != 0 {
		t.Fatalf("known cleanup did not reclaim bytes: %d, %v", usage, err)
	}
	served.mu.Lock()
	count = len(served.files)
	served.mu.Unlock()
	if count != 0 {
		t.Fatalf("known cleanup retained %d capability charges", count)
	}
}
