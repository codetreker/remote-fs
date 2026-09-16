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
	ctx := context.Background()
	s, status, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	cleanup, _ := storage.NewFileActionID(status.ActionEpoch)
	t.Cleanup(func() {
		if _, err := s.Close(ctx, cleanup); err != nil {
			t.Error(err)
		}
	})
	attr, err := client.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	return s.(*remoteFileSession), internalRetainFile(t, s, attr.ID).(*remoteFile)
}
func internalRetainFile(t *testing.T, s storage.FileSession, node uint64) storage.File {
	t.Helper()
	r, err := s.Retain(context.Background(), storage.RetainRequest{NodeID: node, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, retainedAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.Reference(context.Background(), r.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if f.Reference() != r.Reference || f.NodeID() != node {
		t.Fatalf("resolved reference identity = %d/%d, want %d/%d", f.Reference(), f.NodeID(), r.Reference, node)
	}
	return f
}
func internalFileNode(t *testing.T, s storage.Storage) uint64 {
	t.Helper()
	a, e := s.Stat(context.Background(), "file")
	if e != nil {
		t.Fatal(e)
	}
	return a.ID
}

type fileRoundTripFunc func(*http.Request) (*http.Response, error)

func (f fileRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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

func (b *renewOrderBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	s, status, err := b.Storage.NewFileSession(ctx, o)
	if err != nil {
		return nil, status, err
	}
	return &renewOrderSession{FileSession: s, backend: b}, status, nil
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
	session, _, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	defer session.Close(ctx, retainedAction(t, session))
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
	session, _, err := scoped.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx, retainedAction(t, session))
	file := internalRetainFile(t, session, internalFileNode(t, backend))
	if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("live")}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(ctx, storage.FileTruncateRequest{Size: 3}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetAttr(ctx, storage.AttrChange{}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(locking.WithScope(ctx, locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}), storage.FileReadRequest{Length: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(ctx, retainedAction(t, session)); err != nil {
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
	if _, _, err := client.NewFileSession(ctx, options); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("enrollment above server file cap = %v", err)
	}
	options.MaxFileSize = 4
	session, _, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx, retainedAction(t, session))
	file := internalRetainFile(t, session, internalFileNode(t, backend))
	if _, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 4, Data: []byte("x")}, retainedAction(t, session)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("session file cap lost across the wire: %v", err)
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

func (b *closeOrderBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	s, status, err := b.Storage.NewFileSession(ctx, o)
	if err != nil {
		return nil, status, err
	}
	return &closeOrderSession{FileSession: s, backend: b}, status, nil
}
func (s *closeOrderSession) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.backend.once.Do(func() { close(s.backend.entered) })
	select {
	case <-s.backend.release:
	case <-ctx.Done():
		return storage.FileActionReceipt{}, ctx.Err()
	}
	return s.FileSession.Close(ctx, id)
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
			session, _, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			file := internalRetainFile(t, session, internalFileNode(t, backend))
			closeAction := retainedAction(t, session)
			release, err := handler.fileControls.acquire(ctx, handler.fileControls.maxBytes)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			outcome := make(chan error, 1)
			go func() {
				if operation == "file" {
					_, err := file.Close(ctx, closeAction)
					outcome <- err
				} else {
					_, err := session.Close(ctx, closeAction)
					outcome <- err
				}
			}()
			deadline := time.Now().Add(time.Second)
			for {
				handler.fileControls.mu.Lock()
				waiting := handler.fileControls.waiters
				handler.fileControls.mu.Unlock()
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
				body, err = marshalFileJSON(result)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			read, err := file.ReadAt(context.Background(), storage.FileReadRequest{Length: 4})
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

func TestRetainedHTTPRejectsMissingZeroValuedRequestMembers(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	complete := fileRequest{Op: storage.OpFileTruncate, Session: session.id, Reference: file.id, Action: retainedAction(t, session), Truncate: &fileTruncateRequest{Size: 0}}
	encoded, err := marshalFileJSON(complete)
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range []string{"size", "truncate", "action", "reference", "session", "op"} {
		t.Run(missing, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if missing == "size" {
				fields["truncate"] = json.RawMessage(`{}`)
			} else {
				delete(fields, missing)
			}
			body, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := (Request{Op: OpFile}).URL(client.base)
			if err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", contentJSON)
			response, err := client.http.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode == http.StatusOK {
				t.Fatalf("missing %s accepted", missing)
			}
			data, err := backend.Read(ctx, "file")
			if err != nil || string(data) != "preserve" {
				t.Fatalf("invalid request mutated native content=%q,%v", data, err)
			}
		})
	}
}
func TestRetainedHTTPRejectsIncompleteRangeSnapshots(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	_, file := openRetainedFixture(t, client)
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	for _, field := range []string{"Revision", "Own", "Other", "Available", "OwnerAvailable"} {
		t.Run(field, func(t *testing.T) {
			client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				response, err := original.RoundTrip(req)
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
				var ranges map[string]json.RawMessage
				if err := json.Unmarshal(fields["ranges"], &ranges); err != nil {
					return nil, err
				}
				delete(ranges, field)
				fields["ranges"], err = json.Marshal(ranges)
				if err != nil {
					return nil, err
				}
				body, err = json.Marshal(fields)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				response.ContentLength = int64(len(body))
				response.Header.Set("Content-Length", strconv.Itoa(len(body)))
				return response, nil
			})
			if result, err := file.RangeSnapshot(t.Context(), 0, storage.RangeScope{Domain: 1}); !errors.Is(err, syscall.EIO) || result.Revision != 0 {
				t.Fatalf("incomplete range snapshot=%+v,%v", result, err)
			}
		})
	}
}
func TestRetainedHTTPCleanupSurvivesNativeDataHistoryCapacity(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxActions = 1
	session, _, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	file := internalRetainFile(t, session, internalFileNode(t, backend))
	r, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("x")}, retainedAction(t, session))
	if !errors.Is(err, syscall.EAGAIN) || r.Effects != 0 {
		t.Fatalf("history capacity=%+v,%v", r, err)
	}
	if _, err := session.Renew(ctx); err != nil {
		t.Fatalf("renew blocked by data history:%v", err)
	}
	if err := backend.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatalf("cleanup blocked by data history:%v", err)
	}
	if used, err := backend.Usage(ctx); err != nil || used != 0 {
		t.Fatalf("cleanup retained bytes=%d,%v", used, err)
	}
	if _, err := session.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
}
func TestRetainedHTTPPureReadCancellationAfterDispatchIsEINTR(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	attr, err := file.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id := retainedAction(t, session)
	cases := map[string]func(context.Context) error{
		"read": func(ctx context.Context) error {
			_, e := file.ReadAt(ctx, storage.FileReadRequest{Length: 4})
			return e
		},
		"stat": func(ctx context.Context) error { _, e := file.Stat(ctx, storage.ObservationOptions{}); return e },
		"stat-node": func(ctx context.Context) error {
			_, e := session.StatNode(ctx, attr.Attr.ID, storage.ObservationOptions{})
			return e
		},
		"range-snapshot": func(ctx context.Context) error {
			_, e := file.RangeSnapshot(ctx, 0, storage.RangeScope{Domain: 1})
			return e
		},
		"query-action": func(ctx context.Context) error { _, e := session.QueryAction(ctx, id); return e },
		"status":       func(ctx context.Context) error { _, e := session.Status(ctx); return e },
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- call(ctx) }()
			<-entered
			cancel()
			if err := <-result; !errors.Is(err, syscall.EINTR) || !errors.Is(err, context.Canceled) {
				t.Fatalf("read cancellation=%v", err)
			}
		})
	}
}

func TestRetainedHTTPCancelledMutationPreservesConfirmedNativeEffects(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	id := retainedAction(t, session)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	calls := []storage.Operation{}
	client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		var op fileRequest
		if err := json.Unmarshal(body, &op); err != nil {
			return nil, err
		}
		calls = append(calls, op.Op)
		response, err := original.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		if op.Op == storage.OpFileTruncate {
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if err != nil {
				return nil, err
			}
			cancel()
			return nil, context.Canceled
		}
		return response, nil
	})
	receipt, err := file.Truncate(ctx, storage.FileTruncateRequest{Size: 0}, id)
	client.http.Transport = original
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) || storage.IsFileCallNotAdmitted(err) || receipt.Action != id || receipt.State != storage.FileActionUnknown || receipt.Effects != 0 || receipt.Reference != 0 {
		t.Fatalf("uncertain cancelled mutation=%+v,%v", receipt, err)
	}
	if len(calls) != 1 || calls[0] != storage.OpFileTruncate {
		t.Fatalf("HTTP automatically reconciled cancelled mutation: %v", calls)
	}
	receipt, err = session.QueryAction(t.Context(), id)
	if err != nil || receipt.Action != id || receipt.State != storage.FileActionCompleted || receipt.Effects&storage.EffectContentChanged == 0 || receipt.Observation.Attr.Size != 0 {
		t.Fatalf("explicit cancellation query=%+v,%v", receipt, err)
	}
	if err := backend.Write(t.Context(), "file", []byte("later")); err != nil {
		t.Fatal(err)
	}
	repeated, err := session.QueryAction(t.Context(), id)
	if err != nil || repeated.Observation.Attr.Size != 0 {
		t.Fatalf("repeat receipt=%+v,%v", repeated, err)
	}
	if content, err := backend.Read(t.Context(), "file"); err != nil || string(content) != "later" {
		t.Fatalf("replayed cancellation reapplied mutation=%q,%v", content, err)
	}
}
func TestRetainedHTTPHistoryExhaustionCannotStrandOwnedRanges(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxActions = 2
	session, _, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	file := internalRetainFile(t, session, internalFileNode(t, backend))
	scope := storage.RangeScope{Domain: 1}
	snap, err := file.RangeSnapshot(ctx, 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	held := storage.RangeAcquisition{ID: 1, End: 99, Exclusive: true}
	if _, err := file.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{held}}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if r, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("x")}, retainedAction(t, session)); !errors.Is(err, syscall.EAGAIN) || r.Effects != 0 {
		t.Fatalf("full native history=%+v,%v", r, err)
	}
	if _, err := session.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatalf("history stranded cleanup:%v", err)
	}
	observer, probe := openRetainedFixture(t, client)
	_ = observer
	snap, err = probe.RangeSnapshot(ctx, 2, scope)
	if err != nil || len(snap.Other) != 0 {
		t.Fatalf("cleanup retained ranges=%+v,%v", snap, err)
	}
	if _, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 4}); err == nil {
		t.Fatal("retired holder still allowed ordinary I/O")
	}
}
func TestRetainedHTTPMinimumBodyLimitSupportsSessionAndFileCalls(t *testing.T) {
	ctx := context.Background()
	_, backend := memoryfixture.New(t, "small-http-file-body", 1<<20, locking.DefaultOptions())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.MaxBodyBytes = max(fileControlRequestLimit(), fileControlResponseLimit())
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
	dial.MaxBodyBytes = options.MaxBodyBytes
	client, err := DialWithOptions(server.URL, server.Client(), dial)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx, retainedAction(t, session))
	file := internalRetainFile(t, session, internalFileNode(t, backend))
	if _, err := session.Status(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if read, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 4}); err != nil || string(read.Data) != "data" {
		t.Fatalf("minimum-body read=%+v,%v", read, err)
	}
	if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("live")}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	dial.MaxBodyBytes--
	undersized, err := DialWithOptions(server.URL, server.Client(), dial)
	if err == nil {
		if _, _, err := undersized.NewFileSession(ctx, storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("under-sized control response admitted:%v", err)
		}
	}
}
func TestRetainedHTTPRangeActionErrorsUseSymbolicErrnos(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	scope := storage.RangeScope{Domain: 1}
	lock := storage.RangeAcquisition{ID: 1, End: 99, Exclusive: true}
	snap, err := file.RangeSnapshot(ctx, 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{lock}}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	snap, err = file.RangeSnapshot(ctx, 2, scope)
	if err != nil {
		t.Fatal(err)
	}
	id := retainedAction(t, session)
	original := client.http.Transport
	defer func() { client.http.Transport = original }()
	wireErrno := ""
	client.http.Transport = fileRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
		var result fileErrorResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		wireErrno = result.Errno
		if result.Receipt != nil && result.Receipt.Errno != wireErrno {
			t.Errorf("receipt/wrapper errno diverged: %+v", result)
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		return response, nil
	})
	result, err := file.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 2, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{lock}}, id)
	client.http.Transport = original
	if !errors.Is(err, syscall.EAGAIN) || result.State != storage.FileActionNotApplied || result.Errno != syscall.EAGAIN || wireErrno != "EAGAIN" {
		t.Fatalf("rejected range=%+v,wire=%q,%v", result, wireErrno, err)
	}
}

func TestRetainedHTTPRangeReleasePreservesAcquisitionReceipt(t *testing.T) {
	for _, domain := range []storage.RangeDomainID{1, 2} {
		t.Run(strconv.FormatUint(uint64(domain), 10), func(t *testing.T) {
			ctx := context.Background()
			client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
			if err := backend.Write(ctx, "file", []byte("data")); err != nil {
				t.Fatal(err)
			}
			session, file := openRetainedFixture(t, client)
			scope := storage.RangeScope{Domain: domain}
			snap, err := file.RangeSnapshot(ctx, 0, scope)
			if err != nil {
				t.Fatal(err)
			}
			acquisition := retainedAction(t, session)
			lock := storage.RangeAcquisition{ID: 1, End: 99, Exclusive: true}
			grant, err := file.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 0, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{lock}}, acquisition)
			if err != nil || grant.Effects&storage.EffectRangesChanged == 0 {
				t.Fatalf("acquisition=%+v,%v", grant, err)
			}
			snap, err = file.RangeSnapshot(ctx, 0, scope)
			if err != nil || len(snap.Own) != 1 {
				t.Fatalf("held=%+v,%v", snap, err)
			}
			release := retainedAction(t, session)
			request := storage.RangeReplaceRequest{Owner: 0, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{}}
			removed, err := file.ReplaceRanges(ctx, request, release)
			if err != nil || removed.State != storage.FileActionCompleted || removed.Effects&storage.EffectRangesChanged == 0 {
				t.Fatalf("release=%+v,%v", removed, err)
			}
			repeated, err := file.ReplaceRanges(ctx, request, release)
			if err != nil || repeated.Action != release || repeated.RangeRevision != removed.RangeRevision {
				t.Fatalf("release replay=%+v,%v", repeated, err)
			}
			historical, err := session.QueryAction(ctx, acquisition)
			if err != nil || historical.Action != acquisition || historical.RangeRevision != grant.RangeRevision || historical.Effects != grant.Effects {
				t.Fatalf("release rewrote acquisition=%+v,%v", historical, err)
			}
			snap, err = file.RangeSnapshot(ctx, 0, scope)
			if err != nil || len(snap.Own) != 0 || len(snap.Other) != 0 {
				t.Fatalf("release replay installed a range=%+v,%v", snap, err)
			}
		})
	}
}
func TestRetainedHTTPExpiryRetainsSessionChargeUntilNativeClose(t *testing.T) {
	ctx := context.Background()
	_, native := memoryfixture.New(t, "pending-close-ownership", 1<<20, locking.DefaultOptions())
	if err := native.Write(ctx, "file", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	backend := &closeOrderBackend{Storage: native, entered: make(chan struct{}), release: make(chan struct{})}
	options := DefaultHandlerOptions()
	options.Files = DefaultFileLimits()
	options.Files.MaxSessions = 1
	handler, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	var once sync.Once
	release := func() { once.Do(func() { close(backend.release) }) }
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
	session, _, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	file := internalRetainFile(t, session, internalFileNode(t, native))
	_ = file
	closeID := retainedAction(t, session)
	remote := session.(*remoteFileSession)
	if err := native.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	served.expires = time.Now().Add(-time.Second)
	served.mu.Unlock()
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("expiry did not enter native cleanup")
	}
	handler.files.mu.Lock()
	count := len(handler.files.sessions)
	retained := handler.files.sessions[remote.id]
	handler.files.mu.Unlock()
	if retained != served || count != 1 {
		t.Fatalf("pending cleanup lost session ownership/charge=%d", count)
	}
	if used, err := native.Usage(ctx); err != nil || used != 8 {
		t.Fatalf("pending cleanup refunded native bytes=%d,%v", used, err)
	}
	if _, _, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("pending cleanup freed enrollment charge:%v", err)
	}
	outcome := make(chan error, 1)
	go func() { _, err := session.Close(ctx, closeID); outcome <- err }()
	select {
	case err := <-outcome:
		t.Fatalf("explicit close acknowledged undrained cleanup:%v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-outcome; err != nil {
		t.Fatal(err)
	}
	if used, err := native.Usage(ctx); err != nil || used != 0 {
		t.Fatalf("confirmed cleanup retained bytes=%d,%v", used, err)
	}
}

func TestRetainedHTTPStrongConflictPreservesNativeClassificationAndReceipt(t *testing.T) {
	ctx := t.Context()
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	owner := lockServerOwner(t, client)
	grant := lockServerGrant(t, client, owner, "file")
	released := false
	t.Cleanup(func() {
		if !released {
			if _, err := client.Release(context.Background(), owner, grant); err != nil {
				t.Error(err)
			}
		}
	})
	action := retainedAction(t, session)
	receipt, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("CHANGED!")}, action)
	var refusal *locking.Error
	if !errors.As(err, &refusal) || refusal.Code != locking.Conflict || refusal.Recorded || !errors.Is(err, syscall.EBUSY) || storage.ErrnoOf(err) != syscall.EBUSY {
		t.Fatalf("retained strong refusal=%+v, error=%v", refusal, err)
	}
	if receipt.Action != action || receipt.Operation != storage.OpFileWrite || receipt.State != storage.FileActionNotApplied || receipt.Effects != 0 || receipt.Errno != syscall.EBUSY {
		t.Fatalf("strong refusal receipt=%+v", receipt)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	if served == nil {
		t.Fatal("refused mutation lost its native session")
	}
	native, nativeErr := served.native.QueryAction(ctx, action)
	var nativeRefusal *locking.Error
	if !errors.As(nativeErr, &nativeRefusal) || nativeRefusal.Code != refusal.Code || nativeRefusal.Recorded != refusal.Recorded || storage.ErrnoOf(nativeErr) != storage.ErrnoOf(err) {
		t.Fatalf("native/HTTP refusal diverged: native=%+v/%v, HTTP=%+v/%v", nativeRefusal, nativeErr, refusal, err)
	}
	if native.Action != receipt.Action || native.Operation != receipt.Operation || native.State != receipt.State || native.Effects != receipt.Effects || native.Reference != receipt.Reference || native.Errno != receipt.Errno {
		t.Fatalf("HTTP changed native receipt: native=%+v, HTTP=%+v", native, receipt)
	}
	if content, err := backend.Read(ctx, "file"); err != nil || string(content) != "preserve" {
		t.Fatalf("strong refusal changed content=%q,%v", content, err)
	}
	if _, err := client.Release(ctx, owner, grant); err != nil {
		t.Fatal(err)
	}
	released = true
	replay, replayErr := session.QueryAction(ctx, action)
	var replayRefusal *locking.Error
	if !errors.As(replayErr, &replayRefusal) || replayRefusal.Code != locking.Conflict || replayRefusal.Recorded != nativeRefusal.Recorded || replay.State != native.State || replay.Effects != 0 {
		t.Fatalf("release erased historical strong refusal=%+v,%v", replay, replayErr)
	}
	if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("allowed!")}, retainedAction(t, session)); err != nil {
		t.Fatalf("released strong grant still blocked write:%v", err)
	}
	if content, err := backend.Read(ctx, "file"); err != nil || string(content) != "allowed!" {
		t.Fatalf("post-release content=%q,%v", content, err)
	}
}
