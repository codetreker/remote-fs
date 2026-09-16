package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestRetainedHTTPCancelWaitReconcilesPendingAction(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	client, _ := filePair(t, backend)
	if err := client.CheckFileStorage(); err != nil {
		t.Fatal(err)
	}
	sa := fileSession(t, client)
	sb := fileSession(t, client)
	claim := storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}
	first := retainHTTPFile(t, sa, httpNode(t, backend, "file"), claim)
	second := retainHTTPFile(t, sb, httpNode(t, backend, "file"), claim)
	scope := storage.RangeScope{Domain: 1}
	held := storage.RangeAcquisition{ID: 1, End: 99, Exclusive: true}
	snap, err := first.RangeSnapshot(ctx, 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{held}}, fileAction(t, sa)); err != nil {
		t.Fatal(err)
	}
	snap, err = second.RangeSnapshot(ctx, 2, scope)
	if err != nil {
		t.Fatal(err)
	}
	id := fileAction(t, sb)
	type result struct {
		receipt storage.FileActionReceipt
		err     error
	}
	finished := make(chan result, 1)
	go func() {
		r, e := second.WaitRanges(ctx, storage.RangeWaitRequest{Owner: 2, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{held}}, id)
		finished <- result{r, e}
	}()
	for {
		r, e := sb.QueryAction(ctx, id)
		if e == nil && r.State == storage.FileActionPending {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("wait never became pending: %+v,%v", r, e)
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < 2; i++ {
		r, e := sb.CancelAction(ctx, id)
		if !errors.Is(e, syscall.EINTR) || r.Action != id || r.State != storage.FileActionNotApplied || r.Effects != 0 {
			t.Fatalf("cancel %d=%+v,%v", i, r, e)
		}
	}
	select {
	case done := <-finished:
		if !errors.Is(done.err, syscall.EINTR) || done.receipt.State != storage.FileActionNotApplied {
			t.Fatalf("wait result=%+v", done)
		}
	case <-ctx.Done():
		t.Fatal("cancel did not complete pending wait")
	}
	if _, err := first.RetireRangeOwner(ctx, 1, scope, fileAction(t, sa)); err != nil {
		t.Fatal(err)
	}
	observed, err := sb.QueryAction(ctx, id)
	if !errors.Is(err, syscall.EINTR) || observed.State != storage.FileActionNotApplied || observed.Effects != 0 {
		t.Fatalf("cancelled wait changed after release=%+v,%v", observed, err)
	}
	snap, err = first.RangeSnapshot(ctx, 1, scope)
	if err != nil || len(snap.Other) != 0 || len(snap.Own) != 0 {
		t.Fatalf("cancel left ranges=%+v,%v", snap, err)
	}
	if _, err := sb.CancelAction(ctx, "invalid"); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid cancellation=%v", err)
	}
}

func TestRetainedHTTPCancelledRetainReplyReconcilesExactNativeReference(t *testing.T) {
	for _, uses := range []storage.AccessUse{0, storage.ReadContent, storage.ReadContent | storage.WriteContent} {
		t.Run(string(rune('a'+uses)), func(t *testing.T) {
			backend, client, wire := retainedReplyFixture(t)
			ctx := t.Context()
			if err := backend.Storage.Write(ctx, "file", []byte("original bytes")); err != nil {
				t.Fatal(err)
			}
			before, err := backend.Storage.Stat(ctx, "file")
			if err != nil {
				t.Fatal(err)
			}
			session := fileSession(t, client)
			id := fileAction(t, session)
			request, cancel := context.WithCancel(ctx)
			defer cancel()
			wire.after = cancel
			receipt, err := session.Retain(request, storage.RetainRequest{NodeID: before.ID, Claim: storage.AccessClaim{Uses: uses}}, id)
			requireUnknownFileAction(t, receipt, id, storage.OpFileRetain, err)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled call lost cancellation cause:%v", err)
			}
			wire.requireReconciliation(t, id, 0)
			if backend.closed.Load() != 0 {
				t.Fatal("uncertain retain auto-closed native reference")
			}
			receipt, err = session.QueryAction(ctx, id)
			if err != nil || receipt.State != storage.FileActionCompleted || receipt.Reference == 0 {
				t.Fatalf("explicit query=%+v,%v", receipt, err)
			}
			wire.requireReconciliation(t, id, 1)
			if backend.retains.Load() != 1 {
				t.Fatalf("native retain calls=%d", backend.retains.Load())
			}
			file, err := session.Reference(ctx, receipt.Reference)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Close(ctx, fileAction(t, session)); err != nil {
				t.Fatal(err)
			}
			if backend.closed.Load() != 1 {
				t.Fatalf("native close calls=%d", backend.closed.Load())
			}
			after, err := backend.Storage.Stat(ctx, "file")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("retain changed metadata=%+v,%v", after, err)
			}
			if content, err := backend.Storage.Read(ctx, "file"); err != nil || string(content) != "original bytes" {
				t.Fatalf("retain changed content=%q,%v", content, err)
			}
			if err := backend.Storage.Remove(ctx, "file"); err != nil {
				t.Fatal(err)
			}
			if used, err := backend.Storage.Usage(ctx); err != nil || used != 0 {
				t.Fatalf("exact close leaked reference=%d,%v", used, err)
			}
		})
	}
}

func TestRetainedHTTPUnknownRetainAndFailedClosePreserveOwnership(t *testing.T) {
	for _, name := range []string{"query unavailable", "close EIO", "close ESTALE"} {
		t.Run(name, func(t *testing.T) {
			backend, client, wire := retainedReplyFixture(t)
			ctx := t.Context()
			if err := backend.Storage.Write(ctx, "file", []byte("original bytes")); err != nil {
				t.Fatal(err)
			}
			node := httpNode(t, backend.Storage, "file")
			session := fileSession(t, client)
			id := fileAction(t, session)
			wire.failQuery = name == "query unavailable"
			receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: node, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, id)
			requireUnknownFileAction(t, receipt, id, storage.OpFileRetain, err)
			wire.requireReconciliation(t, id, 0)
			if backend.closed.Load() != 0 {
				t.Fatal("uncertain retain auto-closed native reference")
			}
			if wire.failQuery {
				failed, queryErr := session.QueryAction(ctx, id)
				if storage.ErrnoOf(queryErr) != syscall.EIO || failed.Effects != 0 || failed.Reference != 0 {
					t.Fatalf("failed explicit query invented result=%+v,%v", failed, queryErr)
				}
				wire.requireReconciliation(t, id, 1)
				wire.failQuery = false
			}
			receipt, err = session.QueryAction(ctx, id)
			wantQueries := 1
			if name == "query unavailable" {
				wantQueries = 2
			}
			wire.requireReconciliation(t, id, wantQueries)
			if err != nil || receipt.State != storage.FileActionCompleted || receipt.Reference == 0 {
				t.Fatalf("native receipt=%+v,%v", receipt, err)
			}
			if backend.retains.Load() != 1 {
				t.Fatalf("unknown result repeated native retain %d times", backend.retains.Load())
			}
			file, err := session.Reference(ctx, receipt.Reference)
			if err != nil {
				t.Fatal(err)
			}
			if name == "close EIO" {
				backend.closeError.Store(int64(syscall.EIO))
			}
			if name == "close ESTALE" {
				backend.closeError.Store(int64(syscall.ESTALE))
			}
			if err := backend.Storage.Remove(ctx, "file"); err != nil {
				t.Fatal(err)
			}
			closeID := fileAction(t, session)
			closed, closeErr := file.Close(ctx, closeID)
			if name != "query unavailable" {
				if storage.ErrnoOf(closeErr) != syscall.Errno(backend.closeError.Load()) || !storage.IsFileCallNotAdmitted(closeErr) || closed.State != 0 || closed.Action != "" || closed.Reference != 0 || closed.Effects != 0 {
					t.Fatalf("failed close=%+v,%v", closed, closeErr)
				}
				if used, err := backend.Storage.Usage(ctx); err != nil || used != int64(len("original bytes")) {
					t.Fatalf("failed close lost ownership=%d,%v", used, err)
				}
				backend.closeError.Store(0)
				if _, err := session.Close(ctx, fileAction(t, session)); err != nil {
					t.Fatal(err)
				}
			} else if closeErr != nil {
				t.Fatal(closeErr)
			}
			if used, err := backend.Storage.Usage(ctx); err != nil || used != 0 {
				t.Fatalf("cleanup leaked ownership=%d,%v", used, err)
			}
		})
	}
}

type retainedReplyBackend struct {
	*objectstore.Storage
	retains, closed, closeError atomic.Int64
}
type retainedReplySession struct {
	storage.FileSession
	backend *retainedReplyBackend
}
type retainedReplyFile struct {
	storage.File
	backend *retainedReplyBackend
}

func (b *retainedReplyBackend) NewFileSession(ctx context.Context, o storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	s, status, err := b.Storage.NewFileSession(ctx, o)
	if err != nil {
		return nil, status, err
	}
	return &retainedReplySession{FileSession: s, backend: b}, status, nil
}
func (s *retainedReplySession) Retain(ctx context.Context, r storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.backend.retains.Add(1)
	return s.FileSession.Retain(ctx, r, id)
}
func (s *retainedReplySession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	f, err := s.FileSession.Reference(ctx, id)
	if err != nil {
		return nil, err
	}
	return &retainedReplyFile{File: f, backend: s.backend}, nil
}
func (f *retainedReplyFile) Close(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	f.backend.closed.Add(1)
	if code := f.backend.closeError.Load(); code != 0 {
		return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.Errno(code), NotAdmitted: true, Cause: syscall.Errno(code)}
	}
	return f.File.Close(ctx, id)
}

type retainedReplyRequest struct {
	Op        storage.Operation       `json:"op"`
	Session   string                  `json:"session"`
	Reference storage.FileReferenceID `json:"reference"`
	Action    storage.FileActionID    `json:"action"`
}
type retainedReplyTransport struct {
	base      http.RoundTripper
	mu        sync.Mutex
	requests  []retainedReplyRequest
	dropped   bool
	after     func()
	failQuery bool
}

func (w *retainedReplyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	var operation retainedReplyRequest
	if err := json.Unmarshal(body, &operation); err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.requests = append(w.requests, operation)
	drop := operation.Op == storage.OpFileRetain && !w.dropped
	if drop {
		w.dropped = true
	}
	w.mu.Unlock()
	if operation.Op == storage.OpFileQueryAction && w.failQuery {
		return nil, io.ErrUnexpectedEOF
	}
	response, err := w.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if drop && response.StatusCode == http.StatusOK {
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, err
		}
		if w.after != nil {
			w.after()
		}
		return nil, errors.Join(io.ErrUnexpectedEOF, request.Context().Err())
	}
	return response, nil
}
func (w *retainedReplyTransport) requireReconciliation(t *testing.T, id storage.FileActionID, wantQueries int) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	retains, queries := 0, 0
	session := ""
	for _, r := range w.requests {
		if r.Op == storage.OpFileRetain {
			retains++
			session = r.Session
			if r.Action != id {
				t.Fatalf("retain action changed: %+v", r)
			}
		}
		if r.Op == storage.OpFileQueryAction {
			queries++
			if r.Action != id || r.Session != session {
				t.Fatalf("query targeted other action/session: %+v", r)
			}
		}
	}
	if retains != 1 || queries != wantQueries {
		t.Fatalf("retain/query dispatches=%d/%d", retains, queries)
	}
}
func retainedReplyFixture(t *testing.T) (*retainedReplyBackend, *httprest.Storage, *retainedReplyTransport) {
	t.Helper()
	backend := &retainedReplyBackend{Storage: volumeFixture(t)}
	handler, err := httprest.NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	base := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(func() {
		backend.closeError.Store(0)
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
		base.CloseIdleConnections()
	})
	wire := &retainedReplyTransport{base: base}
	client, err := httprest.Dial(server.URL, &http.Client{Transport: wire, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return backend, client, wire
}
