package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	"io"

	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"
)

func filePair(t *testing.T, backend storage.Storage) (*httprest.Storage, *httprest.Handler) {
	t.Helper()
	handler, err := httprest.NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client, handler
}

func fileAction(t *testing.T, s storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func fileSession(t *testing.T, s storage.FileStorage) storage.FileSession {
	t.Helper()
	session, status, err := s.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), cleanup); err != nil {
			t.Error(err)
		}
	})
	return session
}
func retainHTTPFile(t *testing.T, s storage.FileSession, node uint64, claim storage.AccessClaim) storage.File {
	t.Helper()
	receipt, err := s.Retain(context.Background(), storage.RetainRequest{NodeID: node, Claim: claim}, fileAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != storage.FileActionCompleted || receipt.Reference == 0 || receipt.Effects&storage.EffectRetained == 0 {
		t.Fatalf("retain receipt = %+v", receipt)
	}
	f, err := s.Reference(context.Background(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if f.Reference() != receipt.Reference || f.NodeID() != node {
		t.Fatalf("resolved reference identity = %d/%d, want %d/%d", f.Reference(), f.NodeID(), receipt.Reference, node)
	}
	return f
}
func httpNode(t *testing.T, backend storage.Storage, path string) uint64 {
	t.Helper()
	attr, err := backend.Stat(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return attr.ID
}

func TestRetainedHTTPFileTracksCurrentObjectAcrossUnlinkAndReplacement(t *testing.T) {
	ctx := context.Background()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("first")); err != nil {
		t.Fatal(err)
	}
	original, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	client, _ := filePair(t, backend)
	session := fileSession(t, client)
	file := retainHTTPFile(t, session, original.ID, storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent})
	if err := backend.Write(ctx, "file", []byte("newer content")); err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 64})
	if err != nil || string(read.Data) != "newer content" || read.Attr.Size != 13 {
		t.Fatalf("live read = %+v, %v", read, err)
	}
	if err := backend.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("OLDER")}, fileAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(ctx, storage.FileTruncateRequest{Size: 5}, fileAction(t, session)); err != nil {
		t.Fatal(err)
	}
	read, err = file.ReadAt(ctx, storage.FileReadRequest{Length: 64})
	if err != nil || string(read.Data) != "OLDER" || read.Attr.ID != original.ID {
		t.Fatalf("orphan read = %+v, %v", read, err)
	}
	current, err := backend.Read(ctx, "file")
	if err != nil || string(current) != "replacement" {
		t.Fatalf("replacement = %q, %v", current, err)
	}
	currentAttr, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	if currentAttr.ID == original.ID {
		t.Fatal("replacement reused retained identity")
	}
	if _, err := session.Retain(ctx, storage.RetainRequest{NodeID: currentAttr.ID, ExpectedMetadataRevision: currentAttr.MetadataRevision + 1}, fileAction(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stale revision = %v", err)
	}
}

func TestRetainedHTTPAdvisoryCoordinatesAcrossHandlers(t *testing.T) {
	ctx := context.Background()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	a, _ := filePair(t, backend)
	b, _ := filePair(t, backend)
	sa := fileSession(t, a)
	sb := fileSession(t, b)
	claim := storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}
	fa := retainHTTPFile(t, sa, httpNode(t, backend, "file"), claim)
	fb := retainHTTPFile(t, sb, httpNode(t, backend, "file"), claim)
	scope := storage.RangeScope{Domain: 17}
	lock := storage.RangeAcquisition{ID: 1, End: 99, Exclusive: true}
	snap, err := fa.RangeSnapshot(ctx, 17, scope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fa.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 17, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{lock}}, fileAction(t, sa))
	if err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("grant=%+v,%v", result, err)
	}
	other, err := fb.RangeSnapshot(ctx, 17, scope)
	if err != nil || len(other.Other) != 1 || other.Other[0].Range != lock {
		t.Fatalf("cross-session=%+v,%v", other, err)
	}
	if _, err = fb.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: 17, Scope: scope, ExpectedRevision: other.Revision, Ranges: []storage.RangeAcquisition{lock}}, fileAction(t, sb)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("conflict=%v", err)
	}
	if _, err = fb.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("OPEN")}, fileAction(t, sb)); err != nil {
		t.Fatalf("advisory blocked ordinary I/O: %v", err)
	}
	if _, err = fa.RetireRangeOwner(ctx, 17, scope, fileAction(t, sa)); err != nil {
		t.Fatal(err)
	}
	other, err = fb.RangeSnapshot(ctx, 17, scope)
	if err != nil || len(other.Other) != 0 {
		t.Fatalf("release=%+v,%v", other, err)
	}
}

type dropFileReply struct {
	next      http.RoundTripper
	mu        sync.Mutex
	operation storage.Operation
	dropped   bool
	after     func() error
	calls     map[storage.FileActionID][]storage.Operation
}

func (d *dropFileReply) RoundTrip(req *http.Request) (*http.Response, error) {
	var op struct {
		Op     storage.Operation    `json:"op"`
		Action storage.FileActionID `json:"action"`
	}
	if req.Body != nil {
		content, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(content))
		_ = json.Unmarshal(content, &op)
	}
	response, err := d.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.calls == nil {
		d.calls = make(map[storage.FileActionID][]storage.Operation)
	}
	if op.Action != "" {
		d.calls[op.Action] = append(d.calls[op.Action], op.Op)
	}
	drop := op.Op == d.operation && !d.dropped
	if drop {
		d.dropped = true
	}
	d.mu.Unlock()
	if drop && response.StatusCode == http.StatusOK {
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil {
			return nil, err
		}
		if d.after != nil {
			if err := d.after(); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("response lost after the server completed the operation")
	}
	return response, nil
}

func requireUnknownFileAction(t *testing.T, r storage.FileActionReceipt, id storage.FileActionID, op storage.Operation, err error) {
	t.Helper()
	if storage.ErrnoOf(err) != syscall.EIO || storage.IsFileCallNotAdmitted(err) || r.Action != id || r.Operation != op || r.State != storage.FileActionUnknown || r.Effects != 0 || r.Reference != 0 {
		t.Fatalf("uncertain call=%+v,%v", r, err)
	}
}
func (d *dropFileReply) requireCalls(t *testing.T, id storage.FileActionID, want ...storage.Operation) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	got := d.calls[id]
	if len(got) != len(want) {
		t.Fatalf("action %s calls=%v,want %v", id, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("action %s calls=%v,want %v", id, got, want)
		}
	}
}

func TestRetainedHTTPReplaysLostRetainWithoutAnotherReference(t *testing.T) {
	ctx := context.Background()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	handler, err := httprest.NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	transport := &dropFileReply{next: server.Client().Transport, operation: storage.OpFileRetain}
	client, err := httprest.Dial(server.URL, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, _, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx, fileAction(t, session))
	id := fileAction(t, session)
	receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: httpNode(t, backend, "file"), Claim: storage.AccessClaim{Uses: storage.ReadContent}}, id)
	requireUnknownFileAction(t, receipt, id, storage.OpFileRetain, err)
	transport.requireCalls(t, id, storage.OpFileRetain)
	receipt, err = session.QueryAction(ctx, id)
	if err != nil || receipt.State != storage.FileActionCompleted || receipt.Reference == 0 {
		t.Fatalf("explicit query=%+v,%v", receipt, err)
	}
	transport.requireCalls(t, id, storage.OpFileRetain, storage.OpFileQueryAction)
	first, err := session.Reference(ctx, receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if !transport.dropped {
		t.Fatal("retain response was not dropped")
	}
	if _, err := first.Close(ctx, fileAction(t, session)); err != nil {
		t.Fatal(err)
	}
	second := retainHTTPFile(t, session, httpNode(t, backend, "file"), storage.AccessClaim{Uses: storage.ReadContent})
	if second.Reference() == first.Reference() {
		t.Fatal("fresh retain resurrected retired reference")
	}
	if _, err := second.Close(ctx, fileAction(t, session)); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedHTTPReplaysLostTruncateWithoutReapplyingIt(t *testing.T) {
	ctx := context.Background()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	handler, err := httprest.NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	transport := &dropFileReply{next: server.Client().Transport, operation: storage.OpFileTruncate, after: func() error { return backend.Write(ctx, "file", []byte("later update")) }}
	client, err := httprest.Dial(server.URL, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	session := fileSession(t, client)
	file := retainHTTPFile(t, session, httpNode(t, backend, "file"), storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent})
	id := fileAction(t, session)
	attr, err := file.Truncate(ctx, storage.FileTruncateRequest{Size: 1}, id)
	requireUnknownFileAction(t, attr, id, storage.OpFileTruncate, err)
	transport.requireCalls(t, id, storage.OpFileTruncate)
	attr, err = session.QueryAction(ctx, id)
	transport.requireCalls(t, id, storage.OpFileTruncate, storage.OpFileQueryAction)
	if err != nil || attr.State != storage.FileActionCompleted || attr.Observation.Attr.Size != 1 {
		t.Fatalf("truncate receipt = %+v, %v", attr, err)
	}
	got, err := backend.Read(ctx, "file")
	if err != nil || string(got) != "later update" {
		t.Fatalf("replay reapplied truncate: %q, %v", got, err)
	}
}

func TestRetainedHTTPHandlerCloseRetiresOnlyOwnedSessions(t *testing.T) {
	ctx := context.Background()
	backend := volumeFixture(t)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	a, ha := filePair(t, backend)
	b, _ := filePair(t, backend)
	sa, _, err := a.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	sb := fileSession(t, b)
	fa := retainHTTPFile(t, sa, httpNode(t, backend, "file"), storage.AccessClaim{Uses: storage.ReadContent})
	fb := retainHTTPFile(t, sb, httpNode(t, backend, "file"), storage.AccessClaim{Uses: storage.ReadContent})
	if err := ha.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fa.ReadAt(ctx, storage.FileReadRequest{Length: 4}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("retired handler read = %v", err)
	}
	read, err := fb.ReadAt(ctx, storage.FileReadRequest{Length: 4})
	if err != nil || string(read.Data) != "data" {
		t.Fatalf("other handler lost shared backend: %+v, %v", read, err)
	}

}

func TestRetainedHTTPReconcilesLostRetainAndClose(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileRetain, storage.OpFileClose} {
		t.Run(string(operation), func(t *testing.T) {
			ctx := context.Background()
			backend := volumeFixture(t)
			if err := backend.Write(ctx, "file", []byte("retained")); err != nil {
				t.Fatal(err)
			}
			options := httprest.DefaultHandlerOptions()
			options.Files = httprest.DefaultFileLimits()
			options.Files.Session.MaxFiles = 1
			handler, err := httprest.NewHandlerWithOptions(backend, nil, options)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			t.Cleanup(func() {
				server.Close()
				if err := handler.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			transport := &dropFileReply{next: server.Client().Transport, operation: operation}
			client, err := httprest.Dial(server.URL, &http.Client{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			sessionOptions := storage.DefaultFileSessionOptions()
			sessionOptions.MaxFiles = 1
			session, _, err := client.NewFileSession(ctx, sessionOptions)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(ctx, fileAction(t, session))
			retainID := fileAction(t, session)
			receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: httpNode(t, backend, "file"), Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainID)
			if operation == storage.OpFileRetain {
				requireUnknownFileAction(t, receipt, retainID, storage.OpFileRetain, err)
				transport.requireCalls(t, retainID, storage.OpFileRetain)
				receipt, err = session.QueryAction(ctx, retainID)
				transport.requireCalls(t, retainID, storage.OpFileRetain, storage.OpFileQueryAction)
			}
			if err != nil || receipt.Reference == 0 {
				t.Fatalf("retain receipt=%+v,%v", receipt, err)
			}
			file, err := session.Reference(ctx, receipt.Reference)
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Remove(ctx, "file"); err != nil {
				t.Fatal(err)
			}
			closeID := fileAction(t, session)
			closed, err := file.Close(ctx, closeID)
			if operation == storage.OpFileClose {
				requireUnknownFileAction(t, closed, closeID, storage.OpFileClose, err)
				transport.requireCalls(t, closeID, storage.OpFileClose)
				closed, err = session.QueryAction(ctx, closeID)
				transport.requireCalls(t, closeID, storage.OpFileClose, storage.OpFileQueryAction)
			}
			if err != nil || closed.State != storage.FileActionCompleted {
				t.Fatalf("close receipt=%+v,%v", closed, err)
			}
			if !transport.dropped {
				t.Fatalf("%s reply was not dropped", operation)
			}
			usage, err := backend.Usage(ctx)
			if err != nil || usage != 0 {
				t.Fatalf("lost %s reply retained %d bytes: %v", operation, usage, err)
			}
		})
	}
}
