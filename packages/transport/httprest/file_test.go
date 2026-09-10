package httprest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
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

func fileSession(t *testing.T, s storage.FileStorage) storage.FileSession {
	t.Helper()
	session, err := s.NewFileSession(context.Background(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return session
}

func openHTTPFile(t *testing.T, s storage.FileSession, path string, o storage.FileOpenOptions) storage.File {
	t.Helper()
	file, err := s.OpenFile(context.Background(), path, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return file
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
	file := openHTTPFile(t, session, "file", storage.FileOpenOptions{ExpectedID: original.ID, Read: true, Write: true})
	if err := backend.Write(ctx, "file", []byte("newer content")); err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(ctx, 0, 64)
	if err != nil || string(read.Data) != "newer content" || read.Attr.Size != 13 {
		t.Fatalf("live read = %+v, %v", read, err)
	}
	if err := backend.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(ctx, 0, []byte("OLDER")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(ctx, 5); err != nil {
		t.Fatal(err)
	}
	read, err = file.ReadAt(ctx, 0, 64)
	if err != nil || string(read.Data) != "OLDER" || read.Attr.ID != original.ID {
		t.Fatalf("orphan read = %+v, %v", read, err)
	}
	current, err := backend.Read(ctx, "file")
	if err != nil || string(current) != "replacement" {
		t.Fatalf("replacement = %q, %v", current, err)
	}
	if _, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{ExpectedID: original.ID, Read: true}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("identity mismatch = %v", err)
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
	fa := openHTTPFile(t, sa, "file", storage.FileOpenOptions{Read: true, Write: true})
	fb := openHTTPFile(t, sb, "file", storage.FileOpenOptions{Read: true, Write: true})
	status, err := sa.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	result, err := fa.SetLock(ctx, 17, lock, id)
	if err != nil || result.State != storage.LockGranted {
		t.Fatalf("grant = %+v, %v", result, err)
	}
	conflict, err := fb.GetLock(ctx, 17, lock)
	if err != nil || !conflict.Found {
		t.Fatalf("cross-session conflict = %+v, %v", conflict, err)
	}
	status, err = sb.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err = storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	result, err = fb.SetLock(ctx, 17, lock, id)
	if err == nil && (result.State != storage.LockRejected || result.Errno != syscall.EAGAIN) || err != nil && !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("nonblocking conflict = %+v, %v", result, err)
	}
	if _, err := fb.WriteAt(ctx, 0, []byte("OPEN")); err != nil {
		t.Fatalf("advisory blocked ordinary I/O: %v", err)
	}
	if err := fa.DropLocks(ctx, 17, storage.Flock); err != nil {
		t.Fatal(err)
	}
	conflict, err = fb.GetLock(ctx, 17, lock)
	if err != nil || conflict.Found {
		t.Fatalf("released conflict = %+v, %v", conflict, err)
	}
}

type dropFileReply struct {
	next      http.RoundTripper
	mu        sync.Mutex
	operation string
	dropped   bool
	after     func() error
}

func (d *dropFileReply) RoundTrip(req *http.Request) (*http.Response, error) {
	var op struct {
		Op string `json:"op"`
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

func TestRetainedHTTPReplaysLostOpenWithoutAnotherReference(t *testing.T) {
	ctx := context.Background()
	backend := volumeFixture(t)
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
	transport := &dropFileReply{next: server.Client().Transport, operation: "open"}
	client, err := httprest.Dial(server.URL, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(ctx)
	first, err := session.OpenFile(ctx, "created", storage.FileOpenOptions{Read: true, Write: true, Create: true, Exclusive: true, Mode: 0600})
	if err != nil {
		t.Fatal(err)
	}
	if !transport.dropped {
		t.Fatal("open response was not dropped")
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := session.OpenFile(ctx, "created", storage.FileOpenOptions{Read: true})
	if err != nil {
		t.Fatalf("the repeated open retained an extra reference: %v", err)
	}
	if err := second.Close(ctx); err != nil {
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
	transport := &dropFileReply{next: server.Client().Transport, operation: "truncate", after: func() error { return backend.Write(ctx, "file", []byte("later update")) }}
	client, err := httprest.Dial(server.URL, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	session := fileSession(t, client)
	file := openHTTPFile(t, session, "file", storage.FileOpenOptions{Read: true, Write: true})
	attr, err := file.Truncate(ctx, 1)
	if err != nil || attr.Size != 1 {
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
	sa, err := a.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	sb := fileSession(t, b)
	fa, err := sa.OpenFile(ctx, "file", storage.FileOpenOptions{Read: true})
	if err != nil {
		t.Fatal(err)
	}
	fb := openHTTPFile(t, sb, "file", storage.FileOpenOptions{Read: true})
	if err := ha.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fa.ReadAt(ctx, 0, 4); !errors.Is(err, syscall.EIO) {
		t.Fatalf("retired handler read = %v", err)
	}
	read, err := fb.ReadAt(ctx, 0, 4)
	if err != nil || string(read.Data) != "data" {
		t.Fatalf("other handler lost shared backend: %+v, %v", read, err)
	}

}

func TestRetainedHTTPReconcilesLostAcknowledgementAndClose(t *testing.T) {
	for _, operation := range []string{"ack", "close"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			backend := volumeFixture(t)
			if err := backend.Write(ctx, "file", []byte("retained")); err != nil {
				t.Fatal(err)
			}
			options := httprest.DefaultHandlerOptions()
			options.Files = httprest.DefaultFileLimits()
			options.Files.MaxActions = 1
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
			session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(ctx)
			file, err := session.OpenFile(ctx, "file", storage.FileOpenOptions{Read: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Remove(ctx, "file"); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(ctx); err != nil {
				t.Fatal(err)
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
