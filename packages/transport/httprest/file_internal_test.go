package httprest

import (
	"context"
	"errors"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func retainedHTTPFixture(t *testing.T, limits FileLimits) (*Storage, *Handler, *objectstore.Storage) {
	t.Helper()
	_, backend := memoryfixture.New(t, "retained-http", 1<<20, locking.DefaultOptions())
	options := DefaultHandlerOptions()
	options.Files = limits
	h, err := NewHandlerWithOptions(backend, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(func() {
		server.Close()
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client, h, backend
}

func TestRetainedHTTPUnacknowledgedOpenExpiresDuringRenewal(t *testing.T) {
	ctx := context.Background()
	limits := DefaultFileLimits()
	limits.PendingAck = 20 * time.Millisecond
	client, _, backend := retainedHTTPFixture(t, limits)
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	defer remote.Close(ctx)
	if err := backend.Write(ctx, "file", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	response, err := remote.call(ctx, fileRequest{Op: "open", Path: []byte("file"), Open: storage.FileOpenOptions{Read: true}})
	if err != nil || response.File == "" {
		t.Fatalf("pending open = %+v, %v", response, err)
	}
	if err := backend.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := remote.Renew(ctx); err != nil {
			t.Fatal(err)
		}
		usage, err := backend.Usage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if usage == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unacknowledged reference retained %d bytes during renewal", usage)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := remote.call(ctx, fileRequest{Op: "ack", File: response.File}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired reference acknowledgement = %v", err)
	}
}

func TestRetainedHTTPActionEvictionCannotRepeatAnOpen(t *testing.T) {
	ctx := context.Background()
	client, h, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	defer remote.Close(ctx)
	id, err := storage.NewLockRequestID(remote.epoch)
	if err != nil {
		t.Fatal(err)
	}
	request := fileRequest{Op: "open", Session: remote.id, Action: id, Path: []byte("file"), Open: storage.FileOpenOptions{Read: true}}
	original, err := client.fileCall(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := client.fileCall(ctx, request)
	if err != nil || repeated.File != original.File {
		t.Fatalf("replayed open = %+v, %v", repeated, err)
	}
	h.files.mu.Lock()
	serverSession := h.files.sessions[remote.id]
	h.files.mu.Unlock()
	serverSession.mu.Lock()
	serverSession.started = serverSession.started.Add(-3 * serverSession.options.History)
	delete(serverSession.actions, id)
	serverSession.mu.Unlock()
	if _, err := client.fileCall(ctx, request); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("evicted old action ran again: %v", err)
	}
	serverSession.mu.Lock()
	count := len(serverSession.files)
	serverSession.mu.Unlock()
	if count != 1 {
		t.Fatalf("replays retained %d references", count)
	}
}

func TestRetainedHTTPActionWindowRolloverRequiresNonexecutionReceipt(t *testing.T) {
	ctx := context.Background()
	client, h, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	defer remote.Close(ctx)
	h.files.mu.Lock()
	serverSession := h.files.sessions[remote.id]
	h.files.mu.Unlock()
	serverSession.mu.Lock()
	serverSession.started = serverSession.started.Add(-serverSession.options.History)
	serverSession.mu.Unlock()
	file, err := remote.OpenFile(ctx, "file", storage.FileOpenOptions{Read: true})
	if err != nil {
		t.Fatalf("new action did not advance through an exact nonexecution receipt: %v", err)
	}
	if err := file.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedHTTPRegistryLimitsFailBeforeAnotherNativeReference(t *testing.T) {
	ctx := context.Background()
	limits := DefaultFileLimits()
	limits.MaxSessions = 1
	limits.MaxActions = 1
	client, _, backend := retainedHTTPFixture(t, limits)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteFileSession)
	defer remote.Close(ctx)
	if _, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("session admission = %v", err)
	}
	response, err := remote.call(ctx, fileRequest{Op: "open", Path: []byte("file"), Open: storage.FileOpenOptions{Read: true}})
	if err != nil || response.File == "" {
		t.Fatalf("first action = %+v, %v", response, err)
	}
	if _, err := remote.call(ctx, fileRequest{Op: "open", Path: []byte("file"), Open: storage.FileOpenOptions{Read: true}}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("action admission = %v", err)
	}
	if _, err := remote.Renew(ctx); err != nil {
		t.Fatalf("history capacity blocked renewal: %v", err)
	}
}
