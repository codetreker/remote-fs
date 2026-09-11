package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
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

func TestRetainedOpenAccessKeepsFlatStrictJSONFields(t *testing.T) {
	action, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	request := fileRequest{
		Op: authz.FileOpen, Session: strings.Repeat("a", 64), Action: action,
		Path: []byte("file"), Data: []byte{},
		Open: storage.FileOpenOptions{
			OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Truncate: true, Exclusive: true},
			ExpectedID: 9, Mode: 0o640,
		},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"op":"file.open"`)) || !bytes.Contains(encoded, []byte(`"open":{`)) {
		t.Fatalf("file operation or open member changed shape: %s", encoded)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, request) {
		t.Fatalf("flattened open round trip = %+v, %v", decoded, err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	var open map[string]json.RawMessage
	if err := json.Unmarshal(envelope["open"], &open); err != nil {
		t.Fatal(err)
	}
	if len(open) != 7 {
		t.Fatalf("flattened open fields = %v", open)
	}
	for _, name := range []string{"Read", "Write", "Create", "Truncate", "Exclusive", "ExpectedID", "Mode"} {
		if _, exists := open[name]; !exists {
			t.Fatalf("open field %q is missing", name)
		}
	}
	for _, name := range []string{"Read", "Write", "Create", "Truncate", "Exclusive"} {
		t.Run("missing "+name, func(t *testing.T) {
			var changed map[string]json.RawMessage
			if err := json.Unmarshal(envelope["open"], &changed); err != nil {
				t.Fatal(err)
			}
			delete(changed, name)
			body, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			var options storage.FileOpenOptions
			if err := decodeFileJSON(body, &options); err == nil {
				t.Fatalf("missing flattened flag %q was accepted", name)
			}
		})
	}
	for _, body := range [][]byte{
		bytes.Replace(envelope["open"], []byte(`"Read":true`), []byte(`"Read":true,"Read":false`), 1),
		bytes.Replace(envelope["open"], []byte(`"Read":true`), []byte(`"read":true`), 1),
		[]byte(`{"OpenAccess":{"Read":true,"Write":true,"Create":true,"Truncate":true,"Exclusive":true},"ExpectedID":9,"Mode":416}`),
	} {
		var options storage.FileOpenOptions
		if err := decodeFileJSON(body, &options); err == nil {
			t.Fatalf("noncanonical embedded flags were accepted: %s", body)
		}
	}
}

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
	response, err := remote.call(ctx, fileRequest{Op: authz.FileOpen, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}})
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
	if _, err := remote.call(ctx, fileRequest{Op: authz.FileAck, File: response.File}); !errors.Is(err, syscall.ESTALE) {
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
	request := fileRequest{Op: authz.FileOpen, Session: remote.id, Action: id, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}}
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
	file, err := remote.OpenFile(ctx, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
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
	response, err := remote.call(ctx, fileRequest{Op: authz.FileOpen, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}})
	if err != nil || response.File == "" {
		t.Fatalf("first action = %+v, %v", response, err)
	}
	if _, err := remote.call(ctx, fileRequest{Op: authz.FileOpen, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("action admission = %v", err)
	}
	if _, err := remote.Renew(ctx); err != nil {
		t.Fatalf("history capacity blocked renewal: %v", err)
	}
}
