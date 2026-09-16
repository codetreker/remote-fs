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
	"net/http/httptest"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
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

func retainedAction(t *testing.T, session storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := session.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func TestRetainedClaimKeepsStrictJSONFields(t *testing.T) {
	action, _ := storage.NewFileActionID(1)
	request := fileRequest{Op: storage.OpFileRetain, Session: strings.Repeat("a", 64), Action: action, Retain: &fileRetainRequest{NodeID: 9, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent, Excludes: storage.RemoveEntry}}}
	encoded, err := marshalFileJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, request) {
		t.Fatalf("roundtrip=%+v,%v", decoded, err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{
		[]byte(`{"Uses":0}`), []byte(`{"Excludes":0}`), []byte(`{"Uses":0,"Uses":1,"Excludes":0}`), []byte(`{"uses":0,"Excludes":0}`), []byte(`{"Uses":0,"Excludes":0,"Read":true}`),
	} {
		var claim storage.AccessClaim
		if err := decodeFileJSON(body, &claim); err == nil {
			t.Fatalf("noncanonical claim accepted: %s", body)
		}
	}
	if !bytes.Contains(encoded, []byte(`"retain":`)) {
		t.Fatalf("missing retain payload: %s", encoded)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	if _, ok := envelope["open"]; ok {
		t.Fatal("platform open flags escaped into wire")
	}
}
func TestRetainedHTTPUnresolvedRetainStaysOwnedDuringRenewal(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, _, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	action := retainedAction(t, session)
	receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: attr.ID, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, action)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Remove(ctx, "file"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := session.Renew(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if used, err := backend.Usage(ctx); err != nil || used != 8 {
		t.Fatalf("unresolved native ownership=%d,%v", used, err)
	}
	queried, err := session.QueryAction(ctx, action)
	if err != nil || queried.Reference != receipt.Reference || queried.State != storage.FileActionCompleted {
		t.Fatalf("native receipt=%+v,%v", queried, err)
	}
	if _, err := session.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if used, err := backend.Usage(ctx); err != nil || used != 0 {
		t.Fatalf("session cleanup=%d,%v", used, err)
	}
}
func TestRetainedHTTPActionReplayCannotResurrectAClosedReference(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	node, err := file.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	action := retainedAction(t, session)
	request := storage.RetainRequest{NodeID: node.Attr.ID}
	original, err := session.Retain(ctx, request, action)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := session.Retain(ctx, request, action)
	if err != nil || repeated.Reference != original.Reference {
		t.Fatalf("replay=%+v,%v", repeated, err)
	}
	ref, err := session.Reference(ctx, original.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	repeated, err = session.Retain(ctx, request, action)
	if err != nil || repeated.Reference != original.Reference {
		t.Fatalf("historical replay=%+v,%v", repeated, err)
	}
	terminal, err := session.Reference(ctx, original.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := terminal.ReadAt(ctx, storage.FileReadRequest{Length: 4}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("replay resurrected closed reference I/O:%v", err)
	}
}
func TestRetainedHTTPRetiredActionEpochCannotExecute(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.History = 100 * time.Millisecond
	session, status, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := storage.NewFileActionID(status.ActionEpoch)
	time.Sleep(status.HistoryRemaining + 10*time.Millisecond)
	renewed, err := session.Renew(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.ActionEpoch <= status.ActionEpoch {
		t.Fatalf("epoch failed to advance: %+v", renewed)
	}
	attr, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: attr.ID}, old)
	if !errors.Is(err, syscall.ESTALE) || receipt.Effects != 0 || receipt.Reference != 0 {
		t.Fatalf("retired request executed: %+v,%v", receipt, err)
	}
	if _, err := session.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
}
func TestRetainedHTTPRegistryLimitsFailBeforeAnotherNativeReference(t *testing.T) {
	ctx := context.Background()
	limits := DefaultFileLimits()
	limits.MaxSessions = 1
	limits.Session.MaxFiles = 1
	client, _, backend := retainedHTTPFixture(t, limits)
	if err := backend.Write(ctx, "file", []byte("data")); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session, _, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.NewFileSession(ctx, options); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("session admission=%v", err)
	}
	attr, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	request := storage.RetainRequest{NodeID: attr.ID}
	first, err := session.Retain(ctx, request, retainedAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := session.Retain(ctx, request, retainedAction(t, session))
	if !errors.Is(err, syscall.EMFILE) || rejected.Reference != 0 || rejected.Effects != 0 {
		t.Fatalf("reference admission=%+v,%v", rejected, err)
	}
	if _, err := session.Renew(ctx); err != nil {
		t.Fatalf("capacity blocked renewal:%v", err)
	}
	ref, err := session.Reference(ctx, first.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(ctx, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
}
