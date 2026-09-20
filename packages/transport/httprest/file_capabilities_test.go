package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func TestMetadataRequestAndResponseUseDifferentVersionRules(t *testing.T) {
	request := fileRequest{
		Op: storage.OpFileSetNodeMetadata, Session: strings.Repeat("a", 64),
		Action: "1:00000000000000000000000000000000", Node: 7,
		ResultBytes: 4096, Namespace: "client.v1", Version: metadataVersion{}, Payload: metadataPayload{},
		Path: []byte{}, Data: []byte{},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatalf("empty absence precondition was rejected: %v", err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"version", "payload"} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, field)
		damaged, _ := json.Marshal(fields)
		if err := decodeFileJSON(damaged, &decoded); err == nil {
			t.Fatalf("missing %s was accepted", field)
		}
	}
	for name, damaged := range map[string][]byte{
		"noncanonical version": bytes.Replace(encoded, []byte(`"version":""`), []byte(`"version":"AR=="`), 1),
		"invalid payload":      bytes.Replace(encoded, []byte(`"payload":""`), []byte(`"payload":"%%%"`), 1),
	} {
		if err := decodeFileJSON(damaged, &decoded); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
	for _, body := range []string{
		`{"version":"AR==","data":""}`,
		`{"version":"","data":""}`,
		`{"version":"AQ==","data":null}`,
	} {
		var response OpaquePayload
		if err := decodeFileJSON([]byte(body), &response); err == nil {
			t.Fatalf("invalid response payload accepted: %s", body)
		}
	}
}

type advertisedSession struct {
	storage.FileSession
	err error
}

type checkOnlySession struct{ storage.FileSession }

func (checkOnlySession) CheckMetadataAccess() error { return nil }

func (s advertisedSession) CheckMetadataAccess() error { return s.err }
func (advertisedSession) SetMetadata(context.Context, uint64, string, []byte, []byte) (storage.OpaquePayload, error) {
	panic("not called")
}
func (s advertisedSession) CheckUseOwners() error { return s.err }
func (advertisedSession) NewUseOwner(context.Context, uint64, storage.UseScope, storage.OwnerOptions) (storage.UseOwner, error) {
	panic("not called")
}
func (advertisedSession) RetireUseOwner(context.Context, storage.UseOwner) error { panic("not called") }
func (s advertisedSession) CheckRangeControl() error                             { return s.err }
func (advertisedSession) GetConflict(context.Context, storage.UseOwner, storage.RangeCommand) (storage.RangeConflict, error) {
	panic("not called")
}
func (advertisedSession) Apply(context.Context, storage.UseOwner, []storage.RangeCommand, storage.LockRequestID) (storage.RangeAttempt, error) {
	panic("not called")
}
func (advertisedSession) Query(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	panic("not called")
}
func (advertisedSession) Cancel(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	panic("not called")
}
func (advertisedSession) Drop(context.Context, storage.UseOwner, storage.ConflictDomain) error {
	panic("not called")
}

type advertisedFile struct {
	storage.File
	err error
}

func (f advertisedFile) CheckScopedReference() error                   { return f.err }
func (advertisedFile) Scope(context.Context) (storage.UseScope, error) { panic("not called") }
func (f advertisedFile) CheckMetadataAccess() error                    { return f.err }
func (advertisedFile) SetMetadata(context.Context, string, []byte, []byte) (storage.OpaquePayload, error) {
	panic("not called")
}

func TestCapabilityEnvelopeAdvertisesOnlyImplementedFacets(t *testing.T) {
	session, err := sessionCapabilitiesOf(advertisedSession{})
	if err != nil || !session.Metadata || !session.Owners || !session.Ranges {
		t.Fatalf("session capabilities = %#v, %v", session, err)
	}
	if err := validateSessionCapabilities(*session); err != nil {
		t.Fatal(err)
	}
	file, err := referenceCapabilitiesOf(advertisedFile{})
	if err != nil || !file.Metadata || !file.Scope {
		t.Fatalf("file capabilities = %#v, %v", file, err)
	}
	if err := validateReferenceCapabilities(*file); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"atomicOpen", "namespace", "references", "state", "delete", "conditional", "directoryMetadata", "referenceName"} {
		if !strings.Contains(string(encoded), `"`+field+`":false`) {
			t.Fatalf("future capability %q is not explicitly false: %s", field, encoded)
		}
	}
	unsupported, err := sessionCapabilitiesOf(advertisedSession{err: syscall.EOPNOTSUPP})
	if err != nil || unsupported.Metadata || unsupported.Owners || unsupported.Ranges {
		t.Fatalf("unsupported capabilities = %#v, %v", unsupported, err)
	}
	if _, err := sessionCapabilitiesOf(advertisedSession{err: syscall.EIO}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("capability failure became unsupported: %v", err)
	}
	checkOnly, err := sessionCapabilitiesOf(checkOnlySession{})
	if err != nil || checkOnly.Metadata {
		t.Fatalf("incomplete metadata interface was advertised: %#v, %v", checkOnly, err)
	}
}

func TestCapabilityErrorsPreserveIdentityAcrossTheWire(t *testing.T) {
	client := &Storage{}
	for code, failure := range capabilityErrors {
		response := ErrorResponse{Errno: storage.ErrnoNameOf(failure), Message: failure.Error(), CapabilityCode: code}
		encoded, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		decoded := client.storageError(Request{Op: OpFile}, encoded)
		if !errors.Is(decoded, failure) || storage.ErrnoOf(decoded) != storage.ErrnoOf(failure) {
			t.Fatalf("%s changed identity: %v", code, decoded)
		}
	}
}

type retryBarrierLog struct {
	metastore.Log
	calls    atomic.Int32
	failures int32
}

func (l *retryBarrierLog) Barrier(ctx context.Context, maximum int64) (metastore.LogBarrier, error) {
	if l.calls.Add(1) <= l.failures {
		return metastore.LogBarrier{}, syscall.EIO
	}
	return l.Log.Barrier(ctx, maximum)
}

func TestMetadataActionRetriesOnlyThePostCommitBarrier(t *testing.T) {
	meta, backend := memoryfixture.New(t, "metadata-barrier-retry", 1<<20, locking.DefaultOptions())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	log := &retryBarrierLog{Log: meta, failures: 2}
	handler, err := NewHandler(backend, log)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(storage.MetadataAccess)
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.SetMetadata(t.Context(), attr.ID, "client.v1", nil, []byte("once"))
	if storage.ErrnoOf(err) != syscall.EIO || !reflect.DeepEqual(result, storage.OpaquePayload{}) || log.calls.Load() != 2 {
		t.Fatalf("initial unknown metadata result=%+v barrierCalls=%d error=%v", result, log.calls.Load(), err)
	}
	remote := sessionValue.(*remoteFileSession)
	remote.mu.Lock()
	failed, closed, pending := remote.failed, remote.closed, remote.pending != nil
	remote.mu.Unlock()
	if failed != nil || closed || !pending {
		t.Fatalf("barrier failure fenced session: failed=%v closed=%v pending=%v", failed, closed, pending)
	}
	result, err = session.SetMetadata(t.Context(), attr.ID, "client.v1", nil, []byte("once"))
	if err != nil || len(result.Version) == 0 || string(result.Data) != "once" || log.calls.Load() != 3 {
		t.Fatalf("retried metadata result=%+v barrierCalls=%d error=%v", result, log.calls.Load(), err)
	}
	stored, err := backend.Stat(t.Context(), "file")
	if err != nil || string(stored.Metadata["client.v1"].Data) != "once" {
		t.Fatalf("stored metadata=%+v error=%v", stored.Metadata, err)
	}
}

func TestPendingActionReconciliationKeepsFrozenIntentAndScope(t *testing.T) {
	action, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("original")
	request, err := freezeFileRequest(fileRequest{
		Op: storage.OpFileSetNodeMetadata, Session: strings.Repeat("a", 64), Action: action,
		Node: 7, Namespace: "client.v1", Payload: metadataPayload(payload),
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := locking.MutationScope{Owner: lockTestOwner, Grants: []locking.GrantRef{lockTestGrant}}
	expectedScope := locking.CloneScope(scope)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		got, present, err := requestMutationScope(r, OpWrite)
		if err != nil || !present || !reflect.DeepEqual(got, expectedScope) {
			t.Errorf("reconciliation scope=%+v present=%v error=%v", got, present, err)
		}
		var sent fileRequest
		bodyBytes, _ := io.ReadAll(r.Body)
		if err := decodeFileJSON(bodyBytes, &sent); err != nil || string(sent.Payload) != "original" {
			t.Errorf("reconciliation request=%+v error=%v", sent, err)
		}
		body, _ := json.Marshal(fileResponse{Epoch: 1, Data: []byte{}, Metadata: &OpaquePayload{Version: []byte{1}, Data: []byte("original")}})
		w.Header().Set(HeaderProtocol, Version)
		w.Header().Set("Content-Type", contentJSON)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	session := &remoteFileSession{storage: client, id: request.Session, epoch: 1, pending: map[string]pendingFileAction{
		string(action): {request: request, scope: locking.CloneScope(scope), hasScope: true},
	}}
	payload[0] = 'X'
	scope.Grants[0].Generation++
	incoming := fileRequest{Op: storage.OpFileSetNodeMetadata, Node: 7, Namespace: "client.v1", Payload: metadataPayload("original")}
	incoming, err = freezeFileRequest(incoming)
	if err != nil {
		t.Fatal(err)
	}
	response, matched, err := session.reconcilePending(t.Context(), incoming, locking.MutationScope{}, false)
	if err != nil || matched || response.Metadata != nil || calls.Load() != 1 {
		t.Fatalf("reconciliation result=%+v matched=%v error=%v", response, matched, err)
	}
	if len(session.pending) != 1 || session.pending[string(action)].response == nil {
		t.Fatal("unrelated call discarded the resolved receipt")
	}
	response, matched, err = session.reconcilePending(t.Context(), incoming, expectedScope, true)
	if err != nil || !matched || response.Metadata == nil || string(response.Metadata.Data) != "original" || calls.Load() != 1 {
		t.Fatalf("matching reconciliation result=%+v matched=%v calls=%d error=%v", response, matched, calls.Load(), err)
	}
}

func TestPendingActionKeepsOriginalUnknownOutcomeWhenReauthorizationFails(t *testing.T) {
	action, _ := storage.NewLockRequestID(1)
	request, err := freezeFileRequest(fileRequest{
		Op: storage.OpFileSetNodeMetadata, Session: strings.Repeat("a", 64), Action: action,
		Node: 7, Namespace: "client.v1", Payload: metadataPayload("value"),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body, _ := json.Marshal(ErrorResponse{Errno: "EACCES", Message: "access denied"})
		w.Header().Set(HeaderProtocol, Version)
		w.Header().Set("Content-Type", contentJSON)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(StatusStorageError)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	original := unreachable(Request{Op: OpFile}, errors.New("response was lost"))
	session := &remoteFileSession{storage: client, id: request.Session, epoch: 1, pending: map[string]pendingFileAction{
		string(action): {request: request, unknown: original},
	}}
	_, matched, err := session.reconcilePending(t.Context(), fileRequest{Op: storage.OpFileStatus}, locking.MutationScope{}, false)
	if matched || !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EACCES) || len(session.pending) != 1 {
		t.Fatalf("reauthorization changed unknown outcome: matched=%v pending=%d error=%v", matched, len(session.pending), err)
	}
}
