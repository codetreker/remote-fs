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
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
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

func TestInitialMetadataEncodesPresentEmptyValues(t *testing.T) {
	request := fileRequest{Op: storage.OpFileOpen, Session: strings.Repeat("a", 64), Action: "1:00000000000000000000000000000000", Path: []byte("f"), Data: []byte{}, Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Create: true}, InitialMetadata: map[string][]byte{"client.empty": nil}}}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(`"client.empty":null`)) || !bytes.Contains(encoded, []byte(`"client.empty":""`)) {
		t.Fatalf("empty initial metadata is not canonical: %s", encoded)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil || len(decoded.Open.InitialMetadata["client.empty"]) != 0 {
		t.Fatalf("empty initial metadata round trip: %+v, %v", decoded.Open.InitialMetadata, err)
	}
}

func TestKnownFutureCapabilityBitsAreAccepted(t *testing.T) {
	response := fileResponse{Epoch: 1, Session: strings.Repeat("a", 64), Data: []byte{}, Status: &storage.FileSessionStatus{Epoch: "authority", Revision: 1, ActionEpoch: 1}, Capabilities: &fileCapabilities{AtomicOpen: true, Namespace: true, References: true, State: true, Delete: true, Conditional: true, DirectoryMetadata: true, ReferenceName: true}}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileSessionOpen}, response); err != nil {
		t.Fatalf("known future capability bits were rejected: %v", err)
	}
	response.Capabilities.Namespace = false
	if err := validateFileResponse(fileRequest{Op: storage.OpFileSessionOpen}, response); err != nil {
		t.Fatalf("directory observation bundle was coupled to namespace access: %v", err)
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
	file, err := referenceCapabilitiesOf(advertisedFile{})
	if err != nil || !file.Metadata || !file.Scope {
		t.Fatalf("file capabilities = %#v, %v", file, err)
	}
	encoded, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"atomicOpen", "namespace", "references", "actions", "state", "delete", "conditional", "directoryMetadata", "referenceName"} {
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

type partialRangeBackend struct{ *objectstore.Storage }

func (b partialRangeBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &partialRangeSession{FileSession: session}, nil
}

type partialRangeSession struct{ storage.FileSession }

func (*partialRangeSession) CheckRangeControl() error { return nil }
func (*partialRangeSession) GetConflict(context.Context, storage.UseOwner, storage.RangeCommand) (storage.RangeConflict, error) {
	return storage.RangeConflict{}, nil
}
func (*partialRangeSession) Apply(_ context.Context, _ storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	time.Sleep(25 * time.Millisecond)
	failedAt := 1
	return storage.RangeAttempt{Request: request, State: storage.Rejected, Commands: commands, Rejection: storage.RangeBlocked, FailedAt: &failedAt, Effects: []storage.RangeEffect{{Command: commands[0], Released: true}}, HistoryRemaining: time.Second}, syscall.EAGAIN
}
func (*partialRangeSession) Query(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	return storage.RangeAttempt{}, syscall.ESTALE
}
func (*partialRangeSession) Cancel(context.Context, storage.UseOwner, storage.LockRequestID) (storage.RangeAttempt, error) {
	return storage.RangeAttempt{}, syscall.ESTALE
}
func (*partialRangeSession) Drop(context.Context, storage.UseOwner, storage.ConflictDomain) error {
	return nil
}

type invalidScopeBackend struct{ *objectstore.Storage }

func (b invalidScopeBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &invalidScopeSession{FileSession: session}, nil
}

type invalidScopeSession struct{ storage.FileSession }

func (s *invalidScopeSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	file, err := s.FileSession.OpenFile(ctx, path, options)
	if err != nil {
		return nil, err
	}
	return invalidScopeFile{File: file}, nil
}

type invalidScopeFile struct{ storage.File }

func (invalidScopeFile) CheckScopedReference() error { return nil }
func (invalidScopeFile) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: string([]byte{0xff})}, nil
}

func TestRangeErrorPreservesPartialReceiptAcrossHTTP(t *testing.T) {
	meta, backend := memoryfixture.New(t, "partial-range-http", 1<<20, locking.DefaultOptions())
	handler, err := NewHandler(partialRangeBackend{Storage: backend}, meta)
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
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	request, _ := storage.NewLockRequestID(1)
	release := storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: 1}, Edit: storage.Subtract}
	acquire := storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: 1, Length: 1}, Edit: storage.Replace}
	attempt, err := session.(storage.RangeControl).Apply(t.Context(), 1, []storage.RangeCommand{release, acquire}, request)
	if !errors.Is(err, syscall.EAGAIN) || attempt.FailedAt == nil || *attempt.FailedAt != 1 || len(attempt.Effects) != 1 || !attempt.Effects[0].Released || attempt.HistoryRemaining <= 0 || attempt.HistoryRemaining >= time.Second {
		t.Fatalf("partial range result=%+v error=%v", attempt, err)
	}
}

func TestReferenceMetadataAndOwnerLifecycleAcrossHTTP(t *testing.T) {
	meta, backend := memoryfixture.New(t, "reference-capabilities-http", 1<<20, locking.DefaultOptions())
	if err := backend.Write(t.Context(), "file", []byte("content")); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, meta)
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
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(context.Background())
	scoped := file.(storage.ScopedReference)
	scope, err := scoped.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	metadata := file.(ReferenceMetadataAccessWithBarrier)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	first, err := metadata.SetMetadata(t.Context(), "client.reference", nil, []byte("one"))
	if err != nil || len(first.Version) == 0 || string(first.Data) != "one" {
		t.Fatalf("first metadata=%+v error=%v", first, err)
	}
	second, barrier, err := metadata.SetMetadataWithBarrier(t.Context(), "client.reference", first.Version, []byte("two"))
	if err != nil || barrier == nil || len(second.Version) == 0 || string(second.Data) != "two" {
		t.Fatalf("second metadata=%+v barrier=%+v error=%v", second, barrier, err)
	}
	attr, err := file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	owners := session.(storage.UseOwners)
	owner, err := owners.NewUseOwner(t.Context(), attr.ID, scope, storage.OwnerOptions{Lifetime: storage.OwnerExplicit, Diagnostic: 4242})
	if err != nil {
		t.Fatal(err)
	}
	other, err := owners.NewUseOwner(t.Context(), attr.ID, scope, storage.OwnerOptions{Lifetime: storage.OwnerExplicit, Diagnostic: 8484})
	if err != nil {
		t.Fatal(err)
	}
	ranges := session.(storage.RangeControl)
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.RangeCommand{Domain: storage.DomainWholeFile, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Length: ^uint64(0)}, Edit: storage.Replace}
	if attempt, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{command}, request); err != nil || attempt.State != storage.Granted {
		t.Fatalf("range acquisition=%+v error=%v", attempt, err)
	}
	conflict, err := ranges.GetConflict(t.Context(), other, command)
	if err != nil || !conflict.Found || conflict.Owner != 4242 {
		t.Fatalf("range conflict=%+v error=%v", conflict, err)
	}
	if err := ranges.Drop(t.Context(), owner, storage.DomainWholeFile); err != nil {
		t.Fatal(err)
	}
	if err := owners.RetireUseOwner(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if err := owners.RetireUseOwner(t.Context(), other); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPRejectsInvalidBackendUseScopeBeforeEncoding(t *testing.T) {
	meta, backend := memoryfixture.New(t, "invalid-scope-http", 1<<20, locking.DefaultOptions())
	if err := backend.Write(t.Context(), "file", []byte("content")); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(invalidScopeBackend{Storage: backend}, meta)
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
	session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(context.Background())
	if _, err := file.(storage.ScopedReference).Scope(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid backend scope error=%v", err)
	}
}

func TestRetainedFileErrorsPreserveTheirCause(t *testing.T) {
	for name, err := range map[string]error{
		"barrier":  &fileBarrierError{cause: syscall.EIO},
		"recorded": &recordedFileError{cause: syscall.EIO},
	} {
		if err.Error() != syscall.EIO.Error() || !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s error=%v", name, err)
		}
	}
}

func TestImmediateRecoveryReturnsRecordedFileError(t *testing.T) {
	recorded := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body, _ := json.Marshal(ErrorResponse{CapabilityCode: "condition-conflict", FileRecorded: &recorded, Errno: "EAGAIN", Message: "version changed"})
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
	original := client.http.Transport
	var calls atomic.Int32
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := original.RoundTrip(request)
		if err == nil && calls.Add(1) == 1 {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			return nil, errors.New("lost response")
		}
		return response, err
	})
	session := &remoteFileSession{storage: client, id: strings.Repeat("a", 64), epoch: 1, pendingLimit: 4}
	_, err = session.call(t.Context(), fileRequest{Op: storage.OpFileSetNodeMetadata, Node: 7, Namespace: "client.v1", Payload: metadataPayload("value")})
	if !errors.Is(err, storage.ErrConditionConflict) || storage.ErrnoOf(err) != syscall.EAGAIN || calls.Load() != 2 {
		t.Fatalf("recorded recovery error=%v calls=%d", err, calls.Load())
	}
	if len(session.pending) != 0 {
		t.Fatalf("recorded outcome was retained as unknown: %+v", session.pending)
	}
}

func TestRecordedFileReplayPreservesNestedLockError(t *testing.T) {
	failure := &locking.Error{Code: locking.Conflict, Recorded: true, Message: "record lock occupied"}
	response := fileErrorResponse(&recordedFileError{cause: failure}, true)
	if response.FileRecorded == nil || !*response.FileRecorded || response.LockCode != failure.Code || response.Recorded == nil || *response.Recorded != failure.Recorded || response.Message != failure.Message {
		t.Fatalf("recorded response=%+v", response)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded := (&Storage{}).storageError(Request{Op: OpFile}, encoded)
	var operation *operationError
	var lockFailure *locking.Error
	if !errors.As(decoded, &operation) || !operation.recorded || !errors.As(decoded, &lockFailure) || lockFailure.Code != failure.Code || lockFailure.Recorded != failure.Recorded || lockFailure.Message != failure.Message {
		t.Fatalf("decoded recorded lock error=%#v", decoded)
	}
	retained := retainFileError(&locking.Error{Code: locking.Conflict, Recorded: true, Message: strings.Repeat("x", 4097) + string([]byte{0xff})})
	if !errors.As(retained, &lockFailure) || len(lockFailure.Message) > 4096 || !strings.Contains(lockFailure.Message, "cannot be retained") {
		t.Fatalf("unbounded retained lock error=%#v", retained)
	}
}

func TestFileRequestAdmissionPrecedesRequestFreezing(t *testing.T) {
	const maximum = int64(1024)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		body, _ := json.Marshal(ErrorResponse{Errno: "EIO", Message: "probe"})
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
	client.maxBodyBytes, client.maxWriteBytes = maximum, maximum
	client.fileRequests = newBodyAdmission(1, retainedResponseMultiplier*maximum, 0)
	client.lockControls = newBodyAdmission(1, retainedResponseMultiplier*MaxFileControlBytes, 0)
	session := &remoteFileSession{storage: client, id: strings.Repeat("a", 64), epoch: 1, pendingLimit: 1}
	releaseData, err := client.fileRequests.acquire(t.Context(), retainedResponseMultiplier*maximum)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseData()
	if _, err := session.call(t.Context(), fileRequest{Op: storage.OpFileSetNodeMetadata, Node: 7, Namespace: "client.v1", Payload: metadataPayload("value")}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("saturated data request admission error=%v", err)
	}
	if _, err := session.call(t.Context(), fileRequest{Op: storage.OpFileScope, File: strings.Repeat("b", 64)}); !errors.Is(err, syscall.EIO) || calls.Load() != 1 {
		t.Fatalf("data saturation blocked control request: calls=%d error=%v", calls.Load(), err)
	}
	releaseControl, err := client.lockControls.acquire(t.Context(), retainedResponseMultiplier*MaxFileControlBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseControl()
	if _, err := session.call(t.Context(), fileRequest{Op: storage.OpFileScope, File: strings.Repeat("b", 64)}); !errors.Is(err, syscall.EAGAIN) || calls.Load() != 1 {
		t.Fatalf("saturated control request admission: calls=%d error=%v", calls.Load(), err)
	}
}

func TestPendingActionFreezesOriginalResultBudget(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var seen []int64
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		var sent fileRequest
		if err := decodeFileJSON(body, &sent); err != nil {
			return nil, err
		}
		seen = append(seen, sent.ResultBytes)
		return nil, errors.New("response lost")
	})
	session := &remoteFileSession{storage: client, id: strings.Repeat("a", 64), epoch: 1, pendingLimit: 4}
	modTime := time.Unix(1, 0)
	ctx := storage.WithBoundedAttrResult(t.Context(), 1024, func(storage.Attr, int64) error { return nil })
	_, err = session.call(ctx, fileRequest{Op: storage.OpFileSetNodeAttr, Node: 7, Change: AttrChangeOf(storage.AttrChange{ModTime: &modTime})})
	if storage.ErrnoOf(err) != syscall.EIO || !reflect.DeepEqual(seen, []int64{1024, 1024}) {
		t.Fatalf("lost bounded action result=%v seen=%v", err, seen)
	}
	for _, pending := range session.pending {
		if pending.request.ResultBytes != 1024 {
			t.Fatalf("pending result bound=%d, want 1024", pending.request.ResultBytes)
		}
	}
}

func TestOppositeClassPendingActionsReconcileWithoutAdmissionDeadlock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var sent fileRequest
		if err := decodeFileJSON(body, &sent); err != nil {
			t.Error(err)
			return
		}
		response := fileResponse{Epoch: 1, Data: []byte{}}
		if sent.Op == storage.OpFileSetNodeMetadata {
			response.Metadata = &OpaquePayload{Version: []byte{1}, Data: []byte("value")}
		}
		encoded, _ := json.Marshal(response)
		w.Header().Set(HeaderProtocol, Version)
		w.Header().Set("Content-Type", contentJSON)
		w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encoded)
	}))
	defer server.Close()
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client.fileRequests = newBodyAdmission(1, retainedResponseMultiplier*client.maxBodyBytes, 2)
	client.lockControls = newBodyAdmission(1, retainedResponseMultiplier*MaxFileControlBytes, 2)
	dataAction, _ := storage.NewLockRequestID(1)
	controlAction, _ := storage.NewLockRequestID(1)
	sessionID := strings.Repeat("a", 64)
	data := fileRequest{Op: storage.OpFileSetNodeMetadata, Session: sessionID, Action: dataAction, Node: 7, Namespace: "client.v1", Payload: metadataPayload("value"), ResultBytes: client.maxBodyBytes, Path: []byte{}, Data: []byte{}}
	control := fileRequest{Op: storage.OpFileRangeDrop, Session: sessionID, Action: controlAction, Owner: 1, Domain: storage.DomainRecord, Path: []byte{}, Data: []byte{}}
	session := &remoteFileSession{storage: client, id: sessionID, epoch: 1, pendingLimit: 4, pending: map[string]pendingFileAction{
		string(dataAction):    {request: data, unknown: syscall.EIO},
		string(controlAction): {request: control, unknown: syscall.EIO},
	}}
	done := make(chan error, 2)
	go func() {
		_, err := session.call(t.Context(), fileRequest{Op: storage.OpFileSetNodeMetadata, Node: 7, Namespace: "client.v1", Payload: metadataPayload("value")})
		done <- err
	}()
	go func() {
		_, err := session.call(t.Context(), fileRequest{Op: storage.OpFileRangeDrop, Owner: 1, Domain: storage.DomainRecord})
		done <- err
	}()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("opposite-class pending reconciliation deadlocked")
		}
	}
}

func TestFileActionsProgressConcurrentlyWithinThePendingBound(t *testing.T) {
	for _, test := range []struct {
		name        string
		limit       int
		wantEntered int
	}{
		{"two independent actions", 2, 2},
		{"bounded admission", 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				entered <- struct{}{}
				<-release
				body, _ := json.Marshal(ErrorResponse{CapabilityCode: "condition-conflict", Errno: "EAGAIN", Message: "conflict"})
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
			session := &remoteFileSession{storage: client, id: strings.Repeat("a", 64), epoch: 1, pendingLimit: test.limit}
			errorsByCall := make(chan error, 2)
			for index := 0; index < 2; index++ {
				go func(node uint64) {
					_, err := session.call(t.Context(), fileRequest{Op: storage.OpFileSetNodeMetadata, Node: node, Namespace: "client.v1", Payload: metadataPayload("value")})
					errorsByCall <- err
				}(uint64(index + 1))
			}
			for range test.wantEntered {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("independent file action did not reach the server")
				}
			}
			if test.wantEntered == 1 {
				select {
				case err := <-errorsByCall:
					if !errors.Is(err, syscall.EAGAIN) {
						t.Fatalf("bounded action error=%v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("action above the pending bound did not fail")
				}
			}
			close(release)
			remaining := 2
			if test.wantEntered == 1 {
				remaining = 1
			}
			for range remaining {
				if err := <-errorsByCall; !errors.Is(err, storage.ErrConditionConflict) {
					t.Fatalf("dispatched action error=%v", err)
				}
			}
		})
	}
}

func TestUseScopeWirePreservesUTF8AndRejectsInvalidText(t *testing.T) {
	scope := storage.UseScope{Token: "租户/α"}
	request := fileRequest{Op: storage.OpFileNewUseOwner, Session: strings.Repeat("a", 64), Node: 7, Scope: &scope, OwnerOptions: storage.OwnerOptions{Lifetime: storage.OwnerExplicit}, Path: []byte{}, Data: []byte{}}
	if err := validateCapabilityArguments(request); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil || decoded.Scope == nil || decoded.Scope.Token != scope.Token {
		t.Fatalf("scope round trip=%+v error=%v", decoded.Scope, err)
	}
	invalid := storage.UseScope{Token: string([]byte{0xff})}
	request.Scope = &invalid
	if err := validateCapabilityArguments(request); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid request scope error=%v", err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileScope}, fileResponse{Epoch: 1, Data: []byte{}, Scope: &invalid}); err == nil {
		t.Fatal("accepted invalid UTF-8 response scope")
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
