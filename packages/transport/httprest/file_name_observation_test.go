package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNameObservationWireBudgetChargesActualPrefix(t *testing.T) {
	for _, directory := range []bool{false, true} {
		for _, length := range []int{0, 1, 2, 3, 127, storage.MaxLeafBytes} {
			scalar := storage.NameObservation{NodeID: math.MaxUint64, State: storage.NameLinked, ParentID: math.MaxUint64 - 1}
			if length == 0 {
				scalar.State, scalar.ParentID = storage.NameRoot, 0
			}
			charge, err := storage.CheckNameObservationBudget(storage.WithNameObservationBudget(t.Context(), nameObservationWireBudget(1<<20, directory)), scalar, int64(length))
			if err != nil {
				t.Fatal(err)
			}
			full := scalar
			if length > 0 {
				full.RawLeaf = bytes.Repeat([]byte{0xff}, length)
			}
			var wire []byte
			if directory {
				wire, err = json.Marshal(full)
				wire = append([]byte(`,"name":`), wire...)
			} else {
				wire, err = json.Marshal(fileResponse{Epoch: math.MaxUint64, Data: []byte{}, NameObservation: &full})
			}
			retained, _ := storage.NameObservationRetentionBytes(int64(length))
			if err != nil || charge != retained+int64(len(wire)) {
				t.Fatalf("directory=%v length=%d charge=%d wire=%d retained=%d: %v", directory, length, charge, len(wire), retained, err)
			}
			if _, err := nameObservationWireBudget(charge-1, directory)(scalar, int64(length)); !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("smaller prefix bound: %v", err)
			}
		}
	}
	short := storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1}
	if _, err := nameObservationWireBudget(1024, false)(short, 1); err != nil {
		t.Fatalf("short name should fit small result: %v", err)
	}
	if _, err := nameObservationWireBudget(1024, false)(short, storage.MaxLeafBytes); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("maximum name exceeds small result: %v", err)
	}
}

func TestNameObservationWireRequiresClosedShapes(t *testing.T) {
	name := storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff}}
	directory := storage.DirectoryTarget{NodeID: 2}
	options := storage.DirectoryMetadataOptions{IncludeName: true}
	for _, operation := range []storage.Operation{fileObserveName, fileObserveDirectoryMetadata} {
		request := fileRequest{Op: operation, Session: strings.Repeat("a", 64), Path: []byte{}, Data: []byte{}, ResultBytes: 1024}
		response := fileResponse{Epoch: 1, Data: []byte{}}
		if operation == fileObserveName {
			request.File = strings.Repeat("b", 64)
			response.NameObservation = &name
		} else {
			request.Directory, request.DirectoryMetadata = &directory, &options
			response.Directory = &observedDirectory{Observation: storage.DirectoryObservation{ParentID: 2, Revision: []byte("version")}, Name: &name, Entries: []observedEntry{}}
		}
		if err := validateFileRequest(request); err != nil {
			t.Fatal(err)
		}
		if err := validateFileArguments(request, storage.DefaultFileSessionOptions()); err != nil {
			t.Fatal(err)
		}
		if err := validateFileResponse(request, response); err != nil {
			t.Fatal(err)
		}
		if fileMutation(operation) || fileActionRequired(operation) || fileControl(operation) || !fileReadOnly(operation) {
			t.Fatalf("observation escaped read-only data lane: %s", operation)
		}
		bad := request
		bad.Node = 2
		if err := validateFileRequest(bad); err == nil {
			t.Fatal("unrelated operand accepted")
		}
		bad = request
		bad.Action, _ = storage.NewLockRequestID(1)
		if err := validateFileRequest(bad); err == nil {
			t.Fatal("observation accepted an action")
		}
		bad = request
		bad.ResultBytes = 0
		if err := validateFileArguments(bad, storage.DefaultFileSessionOptions()); err == nil {
			t.Fatal("zero result bound accepted")
		}
		badResponse := response
		badResponse.Barrier = &MutationBarrier{Incarnation: "unexpected", Position: 1}
		if err := validateFileResponse(request, badResponse); err == nil {
			t.Fatal("read-only observation accepted a mutation barrier")
		}
		for _, invalid := range []storage.NameObservation{{}, {NodeID: 2, State: storage.NameRoot, ParentID: 1}, {NodeID: 2, State: storage.NameLinked, ParentID: 2, RawLeaf: []byte("x")}, {NodeID: 2, State: storage.NameDetached, RawLeaf: []byte{}}, {NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte("a/b")}} {
			if operation == fileObserveName {
				badResponse = response
				badResponse.NameObservation = &invalid
			} else {
				d := *response.Directory
				d.Name = &invalid
				badResponse = response
				badResponse.Directory = &d
			}
			if err := validateFileResponse(request, badResponse); err == nil {
				t.Fatalf("invalid name accepted: %+v", invalid)
			}
		}
	}
}

type observationTestSession struct {
	*capabilityTestSession
	obsMu        sync.Mutex
	nameRef      *observationTestReference
	calls, loads atomic.Int32
	guards       *storage.NamespaceGuards
	failure      error
	entry        bool
	leafBytes    int
}

func (s *observationTestSession) CheckDirectoryMetadataObservation() error { return nil }
func (s *observationTestSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	result, err := s.capabilityTestSession.OpenAt(ctx, name, options)
	result.File = s.nameRef
	return result, err
}
func (s *observationTestSession) OpenNodeRef(ctx context.Context, node uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	result, err := s.capabilityTestSession.OpenNodeRef(ctx, node, options)
	result.Reference = s.nameRef
	return result, err
}
func (s *observationTestSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	s.calls.Add(1)
	s.guards = options.Guards
	observation := storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte("current")}}
	if options.IncludeName {
		name := storage.NameObservation{NodeID: target.NodeID, State: storage.NameLinked, ParentID: 1}
		charge, err := storage.CheckNameObservationBudget(ctx, name, int64(s.leafBytes))
		if err != nil {
			return storage.DirectoryMetadataObservation{}, result.Fail(err)
		}
		if err := result.ReservePrefix(charge); err != nil {
			return storage.DirectoryMetadataObservation{}, err
		}
		s.loads.Add(1)
		name.RawLeaf = bytes.Repeat([]byte{0xff}, s.leafBytes)
		observation.Name = &name
	}
	if s.entry {
		if err := result.Add(storage.Entry{Name: "child", Attr: storage.Attr{ID: 5, Kind: storage.NodeRegular}}); err != nil {
			return storage.DirectoryMetadataObservation{}, err
		}
	}
	return observation, s.failure
}

type observationTestReference struct {
	*capabilityTestReference
	calls, loads     atomic.Int32
	guards           *storage.NamespaceGuards
	failure          error
	name             storage.NameObservation
	identityError    error
	identityOverride *uint64
	contextValue     any
}

func (r *observationTestReference) CheckReferenceNameObservation() error {
	_, err := storage.ReferenceNodeID(r)
	return err
}
func (r *observationTestReference) ReferenceNodeID() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identityOverride != nil {
		return *r.identityOverride, r.identityError
	}
	return r.attr.ID, r.identityError
}
func (r *observationTestReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls.Add(1)
	r.guards = guards
	r.contextValue = ctx.Value(observationContextKey{})
	scalar := r.name
	scalar.RawLeaf = nil
	if _, err := storage.CheckNameObservationBudget(ctx, scalar, int64(len(r.name.RawLeaf))); err != nil {
		return storage.NameObservation{}, err
	}
	r.loads.Add(1)
	return r.name.Clone(), r.failure
}

type observationTestBackend struct {
	storage.FileStorage
	session *observationTestSession
}

func (b observationTestBackend) CheckFileStorage() error { return nil }
func (b observationTestBackend) NewFileSession(_ context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	b.session.options = options
	return b.session, nil
}

func observationHTTPFixture(t *testing.T) (*Storage, *Handler, *observationTestSession) {
	t.Helper()
	base := &capabilityTestSession{node: &capabilityTestReference{attr: storage.Attr{ID: 2, Kind: storage.NodeDirectory}}}
	client, handler := capabilityHTTPFixture(t, base)
	native := &observationTestSession{capabilityTestSession: base, leafBytes: 1}
	native.nameRef = &observationTestReference{capabilityTestReference: base.node, name: storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff}}}
	handler.files.backend = observationTestBackend{session: native}
	return client, handler, native
}

func observationResult(t *testing.T, limit int64) *storage.ListResult {
	t.Helper()
	result, err := storage.NewListResult(limit, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return 128 + nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestDirectoryMetadataHTTPPreservesSnapshotAuthorizationAndFailure(t *testing.T) {
	client, handler, native := observationHTTPFixture(t)
	handler.volume = "trusted-volume"
	deny := false
	var policyMu sync.Mutex
	var observed []authz.AccessRequest
	handler.authorizer = authz.AuthorizerFunc(func(_ context.Context, access authz.AccessRequest) error {
		policyMu.Lock()
		defer policyMu.Unlock()
		observed = append(observed, access)
		if deny && access.Operation == storage.OpReplicationSnapshot || access.Operation == storage.OpFileReadDirNode {
			return authz.ErrDenied
		}
		return nil
	})
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	if err := session.CheckDirectoryMetadataObservation(); err != nil {
		t.Fatal(err)
	}
	guards := &storage.NamespaceGuards{RootID: 1}
	options := storage.DirectoryMetadataOptions{Guards: guards, IncludeName: true}
	target := storage.DirectoryTarget{NodeID: 2}
	result := observationResult(t, 1024)
	value, err := session.ObserveDirectoryMetadata(t.Context(), target, options, result)
	entries, readErr := result.Entries()
	if err != nil || readErr != nil || value.Name == nil || !bytes.Equal(value.Name.RawLeaf, []byte{0xff}) || len(entries) != 0 || !reflect.DeepEqual(native.observedGuards(), guards) {
		t.Fatalf("observation=%+v entries=%v err=%v/%v guards=%+v", value, entries, err, readErr, native.observedGuards())
	}
	policyMu.Lock()
	got := observed[len(observed)-1]
	policyMu.Unlock()
	if got != (authz.AccessRequest{Volume: "trusted-volume", Operation: storage.OpReplicationSnapshot}) {
		t.Fatalf("wrong disclosure authorization: %+v", got)
	}
	if _, err := session.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("enumeration permission changed: %v", err)
	}
	policyMu.Lock()
	deny = true
	policyMu.Unlock()
	before := native.calls.Load()
	result = observationResult(t, 1024)
	value, err = session.ObserveDirectoryMetadata(t.Context(), target, options, result)
	_, readErr = result.Entries()
	if !errors.Is(err, syscall.EACCES) || readErr == nil || value.Name != nil || value.Observation.ParentID != 0 || native.calls.Load() != before {
		t.Fatalf("denied observation disclosed or captured: %+v %v/%v calls=%d/%d", value, err, readErr, native.calls.Load(), before)
	}
	policyMu.Lock()
	deny = false
	policyMu.Unlock()
	native.obsMu.Lock()
	native.failure, native.entry = syscall.EIO, true
	native.obsMu.Unlock()
	result = observationResult(t, 4096)
	value, err = session.ObserveDirectoryMetadata(t.Context(), target, options, result)
	_, readErr = result.Entries()
	if !errors.Is(err, syscall.EIO) || readErr == nil || value.Name != nil || value.Observation.ParentID != 0 {
		t.Fatalf("partial native failure escaped: %+v %v/%v", value, err, readErr)
	}
}

func TestDirectoryMetadataHTTPBudgetsBeforeNameLoad(t *testing.T) {
	for _, side := range []string{"client", "server", "caller"} {
		t.Run(side, func(t *testing.T) {
			client, handler, native := observationHTTPFixture(t)
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			limit := int64(1 << 20)
			switch side {
			case "client":
				client.maxBodyBytes = 1024
			case "server":
				handler.maxBodyBytes = 1024
			case "caller":
				limit = 1024
			}
			session := opened.(storage.DirectoryMetadataObserver)
			target := storage.DirectoryTarget{NodeID: 2}
			options := storage.DirectoryMetadataOptions{IncludeName: true}
			result := observationResult(t, limit)
			value, err := session.ObserveDirectoryMetadata(t.Context(), target, options, result)
			if err != nil || value.Name == nil || native.loads.Load() != 1 {
				t.Fatalf("short actual prefix should fit: %+v %v loads=%d", value, err, native.loads.Load())
			}
			native.obsMu.Lock()
			native.leafBytes = storage.MaxLeafBytes
			native.obsMu.Unlock()
			result = observationResult(t, limit)
			value, err = session.ObserveDirectoryMetadata(t.Context(), target, options, result)
			_, readErr := result.Entries()
			if !errors.Is(err, syscall.EFBIG) || readErr == nil || value.Name != nil || value.Observation.ParentID != 0 || native.loads.Load() != 1 {
				t.Fatalf("oversized prefix loaded or escaped: %+v %v/%v loads=%d", value, err, readErr, native.loads.Load())
			}
			result = observationResult(t, limit)
			value, err = session.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{}, result)
			if err != nil || value.Name != nil || native.loads.Load() != 1 {
				t.Fatalf("name-less observation changed: %+v %v loads=%d", value, err, native.loads.Load())
			}
		})
	}
}

func TestReferenceNameHTTPPreservesIdentityLifetimeAndReadHistory(t *testing.T) {
	client, handler, native := observationHTTPFixture(t)
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	result, err := session.OpenNodeRef(t.Context(), 2, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}, Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	reference := result.Reference.(*remoteNodeReference)
	if err := reference.CheckReferenceNameObservation(); err != nil {
		t.Fatal(err)
	}
	capturedID, err := storage.ReferenceNodeID(reference)
	if err != nil || capturedID != result.Attr.ID || native.nameRef.calls.Load() != 0 {
		t.Fatalf("reference getter did not preserve captured identity without I/O: id=%d err=%v", capturedID, err)
	}
	handler.files.mu.Lock()
	registered := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	registered.mu.Lock()
	actions, files, expires := len(registered.actions), len(registered.files), registered.expires
	registered.mu.Unlock()
	guards := &storage.NamespaceGuards{RootID: 1}
	for _, name := range []storage.NameObservation{
		{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff}},
		{NodeID: 2, State: storage.NameLinked, ParentID: 3, RawLeaf: []byte("moved")},
		{NodeID: 2, State: storage.NameDetached},
	} {
		native.nameRef.mu.Lock()
		native.nameRef.name = name
		native.nameRef.mu.Unlock()
		value, err := reference.ObserveName(t.Context(), guards)
		if err != nil || !reflect.DeepEqual(value, name) || !reflect.DeepEqual(native.nameRef.observedGuards(), guards) {
			t.Fatalf("name changed across HTTP: %+v %v", value, err)
		}
	}
	registered.mu.Lock()
	if len(registered.actions) != actions || len(registered.files) != files || registered.expires != expires {
		t.Error("observations changed action history, reference count or lease")
	}
	registered.mu.Unlock()
	next := client.http.Transport
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := next.RoundTrip(request)
		if err != nil {
			return response, err
		}
		requestBody, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		var call fileRequest
		err = json.NewDecoder(requestBody).Decode(&call)
		_ = requestBody.Close()
		if err != nil {
			return nil, err
		}
		if call.Op == fileObserveName {
			var wire fileResponse
			err = json.NewDecoder(response.Body).Decode(&wire)
			_ = response.Body.Close()
			if err != nil {
				return nil, err
			}
			wire.NameObservation.NodeID = 3
			body, err := json.Marshal(wire)
			if err != nil {
				return nil, err
			}
			response.Body = io.NopCloser(bytes.NewReader(body))
			response.ContentLength = int64(len(body))
		}
		return response, nil
	})
	value, err := reference.ObserveName(t.Context(), nil)
	client.http.Transport = next
	if !errors.Is(err, syscall.EIO) || value.NodeID != 0 || !strings.Contains(err.Error(), "substituted reference identity") {
		t.Fatalf("malformed remote identity escaped client validation: %+v %v", value, err)
	}
	native.nameRef.mu.Lock()
	native.nameRef.name = storage.NameObservation{NodeID: 3, State: storage.NameDetached}
	native.nameRef.mu.Unlock()
	value, err = reference.ObserveName(t.Context(), nil)
	if !errors.Is(err, syscall.EIO) || value.NodeID != 0 {
		t.Fatalf("substituted identity accepted: %+v %v", value, err)
	}
	native.nameRef.mu.Lock()
	native.nameRef.name = storage.NameObservation{NodeID: 2, State: storage.NameDetached}
	native.nameRef.failure = syscall.ENOENT
	native.nameRef.mu.Unlock()
	value, err = reference.ObserveName(t.Context(), nil)
	if !errors.Is(err, syscall.ENOENT) || value.NodeID != 0 {
		t.Fatalf("native failure became a name fact: %+v %v", value, err)
	}
	native.nameRef.mu.Lock()
	native.nameRef.failure = nil
	native.nameRef.mu.Unlock()
	other, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	before := native.nameRef.calls.Load()
	_, err = other.(*remoteFileSession).call(t.Context(), fileRequest{Op: fileObserveName, File: reference.file.id, ResultBytes: 1024})
	if !errors.Is(err, syscall.ESTALE) || native.nameRef.calls.Load() != before {
		t.Fatalf("foreign session reached reference: %v calls=%d/%d", err, native.nameRef.calls.Load(), before)
	}
	if err := reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err = reference.ObserveName(t.Context(), nil)
	if !errors.Is(err, syscall.EBADF) || value.NodeID != 0 || native.nameRef.calls.Load() != before {
		t.Fatalf("closed reference reached native: %+v %v", value, err)
	}
}

func TestNameObservationUnsupportedAndInvalidInputs(t *testing.T) {
	target := storage.DirectoryTarget{NodeID: 2}
	missing := &remoteFileSession{}
	result := observationResult(t, 1024)
	if _, err := missing.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{}, result); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported directory call: %v", err)
	}
	if _, err := result.Entries(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported result remained usable: %v", err)
	}
	if _, err := (&remoteFile{}).ObserveName(t.Context(), nil); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported reference call: %v", err)
	}
	client, _, native := observationHTTPFixture(t)
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(storage.DirectoryMetadataObserver)
	if _, err := session.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{}, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil result: %v", err)
	}
	for _, invalid := range []struct {
		target  storage.DirectoryTarget
		options storage.DirectoryMetadataOptions
	}{
		{storage.DirectoryTarget{}, storage.DirectoryMetadataOptions{}},
		{target, storage.DirectoryMetadataOptions{Guards: &storage.NamespaceGuards{RootID: 0, Directories: []storage.DirectoryObservation{{ParentID: 0}}}}},
	} {
		result = observationResult(t, 1024)
		value, err := session.ObserveDirectoryMetadata(t.Context(), invalid.target, invalid.options, result)
		if err == nil || value.Observation.ParentID != 0 || native.calls.Load() != 0 {
			t.Fatalf("invalid request reached capture: %+v %v calls=%d", value, err, native.calls.Load())
		}
	}
	base := &capabilityTestSession{}
	request := fileRequest{Op: fileObserveDirectoryMetadata, Directory: &target, DirectoryMetadata: &storage.DirectoryMetadataOptions{}, ResultBytes: 1024}
	if _, err := (&Handler{maxBodyBytes: 1024}).observeDirectoryMetadata(t.Context(), base, request); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported native directory: %v", err)
	}
	if _, err := observeReferenceName(t.Context(), &capabilityTestReference{}, fileRequest{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported native reference: %v", err)
	}
}

func TestDirectoryMetadataCallerPrefixRefusalDropsWholeResult(t *testing.T) {
	client, _, native := observationHTTPFixture(t)
	native.obsMu.Lock()
	native.entry = true
	native.obsMu.Unlock()
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	result := observationResult(t, 4096)
	if err := result.ReservePrefix(0); err != nil {
		t.Fatal(err)
	}
	value, err := opened.(storage.DirectoryMetadataObserver).ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: 2}, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	entries, readErr := result.Entries()
	if !errors.Is(err, syscall.EINVAL) || readErr == nil || entries != nil || value.Name != nil || value.Observation.ParentID != 0 {
		t.Fatalf("prefix failure retained partial directory: %+v %v %v/%v", value, entries, err, readErr)
	}
}

func (s *observationTestSession) observedGuards() *storage.NamespaceGuards {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	return s.guards
}
func (r *observationTestReference) observedGuards() *storage.NamespaceGuards {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.guards
}

func (s *observationTestSession) OpenFile(context.Context, string, storage.FileOpenOptions) (storage.File, error) {
	s.opens.Add(1)
	return s.nameRef, nil
}
func (s *observationTestSession) OpenNode(context.Context, uint64, storage.FileOpenOptions) (storage.File, error) {
	s.opens.Add(1)
	return s.nameRef, nil
}

func TestLegacyNameIdentitySurvivesLostOpenReply(t *testing.T) {
	for _, byID := range []bool{false, true} {
		t.Run(map[bool]string{false: "path", true: "node"}[byID], func(t *testing.T) {
			client, _, native := observationHTTPFixture(t)
			native.node.attr.Kind = storage.NodeRegular
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			operation := storage.OpFileOpen
			if byID {
				operation = storage.OpFileOpenNode
			}
			lost := &loseCapabilityReply{next: client.http.Transport, op: operation}
			client.http.Transport = lost
			var file storage.File
			options := storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}
			if byID {
				file, err = opened.OpenNode(t.Context(), 2, options)
			} else {
				file, err = opened.OpenFile(t.Context(), "file", options)
			}
			if err != nil || file == nil || native.opens.Load() != 1 || !lost.lost.Load() {
				t.Fatalf("open replay: file=%v err=%v opens=%d lost=%v", file, err, native.opens.Load(), lost.lost.Load())
			}
			id, err := storage.ReferenceNodeID(file)
			if err != nil || id != 2 || native.nameRef.calls.Load() != 0 {
				t.Fatalf("legacy identity required an observation or changed: id=%d err=%v calls=%d", id, err, native.nameRef.calls.Load())
			}
			name, err := file.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
			if err != nil || name.NodeID != id {
				t.Fatalf("legacy name identity: %+v %v", name, err)
			}
			if err := file.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if closedID, err := storage.ReferenceNodeID(file); err != nil || closedID != id {
				t.Fatalf("immutable identity changed after close: %d %v", closedID, err)
			}
		})
	}
}

func TestLegacyNameIdentityFailureRetainsCleanupOwnership(t *testing.T) {
	for _, test := range []struct {
		name  string
		id    uint64
		cause error
		path  bool
	}{
		{"getter error", 2, syscall.EINTR, false}, {"zero identity", 0, nil, false}, {"wrong requested identity", 3, nil, false}, {"wrong path identity", 3, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, handler, native := observationHTTPFixture(t)
			native.node.attr.ID = test.id
			native.node.attr.Kind = storage.NodeRegular
			native.nameRef.identityError = test.cause
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := opened.(*remoteFileSession)
			var file storage.File
			if test.path {
				file, err = session.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: 2})
			} else {
				file, err = session.OpenNode(t.Context(), 2, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			}
			if file != nil || storage.ErrnoOf(err) != syscall.EIO || native.opens.Load() != 1 {
				t.Fatalf("failed identity escaped: file=%v err=%v opens=%d", file, err, native.opens.Load())
			}
			if test.cause != nil && !strings.Contains(err.Error(), test.cause.Error()) {
				t.Fatalf("identity failure lost its cause: %v", err)
			}
			handler.files.mu.Lock()
			registered := handler.files.sessions[session.id]
			handler.files.mu.Unlock()
			registered.mu.Lock()
			owned := len(registered.files)
			for _, retained := range registered.files {
				if retained.pending.IsZero() {
					t.Error("failed open was acknowledged")
				}
			}
			registered.mu.Unlock()
			if owned != 1 {
				t.Fatalf("failed open lost cleanup owner: %d", owned)
			}
			if err := session.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			native.node.mu.Lock()
			closed := native.node.closed
			native.node.mu.Unlock()
			if !closed {
				t.Fatal("session retirement did not close failed open")
			}
		})
	}
}

func TestLegacyNameIdentityWireShapes(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpen, storage.OpFileOpenNode} {
		request := fileRequest{Op: operation, Node: 2}
		for _, test := range []struct {
			name      string
			supported bool
			node      uint64
			valid     bool
		}{
			{"legacy unsupported", false, 0, true}, {"captured", true, 2, true},
			{"missing", true, 0, false}, {"unsolicited", false, 2, false},
		} {
			response := fileResponse{Epoch: 1, File: strings.Repeat("a", 64), Data: []byte{}, Node: test.node, Capabilities: &fileCapabilities{ReferenceName: test.supported}}
			if err := validateFileResponse(request, response); (err == nil) != test.valid {
				t.Fatalf("%s/%s: %v", operation, test.name, err)
			}
		}
	}
	request := fileRequest{Op: storage.OpFileOpenNode, Node: 2}
	response := fileResponse{Epoch: 1, File: strings.Repeat("a", 64), Data: []byte{}, Node: 3, Capabilities: &fileCapabilities{ReferenceName: true}}
	if err := validateFileResponse(request, response); err == nil {
		t.Fatal("identity open accepted substituted node")
	}
	request = fileRequest{Op: storage.OpFileOpen, Open: storage.FileOpenOptions{ExpectedID: 2}}
	if err := validateFileResponse(request, response); err == nil {
		t.Fatal("path open accepted substituted expected identity")
	}
}

func TestNameObservationAtomicOpenRejectsCapturedIdentityMismatch(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			client, handler, native := observationHTTPFixture(t)
			native.node.attr.Kind = storage.NodeRegular
			otherID := uint64(3)
			native.nameRef.identityOverride = &otherID
			opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := opened.(*remoteFileSession)
			name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
			var exposed storage.NodeReference
			switch operation {
			case storage.OpFileOpenAt:
				value, failure := session.OpenAt(t.Context(), name, storage.OpenAtOptions{Read: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData}})
				exposed, err = value.File, failure
			case storage.OpFileOpenNodeRef:
				value, failure := session.OpenNodeRef(t.Context(), 2, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}})
				exposed, err = value.Reference, failure
			case storage.OpFileOpenChildRef:
				value, failure := session.OpenChildRef(t.Context(), name, storage.NodeRefOptions{Kind: storage.NodeRegular, Create: true, Target: storage.ChildCondition{State: storage.Absent}})
				exposed, err = value.Reference, failure
			}
			if err == nil || storage.ErrnoOf(err) != syscall.EIO || exposed != nil || native.opens.Load() != 1 {
				t.Fatalf("inconsistent capture exposed: ref=%v err=%v opens=%d", exposed, err, native.opens.Load())
			}
			handler.files.mu.Lock()
			registered := handler.files.sessions[session.id]
			handler.files.mu.Unlock()
			registered.mu.Lock()
			owned := len(registered.files)
			registered.mu.Unlock()
			if owned != 1 {
				t.Fatalf("post-open failure lost cleanup ownership: %d", owned)
			}
			if err := session.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func (s *observationTestSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	result, err := s.capabilityTestSession.OpenChildRef(ctx, name, options)
	result.Reference = s.nameRef
	return result, err
}

type observationContextKey struct{}

func TestReferenceNameHTTPAuthorizationAndContextReachCapture(t *testing.T) {
	client, handler, native := observationHTTPFixture(t)
	var allow atomic.Bool
	var sawPrincipal atomic.Bool
	handler.volume = "trusted"
	handler.authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
		if access.Operation != storage.OpReplicationSnapshot {
			return nil
		}
		if access.Volume != "trusted" || access.Open != (storage.OpenAccess{}) {
			return syscall.EIO
		}
		sawPrincipal.Store(ctx.Value(observationContextKey{}) == "host-principal")
		if !allow.Load() {
			return authz.ErrDenied
		}
		return nil
	})
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*remoteFileSession)
	result, err := session.OpenNodeRef(t.Context(), 2, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}, MetadataAccess: storage.ReadMetadata})
	if err != nil {
		t.Fatal(err)
	}
	reference := result.Reference.(*remoteNodeReference)
	if _, err := reference.State(t.Context()); err != nil {
		t.Fatalf("snapshot denial changed State permission: %v", err)
	}
	call := fileRequest{Op: fileObserveName, Session: session.id, File: reference.file.id, Path: []byte{}, Data: []byte{}, ResultBytes: 1024, Guards: &storage.NamespaceGuards{RootID: 1}}
	body, err := json.Marshal(call)
	if err != nil {
		t.Fatal(err)
	}
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, Prefix+string(OpFile), bytes.NewReader(body))
		r.Header.Set("Content-Type", contentJSON)
		r = r.WithContext(context.WithValue(t.Context(), observationContextKey{}, "host-principal"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	denied := request()
	if denied.Code == http.StatusOK || native.nameRef.calls.Load() != 0 || !sawPrincipal.Load() {
		t.Fatalf("authorization did not precede capture: status=%d calls=%d principal=%v", denied.Code, native.nameRef.calls.Load(), sawPrincipal.Load())
	}
	allow.Store(true)
	allowed := request()
	native.nameRef.mu.Lock()
	principal := native.nameRef.contextValue
	guards := native.nameRef.guards
	native.nameRef.mu.Unlock()
	if allowed.Code != http.StatusOK || native.nameRef.calls.Load() != 1 || principal != "host-principal" || !reflect.DeepEqual(guards, call.Guards) {
		t.Fatalf("request context or guards lost before capture: status=%d calls=%d principal=%v guards=%+v", allowed.Code, native.nameRef.calls.Load(), principal, guards)
	}
}
