package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNameObservationWireUsesCanonicalClosedDTOs(t *testing.T) {
	guards := &storage.NamespaceGuards{
		RootID:      1,
		Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1, 2}}},
		Edges:       []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte{0xff}, ChildID: 2}},
	}
	request := fileRequest{
		Op:          storage.OpFileObserveName,
		Session:     strings.Repeat("a", 64),
		File:        strings.Repeat("b", 64),
		Path:        []byte{},
		Data:        []byte{},
		ResultBytes: 4096,
		Guards:      namespaceGuardsOf(guards),
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range [][]byte{[]byte(`"rootId":1`), []byte(`"parentId":1`), []byte(`"childId":2`), []byte(`"rawLeaf":"/w=="`), []byte(`"revision":"AQI="`)} {
		if !bytes.Contains(encoded, fragment) {
			t.Fatalf("wire omitted canonical field %s: %s", fragment, encoded)
		}
	}
	for _, forbidden := range [][]byte{[]byte(`"RootID"`), []byte(`"ParentID"`), []byte(`"RawLeaf"`)} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatalf("wire exposed Go field name %s: %s", forbidden, encoded)
		}
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.Guards.storage(); !equalNamespaceGuards(got, guards) {
		t.Fatalf("guards changed across wire: got=%+v want=%+v", got, guards)
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	var wireGuards map[string]json.RawMessage
	if err := json.Unmarshal(object["guards"], &wireGuards); err != nil {
		t.Fatal(err)
	}
	wireGuards["unknown"] = json.RawMessage(`true`)
	object["guards"], _ = json.Marshal(wireGuards)
	damaged, _ := json.Marshal(object)
	if err := decodeFileJSON(damaged, &decoded); err == nil {
		t.Fatal("nested unknown guard field was accepted")
	}
	damaged = bytes.Replace(encoded, []byte(`"rawLeaf":"/w=="`), []byte(`"rawLeaf":"/x=="`), 1)
	if err := decodeFileJSON(damaged, &decoded); err == nil {
		t.Fatal("non-canonical raw leaf encoding was accepted")
	}
}

type observationBudgetSession struct {
	storage.FileSession
	loads atomic.Int32
}

func (*observationBudgetSession) CheckDirectoryMetadataObservation() error { return nil }

func (session *observationBudgetSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	observation := storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{1}}}
	if options.IncludeName {
		scalar := storage.NameObservation{NodeID: target.NodeID, State: storage.NameLinked, ParentID: 1}
		charge, err := storage.CheckNameObservationBudget(ctx, scalar, storage.MaxLeafBytes)
		if err != nil {
			return storage.DirectoryMetadataObservation{}, result.Fail(err)
		}
		if err := result.ReservePrefix(charge); err != nil {
			return storage.DirectoryMetadataObservation{}, err
		}
		session.loads.Add(1)
		scalar.RawLeaf = bytes.Repeat([]byte("x"), storage.MaxLeafBytes)
		observation.Name = &scalar
	}
	return observation, nil
}

func TestDirectoryMetadataHTTPBudgetRefusesBeforeNameLoad(t *testing.T) {
	session := &observationBudgetSession{}
	handler := &Handler{maxBodyBytes: 1024}
	target := storage.DirectoryTarget{NodeID: 2}
	request := fileRequest{
		Op:                storage.OpFileObserveDirectoryMetadata,
		Directory:         &target,
		DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{IncludeName: true}),
		ResultBytes:       1024,
	}
	ctx := storage.WithNameObservationBudget(t.Context(), nameObservationWireBudget(1024, true))
	response, err := handler.observeDirectoryMetadata(ctx, session, request)
	if !errors.Is(err, syscall.EFBIG) || response.Directory != nil || session.loads.Load() != 0 {
		t.Fatalf("oversized name loaded or escaped: response=%+v err=%v loads=%d", response, err, session.loads.Load())
	}
}

func equalNamespaceGuards(left, right *storage.NamespaceGuards) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return bytes.Equal(leftJSON, rightJSON)
}

func TestObservationResponsesRejectWrongIdentityAndShape(t *testing.T) {
	nameRequest := fileRequest{Op: storage.OpFileObserveName, Session: strings.Repeat("a", 64), File: strings.Repeat("b", 64), Path: []byte{}, Data: []byte{}, ResultBytes: 4096}
	name := nameObservationOf(storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff}})
	nameResponse := fileResponse{Epoch: 1, Data: []byte{}, NameObservation: name}
	if err := validateFileResponse(nameRequest, nameResponse); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(nameResponse)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"nodeId":2`)) || !bytes.Contains(encoded, []byte(`"rawLeaf":"/w=="`)) || bytes.Contains(encoded, []byte(`"NodeID"`)) {
		t.Fatalf("name response is not canonical: %s", encoded)
	}
	var decoded fileResponse
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(nameRequest, decoded); err != nil {
		t.Fatalf("name response round trip failed: %v", err)
	}
	badName := *name
	badName.ParentID = badName.NodeID
	nameResponse.NameObservation = &badName
	if err := validateFileResponse(nameRequest, nameResponse); err == nil {
		t.Fatal("self-parented name observation was accepted")
	}
	emptyLeaf := canonicalBytes{}
	nameResponse.NameObservation = &nameObservation{NodeID: 2, State: storage.NameDetached, RawLeaf: &emptyLeaf}
	if err := validateFileResponse(nameRequest, nameResponse); err == nil {
		t.Fatal("detached observation with an explicitly present empty leaf was accepted")
	}

	target := storage.DirectoryTarget{NodeID: 2}
	directoryRequest := fileRequest{Op: storage.OpFileReadDirNode, Session: strings.Repeat("a", 64), Path: []byte{}, Data: []byte{}, ResultBytes: 4096, Directory: &target}
	directoryResponse := fileResponse{Epoch: 1, Data: []byte{}, Directory: observedDirectoryOf(storage.ObservedDirectory{
		Observation: storage.DirectoryObservation{ParentID: 2, Revision: []byte{1}},
		Entries:     []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular}}},
	})}
	if err := validateFileResponse(directoryRequest, directoryResponse); err != nil {
		t.Fatal(err)
	}
	substituted := *directoryResponse.Directory
	substituted.Observation.ParentID = 4
	directoryResponse.Directory = &substituted
	if err := validateFileResponse(directoryRequest, directoryResponse); err == nil {
		t.Fatal("directory response substituted its requested identity")
	}
	directoryResponse.Directory = observedDirectoryOf(storage.ObservedDirectory{
		Observation: storage.DirectoryObservation{ParentID: 2, Revision: []byte{1}},
		Entries:     []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular}}},
	})
	directoryResponse.Directory.Entries[0].Attr = nil
	if err := validateFileResponse(directoryRequest, directoryResponse); err == nil {
		t.Fatal("directory entry without attributes was accepted")
	}
}

func TestNameObservationWireBudgetChargesExactEncodingBeforeLeafLoad(t *testing.T) {
	for _, directory := range []bool{false, true} {
		for _, length := range []int{0, 1, 2, 3, 127, storage.MaxLeafBytes} {
			scalar := storage.NameObservation{NodeID: math.MaxUint64, State: storage.NameLinked, ParentID: math.MaxUint64 - 1}
			if length == 0 {
				scalar.State = storage.NameRoot
				scalar.ParentID = 0
			}
			budget := nameObservationWireBudget(1<<20, directory)
			charge, err := storage.CheckNameObservationBudget(storage.WithNameObservationBudget(t.Context(), budget), scalar, int64(length))
			if err != nil {
				t.Fatal(err)
			}
			full := scalar
			if length != 0 {
				full.RawLeaf = bytes.Repeat([]byte{0xff}, length)
			}
			var encoded []byte
			if directory {
				encoded, err = json.Marshal(nameObservationOf(full))
				encoded = append([]byte(`,"name":`), encoded...)
			} else {
				encoded, err = json.Marshal(fileResponse{Epoch: math.MaxUint64, Data: []byte{}, NameObservation: nameObservationOf(full)})
			}
			retained, _ := storage.NameObservationRetentionBytes(int64(length))
			if err != nil || charge != retained+int64(len(encoded)) {
				t.Fatalf("directory=%v length=%d charge=%d encoded=%d retained=%d err=%v", directory, length, charge, len(encoded), retained, err)
			}
			if _, err := nameObservationWireBudget(charge-1, directory)(scalar, int64(length)); !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("smaller result bound returned %v", err)
			}
		}
	}
}

func TestObservationOperationsStayBoundedReadOnlyAndActionless(t *testing.T) {
	target := storage.DirectoryTarget{NodeID: 2}
	for _, request := range []fileRequest{
		{Op: storage.OpFileReadDirNode, Session: strings.Repeat("a", 64), Directory: &target, ResultBytes: 1024, Path: []byte{}, Data: []byte{}},
		{Op: storage.OpFileObserveDirectoryMetadata, Session: strings.Repeat("a", 64), Directory: &target, DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{}), ResultBytes: 1024, Path: []byte{}, Data: []byte{}},
		{Op: storage.OpFileObserveName, Session: strings.Repeat("a", 64), File: strings.Repeat("b", 64), ResultBytes: 1024, Path: []byte{}, Data: []byte{}},
	} {
		if err := validateFileRequest(request); err != nil {
			t.Fatalf("%s request: %v", request.Op, err)
		}
		if err := validateFileArguments(request, storage.DefaultFileSessionOptions()); err != nil {
			t.Fatalf("%s arguments: %v", request.Op, err)
		}
		if !fileReadOnly(request.Op) || fileMutation(request.Op) || fileActionRequired(request.Op) || fileControl(request.Op) || !fileBoundedResult(request.Op) {
			t.Fatalf("%s has wrong transport classification", request.Op)
		}
		bad := request
		bad.Action = "1:00000000000000000000000000000000"
		if err := validateFileRequest(bad); err == nil {
			t.Fatalf("%s accepted action identity", request.Op)
		}
		bad = request
		bad.ResultBytes = 0
		if err := validateFileArguments(bad, storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("%s zero result bound: %v", request.Op, err)
		}
	}
}

func TestAuthoritativeNameObservationsRoundTripOverHTTP(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Create(t.Context(), "directory/child"); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := backend.Stat(t.Context(), "directory")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close(context.Background())
	session := opened.(*remoteFileSession)
	if err := session.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	if err := session.CheckDirectoryMetadataObservation(); err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: directory.ID}
	listed, err := session.ReadDirNode(t.Context(), target)
	if err != nil || listed.Observation.ParentID != directory.ID || len(listed.Entries) != 1 || string(listed.Entries[0].RawLeaf) != "child" {
		t.Fatalf("identity directory read=%+v err=%v", listed, err)
	}
	originalTransport := client.http.Transport
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := originalTransport.RoundTrip(request)
		if err != nil {
			return response, err
		}
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		var call fileRequest
		err = json.NewDecoder(body).Decode(&call)
		_ = body.Close()
		if err != nil || call.Op != storage.OpFileReadDirNode {
			return response, err
		}
		var wire fileResponse
		err = json.NewDecoder(response.Body).Decode(&wire)
		_ = response.Body.Close()
		if err != nil {
			return nil, err
		}
		wire.Directory.Observation.ParentID++
		encoded, err := json.Marshal(wire)
		if err != nil {
			return nil, err
		}
		response.Body = io.NopCloser(bytes.NewReader(encoded))
		response.ContentLength = int64(len(encoded))
		return response, nil
	})
	malicious := observationListResult(t, 1<<20)
	wrong, err := session.ReadDirNodeBounded(t.Context(), target, malicious)
	partial, partialErr := malicious.Entries()
	client.http.Transport = originalTransport
	if storage.ErrnoOf(err) != syscall.EIO || wrong.ParentID != 0 || partialErr == nil || partial != nil {
		t.Fatalf("substituted directory escaped: observation=%+v entries=%+v err=%v/%v", wrong, partial, err, partialErr)
	}

	result := observationListResult(t, 1<<20)
	metadata, err := session.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{
		Guards:      &storage.NamespaceGuards{RootID: root.ID},
		IncludeName: true,
	}, result)
	entries, entriesErr := result.Entries()
	if err != nil || entriesErr != nil || metadata.Name == nil || metadata.Name.NodeID != directory.ID || string(metadata.Name.RawLeaf) != "directory" || len(entries) != 1 || entries[0].Name != "child" {
		t.Fatalf("directory metadata=%+v entries=%+v err=%v/%v", metadata, entries, err, entriesErr)
	}
	tiny := observationListResult(t, 64)
	failed, err := session.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{IncludeName: true}, tiny)
	partial, entriesErr = tiny.Entries()
	if err == nil || entriesErr == nil || partial != nil || failed.Observation.ParentID != 0 || failed.Name != nil {
		t.Fatalf("bounded failure exposed partial facts: observation=%+v entries=%+v err=%v/%v", failed, partial, err, entriesErr)
	}

	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	referenceResult, err := session.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{
		Kind:   storage.NodeDirectory,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID},
		Action: action,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer referenceResult.Reference.Close(context.Background())
	observer := referenceResult.Reference.(storage.ReferenceNameObserver)
	if err := observer.CheckReferenceNameObservation(); err != nil {
		t.Fatal(err)
	}
	identity, err := storage.ReferenceNodeID(referenceResult.Reference)
	if err != nil || identity != directory.ID {
		t.Fatalf("reference identity=%d err=%v", identity, err)
	}
	if err := backend.Rename(t.Context(), "directory", "moved"); err != nil {
		t.Fatal(err)
	}
	name, err := observer.ObserveName(t.Context(), &storage.NamespaceGuards{RootID: root.ID})
	if err != nil || name.NodeID != directory.ID || name.ParentID != root.ID || string(name.RawLeaf) != "moved" {
		t.Fatalf("renamed reference observation=%+v err=%v", name, err)
	}
}

func TestLegacyReferenceIdentitySurvivesLostOpenReply(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close(context.Background())
	lost := loseFileResponses(t, client, storage.OpFileOpenNode, 1)
	file, err := opened.OpenNode(t.Context(), attr.ID, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil || file == nil || lost.Load() != 2 {
		t.Fatalf("replayed open returned file=%v err=%v calls=%d", file != nil, err, lost.Load())
	}
	defer file.Close(context.Background())
	identity, err := storage.ReferenceNodeID(file)
	if err != nil || identity != attr.ID {
		t.Fatalf("replayed reference identity=%d err=%v", identity, err)
	}
	name, err := file.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
	if err != nil || name.NodeID != attr.ID || string(name.RawLeaf) != "file" {
		t.Fatalf("replayed reference name=%+v err=%v", name, err)
	}
}

type mismatchedIdentityReference struct{}

func (*mismatchedIdentityReference) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{ID: 2, Kind: storage.NodeDirectory}, nil
}
func (*mismatchedIdentityReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{ID: 2, Kind: storage.NodeDirectory}, nil
}
func (*mismatchedIdentityReference) Close(context.Context) error { return nil }
func (*mismatchedIdentityReference) CheckScopedReference() error { return nil }
func (*mismatchedIdentityReference) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "scope"}, nil
}
func (*mismatchedIdentityReference) CheckReferenceState() error { return nil }
func (*mismatchedIdentityReference) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{Attr: storage.Attr{ID: 2, Kind: storage.NodeDirectory}}, nil
}
func (*mismatchedIdentityReference) CheckReferenceNameObservation() error { return nil }
func (*mismatchedIdentityReference) ReferenceNodeID() (uint64, error)     { return 3, nil }
func (*mismatchedIdentityReference) ObserveName(context.Context, *storage.NamespaceGuards) (storage.NameObservation, error) {
	return storage.NameObservation{NodeID: 3, State: storage.NameDetached}, nil
}

type mismatchedIdentitySession struct{ storage.FileSession }

func (*mismatchedIdentitySession) CheckNodeReferences() error { return nil }
func (*mismatchedIdentitySession) OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return storage.NodeOpenResult{Reference: &mismatchedIdentityReference{}, Attr: storage.Attr{ID: 2, Kind: storage.NodeDirectory}, Outcome: storage.Opened}, nil
}
func (*mismatchedIdentitySession) OpenChildRef(context.Context, storage.ChildName, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	panic("not called")
}

func TestOpenRejectsMismatchedReferenceIdentityWithoutLosingCleanupOwnership(t *testing.T) {
	handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
	served := &servedFileSession{
		native:  &mismatchedIdentitySession{},
		files:   make(map[string]*servedFile),
		options: storage.DefaultFileSessionOptions(),
	}
	response, err := handler.openReference(t.Context(), served, fileRequest{Op: storage.OpFileOpenNodeRef, Node: 2, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{})})
	if storage.ErrnoOf(err) != syscall.EIO || response.File != "" || response.Capabilities != nil || response.Attr == nil || response.Attr.ID != 2 {
		t.Fatalf("identity mismatch response=%+v err=%v", response, err)
	}
	served.mu.Lock()
	defer served.mu.Unlock()
	if len(served.files) != 1 {
		t.Fatalf("identity mismatch lost cleanup ownership: files=%d", len(served.files))
	}
	for _, retained := range served.files {
		if retained.native == nil || retained.pending.IsZero() || retained.closing {
			t.Fatalf("invalid retained cleanup state: %+v", retained)
		}
	}
}

func observationListResult(t *testing.T, limit int64) *storage.ListResult {
	t.Helper()
	result, err := storage.NewListResult(limit, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return 128 + nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
