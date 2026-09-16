package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNeutralFileControlWirePreservesIdentityAndMetadata(t *testing.T) {
	action, _ := storage.NewLockRequestID(1)
	stamp := time.Unix(time.Date(1500, 2, 3, 4, 5, 6, 7, time.UTC).Unix(), 7)
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 4, Scope: &storage.UseScope{Token: strings.Repeat("a", 64)}}, RawLeaf: []byte{'f', 0xff}}
	options := storage.OpenAtOptions{Read: true, Write: true, Create: true, Target: storage.ChildCondition{State: storage.Absent}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Initial: storage.InitialState{OnCreate: storage.InitialFields{Attr: storage.AttrChange{BirthTime: &stamp}, Metadata: map[string][]byte{"client.v1": {0, 0xff}}}}}
	request := fileRequest{Op: storage.OpFileOpenAt, Session: strings.Repeat("a", 64), Action: action, ResultBytes: 1 << 20, Path: []byte{}, Data: []byte{}, Child: &name, OpenAt: openAtOptionsOf(options)}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileRequest
	if err := decodeFileJSON(body, &decoded); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if err := validateFileRequest(decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileArguments(decoded, storage.DefaultFileSessionOptions()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Child.RawLeaf, name.RawLeaf) || !reflect.DeepEqual(decoded.OpenAt.storage(), options) {
		t.Fatalf("request changed: %#v", decoded)
	}
	for _, field := range []string{"child", "openAt"} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, field)
		damaged, _ := json.Marshal(fields)
		decoded = fileRequest{}
		if err := decodeFileJSON(damaged, &decoded); err == nil {
			if err := validateFileRequest(decoded); err == nil {
				t.Fatalf("accepted absent %s", field)
			}
		}
	}
}

func TestNeutralOpenResponseKeepsCapturedOutcome(t *testing.T) {
	request := fileRequest{Op: storage.OpFileOpenAt, OpenAt: &openAtOptions{Create: true, Existing: storage.ReplaceNode, Target: storage.ChildCondition{State: storage.Any}}}
	attr := storage.Attr{ID: 17, Kind: storage.NodeRegular, Size: 8, AccessTime: time.Unix(1, 2), ModTime: time.Unix(3, 4), Metadata: map[string]storage.OpaquePayload{"client.v1": {Version: []byte{1}, Data: []byte{0xff}}}}
	response := fileResponse{Epoch: 1, File: strings.Repeat("a", 64), Data: []byte{}, Attr: AttrOf(attr), Outcome: storage.Replaced, Capabilities: &fileCapabilities{State: true, Scope: true}}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileResponse
	if err := decodeFileJSON(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(request, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Outcome != storage.Replaced || !reflect.DeepEqual(decoded.Attr.Storage(), attr) {
		t.Fatalf("capture changed: %#v", decoded)
	}
	decoded.Outcome = 0
	if err := validateFileResponse(request, decoded); err == nil {
		t.Fatal("accepted missing atomic outcome")
	}
}

func TestNeutralDirectoryWireRefusesPartialObservations(t *testing.T) {
	target := storage.DirectoryTarget{NodeID: 4}
	request := fileRequest{Op: storage.OpFileReadDirNode, Directory: &target}
	response := fileResponse{Epoch: 1, Data: []byte{}, Directory: observedDirectoryOf(storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: 4, Revision: []byte{1}}, Entries: []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: storage.Attr{ID: 8, Kind: storage.NodeRegular}}}})}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileResponse
	if err := decodeFileJSON(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(request, decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Directory.Entries[0].RawLeaf, []byte{0xff}) {
		t.Fatal("name changed")
	}
	decoded.Directory.Entries[0].Attr = nil
	if err := validateFileResponse(request, decoded); err == nil {
		t.Fatal("accepted partial directory")
	}
}

type capabilityCheckFailure struct {
	storage.FileSession
	err error
}

func (s *capabilityCheckFailure) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	panic("not called")
}
func (s *capabilityCheckFailure) ReadDirNodeBounded(context.Context, storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error) {
	panic("not called")
}
func (s *capabilityCheckFailure) CheckNamespaceAccess() error { return s.err }
func (s *capabilityCheckFailure) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	panic("not called")
}
func (s *capabilityCheckFailure) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	panic("not called")
}
func TestCapabilityAdvertisementPreservesFailures(t *testing.T) {
	caps, err := capabilitiesOf(&capabilityCheckFailure{err: syscall.EOPNOTSUPP})
	if err != nil || caps.Namespace {
		t.Fatalf("unsupported: %#v, %v", caps, err)
	}
	_, err = capabilitiesOf(&capabilityCheckFailure{err: syscall.EIO})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("capability failure became unsupported: %v", err)
	}
	_, err = capabilitiesOf(&capabilityCheckFailure{err: errors.Join(syscall.EOPNOTSUPP, syscall.EIO)})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("joined capability failure became unsupported: %v", err)
	}
	caps, err = capabilitiesOf(&capabilityCheckFailure{})
	if err != nil || !caps.Namespace {
		t.Fatalf("supported: %#v, %v", caps, err)
	}
}

func TestNodeReferenceDoesNotExposeByteIO(t *testing.T) {
	var reference storage.NodeReference = &remoteNodeReference{file: &remoteFile{}}
	if _, ok := reference.(storage.File); ok {
		t.Fatal("metadata reference exposes byte I/O")
	}
}

func TestCapabilityErrorsSurviveTransportAndActionHistory(t *testing.T) {
	client := &Storage{}
	for code, failure := range capabilityErrors {
		t.Run(code, func(t *testing.T) {
			retained := retainFileError(failure)
			if !errors.Is(retained, failure) {
				t.Fatalf("history lost capability: %v", retained)
			}
			response := ErrorResponse{Errno: storage.ErrnoNameOf(retained), Message: retained.Error(), CapabilityCode: capabilityErrorCode(retained)}
			encoded, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			decoded := client.storageError(Request{Op: OpFile}, encoded)
			if !errors.Is(decoded, failure) || storage.ErrnoOf(decoded) != storage.ErrnoOf(failure) {
				t.Fatalf("capability changed: %v", decoded)
			}
		})
	}
	for _, body := range []string{
		`{"errno":"EAGAIN","capabilityCode":"unknown"}`,
		`{"errno":"ENOENT","capabilityCode":"condition-conflict"}`,
		`{"errno":"EAGAIN","capabilityCode":"condition-conflict","capabilityCode":"use-conflict"}`,
		`{"errno":"EAGAIN","capabilityCode":null}`,
		`{"errno":"EAGAIN","capabilityCode":"condition-conflict","lockCode":"conflict"}`,
	} {
		if err := client.storageError(Request{Op: OpFile}, []byte(body)); storage.ErrnoOf(err) != syscall.EIO {
			t.Fatalf("accepted malformed capability %s: %v", body, err)
		}
	}
}

func TestMissingCapabilitiesFailOnBothSides(t *testing.T) {
	ctx := context.Background()
	session := &remoteFileSession{}
	for _, check := range []func() error{session.CheckAtomicFileOpen, session.CheckNamespaceAccess, session.CheckNodeReferences, session.CheckMetadataAccess, session.CheckUseOwners, session.CheckRangeControl} {
		if !errors.Is(check(), syscall.EOPNOTSUPP) {
			t.Fatal("missing session capability succeeded")
		}
	}
	file := &remoteFile{}
	node := &remoteNodeReference{file: file}
	for _, check := range []func() error{node.CheckReferenceState, node.CheckScopedReference, node.CheckMetadataAccess, node.CheckDeleteIntent, file.CheckConditionalFileMutation} {
		if !errors.Is(check(), syscall.EOPNOTSUPP) {
			t.Fatal("missing reference capability succeeded")
		}
	}
	for _, call := range []func() error{
		func() error { _, e := session.OpenAt(ctx, storage.ChildName{}, storage.OpenAtOptions{}); return e },
		func() error { _, e := session.LookupAt(ctx, storage.ChildName{}); return e },
		func() error { _, e := session.ReadDirNode(ctx, storage.DirectoryTarget{}); return e },
		func() error { _, e := session.MutateName(ctx, storage.NameCommand{}); return e },
		func() error { _, e := session.OpenNodeRef(ctx, 1, storage.NodeRefOptions{}); return e },
		func() error { _, e := session.SetMetadata(ctx, 1, "test", nil, nil); return e },
		func() error {
			_, e := session.NewUseOwner(ctx, 1, storage.UseScope{}, storage.OwnerOptions{})
			return e
		},
		func() error { return session.RetireUseOwner(ctx, 1) },
		func() error { _, e := session.GetConflict(ctx, 1, storage.RangeCommand{}); return e },
		func() error { _, e := session.Apply(ctx, 1, nil, ""); return e },
		func() error { return session.Drop(ctx, 1, storage.DomainRecord) },
		func() error { _, e := node.State(ctx); return e }, func() error { _, e := node.Scope(ctx); return e },
		func() error { _, e := node.SetMetadata(ctx, "test", nil, nil); return e },
		func() error { _, e := node.SetPendingUnlink(ctx, storage.PendingUnlinkCommand{}); return e },
		func() error { _, e := node.ClearPendingUnlink(ctx, storage.ClearPendingUnlinkCommand{}); return e },
		func() error { _, e := file.MutateFile(ctx, storage.FileMutation{}); return e },
	} {
		if err := call(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing capability call: %v", err)
		}
	}
	handler := &Handler{}
	nativeSession := struct{ storage.FileSession }{}
	for _, op := range []storage.Operation{storage.OpFileLookupAt, storage.OpFileReadDirNode, storage.OpFileMutateName, storage.OpFileSetNodeMetadata, storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner, storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop} {
		if _, err := handler.performSessionCapability(ctx, nativeSession, fileRequest{Op: op}); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported native session %s: %v", op, err)
		}
	}
	nativeReference := struct{ storage.NodeReference }{}
	for _, op := range []storage.Operation{storage.OpFileState, storage.OpFileScope, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate} {
		if _, err := performReferenceCapability(ctx, nativeReference, fileRequest{Op: op}); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("unsupported native reference %s: %v", op, err)
		}
	}
}

func TestWireNodePreservesOpaqueDirectoryTokensAndRequiresLinkFacts(t *testing.T) {
	directory := Node{ID: 1, Kind: storage.NodeDirectory, Content: []byte{}}
	for _, token := range [][]byte{nil, {0xff, 0, 7}} {
		directory.DirectoryRevision = token
		encoded, err := json.Marshal(directory)
		if err != nil {
			t.Fatal(err)
		}
		var got Node
		if err := json.Unmarshal(encoded, &got); err != nil {
			t.Fatalf("opaque or unknown directory token: %v", err)
		}
		if !bytes.Equal(got.DirectoryRevision, token) {
			t.Fatal("directory token changed")
		}
	}
	link := Node{ID: 2, Kind: storage.NodeSymlink, Size: 2, LinkTarget: []byte{0xff, 'x'}, Content: []byte{}}
	encoded, err := json.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	var got Node
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.LinkTarget, link.LinkTarget) {
		t.Fatal("link target changed")
	}
	for _, invalid := range []Node{
		{ID: 1, Kind: storage.NodeRegular, DirectoryRevision: []byte{1}, Content: []byte{}},
		{ID: 1, Kind: storage.NodeDirectory, DirectoryRevision: make([]byte, storage.MaxObservationTokenBytes+1), Content: []byte{}},
		{ID: 1, Kind: storage.NodeDirectory, LinkTarget: []byte("x"), Content: []byte{}},
		{ID: 1, Kind: storage.NodeSymlink, Size: 1, Content: []byte{}},
		{ID: 1, Kind: storage.NodeSymlink, Size: 2, LinkTarget: []byte("x"), Content: []byte{}},
	} {
		encoded, _ := json.Marshal(invalid)
		if err := json.Unmarshal(encoded, &got); err == nil {
			t.Fatalf("accepted incomplete node %s", encoded)
		}
	}
	request := fileRequest{Op: storage.OpFileState}
	response := fileResponse{Epoch: 1, Data: []byte{}, State: &referenceState{Attr: AttrOf(storage.Attr{ID: 2, Kind: storage.NodeSymlink, Size: 1}), PendingGeneration: []byte{}}}
	if err := validateFileResponse(request, response); err == nil {
		t.Fatal("reference state accepted missing link target")
	}
}

func openMetadataRequest(t *testing.T, operation storage.Operation, session string) fileRequest {
	t.Helper()
	action, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	target := storage.ChildCondition{State: storage.SameNode, NodeID: 41}
	conditions := map[string][]byte{"test.attributes": {1, 0, 0xff}, "test.absent": nil}
	request := fileRequest{Op: operation, Session: session, Action: action, Path: []byte{}, Data: []byte{}, ResultBytes: 1 << 20}
	if operation == storage.OpFileOpenAt {
		request.Child = &storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
		request.OpenAt = openAtOptionsOf(storage.OpenAtOptions{Read: true, Target: target, ExpectedMetadata: conditions, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData}})
	} else {
		request.NodeRef = nodeRefOptionsOf(storage.NodeRefOptions{Kind: storage.NodeRegular, Target: target, ExpectedMetadata: conditions, MetadataAccess: storage.ReadMetadata})
		if operation == storage.OpFileOpenNodeRef {
			request.Node = 41
		} else {
			request.Child = &storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
		}
	}
	return request
}

func requestMetadataConditions(request fileRequest) map[string][]byte {
	if request.OpenAt != nil {
		return request.OpenAt.storage().ExpectedMetadata
	}
	return request.NodeRef.storage().ExpectedMetadata
}

func TestOpenMetadataConditionsWirePreservesTokensAndOwnership(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			request := openMetadataRequest(t, operation, strings.Repeat("a", 64))
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(body, []byte(`"test.absent":""`)) || bytes.Contains(body, []byte(`"test.absent":null`)) {
				t.Fatalf("absence token changed: %s", body)
			}
			var decoded fileRequest
			if err := decodeFileJSON(body, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := validateFileRequest(decoded); err != nil {
				t.Fatal(err)
			}
			if err := validateFileArguments(decoded, storage.DefaultFileSessionOptions()); err != nil {
				t.Fatal(err)
			}
			conditions := requestMetadataConditions(decoded)
			absent, exists := conditions["test.absent"]
			if len(conditions) != 2 || !exists || len(absent) != 0 || !bytes.Equal(conditions["test.attributes"], []byte{1, 0, 0xff}) {
				t.Fatalf("conditions changed: %#v", conditions)
			}
		})
	}
	original := map[string][]byte{"test.attributes": {1, 2}, "test.absent": nil}
	file := openAtOptionsOf(storage.OpenAtOptions{ExpectedMetadata: original})
	node := nodeRefOptionsOf(storage.NodeRefOptions{ExpectedMetadata: original})
	original["test.attributes"][0] = 9
	delete(original, "test.absent")
	for _, copied := range []map[string][]byte{file.ExpectedMetadata, node.ExpectedMetadata} {
		if !bytes.Equal(copied["test.attributes"], []byte{1, 2}) {
			t.Fatalf("caller mutation changed copied condition: %#v", copied)
		}
		if value, exists := copied["test.absent"]; !exists || value == nil {
			t.Fatalf("copied absence lost its canonical empty value: %#v", copied)
		}
	}
}
