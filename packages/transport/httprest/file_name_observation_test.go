package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

func TestObservedDirectoryServerCollectorEnforcesRawLimitSeparately(t *testing.T) {
	result, err := newObservedDirectoryResultWithRawLimit(1<<20, 64)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := storage.EncodeMetadata(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := result.Reserve(1, int64(len(metadata)), storage.Attr{ID: 2, Kind: storage.NodeRegular}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("native raw-entry limit was conflated with wire capacity: %v", err)
	}
}

func TestServerPropagatesObservationResultBounds(t *testing.T) {
	handler := &Handler{maxBodyBytes: 4096}
	outer := storage.WithBoundedListResult(t.Context(), 1024)
	target := storage.DirectoryTarget{NodeID: 2}
	for _, request := range []fileRequest{
		{Op: storage.OpFileReadDirNode, Directory: &target, ResultBytes: 2048},
		{Op: storage.OpFileObserveDirectoryMetadata, Directory: &target, DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{}), ResultBytes: 2048},
	} {
		ctx := handler.observationResultContext(outer, request)
		if limit, ok := storage.ListResultByteLimit(ctx); !ok || limit != 1024 {
			t.Fatalf("%s propagated limit=%d present=%v", request.Op, limit, ok)
		}
	}
}

type substitutedDirectorySession struct{ storage.FileSession }

func (*substitutedDirectorySession) CheckDirectoryRead() error { return nil }
func (*substitutedDirectorySession) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	panic("not called")
}
func (*substitutedDirectorySession) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	panic("not called")
}
func (*substitutedDirectorySession) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, _ *storage.ListResult) (storage.DirectoryObservation, error) {
	return storage.DirectoryObservation{ParentID: target.NodeID + 1, Revision: []byte{1}}, nil
}
func (*substitutedDirectorySession) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	panic("not called")
}

func TestServerRejectsSubstitutedDirectoryObservation(t *testing.T) {
	target := storage.DirectoryTarget{NodeID: 2}
	response, err := (&Handler{maxBodyBytes: 4096}).performSessionCapability(t.Context(), &substitutedDirectorySession{}, fileRequest{
		Op: storage.OpFileReadDirNode, Directory: &target, ResultBytes: 4096,
	})
	if storage.ErrnoOf(err) != syscall.EIO || response.Directory != nil {
		t.Fatalf("substituted native directory escaped: response=%+v err=%v", response, err)
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

func TestObservationDecoderBudgetsBeforeDecodingVariableBytes(t *testing.T) {
	refused := errors.New("budget refused before decode")
	var budgetCalls atomic.Int32
	attr, err := json.Marshal(AttrOf(storage.Attr{ID: 3, Kind: storage.NodeRegular}))
	if err != nil {
		t.Fatal(err)
	}
	directoryBody := []byte(`{"epoch":1,"data":"","directory":{"observation":{"parentId":2,"revision":"AQ=="},"entries":[{"rawLeaf":"!!!!","attr":` + string(attr) + `}]}}`)
	result, err := storage.NewListResult(1<<20, 0, func(int, int64, int64, storage.Attr) (int64, error) {
		budgetCalls.Add(1)
		return 0, refused
	})
	if err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: 2}
	ctx := withDirectoryResponseCollector(t.Context(), result, false)
	_, err = decodeObservedFileResponse(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target}, directoryBody)
	if err == nil || errors.Is(err, refused) || budgetCalls.Load() != 0 {
		t.Fatalf("malformed entry reached caller budget: err=%v calls=%d", err, budgetCalls.Load())
	}
	directoryBody = bytes.Replace(directoryBody, []byte(`"!!!!"`), []byte(`"YQ=="`), 1)
	result, err = storage.NewListResult(1<<20, 0, func(int, int64, int64, storage.Attr) (int64, error) {
		budgetCalls.Add(1)
		return 0, refused
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx = withDirectoryResponseCollector(t.Context(), result, false)
	_, err = decodeObservedFileResponse(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target}, directoryBody)
	if !errors.Is(err, refused) || budgetCalls.Load() != 1 {
		t.Fatalf("valid entry did not reach caller budget before retention: err=%v calls=%d", err, budgetCalls.Load())
	}
	metadataAttr, err := json.Marshal(AttrOf(storage.Attr{ID: 3, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{
		"test.value": {Version: []byte{1}},
	}}))
	if err != nil {
		t.Fatal(err)
	}
	metadataAttr = bytes.Replace(metadataAttr, []byte(`"data":""`), []byte(`"data":"!!!!"`), 1)
	metadataBody := []byte(`{"epoch":1,"data":"","directory":{"observation":{"parentId":2,"revision":"AQ=="},"entries":[{"rawLeaf":"YQ==","attr":` + string(metadataAttr) + `}]}}`)
	result, err = storage.NewListResult(1<<20, 0, func(int, int64, int64, storage.Attr) (int64, error) {
		budgetCalls.Add(1)
		return 0, refused
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx = withDirectoryResponseCollector(t.Context(), result, false)
	_, err = decodeObservedFileResponse(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target}, metadataBody)
	if err == nil || errors.Is(err, refused) || budgetCalls.Load() != 1 {
		t.Fatalf("malformed metadata reached caller budget: err=%v calls=%d", err, budgetCalls.Load())
	}

	nameBody := []byte(`{"epoch":1,"data":"","nameObservation":{"nodeId":2,"state":2,"parentId":1,"rawLeaf":"!!!!"}}`)
	ctx = storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) {
		budgetCalls.Add(1)
		return 0, refused
	})
	_, err = decodeObservedFileResponse(ctx, fileRequest{Op: storage.OpFileObserveName}, nameBody)
	if err == nil || errors.Is(err, refused) || budgetCalls.Load() != 1 {
		t.Fatalf("malformed name reached caller budget: err=%v calls=%d", err, budgetCalls.Load())
	}
	nameBody = bytes.Replace(nameBody, []byte(`"!!!!"`), []byte(`"YQ=="`), 1)
	_, err = decodeObservedFileResponse(ctx, fileRequest{Op: storage.OpFileObserveName}, nameBody)
	if !errors.Is(err, refused) || budgetCalls.Load() != 2 {
		t.Fatalf("valid name did not reach caller budget before retention: err=%v calls=%d", err, budgetCalls.Load())
	}
}

func TestObservationDecoderRejectsInvalidRawLeavesBeforeCallerBudget(t *testing.T) {
	attr, err := json.Marshal(AttrOf(storage.Attr{ID: 3, Kind: storage.NodeRegular}))
	if err != nil {
		t.Fatal(err)
	}
	for _, leaf := range [][]byte{[]byte("."), []byte(".."), []byte("a/b"), {0}} {
		t.Run(fmt.Sprintf("%x", leaf), func(t *testing.T) {
			var calls atomic.Int32
			result, err := storage.NewListResult(1<<20, 0, func(int, int64, int64, storage.Attr) (int64, error) {
				calls.Add(1)
				return 1, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"epoch":1,"data":"","directory":{"observation":{"parentId":2,"revision":"AQ=="},"entries":[{"rawLeaf":"` + base64.StdEncoding.EncodeToString(leaf) + `","attr":` + string(attr) + `}]}}`)
			target := storage.DirectoryTarget{NodeID: 2}
			_, err = decodeObservedFileResponse(withDirectoryResponseCollector(t.Context(), result, false), fileRequest{Op: storage.OpFileReadDirNode, Directory: &target}, body)
			if err == nil || calls.Load() != 0 {
				t.Fatalf("invalid leaf reached caller budget: err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestObservationDecoderRejectsDecodedRawLeavesBeyondBound(t *testing.T) {
	attr, err := json.Marshal(AttrOf(storage.Attr{ID: 3, Kind: storage.NodeRegular}))
	if err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: 2}
	for _, test := range []struct {
		name    string
		excess  int
		padding int
	}{
		{name: "one-padding-byte", excess: 1, padding: 1},
		{name: "no-padding", excess: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			encodedLeaf := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, storage.MaxLeafBytes+test.excess))
			if len(encodedLeaf) != base64.StdEncoding.EncodedLen(storage.MaxLeafBytes) || strings.Count(encodedLeaf, "=") != test.padding {
				t.Fatalf("test leaf does not exercise the encoded-length boundary: encoded=%d padding=%d", len(encodedLeaf), strings.Count(encodedLeaf, "="))
			}
			nameBody := []byte(`{"epoch":1,"data":"","nameObservation":{"nodeId":2,"state":2,"parentId":1,"rawLeaf":"` + encodedLeaf + `"}}`)
			if _, err := decodeObservedFileResponse(t.Context(), fileRequest{Op: storage.OpFileObserveName}, nameBody); err == nil {
				t.Fatal("standalone name observation accepted a raw leaf whose decoded length exceeds the bound")
			}

			directoryBody := []byte(`{"epoch":1,"data":"","directory":{"observation":{"parentId":2,"revision":"AQ=="},"entries":[{"rawLeaf":"` + encodedLeaf + `","attr":` + string(attr) + `}]}}`)
			result, err := storage.NewListResult(1<<20, 0, func(int, int64, int64, storage.Attr) (int64, error) {
				return 1, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			request := fileRequest{
				Op:                storage.OpFileObserveDirectoryMetadata,
				Directory:         &target,
				DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{}),
			}
			if _, err := decodeObservedFileResponse(withDirectoryResponseCollector(t.Context(), result, false), request, directoryBody); err == nil {
				t.Fatal("directory metadata observation accepted a raw leaf whose decoded length exceeds the bound")
			}
		})
	}
}

func TestObservationBudgetErrorReportsAndPreservesCause(t *testing.T) {
	cause := errors.New("caller result budget at entry 7")
	failure := budgetObservationError(cause)
	if failure.Error() != cause.Error() || !errors.Is(failure, cause) {
		t.Fatalf("budget error changed reporting or identity: %v", failure)
	}
	if budgetObservationError(nil) != nil {
		t.Fatal("nil budget error became a failure")
	}
}

func TestNameObservationScalarValidation(t *testing.T) {
	for _, test := range []struct {
		name        string
		observation storage.NameObservation
		leafBytes   int64
		leafPresent bool
		valid       bool
	}{
		{name: "root", observation: storage.NameObservation{NodeID: 1, State: storage.NameRoot}, valid: true},
		{name: "detached", observation: storage.NameObservation{NodeID: 1, State: storage.NameDetached}, valid: true},
		{name: "linked", observation: storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1}, leafBytes: 1, leafPresent: true, valid: true},
		{name: "missing identity", observation: storage.NameObservation{State: storage.NameRoot}},
		{name: "root parent", observation: storage.NameObservation{NodeID: 1, State: storage.NameRoot, ParentID: 2}},
		{name: "root leaf length", observation: storage.NameObservation{NodeID: 1, State: storage.NameRoot}, leafBytes: 1},
		{name: "detached leaf presence", observation: storage.NameObservation{NodeID: 1, State: storage.NameDetached}, leafPresent: true},
		{name: "linked no parent", observation: storage.NameObservation{NodeID: 2, State: storage.NameLinked}, leafBytes: 1, leafPresent: true},
		{name: "linked self parent", observation: storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 2}, leafBytes: 1, leafPresent: true},
		{name: "linked empty leaf", observation: storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1}, leafPresent: true},
		{name: "linked absent leaf", observation: storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1}, leafBytes: 1},
		{name: "unknown state", observation: storage.NameObservation{NodeID: 1, State: 255}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := checkNameObservationScalar(test.observation, test.leafBytes, test.leafPresent)
			if (err == nil) != test.valid {
				t.Fatalf("validation error=%v valid=%v", err, test.valid)
			}
		})
	}
}

func TestDeferredObservationMetadataDecodePaths(t *testing.T) {
	empty, err := (deferredObservedAttr{}).decodeMetadataBytes()
	if err != nil || empty != nil {
		t.Fatalf("empty metadata=%v err=%v", empty, err)
	}
	valid := deferredObservedAttr{metadata: map[string]encodedObservedPayload{
		"test.value": {version: []byte("AQ=="), data: []byte("dmFsdWU=")},
		"test.empty": {version: []byte("Ag=="), data: []byte("")},
	}}
	decoded, err := valid.decodeMetadataBytes()
	if err != nil || string(decoded["test.value"].Version) != "\x01" || string(decoded["test.value"].Data) != "value" || len(decoded["test.empty"].Data) != 0 {
		t.Fatalf("decoded metadata=%+v err=%v", decoded, err)
	}
	for _, test := range []struct {
		name     string
		metadata map[string]encodedObservedPayload
	}{
		{name: "invalid version", metadata: map[string]encodedObservedPayload{"test.value": {version: []byte("!!!!"), data: []byte("")}}},
		{name: "invalid data", metadata: map[string]encodedObservedPayload{"test.value": {version: []byte("AQ=="), data: []byte("!!!!")}}},
		{name: "invalid namespace", metadata: map[string]encodedObservedPayload{"INVALID": {version: []byte("AQ=="), data: []byte("")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if decoded, err := (deferredObservedAttr{metadata: test.metadata}).decodeMetadataBytes(); err == nil || decoded != nil {
				t.Fatalf("invalid metadata decoded=%+v err=%v", decoded, err)
			}
		})
	}
}

func TestObservationDecoderAcceptsReorderedObjectMembers(t *testing.T) {
	attr, err := json.Marshal(AttrOf(storage.Attr{ID: 3, Kind: storage.NodeRegular}))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"directory":{"entries":[{"attr":` + string(attr) + `,"rawLeaf":"Y2hpbGQ="}],"name":{"rawLeaf":"ZGly","parentId":1,"state":2,"nodeId":2},"observation":{"revision":"AQ==","parentId":2}},"data":"","epoch":1}`)
	result := observationListResult(t, 1<<20)
	target := storage.DirectoryTarget{NodeID: 2}
	request := fileRequest{Op: storage.OpFileObserveDirectoryMetadata, Directory: &target, DirectoryMetadata: directoryMetadataOptionsOf(storage.DirectoryMetadataOptions{IncludeName: true})}
	response, err := decodeObservedFileResponse(withDirectoryResponseCollector(t.Context(), result, false), request, body)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(request, response); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "child" || response.Directory.Name == nil || string(response.Directory.Name.storage().RawLeaf) != "dir" {
		t.Fatalf("reordered response=%+v entries=%+v err=%v", response, entries, err)
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

func TestIdentityDirectoryReadRequiresObservationProtocolCapability(t *testing.T) {
	session := &remoteFileSession{capabilities: fileCapabilities{Namespace: true}}
	target := storage.DirectoryTarget{NodeID: 2}
	if _, err := session.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("legacy namespace capability reached directory read: %v", err)
	}
	result := observationListResult(t, 1024)
	observation, err := session.ReadDirNodeBounded(t.Context(), target, result)
	entries, resultErr := result.Entries()
	if !errors.Is(err, syscall.EOPNOTSUPP) || observation.ParentID != 0 || resultErr == nil || entries != nil {
		t.Fatalf("legacy bounded read escaped: observation=%+v entries=%+v err=%v/%v", observation, entries, err, resultErr)
	}
	bundled := &remoteFileSession{capabilities: fileCapabilities{DirectoryMetadata: true}}
	if err := bundled.CheckDirectoryRead(); err != nil {
		t.Fatalf("directory bundle depended on namespace access: %v", err)
	}
	if err := bundled.CheckNamespaceAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("directory bundle invented namespace access: %v", err)
	}
}

type namespaceOnlySession struct{ storage.FileSession }

func (namespaceOnlySession) CheckNamespaceAccess() error { return nil }
func (namespaceOnlySession) LookupAt(context.Context, storage.ChildName) (storage.Attr, error) {
	panic("not called")
}
func (namespaceOnlySession) MutateName(context.Context, storage.NameCommand) (storage.NameResult, error) {
	panic("not called")
}

type directoryReaderOnlySession struct{ storage.FileSession }

func (directoryReaderOnlySession) CheckDirectoryRead() error { return nil }
func (directoryReaderOnlySession) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	panic("not called")
}
func (directoryReaderOnlySession) ReadDirNodeBounded(context.Context, storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error) {
	panic("not called")
}

type directoryOnlySession struct{ storage.FileSession }

func (directoryOnlySession) CheckDirectoryMetadataObservation() error { return nil }
func (directoryOnlySession) ObserveDirectoryMetadata(context.Context, storage.DirectoryTarget, storage.DirectoryMetadataOptions, *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	panic("not called")
}

type directoryBundleSession struct {
	directoryReaderOnlySession
}

func (directoryBundleSession) CheckDirectoryMetadataObservation() error { return nil }
func (directoryBundleSession) ObserveDirectoryMetadata(context.Context, storage.DirectoryTarget, storage.DirectoryMetadataOptions, *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	panic("not called")
}

func TestHTTPAdvertisesDirectoryObservationOnlyAsCompleteBundle(t *testing.T) {
	for name, test := range map[string]struct {
		session storage.FileSession
		ns, dir bool
	}{
		"namespace only":  {session: namespaceOnlySession{}, ns: true},
		"reader only":     {session: directoryReaderOnlySession{}},
		"observer only":   {session: directoryOnlySession{}},
		"complete bundle": {session: directoryBundleSession{}, dir: true},
	} {
		t.Run(name, func(t *testing.T) {
			capabilities, err := sessionCapabilitiesOf(test.session)
			if err != nil || capabilities.Namespace != test.ns || capabilities.DirectoryMetadata != test.dir {
				t.Fatalf("capabilities=%+v err=%v", capabilities, err)
			}
		})
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
	boundedTransport := client.http.Transport
	var requestedLimits []int64
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		var call fileRequest
		err = json.NewDecoder(body).Decode(&call)
		_ = body.Close()
		if err != nil {
			return nil, err
		}
		if call.Op == storage.OpFileReadDirNode || call.Op == storage.OpFileObserveDirectoryMetadata {
			requestedLimits = append(requestedLimits, call.ResultBytes)
		}
		return boundedTransport.RoundTrip(request)
	})
	outer := storage.WithBoundedListResult(t.Context(), 4096)
	if _, err := session.ReadDirNodeBounded(outer, target, observationListResult(t, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ObserveDirectoryMetadata(outer, target, storage.DirectoryMetadataOptions{}, observationListResult(t, 1<<20)); err != nil {
		t.Fatal(err)
	}
	client.http.Transport = boundedTransport
	if len(requestedLimits) != 2 || requestedLimits[0] != 4096 || requestedLimits[1] != 4096 {
		t.Fatalf("outer listing limits were not propagated: %v", requestedLimits)
	}
	refusing, err := storage.NewListResult(1<<20, 0, func(int, int64, int64, storage.Attr) (int64, error) {
		return 0, syscall.ENOSPC
	})
	if err != nil {
		t.Fatal(err)
	}
	failedObservation, err := session.ReadDirNodeBounded(t.Context(), target, refusing)
	refusedEntries, resultErr := refusing.Entries()
	if !errors.Is(err, syscall.ENOSPC) || failedObservation.ParentID != 0 || !errors.Is(resultErr, syscall.ENOSPC) || refusedEntries != nil {
		t.Fatalf("caller budget error changed across HTTP: observation=%+v entries=%+v err=%v/%v", failedObservation, refusedEntries, err, resultErr)
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
func (*mismatchedIdentityReference) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	return storage.ReferenceCloseResult{Released: true}, nil
}
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
func (*mismatchedIdentitySession) OpenChildRef(context.Context, storage.ChildSelection, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	panic("not called")
}

type substitutedOpenNodeSession struct{ mismatchedIdentitySession }

func (*substitutedOpenNodeSession) OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return storage.NodeOpenResult{Reference: &mismatchedIdentityReference{}, Attr: storage.Attr{ID: 3, Kind: storage.NodeDirectory}, Outcome: storage.Opened}, nil
}

type substitutedChildSession struct{ mismatchedIdentitySession }

func (*substitutedChildSession) OpenChildRef(context.Context, storage.ChildSelection, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return storage.NodeOpenResult{Reference: &mismatchedIdentityReference{}, Attr: storage.Attr{ID: 3, Kind: storage.NodeDirectory}, Outcome: storage.Opened}, nil
}

type mismatchedIdentityFile struct{ mismatchedIdentityReference }

func (*mismatchedIdentityFile) ReadAt(context.Context, int64, int) (storage.FileRead, error) {
	panic("not called")
}
func (*mismatchedIdentityFile) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	panic("not called")
}
func (*mismatchedIdentityFile) Truncate(context.Context, int64) (storage.Attr, error) {
	panic("not called")
}
func (*mismatchedIdentityFile) Sync(context.Context) error { panic("not called") }

type substitutedAtomicSession struct{ storage.FileSession }

func (*substitutedAtomicSession) CheckAtomicFileOpen() error { return nil }
func (*substitutedAtomicSession) OpenAt(context.Context, storage.ChildSelection, storage.OpenAtOptions) (storage.OpenResult, error) {
	return storage.OpenResult{File: &mismatchedIdentityFile{}, Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular}, Outcome: storage.Opened}, nil
}

type replacementIdentityFile struct {
	mismatchedIdentityFile
	id uint64
}

func (file *replacementIdentityFile) ReferenceNodeID() (uint64, error) { return file.id, nil }

type replacementAtomicSession struct {
	storage.FileSession
	id      uint64
	outcome storage.OpenOutcome
}

func (*replacementAtomicSession) CheckAtomicFileOpen() error { return nil }
func (session *replacementAtomicSession) OpenAt(context.Context, storage.ChildSelection, storage.OpenAtOptions) (storage.OpenResult, error) {
	return storage.OpenResult{
		File:    &replacementIdentityFile{id: session.id},
		Attr:    storage.Attr{ID: session.id, Kind: storage.NodeRegular},
		Outcome: session.outcome,
	}, nil
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

func TestOpenNodeReferenceResponseCannotSubstituteRequestedNode(t *testing.T) {
	response := fileResponse{
		Epoch:        1,
		File:         strings.Repeat("a", 64),
		Data:         []byte{},
		Attr:         AttrOf(storage.Attr{ID: 3, Kind: storage.NodeRegular}),
		Outcome:      storage.Opened,
		Capabilities: &fileCapabilities{Scope: true, State: true, ReferenceName: true},
	}
	for _, request := range []fileRequest{
		{Op: storage.OpFileOpenNodeRef, Node: 2},
		{Op: storage.OpFileOpenChildRef, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}})},
		{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}, Existing: storage.Keep})},
	} {
		if err := validateFileResponse(request, response); err == nil {
			t.Fatalf("%s response substituted the requested node", request.Op)
		}
	}
	replacement := fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}, Existing: storage.ReplaceNode})}
	if err := validateFileResponse(replacement, response); err == nil {
		t.Fatal("replacement open accepted an ordinary opened outcome")
	}
	response.Outcome = storage.Replaced
	if err := validateFileResponse(replacement, response); err != nil {
		t.Fatalf("replacement open rejected its new node identity: %v", err)
	}
	response.Attr = AttrOf(storage.Attr{ID: 2, Kind: storage.NodeRegular})
	if err := validateFileResponse(replacement, response); err == nil {
		t.Fatal("replacement open reused the replaced node identity")
	}
}

func TestServerRejectsOpenNodeReferenceTargetSubstitution(t *testing.T) {
	handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
	served := &servedFileSession{
		native:  &substitutedOpenNodeSession{},
		files:   make(map[string]*servedFile),
		options: storage.DefaultFileSessionOptions(),
	}
	response, err := handler.openReference(t.Context(), served, fileRequest{Op: storage.OpFileOpenNodeRef, Node: 2, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{})})
	if storage.ErrnoOf(err) != syscall.EIO || response.File != "" || response.Capabilities != nil || response.Attr == nil || response.Attr.ID != 3 {
		t.Fatalf("server accepted substituted node reference: response=%+v err=%v", response, err)
	}
	served.mu.Lock()
	defer served.mu.Unlock()
	if len(served.files) != 1 {
		t.Fatalf("substituted reference lost cleanup ownership: files=%d", len(served.files))
	}
}

func TestServerRejectsSameNodeChildAndAtomicSubstitution(t *testing.T) {
	child := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
	for _, test := range []struct {
		name    string
		session storage.FileSession
		request fileRequest
	}{
		{
			name:    "child reference",
			session: &substitutedChildSession{},
			request: fileRequest{Op: storage.OpFileOpenChildRef, Child: childNameOf(child), NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}})},
		},
		{
			name:    "atomic open",
			session: &substitutedAtomicSession{},
			request: fileRequest{Op: storage.OpFileOpenAt, Child: childNameOf(child), OpenAt: openAtOptionsOf(storage.OpenAtOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}, Existing: storage.Keep})},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
			served := &servedFileSession{native: test.session, files: make(map[string]*servedFile), options: storage.DefaultFileSessionOptions()}
			response, err := handler.openReference(t.Context(), served, test.request)
			if storage.ErrnoOf(err) != syscall.EIO || response.File != "" || response.Capabilities != nil {
				t.Fatalf("substituted open response=%+v err=%v", response, err)
			}
			served.mu.Lock()
			defer served.mu.Unlock()
			if len(served.files) != 1 {
				t.Fatalf("substituted open lost cleanup ownership: files=%d", len(served.files))
			}
		})
	}
}

func TestServerRequiresReplacementIdentityAndOutcome(t *testing.T) {
	child := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}
	request := fileRequest{Op: storage.OpFileOpenAt, Child: childNameOf(child), OpenAt: openAtOptionsOf(storage.OpenAtOptions{
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: 2}, Existing: storage.ReplaceNode,
	})}
	for _, test := range []struct {
		name    string
		id      uint64
		outcome storage.OpenOutcome
		valid   bool
	}{
		{name: "reused identity", id: 2, outcome: storage.Replaced},
		{name: "wrong outcome", id: 3, outcome: storage.Opened},
		{name: "distinct replacement", id: 3, outcome: storage.Replaced, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := &Handler{files: &fileRegistry{limits: DefaultFileLimits()}}
			served := &servedFileSession{
				native:  &replacementAtomicSession{id: test.id, outcome: test.outcome},
				files:   make(map[string]*servedFile),
				options: storage.DefaultFileSessionOptions(),
			}
			response, err := handler.openReference(t.Context(), served, request)
			if test.valid {
				if err != nil || response.File == "" || response.Attr == nil || response.Attr.ID != 3 || response.Outcome != storage.Replaced {
					t.Fatalf("valid replacement response=%+v err=%v", response, err)
				}
				return
			}
			if storage.ErrnoOf(err) != syscall.EIO || response.File != "" || response.Capabilities != nil {
				t.Fatalf("invalid replacement response=%+v err=%v", response, err)
			}
		})
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
