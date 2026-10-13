package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func httpGuardedRename(t *testing.T, session *remoteFileSession, root, node uint64, source, destination string) storage.NameCommand {
	t.Helper()
	observed, err := session.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: root})
	if err != nil {
		t.Fatal(err)
	}
	return storage.NameCommand{
		Kind: storage.NameRename, Action: httpFileAction(t, session),
		Name:                     storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root}, RawLeaf: []byte(source)},
		Target:                   storage.ChildCondition{State: storage.SameNode, NodeID: node},
		Destination:              &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: root}, ObservedLeaf: []byte(destination), OutputLeaf: []byte(destination), Expected: storage.ChildCondition{State: storage.Absent}},
		Guards:                   &storage.NamespaceGuards{RootID: root, Directories: []storage.DirectoryObservation{observed.Observation}, Edges: []storage.ObservedEdge{{ParentID: root, RawLeaf: []byte(source), ChildID: node}}},
		RequireNoOpenDescendants: true,
	}
}

func TestHTTPGuardedNameJournalPreservesCanonicalUnionAndAdmission(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	for _, name := range []string{"a", "b"} {
		if err := backend.Mkdir(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Write(t.Context(), "a/file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	a, err := backend.Stat(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.Stat(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	file, err := backend.Stat(t.Context(), "a/file")
	if err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	command := httpGuardedRename(t, session, root.ID, file.ID, "file", "new")
	command.Name.Parent.NodeID = a.ID
	command.Destination.Parent.NodeID = b.ID
	command.Guards.Edges = []storage.ObservedEdge{{ParentID: a.ID, RawLeaf: []byte("file"), ChildID: file.ID}, {ParentID: root.ID, RawLeaf: []byte("b"), ChildID: b.ID}, {ParentID: root.ID, RawLeaf: []byte("a"), ChildID: a.ID}}
	for _, id := range []uint64{b.ID, a.ID} {
		observed, err := session.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: id})
		if err != nil {
			t.Fatal(err)
		}
		command.Guards.Directories = append(command.Guards.Directories, observed.Observation)
	}
	result, err := session.MutateName(t.Context(), command)
	if err != nil || result.Attr == nil || result.Attr.ID != file.ID {
		t.Fatalf("guarded rename=%+v err=%v", result, err)
	}
	reordered := nameCommandOf(command).storage()
	reordered.Guards.Directories[0], reordered.Guards.Directories[2] = reordered.Guards.Directories[2], reordered.Guards.Directories[0]
	reordered.Guards.Edges[0], reordered.Guards.Edges[2] = reordered.Guards.Edges[2], reordered.Guards.Edges[0]
	if got, err := session.MutateName(t.Context(), reordered); err != nil || !reflect.DeepEqual(got, result) {
		t.Fatalf("reordered replay=%+v err=%v", got, err)
	}
	for _, change := range []func(*storage.NameCommand){
		func(value *storage.NameCommand) { value.Guards.Directories[0].Revision[0]++ },
		func(value *storage.NameCommand) { value.RequireNoOpenDescendants = false },
		func(value *storage.NameCommand) { value.Destination.OutputLeaf = []byte("other") },
	} {
		changed := nameCommandOf(command).storage()
		change(&changed)
		if got, err := session.MutateName(t.Context(), changed); !errors.Is(err, syscall.EINVAL) || got.Attr != nil {
			t.Fatalf("changed replay=%+v err=%v", got, err)
		}
	}
	if attr, err := backend.Stat(t.Context(), "b/new"); err != nil || attr.ID != file.ID {
		t.Fatalf("new binding=%+v err=%v", attr, err)
	}
	if _, err := backend.Stat(t.Context(), "b/other"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("changed action effect=%v", err)
	}
}

func TestHTTPGuardedNameLostResultRetainsOriginalIdentityAndPayload(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	node, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	command := httpGuardedRename(t, session, root.ID, node.ID, "file", "moved")
	saved := nameCommandOf(command).storage()
	calls := loseFileResponses(t, client, storage.OpFileMutateName, 2)
	if got, err := session.MutateName(t.Context(), command); !errors.Is(err, syscall.EIO) || got.Attr != nil {
		t.Fatalf("lost result=%+v err=%v", got, err)
	}
	command.Name.RawLeaf[0] = 'x'
	command.Guards.Directories[0].Revision[0]++
	command.Guards.Edges[0].RawLeaf[0] = 'x'
	session.mu.Lock()
	retained := session.pending[string(saved.Action)].request.Name
	session.mu.Unlock()
	if retained == nil || !reflect.DeepEqual(retained, nameCommandOf(saved)) {
		t.Fatalf("pending intent changed=%+v", retained)
	}
	if err := backend.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	got, err := session.MutateName(t.Context(), saved)
	if err != nil || got.Attr == nil || got.Attr.ID != node.ID || calls.Load() != 3 {
		t.Fatalf("reconciled result=%+v calls=%d err=%v", got, calls.Load(), err)
	}
	if data, err := backend.Read(t.Context(), "file"); err != nil || string(data) != "replacement" {
		t.Fatalf("replacement=%q err=%v", data, err)
	}
	if data, err := backend.Read(t.Context(), "moved"); err != nil || string(data) != "original" {
		t.Fatalf("original=%q err=%v", data, err)
	}
}

func TestHTTPGuardedNameRejectsChangedEvidenceAndOpenedDescendant(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Mkdir(t.Context(), "dir"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(t.Context(), "dir/child", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directory, err := backend.Stat(t.Context(), "dir")
	if err != nil {
		t.Fatal(err)
	}
	child, err := backend.Stat(t.Context(), "dir/child")
	if err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	stale := httpGuardedRename(t, session, root.ID, directory.ID, "dir", "moved")
	if err := backend.Create(t.Context(), "sibling"); err != nil {
		t.Fatal(err)
	}
	if got, err := session.MutateName(t.Context(), stale); !errors.Is(err, storage.ErrConditionConflict) || got.Attr != nil {
		t.Fatalf("stale rename=%+v err=%v", got, err)
	}
	receipt, err := session.QueryFileAction(t.Context(), stale.Action)
	if err != nil || receipt.Action != stale.Action || receipt.Operation != storage.OpFileMutateName || receipt.Outcome != storage.FileActionNotExecuted {
		t.Fatalf("stale receipt=%+v err=%v", receipt, err)
	}
	opened, err := session.OpenNodeRef(t.Context(), child.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: child.ID}, Action: httpFileAction(t, session)})
	if err != nil {
		t.Fatal(err)
	}
	command := httpGuardedRename(t, session, root.ID, directory.ID, "dir", "moved")
	if got, err := session.MutateName(t.Context(), command); !errors.Is(err, syscall.EACCES) || got.Attr != nil {
		t.Fatalf("opened descendant=%+v err=%v", got, err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	command.Action = httpFileAction(t, session)
	if got, err := session.MutateName(t.Context(), command); err != nil || got.Attr == nil || got.Attr.ID != directory.ID {
		t.Fatalf("released descendant=%+v err=%v", got, err)
	}
}

func boundedNameRequest(t *testing.T, excess int) fileRequest {
	t.Helper()
	const depth = 248
	guards := &storage.NamespaceGuards{RootID: 1}
	remaining := storage.MaxNamespaceGuardBytes - (depth+1)*9 - (depth+1)*16 - len("file") + excess
	for index := 0; index <= depth; index++ {
		guards.Directories = append(guards.Directories, storage.DirectoryObservation{ParentID: uint64(index + 1), Revision: []byte{'r'}})
		if index == depth {
			break
		}
		leafBytes := min(storage.MaxLeafBytes, remaining-(depth-index-1))
		guards.Edges = append(guards.Edges, storage.ObservedEdge{ParentID: uint64(index + 1), ChildID: uint64(index + 2), RawLeaf: bytes.Repeat([]byte{'a'}, leafBytes)})
		remaining -= leafBytes
	}
	guards.Edges = append(guards.Edges, storage.ObservedEdge{ParentID: depth + 1, ChildID: depth + 2, RawLeaf: []byte("file")})
	command := storage.NameCommand{
		Kind: storage.NameRename, Action: storage.FileActionID("1:00000000000000000000000000000000"),
		Name:        storage.ChildName{Parent: storage.DirectoryTarget{NodeID: depth + 1}, RawLeaf: []byte("file")},
		Target:      storage.ChildCondition{State: storage.SameNode, NodeID: depth + 2},
		Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: 1}, ObservedLeaf: []byte("new"), OutputLeaf: []byte("new"), Expected: storage.ChildCondition{State: storage.Absent}},
		Guards:      guards, RequireNoOpenDescendants: true,
	}
	if excess == 0 {
		if err := command.Check(); err != nil {
			t.Fatalf("exact-bound command=%v", err)
		}
	}
	return fileRequest{Op: storage.OpFileMutateName, Action: storage.LockRequestID(command.Action), Session: strings.Repeat("a", 64), Name: nameCommandOf(command), Path: []byte{}, Data: []byte{}, ResultBytes: DefaultMaxBodyBytes}
}

func TestHTTPGuardedNameRequestBoundsAndStrictFields(t *testing.T) {
	policy := &fileAuthorizationPolicy{err: authz.ErrDenied}
	handler, _ := fileAuthorizationFixture(t, policy, DefaultFileLimits())
	fileAuthorizationDenied(t, fileAuthorizationRequest(t, handler, boundedNameRequest(t, 0)), "EACCES", "access denied")
	policy.reset(authz.ErrDenied)
	if response := fileAuthorizationRequest(t, handler, boundedNameRequest(t, 1)); response.Code != http.StatusBadRequest {
		t.Fatalf("oversized union status=%d body=%s", response.Code, response.Body.String())
	}
	policy.mu.Lock()
	calls := len(policy.requests)
	policy.mu.Unlock()
	if calls != 0 {
		t.Fatalf("oversized union reached authorization: %d", calls)
	}
	empty := boundedNameRequest(t, 0)
	empty.Name.Guards = namespaceGuardsOf(&storage.NamespaceGuards{})
	if err := validateCapabilityArguments(empty); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("supplied empty union became unguarded: %v", err)
	}
	encoded, err := json.Marshal(boundedNameRequest(t, 0))
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"duplicate guards": bytes.Replace(encoded, []byte(`"guards":`), []byte(`"guards":{},"guards":`), 1),
		"unknown flag":     bytes.Replace(encoded, []byte(`"requireNoOpenDescendants":`), []byte(`"requireNoPins":`), 1),
		"null flag":        bytes.Replace(encoded, []byte(`"requireNoOpenDescendants":true`), []byte(`"requireNoOpenDescendants":null`), 1),
		"null guards":      bytes.Replace(encoded, []byte(`"guards":{`), []byte(`"guards":null,"ignored":{`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			var decoded fileRequest
			if err := decodeFileJSON(body, &decoded); err == nil {
				t.Fatal("accepted invalid name command JSON")
			}
		})
	}
}

func TestHTTPDeleteResetAuthorizationDescribesActualAccessAndEffects(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	node, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	policy := &httpStatePolicy{}
	handler.authorizer = policy
	options := storage.OpenAtOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Existing: storage.ResetContent, ResetUse: storage.DeleteName, Use: storage.UseClaim{Uses: storage.DeleteName}, Action: httpFileAction(t, session)}
	selection := storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}}
	policy.deny(storage.OpVolumeRemove)
	if got, err := session.OpenAt(t.Context(), selection, options); !errors.Is(err, syscall.EACCES) || got.File != nil || got.Attr.ID != 0 {
		t.Fatalf("denied delete reset=%+v err=%v", got, err)
	}
	if data, err := backend.Read(t.Context(), "file"); err != nil || string(data) != "original" {
		t.Fatalf("denied reset changed content=%q err=%v", data, err)
	}
	policy.deny("")
	policy.mu.Lock()
	policy.requests = nil
	policy.mu.Unlock()
	opened, err := session.OpenAt(t.Context(), selection, options)
	if err != nil || opened.File == nil || opened.Attr.ID != node.ID || opened.Attr.Size != 0 || opened.Outcome != storage.Reset {
		t.Fatalf("delete reset=%+v err=%v", opened, err)
	}
	policy.mu.Lock()
	requests := append([]authz.AccessRequest(nil), policy.requests...)
	policy.mu.Unlock()
	var actualOpen *storage.OpenAccess
	remove := false
	for _, request := range requests {
		if request.Operation == storage.OpFileOpenAt {
			value := request.Open
			actualOpen = &value
		}
		if request.Operation == storage.OpVolumeRemove {
			remove = true
		}
	}
	if actualOpen == nil || !reflect.DeepEqual(*actualOpen, storage.OpenAccess{Truncate: true}) || !remove {
		t.Fatalf("delete reset grants/effects=%+v", requests)
	}
	if _, err := opened.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete-only read=%v", err)
	}
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete-only write=%v", err)
	}
	changed := options
	changed.ResetUse = 0
	changed.Write = true
	changed.Use.Uses |= storage.WriteData
	if got, err := session.OpenAt(t.Context(), selection, changed); !errors.Is(err, syscall.EINVAL) || got.File != nil {
		t.Fatalf("changed reset action=%+v err=%v", got, err)
	}
	if err := opened.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPDeleteResetWireAndCompositeMetadataAuthorization(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	options := storage.OpenAtOptions{Target: storage.ChildCondition{State: storage.SameNode, NodeID: 7}, Existing: storage.ResetContent, ResetUse: storage.DeleteName, Use: storage.UseClaim{Uses: storage.DeleteName}, Action: action, Initial: storage.InitialState{OnReset: storage.InitialFields{Attr: storage.AttrChange{BirthTime: new(time.Unix(1, 0))}, Metadata: map[string][]byte{"client.v1": {1}}}}}
	var requests []authz.AccessRequest
	handler := &Handler{stopping: make(chan struct{}), authorizer: authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		requests = append(requests, request)
		return nil
	})}
	if err := handler.authorizeFile(t.Context(), fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(options)}); err != nil {
		t.Fatal(err)
	}
	var operations []storage.Operation
	for _, request := range requests {
		operations = append(operations, request.Operation)
	}
	if !reflect.DeepEqual(operations, []storage.Operation{storage.OpFileOpenAt, storage.OpVolumeRemove, storage.OpFileSetAttr, storage.OpFileSetMetadata}) {
		t.Fatalf("reset authorization=%v", operations)
	}
	encoded, err := json.Marshal(openAtOptionsOf(options))
	if err != nil {
		t.Fatal(err)
	}
	var decoded openAtOptions
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.storage(); got.ResetUse != storage.DeleteName || got.Write || got.Read || !reflect.DeepEqual(got.Initial, options.Initial) {
		t.Fatalf("reset DTO=%+v", got)
	}
	for _, body := range [][]byte{
		bytes.Replace(encoded, []byte(`"resetUse":8`), []byte(`"resetUse":null`), 1),
		bytes.Replace(encoded, []byte(`"resetUse":`), []byte(`"resetUse":8,"resetUse":`), 1),
	} {
		if err := decodeFileJSON(body, &decoded); err == nil {
			t.Fatal("accepted invalid reset JSON")
		}
	}
}

func TestHTTPPendingGenerationScalarBoundsAndPrivacy(t *testing.T) {
	attr := storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096}
	state := storage.ReferenceState{Attr: attr, PendingUnlink: true, PendingGeneration: bytes.Repeat([]byte{'g'}, storage.MaxObservationTokenBytes)}
	wire := referenceStateOfProjection(state, storage.StateScalars)
	state.PendingGeneration[0] = 'x'
	if wire.PendingGeneration == nil || (*wire.PendingGeneration)[0] != 'g' {
		t.Fatal("scalar generation aliases the source")
	}
	response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, State: wire}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileResponse
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileState, StateProjection: storage.StateScalars}, decoded); err != nil {
		t.Fatal(err)
	}
	for name, modify := range map[string]func(*referenceState){
		"missing": func(s *referenceState) { s.PendingGeneration = nil },
		"empty":   func(s *referenceState) { generation := metadataVersion{}; s.PendingGeneration = &generation },
		"one byte too large": func(s *referenceState) {
			generation := metadataVersion(make([]byte, storage.MaxObservationTokenBytes+1))
			s.PendingGeneration = &generation
		},
		"token without pending": func(s *referenceState) { s.PendingUnlink = false },
		"empty without pending": func(s *referenceState) {
			s.PendingUnlink = false
			generation := metadataVersion{}
			s.PendingGeneration = &generation
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := *wire
			modify(&copy)
			if err := validateReferenceStateProjection(&copy, storage.StateScalars); err == nil {
				t.Fatal("accepted inconsistent scalar generation")
			}
		})
	}
	for _, limit := range []int64{int64(len(encoded)) - 1, int64(len(encoded))} {
		handler := &Handler{maxBodyBytes: limit}
		err := handler.attrResultBudget(fileRequest{Op: storage.OpFileState, StateProjection: storage.StateScalars, ResultBytes: limit})(attr, 0)
		if limit == int64(len(encoded)) && err != nil || limit < int64(len(encoded)) && !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("scalar bound %d=%v", limit, err)
		}
	}
	if err := (&Handler{maxBodyBytes: 4096}).attrResultBudget(fileRequest{Op: storage.OpFileState, StateProjection: storage.StateScalars, ResultBytes: 4096})(attr, 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("scalar metadata charge=%v", err)
	}
}

func TestHTTPClearWithoutGenerationStillAuthorizesAndChecksCurrentState(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	node, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	opened, err := session.OpenNodeRef(t.Context(), node.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Action: httpFileAction(t, session), Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	reference := opened.Reference.(*remoteNodeReference)
	policy := &httpStatePolicy{}
	handler.authorizer = policy
	clear := storage.ClearPendingUnlinkCommand{Action: httpFileAction(t, session)}
	policy.deny(storage.OpFileClearPendingUnlink)
	if state, err := reference.ClearPendingUnlink(t.Context(), clear); !errors.Is(err, syscall.EACCES) || state.Attr.ID != 0 {
		t.Fatalf("denied no-op=%+v err=%v", state, err)
	}
	policy.deny("")
	if state, err := reference.ClearPendingUnlink(t.Context(), clear); err != nil || state.Attr.ID != node.ID || state.PendingUnlink {
		t.Fatalf("authorized no-op=%+v err=%v", state, err)
	}
	pending, err := reference.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: httpFileAction(t, session), Condition: storage.UnlinkFile})
	if err != nil || !pending.PendingUnlink || len(pending.PendingGeneration) == 0 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if state, err := reference.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: httpFileAction(t, session)}); !errors.Is(err, storage.ErrConditionConflict) || state.Attr.ID != 0 {
		t.Fatalf("observed false was stale=%+v err=%v", state, err)
	}
	scalars, err := reference.State(t.Context(), storage.StateScalars)
	if err != nil || !scalars.PendingUnlink || !bytes.Equal(scalars.PendingGeneration, pending.PendingGeneration) || scalars.Attr.Metadata != nil || !scalars.Attr.ModTime.IsZero() {
		t.Fatalf("delete-only scalar=%+v err=%v", scalars, err)
	}
	if _, err := reference.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("delete-only full state=%v", err)
	}
	if _, err := reference.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: httpFileAction(t, session), Generation: scalars.PendingGeneration}); err != nil {
		t.Fatal(err)
	}
	if err := reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	wire := clearPendingUnlinkCommandOf(clear)
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"generation":""`)) {
		t.Fatalf("absent aggregate is not explicit=%s", encoded)
	}
	for _, body := range [][]byte{bytes.Replace(encoded, []byte(`,"generation":""`), nil, 1), bytes.Replace(encoded, []byte(`"generation":""`), []byte(`"generation":null`), 1)} {
		var decoded clearPendingUnlinkCommand
		if err := decodeFileJSON(body, &decoded); err == nil {
			t.Fatal("missing/null generation accepted")
		}
	}
}

func TestHTTPPendingScalarIgnoresOpaqueMetadataWhileMutationBudgetRemainsFull(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	node, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	nativeSession, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer nativeSession.Close(context.Background())
	native, err := nativeSession.(storage.NodeReferences).OpenNodeRef(t.Context(), node.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Action: httpFileAction(t, nativeSession), MetadataAccess: storage.WriteMetadata})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"opaque.large", "opaque.other"} {
		if _, err := native.Reference.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), name, nil, bytes.Repeat([]byte{'m'}, 30<<10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := native.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	session := httpStateSession(t, client)
	opened, err := session.OpenNodeRef(t.Context(), node.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: node.ID}, Action: httpFileAction(t, session), Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	reference := opened.Reference.(*remoteNodeReference)
	bounded := storage.WithBoundedAttrResult(t.Context(), 1024, func(storage.Attr, int64) error { return nil })
	if got, err := reference.SetPendingUnlink(bounded, storage.PendingUnlinkCommand{Action: httpFileAction(t, session), Condition: storage.UnlinkFile}); !errors.Is(err, syscall.EFBIG) || got.Attr.ID != 0 {
		t.Fatalf("full SET result bound=%+v err=%v", got, err)
	}
	if got, err := reference.State(bounded, storage.StateScalars); err != nil || got.PendingUnlink || got.Attr.Metadata != nil {
		t.Fatalf("failed SET had effect=%+v err=%v", got, err)
	}
	pending, err := reference.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: httpFileAction(t, session), Condition: storage.UnlinkFile})
	if err != nil || !pending.PendingUnlink || len(pending.Attr.Metadata["opaque.large"].Data) != 30<<10 {
		t.Fatalf("full SET result=%+v err=%v", pending, err)
	}
	scalar, err := reference.State(bounded, storage.StateScalars)
	if err != nil || !scalar.PendingUnlink || !bytes.Equal(scalar.PendingGeneration, pending.PendingGeneration) || scalar.Attr.Metadata != nil || !scalar.Attr.AccessTime.IsZero() || !scalar.Attr.ModTime.IsZero() || scalar.Attr.BirthTime != nil || scalar.Attr.ChangeTime != nil || scalar.LinkTarget != nil {
		t.Fatalf("bounded pending scalar=%+v err=%v", scalar, err)
	}
	if got, err := reference.ClearPendingUnlink(bounded, storage.ClearPendingUnlinkCommand{Action: httpFileAction(t, session), Generation: scalar.PendingGeneration}); !errors.Is(err, syscall.EFBIG) || got.Attr.ID != 0 {
		t.Fatalf("full CLEAR result bound=%+v err=%v", got, err)
	}
	if got, err := reference.State(bounded, storage.StateScalars); err != nil || !got.PendingUnlink || !bytes.Equal(got.PendingGeneration, scalar.PendingGeneration) {
		t.Fatalf("failed CLEAR had effect=%+v err=%v", got, err)
	}
	if _, err := reference.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: httpFileAction(t, session), Generation: scalar.PendingGeneration}); err != nil {
		t.Fatal(err)
	}
	if err := reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPGuardedNameResultRejectsSubstitutedIdentity(t *testing.T) {
	for _, kind := range []storage.NameOperation{storage.NameRename, storage.NameRemove, storage.NameRemoveDir} {
		request := fileRequest{Op: storage.OpFileMutateName, Name: nameCommandOf(storage.NameCommand{Kind: kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 7}})}
		for _, id := range []uint64{7, 8} {
			response := fileResponse{Epoch: 1, Data: []byte{}, Attr: AttrOf(storage.Attr{ID: id, Kind: storage.NodeRegular})}
			for _, validate := range []func(fileRequest, fileResponse) error{validateFileResponse, validatePartialFileResponse} {
				err := validate(request, response)
				if id == 7 && err != nil || id != 7 && err == nil {
					t.Fatalf("name kind=%d identity=%d error=%v", kind, id, err)
				}
			}
		}
		if kind != storage.NameRename {
			if err := validateFileResponse(request, fileResponse{Epoch: 1, Data: []byte{}}); err != nil {
				t.Fatalf("no-attr removal=%v", err)
			}
		}
	}
}
