package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestIdentityCapabilitiesRoundTripOverHTTP(t *testing.T) {
	ctx := t.Context()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Mkdir(ctx, "dir"); err != nil {
		t.Fatal(err)
	}
	parent, err := backend.Stat(ctx, "dir")
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer sessionValue.Close(context.Background())
	session := sessionValue.(*remoteFileSession)
	status, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	openAction, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenAt(ctx, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte("file")}, storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Exclusive: true,
		Target: storage.ChildCondition{State: storage.Absent}, Action: openAction,
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName}, Existing: storage.Keep,
		Initial: storage.InitialState{OnCreate: storage.InitialFields{Metadata: map[string][]byte{"client.empty": nil}}},
	})
	if err != nil || opened.File == nil || opened.Outcome != storage.Created || opened.Attr.ID == 0 {
		t.Fatalf("open-at result=%+v err=%v", opened, err)
	}
	defer opened.File.Close(context.Background())

	lookedUp, err := session.LookupAt(ctx, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte("file")})
	if err != nil || lookedUp.ID != opened.Attr.ID {
		t.Fatalf("lookup=%+v err=%v", lookedUp, err)
	}

	mutationAction, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := int64(0)
	conditional := opened.File.(storage.ConditionalFileMutation)
	mutated, err := conditional.MutateFile(ctx, storage.FileMutation{Action: mutationAction, Kind: storage.MutateWriteAt, ExpectedSize: &expectedSize, Data: []byte("value")})
	if err != nil || mutated.Size != 5 {
		t.Fatalf("conditional mutation=%+v err=%v", mutated, err)
	}
	receipt, err := session.QueryFileAction(ctx, mutationAction)
	if err != nil || receipt.Operation != storage.OpFileMutate || receipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("action receipt=%+v err=%v", receipt, err)
	}

	refAction, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	referenceResult, err := session.OpenChildRef(ctx, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte("file")}, storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: opened.Attr.ID}, Action: refAction, Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil || referenceResult.Reference == nil {
		t.Fatalf("node reference=%+v err=%v", referenceResult, err)
	}
	defer referenceResult.Reference.Close(context.Background())
	if _, ok := referenceResult.Reference.(storage.File); ok {
		t.Fatal("node reference exposed byte methods")
	}
	state, err := referenceResult.Reference.State(ctx)
	if err != nil || state.Attr.ID != opened.Attr.ID || state.Detached || state.PendingUnlink {
		t.Fatalf("reference state=%+v err=%v", state, err)
	}
	scope, err := referenceResult.Reference.Scope(ctx)
	if err != nil || scope.Check() != nil {
		t.Fatalf("reference scope=%+v err=%v", scope, err)
	}

	deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := referenceResult.Reference.Close(deadline); err != nil {
		t.Fatal(err)
	}

	if err := backend.Create(ctx, "dir/delete"); err != nil {
		t.Fatal(err)
	}
	deleteAttr, err := backend.Stat(ctx, "dir/delete")
	if err != nil {
		t.Fatal(err)
	}
	deleteAction, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	deleteID, err := storage.NewDeleteIntentID()
	if err != nil {
		t.Fatal(err)
	}
	deleteOpen, err := session.OpenAt(ctx, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte("delete")}, storage.OpenAtOptions{
		Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: deleteAttr.ID}, Action: deleteAction,
		Use: storage.UseClaim{Uses: storage.ReadData | storage.DeleteName}, Existing: storage.Keep,
		CloseIntent: &storage.CloseIntent{ID: deleteID, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := deleteOpen.File.Close(ctx); err != nil {
		t.Fatal(err)
	}
	deleteStatus, err := session.QueryDeleteIntent(ctx, deleteID)
	if err != nil || deleteStatus.Outcome != storage.DeleteIntentCompleted || deleteStatus.NodeID != deleteAttr.ID {
		t.Fatalf("delete intent status=%+v err=%v", deleteStatus, err)
	}
	ackAction, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.AcknowledgeDeleteIntent(ctx, storage.AcknowledgeDeleteIntentCommand{Action: ackAction, Intent: deleteID}); err != nil {
		t.Fatal(err)
	}
	ackReceipt, err := session.QueryFileAction(ctx, ackAction)
	if err != nil || ackReceipt.Operation != storage.OpFileAcknowledgeDeleteIntent || ackReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("delete acknowledgement receipt=%+v err=%v", ackReceipt, err)
	}
	deleteStatus, err = session.QueryDeleteIntent(ctx, deleteID)
	if err != nil || deleteStatus.Outcome != storage.DeleteIntentUnknown || deleteStatus.NodeID != 0 {
		t.Fatalf("acknowledged delete status=%+v err=%v", deleteStatus, err)
	}
}

func TestIdentityCapabilityWireKeepsRequestConditionsSeparateFromResponseVersions(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	request := fileRequest{
		Op:      storage.OpFileOpenAt,
		Session: strings.Repeat("a", 64),
		Child: childNameOf(storage.ChildName{
			Parent:  storage.DirectoryTarget{NodeID: 7},
			RawLeaf: []byte("file"),
		}),
		OpenAt: openAtOptionsOf(storage.OpenAtOptions{
			Read:     true,
			Create:   true,
			Existing: storage.Keep,
			Target: storage.ChildCondition{
				State:            storage.SameNode,
				NodeID:           9,
				ExpectedMetadata: map[string][]byte{"client.empty": nil},
			},
			Action: action,
			Use:    storage.UseClaim{Uses: storage.ReadData},
		}),
		Path: []byte{},
		Data: []byte{},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"client.empty":""`) {
		t.Fatalf("absent metadata condition was not encoded explicitly: %s", encoded)
	}
	var decoded fileRequest
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	options := decoded.OpenAt.storage()
	if options.Action != action || options.Target.NodeID != 9 || len(options.Target.ExpectedMetadata["client.empty"]) != 0 {
		t.Fatalf("condition round trip changed intent: %+v", options)
	}

	noncanonical := strings.Replace(string(encoded), `"ZmlsZQ=="`, `"ZmlsZR=="`, 1)
	if err := decodeFileJSON([]byte(noncanonical), &decoded); err == nil {
		t.Fatal("accepted a noncanonical child name")
	}

	response := OpaquePayload{Version: nil, Data: []byte{}}
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decodedResponse OpaquePayload
	if err := json.Unmarshal(encodedResponse, &decodedResponse); err == nil {
		t.Fatal("response metadata accepted an empty authority version")
	}
}

func TestSemanticFileActionsDoNotCreateASecondTransportJournal(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	requests := []fileRequest{
		{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Action: action})},
		{Op: storage.OpFileOpenNodeRef, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Action: action})},
		{Op: storage.OpFileMutateName, Name: nameCommandOf(storage.NameCommand{Action: action})},
		{Op: storage.OpFileSetPendingUnlink, Pending: pendingUnlinkCommandOf(storage.PendingUnlinkCommand{Action: action})},
		{Op: storage.OpFileClearPendingUnlink, ClearPending: clearPendingUnlinkCommandOf(storage.ClearPendingUnlinkCommand{Action: action})},
		{Op: storage.OpFileMutate, Mutation: fileMutationOf(storage.FileMutation{Action: action})},
	}
	for _, request := range requests {
		if fileActionRequired(request.Op) {
			t.Fatalf("%s allocated a transport action in addition to %s", request.Op, action)
		}
		if got := semanticFileAction(request); got != storage.LockRequestID(action) {
			t.Fatalf("%s recovery action=%q", request.Op, got)
		}
	}
}

func TestNodeReferenceDoesNotExposeFileByteMethods(t *testing.T) {
	var reference storage.NodeReference = &remoteNodeReference{file: &remoteFile{}}
	if _, ok := reference.(storage.File); ok {
		t.Fatal("metadata-only reference exposed file byte methods")
	}
}

func TestPartialOpenWithoutCapabilityReturnsANilFileInterface(t *testing.T) {
	var reference *remoteFile
	result := openResultOf(reference, fileResponse{Outcome: storage.Created, Attr: AttrOf(storage.Attr{ID: 7, Kind: storage.NodeRegular})})
	if result.File != nil {
		t.Fatalf("nil capability became a nonnil File interface: %#v", result.File)
	}
}

func TestPartialOpenResultSurvivesAStorageError(t *testing.T) {
	request := fileRequest{Op: storage.OpFileOpenAt}
	result := fileResponse{
		Epoch:        1,
		File:         strings.Repeat("b", 64),
		Data:         []byte{},
		Attr:         AttrOf(storage.Attr{ID: 9, Kind: storage.NodeRegular}),
		Outcome:      storage.Created,
		Capabilities: &fileCapabilities{Scope: true, State: true},
	}
	encoded, err := json.Marshal(ErrorResponse{Errno: "EIO", Message: "result unknown", FileResult: &result})
	if err != nil {
		t.Fatal(err)
	}
	decoded := (&Storage{}).storageError(Request{Op: OpFile}, encoded)
	var operation *operationError
	if !errors.As(decoded, &operation) || operation.fileResult == nil {
		t.Fatalf("partial result was discarded: %v", decoded)
	}
	if err := validatePartialFileResponse(request, *operation.fileResult); err != nil {
		t.Fatalf("partial result changed on the wire: %v", err)
	}
}

func TestIdentityAuthorizationCoversCompositeEffectsAndCompatibilityClaims(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	var requests []authz.AccessRequest
	handler := &Handler{
		volume:   "trusted",
		stopping: make(chan struct{}),
		authorizer: authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
			requests = append(requests, request)
			if request.Operation == storage.OpVolumeRemove {
				return authz.ErrDenied
			}
			return nil
		}),
	}
	open := storage.OpenAtOptions{
		Read:     true,
		Write:    true,
		Create:   true,
		Existing: storage.ReplaceNode,
		Target:   storage.ChildCondition{State: storage.Any},
		Action:   action,
		Use:      storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName},
	}
	err := handler.authorizeFile(t.Context(), fileRequest{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(open)})
	if !errors.Is(err, authz.ErrDenied) || len(requests) != 2 || requests[0].Operation != storage.OpFileOpenAt || requests[0].Open != (storage.OpenAccess{Read: true, Write: true, Create: true}) || requests[1].Operation != storage.OpVolumeRemove {
		t.Fatalf("replace authorization=%+v err=%v", requests, err)
	}

	requests = nil
	handler.authorizer = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		requests = append(requests, request)
		return nil
	})
	options := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 3}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}}
	if err := handler.authorizeFile(t.Context(), fileRequest{Op: storage.OpFileOpenNodeRef, NodeRef: nodeRefOptionsOf(options)}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].Open != (storage.OpenAccess{Read: true, Write: true}) {
		t.Fatalf("compatibility claims were not authorized as open access: %+v", requests)
	}
}

func TestFileActionAndDeleteIntentQueryResponsesAreValidated(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	receipt := storage.FileActionReceipt{Action: action, Operation: storage.OpFileMutate, Outcome: storage.FileActionCompleted}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileQueryAction, FileAction: action}, fileResponse{Epoch: 1, Data: []byte{}, ActionReceipt: &receipt}); err != nil {
		t.Fatal(err)
	}
	intent := storage.DeleteIntentID(strings.Repeat("d", storage.DeleteIntentIDBytes))
	status := storage.DeleteIntentStatus{ID: intent, NodeID: 9, Outcome: storage.DeleteIntentPending}
	wireStatus, err := deleteIntentStatusOf(status)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFileResponse(fileRequest{Op: storage.OpFileQueryDeleteIntent, DeleteIntent: intent}, fileResponse{Epoch: 1, Data: []byte{}, DeleteStatus: wireStatus}); err != nil {
		t.Fatal(err)
	}
	wireStatus.ID = storage.DeleteIntentID(strings.Repeat("e", storage.DeleteIntentIDBytes))
	if err := validateFileResponse(fileRequest{Op: storage.OpFileQueryDeleteIntent, DeleteIntent: intent}, fileResponse{Epoch: 1, Data: []byte{}, DeleteStatus: wireStatus}); err == nil {
		t.Fatal("accepted a delete intent result for another action")
	}
}

func TestDeleteIntentStatusUsesPortableFailureNames(t *testing.T) {
	intent := storage.DeleteIntentID(strings.Repeat("d", storage.DeleteIntentIDBytes))
	wire, err := deleteIntentStatusOf(storage.DeleteIntentStatus{ID: intent, NodeID: 7, Outcome: storage.DeleteIntentCleanupFailed, Failure: syscall.EACCES})
	if err != nil || wire.Failure != "EACCES" {
		t.Fatalf("wire status=%+v err=%v", wire, err)
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"failure":13`) || !strings.Contains(string(encoded), `"failure":"EACCES"`) {
		t.Fatalf("platform errno leaked onto wire: %s", encoded)
	}
	var decoded deleteIntentStatus
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	status, err := decoded.storage()
	if err != nil || status.Failure != syscall.EACCES {
		t.Fatalf("decoded status=%+v err=%v", status, err)
	}
	unknown, err := deleteIntentStatusOf(storage.DeleteIntentStatus{ID: intent, Outcome: storage.DeleteIntentUnknown})
	if err != nil {
		t.Fatal(err)
	}
	if status, err := unknown.storage(); err != nil || status.NodeID != 0 || status.Outcome != storage.DeleteIntentUnknown {
		t.Fatalf("unknown status=%+v err=%v", status, err)
	}
}
