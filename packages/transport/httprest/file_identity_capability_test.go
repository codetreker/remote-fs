package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

func loseFileResponses(t *testing.T, client *Storage, operation storage.Operation, losses int32) *atomic.Int32 {
	t.Helper()
	original := client.http.Transport
	var calls atomic.Int32
	client.http.Transport = fileRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var envelope struct {
			Op storage.Operation `json:"op"`
		}
		if request.GetBody != nil {
			body, err := request.GetBody()
			if err != nil {
				return nil, err
			}
			err = json.NewDecoder(body).Decode(&envelope)
			_ = body.Close()
			if err != nil {
				return nil, err
			}
		}
		response, err := original.RoundTrip(request)
		if envelope.Op != operation {
			return response, err
		}
		attempt := calls.Add(1)
		if err == nil && attempt <= losses {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			return nil, errors.New("lost semantic file response")
		}
		return response, err
	})
	t.Cleanup(func() { client.http.Transport = original })
	return &calls
}

type partialNameBackend struct {
	*objectstore.Storage
	calls atomic.Int32
}

type partialNameSession struct {
	storage.FileSession
	backend *partialNameBackend
}

func (b *partialNameBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &partialNameSession{FileSession: session, backend: b}, nil
}

func (*partialNameSession) CheckNamespaceAccess() error { return nil }

func (s *partialNameSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	return s.FileSession.(storage.NamespaceAccess).LookupAt(ctx, name)
}

func (s *partialNameSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	s.backend.calls.Add(1)
	result, err := s.FileSession.(storage.NamespaceAccess).MutateName(ctx, command)
	if err != nil {
		return result, err
	}
	return result, syscall.EIO
}

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

func TestLostSemanticOpenResponsesReplayTheOriginalCapability(t *testing.T) {
	for _, operation := range []storage.Operation{storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef} {
		t.Run(string(operation), func(t *testing.T) {
			limits := DefaultFileLimits()
			limits.PendingAck = 20 * time.Millisecond
			client, handler, backend := retainedHTTPFixture(t, limits)
			if err := backend.Mkdir(t.Context(), "dir"); err != nil {
				t.Fatal(err)
			}
			if err := backend.Write(t.Context(), "dir/file", []byte("body")); err != nil {
				t.Fatal(err)
			}
			parent, err := backend.Stat(t.Context(), "dir")
			if err != nil {
				t.Fatal(err)
			}
			file, err := backend.Stat(t.Context(), "dir/file")
			if err != nil {
				t.Fatal(err)
			}
			sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := sessionValue.(*remoteFileSession)
			defer session.Close(context.Background())
			status, err := session.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			action, err := storage.NewFileActionID(status.ActionEpoch)
			if err != nil {
				t.Fatal(err)
			}
			calls := loseFileResponses(t, client, operation, 2)

			open := func() (retainedReference, error) {
				switch operation {
				case storage.OpFileOpenAt:
					result, openErr := session.OpenAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte("file")}, storage.OpenAtOptions{
						Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID}, Action: action,
						Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
					})
					return result.File, openErr
				case storage.OpFileOpenNodeRef:
					result, openErr := session.OpenNodeRef(t.Context(), file.ID, storage.NodeRefOptions{
						Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID}, Action: action,
						Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.ReadMetadata,
					})
					return result.Reference, openErr
				default:
					result, openErr := session.OpenChildRef(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent.ID}, RawLeaf: []byte("file")}, storage.NodeRefOptions{
						Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID}, Action: action,
						Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.ReadMetadata,
					})
					return result.Reference, openErr
				}
			}
			if reference, lostErr := open(); storage.ErrnoOf(lostErr) != syscall.EIO || reference != nil {
				t.Fatalf("lost open response reference=%T error=%v", reference, lostErr)
			}
			time.Sleep(250 * time.Millisecond)
			reference, err := open()
			if err != nil || reference == nil || calls.Load() != 3 {
				t.Fatalf("open reference=%T calls=%d error=%v", reference, calls.Load(), err)
			}
			defer reference.Close(context.Background())
			attr, err := reference.Stat(t.Context())
			if err != nil || attr.ID != file.ID {
				t.Fatalf("replayed capability was invalidated by pending cleanup: attr=%+v error=%v", attr, err)
			}
			handler.files.mu.Lock()
			served := handler.files.sessions[session.id]
			handler.files.mu.Unlock()
			served.mu.Lock()
			retained := len(served.files)
			served.mu.Unlock()
			if retained != 1 {
				t.Fatalf("semantic open retained %d HTTP capabilities", retained)
			}
		})
	}
}

func TestSemanticOpenReplayGetsAFreshAcknowledgementWindow(t *testing.T) {
	limits := DefaultFileLimits()
	limits.PendingAck = 500 * time.Millisecond
	client, handler, backend := retainedHTTPFixture(t, limits)
	if err := backend.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	file, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*remoteFileSession)
	defer session.Close(context.Background())
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	request := fileRequest{
		Op:    storage.OpFileOpenAt,
		Child: childNameOf(storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("file")}),
		OpenAt: openAtOptionsOf(storage.OpenAtOptions{
			Read: true, Target: storage.ChildCondition{State: storage.SameNode, NodeID: file.ID}, Action: action,
			Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
		}),
	}
	first, err := session.call(t.Context(), request)
	if err != nil || first.File == "" {
		t.Fatalf("first open=%+v error=%v", first, err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	deadline := time.Now().Add(150 * time.Millisecond)
	served.actions[storage.LockRequestID(action)].expires = deadline
	served.files[first.File].pending = deadline
	served.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	replayed, err := session.call(t.Context(), request)
	if err != nil || replayed.File != first.File {
		t.Fatalf("replayed open=%+v error=%v", replayed, err)
	}
	waitUntil := time.Now().Add(2 * time.Second)
	available := false
	for time.Now().Before(waitUntil) {
		served.mu.Lock()
		_, actionPresent := served.actions[storage.LockRequestID(action)]
		file := served.files[first.File]
		available = !actionPresent && file != nil && !file.closing
		served.mu.Unlock()
		if available {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !available {
		t.Fatal("action history expired without leaving the replayed capability available for acknowledgement")
	}
	if _, err := session.call(t.Context(), fileRequest{Op: storage.OpFileAck, File: first.File}); err != nil {
		t.Fatalf("acknowledge replayed capability=%v", err)
	}
}

func TestLostSemanticActionRefusalIsRecoveredAsRecorded(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*remoteFileSession)
	defer session.Close(context.Background())
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	calls := loseFileResponses(t, client, storage.OpFileMutateName, 2)
	command := storage.NameCommand{
		Kind: storage.NameRemove, Action: action,
		Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("missing")},
		Target: storage.ChildCondition{State: storage.Any},
	}
	if result, lostErr := session.MutateName(t.Context(), command); storage.ErrnoOf(lostErr) != syscall.EIO || result.Attr != nil {
		t.Fatalf("lost refusal response result=%+v error=%v", result, lostErr)
	}
	_, err = session.MutateName(t.Context(), command)
	if !errors.Is(err, syscall.ENOENT) || calls.Load() != 3 {
		t.Fatalf("known refusal calls=%d error=%v", calls.Load(), err)
	}
	session.mu.Lock()
	pending := len(session.pending)
	session.mu.Unlock()
	if pending != 0 {
		t.Fatalf("known refusal remained pending after replay: %d", pending)
	}
}

func TestLostSemanticPartialResultIsRecoveredAsRecorded(t *testing.T) {
	_, native := memoryfixture.New(t, "partial-semantic-http", 1<<20, locking.DefaultOptions())
	backend := &partialNameBackend{Storage: native}
	options := DefaultHandlerOptions()
	options.Files = DefaultFileLimits()
	handler, err := NewHandlerWithOptions(backend, nil, options)
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
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*remoteFileSession)
	defer session.Close(context.Background())
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	calls := loseFileResponses(t, client, storage.OpFileMutateName, 2)
	command := storage.NameCommand{
		Kind: storage.NameCreate, Action: action,
		Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("created")},
		Target: storage.ChildCondition{State: storage.Absent},
	}
	if result, lostErr := session.MutateName(t.Context(), command); storage.ErrnoOf(lostErr) != syscall.EIO || result.Attr != nil {
		t.Fatalf("lost partial response result=%+v error=%v", result, lostErr)
	}
	result, err := session.MutateName(t.Context(), command)
	if !errors.Is(err, syscall.EIO) || result.Attr == nil || result.Attr.ID == 0 || calls.Load() != 3 || backend.calls.Load() != 1 {
		t.Fatalf("partial result=%+v transport calls=%d native calls=%d error=%v", result, calls.Load(), backend.calls.Load(), err)
	}
	session.mu.Lock()
	pending := len(session.pending)
	session.mu.Unlock()
	if pending != 0 {
		t.Fatalf("partial result remained pending after replay: %d", pending)
	}
}

func TestNodeReferenceStatAndSetAttrAcceptDirectoryAndSymlinkAttributes(t *testing.T) {
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Mkdir(t.Context(), "dir"); err != nil {
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
	sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*remoteFileSession)
	defer session.Close(context.Background())
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	newAction := func() storage.FileActionID {
		action, actionErr := storage.NewFileActionID(status.ActionEpoch)
		if actionErr != nil {
			t.Fatal(actionErr)
		}
		return action
	}
	created, err := session.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Action: newAction(),
		Name:    storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("link")},
		Target:  storage.ChildCondition{State: storage.Absent},
		Initial: storage.InitialFields{LinkTarget: []byte("target")},
	})
	if err != nil || created.Attr == nil {
		t.Fatalf("create symlink=%+v error=%v", created, err)
	}

	tests := []struct {
		name string
		attr storage.Attr
	}{
		{name: "directory", attr: directory},
		{name: "symlink", attr: *created.Attr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opened, err := session.OpenNodeRef(t.Context(), test.attr.ID, storage.NodeRefOptions{
				Kind: test.attr.Kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: test.attr.ID}, Action: newAction(),
				MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
			})
			if err != nil || opened.Reference == nil {
				t.Fatalf("open=%+v error=%v", opened, err)
			}
			defer opened.Reference.Close(context.Background())
			observed, err := opened.Reference.Stat(t.Context())
			if err != nil || observed.ID != test.attr.ID || observed.Kind != test.attr.Kind {
				t.Fatalf("stat=%+v error=%v", observed, err)
			}
			changed := time.Unix(123, 456).UTC()
			observed, err = opened.Reference.SetAttr(t.Context(), storage.AttrChange{ModTime: &changed})
			if err != nil || observed.ID != test.attr.ID || observed.Kind != test.attr.Kind || !observed.ModTime.Equal(changed) {
				t.Fatalf("setattr=%+v error=%v", observed, err)
			}
		})
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

func TestSemanticFileActionsShareTheirTransportJournalIdentity(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	requests := []fileRequest{
		{Op: storage.OpFileOpenAt, OpenAt: openAtOptionsOf(storage.OpenAtOptions{Action: action})},
		{Op: storage.OpFileOpenNodeRef, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Action: action})},
		{Op: storage.OpFileOpenChildRef, NodeRef: nodeRefOptionsOf(storage.NodeRefOptions{Action: action})},
		{Op: storage.OpFileMutateName, Name: nameCommandOf(storage.NameCommand{Action: action})},
		{Op: storage.OpFileSetPendingUnlink, Pending: pendingUnlinkCommandOf(storage.PendingUnlinkCommand{Action: action})},
		{Op: storage.OpFileClearPendingUnlink, ClearPending: clearPendingUnlinkCommandOf(storage.ClearPendingUnlinkCommand{Action: action})},
		{Op: storage.OpFileMutate, Mutation: fileMutationOf(storage.FileMutation{Action: action})},
		{Op: storage.OpFileAcknowledgeDeleteIntent, Acknowledge: acknowledgeDeleteIntentCommandOf(storage.AcknowledgeDeleteIntentCommand{Action: action})},
	}
	for _, request := range requests {
		if !fileActionRequired(request.Op) {
			t.Fatalf("%s did not retain its transport result", request.Op)
		}
		if got := semanticFileAction(request); got != storage.LockRequestID(action) {
			t.Fatalf("%s recovery action=%q", request.Op, got)
		}
	}
}

func TestSemanticFileActionMustMatchTheTransportAction(t *testing.T) {
	action := storage.FileActionID("1:00000000000000000000000000000000")
	request := fileRequest{
		Op:      storage.OpFileAcknowledgeDeleteIntent,
		Session: strings.Repeat("a", 64),
		Action:  storage.LockRequestID(action),
		Acknowledge: acknowledgeDeleteIntentCommandOf(storage.AcknowledgeDeleteIntentCommand{
			Action: action,
			Intent: storage.DeleteIntentID(strings.Repeat("d", storage.DeleteIntentIDBytes)),
		}),
		Path: []byte{},
		Data: []byte{},
	}
	if err := validateFileRequest(request); err != nil {
		t.Fatal(err)
	}
	request.Action = "1:11111111111111111111111111111111"
	if err := validateFileRequest(request); err == nil {
		t.Fatal("accepted different transport and semantic action identities")
	}
	request.Action = ""
	if err := validateFileRequest(request); err == nil {
		t.Fatal("accepted a semantic action without its transport journal identity")
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

func TestPendingUnlinkAuthorizationUsesOnlyItsCanonicalOperation(t *testing.T) {
	for _, op := range []storage.Operation{storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink} {
		t.Run(string(op), func(t *testing.T) {
			var requests []authz.AccessRequest
			handler := &Handler{
				volume:   "trusted",
				stopping: make(chan struct{}),
				authorizer: authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
					requests = append(requests, request)
					return nil
				}),
			}
			if err := handler.authorizeFile(t.Context(), fileRequest{Op: op}); err != nil {
				t.Fatal(err)
			}
			if len(requests) != 1 || requests[0].Operation != op {
				t.Fatalf("authorization requests=%+v", requests)
			}
		})
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
