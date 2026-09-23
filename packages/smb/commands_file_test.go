package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCreateIntentMapsWindowsShareBothDirections(t *testing.T) {
	request := wire.CreateRequest{DesiredAccess: accessGenericRead | accessDelete, ShareAccess: 2, Disposition: 1}
	intent, err := classifyCreate(request)
	if err != nil {
		t.Fatal(err)
	}
	if !intent.read || intent.write || intent.create || intent.reset || intent.use.Uses != storage.ReadData|storage.DeleteName ||
		intent.use.Deny != storage.ReadData|storage.ReadEntries|storage.DeleteName {
		t.Fatalf("share/access mapping: %+v", intent)
	}
	request.DesiredAccess = accessReadAttr
	request.ShareAccess = 7
	intent, err = classifyCreate(request)
	if err != nil || !intent.metadataOnly || intent.use.Uses != 0 || intent.use.Deny != 0 {
		t.Fatalf("metadata-only mapping: %+v, %v", intent, err)
	}
}

type noAllocationSession struct{ storage.FileSession }

func TestCreateRejectsMissingAllocationCapabilityBeforeEffect(t *testing.T) {
	if err := checkCreateCapabilities(&noAllocationSession{FileSession: newEndpointFileSession()}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing allocation capability: %v", err)
	}
}

func TestCloseRetiresFileIDAndRejectsReuse(t *testing.T) {
	s := &session{id: 12}
	server := &Server{config: Config{Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}}
	tree := &tree{export: &Export{share: Share{Volume: "v"}}}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true}}
	handle.file = file
	tree.endFileWork()
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	copy(body[8:], handle.id[:])
	request := wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}
	c := &connection{server: server}
	if _, status := c.closeFile(t.Context(), s, tree, request, wire.FileID{}); status != statusOK {
		t.Fatalf("first close status = %#x", status)
	}
	if _, status := c.closeFile(t.Context(), s, tree, request, wire.FileID{}); status != closeStatusError(syscall.EBADF) {
		t.Fatalf("reused FileId status = %#x", status)
	}
	if file.closes.Load() != 1 {
		t.Fatalf("reference close count = %d", file.closes.Load())
	}
}

type actionSessionStub struct {
	*endpointFileSession
	receipt storage.FileActionReceipt
}

func (s *actionSessionStub) QueryFileAction(context.Context, storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.receipt, nil
}

func TestPendingOpenRecoveryUsesReceiptAndOriginalAction(t *testing.T) {
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	var authorized []storage.Operation
	server := &Server{config: Config{Authorize: authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		authorized = append(authorized, request.Operation)
		return nil
	})}}
	raw := &actionSessionStub{endpointFileSession: newEndpointFileSession(), receipt: storage.FileActionReceipt{Action: action, Operation: storage.OpFileOpenAt, Outcome: storage.FileActionNotExecuted}}
	tree := &tree{export: &Export{share: Share{Volume: "v"}}, authority: &authoritySession{raw: raw}}
	handle := &fileHandle{action: action}
	opened := 0
	recover := (&connection{server: server}).pendingOpenRecovery(tree, handle, storage.OpFileOpenAt, openIntent{read: true}, storage.InitialState{}, func(context.Context) (storage.Attr, storage.OpenOutcome, error) {
		opened++
		return storage.Attr{}, 0, nil
	})
	if noReference, err := recover(t.Context()); err != nil || !noReference || opened != 0 || len(authorized) != 1 || authorized[0] != storage.OpFileQueryAction {
		t.Fatalf("not-executed receipt: noReference=%v err=%v opens=%d auth=%v", noReference, err, opened, authorized)
	}
	raw.receipt.Outcome = storage.FileActionUnknown
	if noReference, err := recover(t.Context()); err == nil || noReference || opened != 1 {
		t.Fatalf("unknown empty result: noReference=%v err=%v opens=%d", noReference, err, opened)
	}
	raw.receipt.Operation = ""
	if noReference, err := recover(t.Context()); err == nil || noReference || opened != 1 {
		t.Fatalf("history-less unknown replayed: noReference=%v err=%v opens=%d", noReference, err, opened)
	}
	raw.receipt.Outcome = storage.FileActionNotExecuted
	if noReference, err := recover(t.Context()); err == nil || noReference || opened != 2 {
		t.Fatalf("unrecorded action was assumed absent: noReference=%v err=%v opens=%d", noReference, err, opened)
	}
}

func TestFailedOpenClosesReturnedReference(t *testing.T) {
	s := &session{id: 13}
	tree := &tree{}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true}}
	handle.file = file
	server := &Server{config: Config{Limits: DefaultLimits()}}
	status := (&connection{server: server}).finishFailedOpen(t.Context(), tree, handle, syscall.EIO)
	tree.endFileWork()
	if status != createStatusError(syscall.EIO) || tree.findFileHandle(handle.id) != nil || file.closes.Load() != 1 {
		t.Fatalf("failed open cleanup: status=%#x closes=%d", status, file.closes.Load())
	}
}

func TestClosePostqueryFailureStillReleasesReference(t *testing.T) {
	s := &session{id: 14}
	server := &Server{config: Config{Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}}
	tree := &tree{export: &Export{share: Share{Volume: "v"}}}
	if !tree.beginFileWork(s) {
		t.Fatal("file admission refused")
	}
	handle, err := tree.reserveFileHandle(s, 1)
	if err != nil {
		t.Fatal(err)
	}
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true}}
	handle.file = file
	tree.endFileWork()
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	binary.LittleEndian.PutUint16(body[2:], 1)
	copy(body[8:], handle.id[:])
	response, status := (&connection{server: server}).closeFile(t.Context(), s, tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{})
	if status != statusOK || len(response) != 60 || binary.LittleEndian.Uint16(response[2:]) != 0 || file.closes.Load() != 1 {
		t.Fatalf("postquery failure close: status=%#x response=%v closes=%d", status, response, file.closes.Load())
	}
}

func TestCreateRejectsUnsupportedEffectsBeforeOpen(t *testing.T) {
	for _, request := range []wire.CreateRequest{
		{Disposition: 0},
		{Disposition: 1, Options: createDeleteOnClose},
		{Disposition: 1, Options: createOpenByFileID},
		{Disposition: 1, DesiredAccess: 0x00040000},
		{Disposition: 4, DesiredAccess: accessReadData},
	} {
		if _, err := classifyCreate(request); err == nil {
			t.Fatalf("unsupported CREATE accepted: %+v", request)
		}
	}
	if _, err := classifyCreate(wire.CreateRequest{Disposition: 1, Contexts: []wire.CreateContext{{Name: []byte("EA"), Data: []byte{1}}}}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("effectful context: %v", err)
	}
}
