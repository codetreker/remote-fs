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
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}
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
	recover := (&connection{server: server}).pendingOpenRecovery(tree, handle, storage.OpFileOpenAt, func(context.Context) (storage.Attr, storage.OpenOutcome, error) {
		opened++
		return storage.Attr{}, 0, nil
	})
	if noReference, err := recover(t.Context()); err != nil || !noReference || opened != 0 || len(authorized) != 0 {
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
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}
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
	file := &handleFileStub{result: storage.ReferenceCloseResult{Released: true, Determined: true}}
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

func TestCreateIntentSeparatesByteMetadataAndShareRights(t *testing.T) {
	for _, test := range []struct {
		name              string
		access            uint32
		read, write, node bool
		uses              storage.Uses
		metadata          storage.MetadataPermissions
	}{
		{"execute", accessExecute, false, false, true, storage.ReadData, 0},
		{"execute-write", accessExecute | accessWriteData, false, true, false, storage.ReadData | storage.WriteData, 0},
		{"read-attribute-write", accessReadData | accessWriteAttr, true, false, false, storage.ReadData, storage.WriteMetadata},
		{"write-attribute-read", accessWriteData | accessReadAttr, false, true, false, storage.WriteData, storage.ReadMetadata},
		{"read-ea", accessReadEA, false, false, true, 0, storage.ReadMetadata},
		{"write-ea", accessWriteEA, false, false, true, 0, storage.WriteMetadata},
		{"delete", accessDelete, false, false, true, storage.DeleteName, 0},
		{"control", accessReadCtrl | accessSync, false, false, true, 0, 0},
		{"generic-execute", accessGenericExecute, false, false, true, storage.ReadData, storage.ReadMetadata},
	} {
		t.Run(test.name, func(t *testing.T) {
			intent, err := classifyCreate(wire.CreateRequest{DesiredAccess: test.access, ShareAccess: 7, Disposition: 3})
			if err != nil || intent.read != test.read || intent.write != test.write || intent.metadataOnly != test.node || intent.use.Uses != test.uses || intent.metadata != test.metadata {
				t.Fatalf("intent=%+v err=%v", intent, err)
			}
		})
	}
	directory, err := classifyCreate(wire.CreateRequest{DesiredAccess: accessExecute | accessWriteData, ShareAccess: 7, Disposition: 1, Options: createDirectory})
	if err != nil || directory.read || !directory.write || directory.use.Uses != storage.ReadEntries|storage.WriteData {
		t.Fatalf("directory intent=%+v err=%v", directory, err)
	}
}

func TestCreateAllocationHintPreservesOrdinaryOpenIntent(t *testing.T) {
	request := wire.CreateRequest{DesiredAccess: accessReadData, ShareAccess: 7, Disposition: 1, SecurityFlags: 255}
	ordinary, err := classifyCreate(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Contexts = []wire.CreateContext{{Name: []byte("AlSi"), Data: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}}}
	hinted, err := classifyCreate(request)
	if err != nil || hinted != ordinary {
		t.Fatalf("allocation hint changed ordinary open: %+v, %v", hinted, err)
	}
}

func TestCreateSynchronousOptionsRequireSynchronizedAccess(t *testing.T) {
	for _, option := range []uint32{createSyncAlert, createSyncNonAlert} {
		request := wire.CreateRequest{DesiredAccess: accessReadData, Disposition: 1, Options: option}
		if _, err := classifyCreate(request); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("missing SYNCHRONIZE: %v", err)
		}
		request.DesiredAccess |= accessSync
		if _, err := classifyCreate(request); err != nil {
			t.Fatalf("synchronous intent rejected: %v", err)
		}
	}
	if _, err := classifyCreate(wire.CreateRequest{DesiredAccess: accessSync, Disposition: 1, Options: createSyncAlert | createSyncNonAlert}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("contradictory sync options: %v", err)
	}
}

func TestCreateOutcomeMatchesDisposition(t *testing.T) {
	expected := map[uint32][]storage.OpenOutcome{1: {storage.Opened}, 2: {storage.Created}, 3: {storage.Opened, storage.Created}, 4: {storage.Reset}, 5: {storage.Created, storage.Reset}}
	for disposition := uint32(0); disposition <= 6; disposition++ {
		for outcome := storage.OpenOutcome(0); outcome <= storage.Replaced+1; outcome++ {
			want := false
			for _, valid := range expected[disposition] {
				want = want || outcome == valid
			}
			if got := validCreateOutcome(disposition, outcome); got != want {
				t.Fatalf("disposition %d outcome %d = %v", disposition, outcome, got)
			}
		}
	}
}

func TestCreateGenericAccessPreservesEveryMappedRight(t *testing.T) {
	for _, test := range []struct{ generic, want uint32 }{
		{accessGenericRead, 0x00120089}, {accessGenericWrite, 0x00120116}, {accessGenericExecute, 0x001200a0},
	} {
		got, err := normalizeAccess(test.generic)
		if err != nil || got != test.want {
			t.Fatalf("generic %x mapped=%x want=%x err=%v", test.generic, got, test.want, err)
		}
	}
	for _, access := range []uint32{accessGenericAll, accessGenericAll | accessReadData, accessGenericAll | accessGenericRead} {
		if _, err := normalizeAccess(access); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("genericALL dropped unsupported rights: access=%x err=%v", access, err)
		}
	}
}

func TestCreateDirectoryAttributesFollowOptionsAndRetainOnlySettableFlags(t *testing.T) {
	for _, test := range []struct {
		attributes, options uint32
		directory           bool
		retained            uint32
	}{
		{dosDirectory, createDirectory, true, 0}, {dosDirectory | dosHidden, createDirectory, true, dosHidden},
		{dosNormal | dosDirectory | dosHidden, createDirectory, true, dosHidden},
		{dosDirectory, 0, false, 0}, {dosDirectory | dosHidden, createNonDirectory, false, dosHidden},
	} {
		intent, err := classifyCreate(wire.CreateRequest{Disposition: 2, DesiredAccess: accessReadAttr, Attributes: test.attributes, Options: test.options})
		if err != nil || intent.directory != test.directory || intent.attributes != test.retained {
			t.Fatalf("attributes=%x options=%x intent=%+v err=%v", test.attributes, test.options, intent, err)
		}
	}
}
