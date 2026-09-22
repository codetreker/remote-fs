package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func createTestOwner(t *testing.T) storage.DeleteIntentOwner {
	t.Helper()
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func createTestAttr(t *testing.T, id uint64, kind storage.NodeKind, attributes uint32) storage.Attr {
	t.Helper()
	birth, change := time.Unix(1, 0).UTC(), time.Unix(4, 0).UTC()
	payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	return storage.Attr{
		ID: id, Kind: kind, BirthTime: &birth, ChangeTime: &change,
		AccessTime: time.Unix(2, 0).UTC(), ModTime: time.Unix(3, 0).UTC(),
		Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}, Data: payload}},
	}
}

func createTestResolved(attr *storage.Attr) resolvedName {
	condition := storage.ChildCondition{State: storage.Absent}
	if attr != nil {
		condition = storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}
	}
	return resolvedName{
		rootID: 1,
		selection: storage.ChildSelection{
			Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1, Scope: &storage.UseScope{Token: "root"}}, RawLeaf: []byte("file")},
			Guards: &storage.NamespaceGuards{RootID: 1, Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1}}}},
		},
		condition: condition, attr: attr,
	}
}

func createTestRequest(disposition uint32) wire.CreateRequest {
	return wire.CreateRequest{
		Impersonation: 2, DesiredAccess: fileReadData | fileWriteData | fileReadAttributes,
		ShareAccess: 7, Disposition: disposition, Attributes: dosNormal,
	}
}

func TestCreatePlansExactSupportedDispositionSubset(t *testing.T) {
	existing := createTestAttr(t, 9, storage.NodeRegular, dosArchive)
	for _, test := range []struct {
		name        string
		disposition uint32
		attr        *storage.Attr
		outcome     storage.OpenOutcome
		create      bool
		exclusive   bool
		existing    storage.ExistingEffect
		wantErr     error
	}{
		{name: "open existing", disposition: 1, attr: &existing, outcome: storage.Opened, existing: storage.Keep},
		{name: "open missing", disposition: 1, wantErr: syscall.ENOENT},
		{name: "create missing", disposition: 2, outcome: storage.Created, create: true, exclusive: true, existing: storage.Keep},
		{name: "create existing", disposition: 2, attr: &existing, wantErr: syscall.EEXIST},
		{name: "open-if existing", disposition: 3, attr: &existing, outcome: storage.Opened, create: true, existing: storage.Keep},
		{name: "open-if missing", disposition: 3, outcome: storage.Created, create: true, existing: storage.Keep},
		{name: "overwrite existing", disposition: 4, attr: &existing, outcome: storage.Reset, existing: storage.ResetContent},
		{name: "overwrite missing", disposition: 4, wantErr: syscall.ENOENT},
		{name: "overwrite-if existing", disposition: 5, attr: &existing, outcome: storage.Reset, create: true, existing: storage.ResetContent},
		{name: "overwrite-if missing", disposition: 5, outcome: storage.Created, create: true, existing: storage.Keep},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := createTestRequest(test.disposition)
			plan, err := buildCreatePlan(request, createTestResolved(test.attr), createTestOwner(t))
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("plan error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if plan.outcome != test.outcome || plan.file == nil || plan.node != nil || plan.file.Create != test.create ||
				plan.file.Exclusive != test.exclusive || plan.file.Existing != test.existing {
				t.Fatalf("plan = %+v", plan)
			}
		})
	}
	request := createTestRequest(0)
	if _, err := buildCreatePlan(request, createTestResolved(&existing), createTestOwner(t)); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("SUPERSEDE = %v", err)
	}
}

func TestCreatePlanMapsShareAndDeleteIntentIntoOneAtomicClaim(t *testing.T) {
	request := createTestRequest(3)
	request.DesiredAccess = fileReadData | fileWriteData | fileDelete
	request.ShareAccess = 1
	request.Options = createDeleteOnClose
	owner := createTestOwner(t)
	plan, err := buildCreatePlan(request, createTestResolved(nil), owner)
	if err != nil {
		t.Fatal(err)
	}
	wantUses := storage.ReadData | storage.WriteData | storage.DeleteName
	wantDeny := storage.WriteData | storage.DeleteName
	if plan.file == nil || plan.file.Use.Uses != wantUses || plan.file.Use.Deny != wantDeny || plan.file.CloseIntent == nil ||
		plan.file.CloseIntent.Owner != owner || plan.file.CloseIntent.Trigger != storage.OnReferenceClose ||
		plan.file.CloseIntent.Condition != storage.UnlinkFile || plan.file.CloseIntent.ID == "" {
		t.Fatalf("atomic open claim = %+v", plan.file)
	}
	plan.file.Action, err = storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.file.Check(); err != nil {
		t.Fatalf("mapped open is invalid: %v", err)
	}
}

func TestCreatePlanUsesMetadataReferenceForDirectoriesAndMetadataOnlyFiles(t *testing.T) {
	for _, test := range []struct {
		name    string
		kind    storage.NodeKind
		options uint32
		access  uint32
		uses    storage.Uses
	}{
		{name: "directory traverse", kind: storage.NodeDirectory, options: createDirectory, access: fileExecute | fileReadAttributes, uses: storage.ReadEntries},
		{name: "metadata file", kind: storage.NodeRegular, options: createNonDirectory, access: fileReadAttributes, uses: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			attr := createTestAttr(t, 8, test.kind, 0)
			request := createTestRequest(1)
			request.Options, request.DesiredAccess = test.options, test.access
			plan, err := buildCreatePlan(request, createTestResolved(&attr), createTestOwner(t))
			if err != nil {
				t.Fatal(err)
			}
			if plan.file != nil || plan.node == nil || plan.node.Kind != test.kind || plan.node.Use.Uses != test.uses ||
				plan.node.MetadataAccess != storage.ReadMetadata {
				t.Fatalf("metadata plan = %+v", plan)
			}
		})
	}
}

func TestCreateValidationRejectsUnsupportedOrContradictoryRequestsBeforeAuthority(t *testing.T) {
	for _, request := range []wire.CreateRequest{
		{Impersonation: 2, Disposition: 0},
		{Impersonation: 2, Disposition: 1, ShareAccess: 8},
		{Impersonation: 2, Disposition: 1, Options: createDirectory | createNonDirectory},
		{Impersonation: 2, Disposition: 1, Options: createDeleteOnClose},
		{Impersonation: 2, Disposition: 1, Options: 0x10000},
		{Impersonation: 2, Disposition: 1, Attributes: 0x100},
		{Impersonation: 2, Disposition: 1, Attributes: dosNormal | dosArchive},
		{Impersonation: 2, Disposition: 1, Contexts: []wire.CreateContext{{Name: []byte("DH2C")}}},
	} {
		if _, err := validateCreateRequest(request); err == nil {
			t.Fatalf("invalid request accepted: %+v", request)
		}
	}
}

func TestCreateAuthorizationCoversEveryAtomicEffect(t *testing.T) {
	request := createTestRequest(3)
	request.DesiredAccess |= fileDelete
	request.Options = createDeleteOnClose
	plan, err := buildCreatePlan(request, createTestResolved(nil), createTestOwner(t))
	if err != nil {
		t.Fatal(err)
	}
	tree, _, _ := namespaceTree(t)
	var got []storage.Operation
	err = authorizeCreate(t.Context(), tree, plan, func(_ context.Context, request authz.AccessRequest) error {
		got = append(got, request.Operation)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []storage.Operation{storage.OpFileOpenAt, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("authorization order = %v, want %v", got, want)
	}
}

func TestCreateAuthorizationPreservesCreateCapableDispositionOnExistingNode(t *testing.T) {
	attr := createTestAttr(t, 9, storage.NodeRegular, dosArchive)
	request := createTestRequest(3)
	plan, err := buildCreatePlan(request, createTestResolved(&attr), createTestOwner(t))
	if err != nil {
		t.Fatal(err)
	}
	tree, _, _ := namespaceTree(t)
	var open storage.OpenAccess
	err = authorizeCreate(t.Context(), tree, plan, func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpFileOpenAt {
			open = request.Open
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !open.Create || open.Exclusive || open.Truncate {
		t.Fatalf("OPEN_IF authorization = %+v", open)
	}
}

func TestCreateStatusAndContextResponsesPreserveWindowsDistinctions(t *testing.T) {
	for _, test := range []struct {
		err  error
		want uint32
	}{
		{nil, statusOK},
		{createFailure(0x1234, syscall.EPERM), 0x1234},
		{errNameInvalid, statusObjectNameInvalid},
		{errPathMissing, statusObjectPathNotFound},
		{storage.ErrConditionConflict, statusRetry},
		{storage.ErrUseConflict, statusSharingViolation},
		{storage.ErrPendingDelete, statusDeletePending},
		{syscall.EEXIST, statusObjectNameCollision},
		{syscall.EISDIR, statusFileIsADirectory},
		{syscall.EDQUOT, statusQuotaExceeded},
		{syscall.ENOSPC, statusDiskFull},
	} {
		if got := createStatus(test.err); got != test.want {
			t.Fatalf("createStatus(%v) = %#x, want %#x", test.err, got, test.want)
		}
	}
	failure := createFailure(statusIO, syscall.EIO)
	if failure.Error() == "" || !errors.Is(failure, syscall.EIO) {
		t.Fatalf("status error lost its cause: %v", failure)
	}

	base := wire.CreateResponseBody(wire.CreateResult{})
	body := createResponseContexts(base, []wire.CreateContext{
		{Name: []byte("QFid")},
		{Name: []byte("MxAc"), Data: make([]byte, 8)},
	}, 7, 11, 0)
	if len(body) != 176 || binary.LittleEndian.Uint32(body[80:84]) != wire.HeaderSize+88 ||
		binary.LittleEndian.Uint32(body[84:88]) != 88 || binary.LittleEndian.Uint32(body[88:92]) != 56 ||
		binary.LittleEndian.Uint64(body[112:120]) != 7 || binary.LittleEndian.Uint64(body[120:128]) != 11 ||
		binary.LittleEndian.Uint32(body[168:172]) != 0xc0000073 {
		t.Fatalf("CREATE contexts = %x", body)
	}
}

func TestCreateAndResolverRejectMalformedInputBeforeAuthority(t *testing.T) {
	tree, _, _ := namespaceTree(t)
	connection := &connection{server: tree.export.server}
	if body, status := connection.create(t.Context(), tree, wire.Request{Header: wire.Header{Command: wire.Create}}); body != nil || status != statusInvalid {
		t.Fatalf("malformed CREATE = %x, %#x", body, status)
	}
	if _, err := resolveName(t.Context(), tree, `a\\b`, func(handleReference) {},
		func(context.Context, authz.AccessRequest) error {
			t.Fatal("invalid path reached authorization")
			return nil
		},
		testNamespaceAction(t)); !errors.Is(err, errNameInvalid) {
		t.Fatalf("invalid resolver path = %v", err)
	}
}
