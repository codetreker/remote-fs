package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func createTestName(t *testing.T, present bool, kind storage.NodeKind) resolvedName {
	t.Helper()
	name := resolvedName{RootID: 1, Target: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("item")},
		Condition: storage.ChildCondition{State: storage.Absent}, Guards: storage.NamespaceGuards{RootID: 1, Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1}}}}}
	if present {
		attr := informationTestAttr(t)
		attr.Kind = kind
		attr.Metadata = nil
		name.Attr = &attr
		name.Condition = storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}
		name.Guards.Edges = []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte("item"), ChildID: attr.ID}}
	}
	return name
}

func createTestRequest() wire.CreateRequest {
	return wire.CreateRequest{Name: "item", Disposition: 1, DesiredAccess: fileReadData | fileReadAttributes, ShareAccess: 7, Impersonation: 2}
}

func TestCreateDispositionPlansKeepAtomicIdentityAndBranches(t *testing.T) {
	for _, present := range []bool{false, true} {
		for disposition := uint32(0); disposition <= 5; disposition++ {
			request := createTestRequest()
			request.Disposition = disposition
			request.DesiredAccess |= fileWriteData | fileDelete
			name := createTestName(t, present, storage.NodeRegular)
			plan, err := buildCreatePlan(request, name)
			var want error
			if present && disposition == 0 {
				want = syscall.EOPNOTSUPP
			}
			if present && disposition == 2 {
				want = syscall.EEXIST
			}
			if !present && (disposition == 1 || disposition == 4) {
				want = syscall.ENOENT
			}
			if want != nil {
				if !errors.Is(err, want) {
					t.Fatalf("present=%v disp=%d: %v", present, disposition, err)
				}
				continue
			}
			if err != nil || plan.file == nil {
				t.Fatalf("present=%v disp=%d: %+v %v", present, disposition, plan, err)
			}
			wantOutcome := storage.Opened
			if !present {
				wantOutcome = storage.Created
			} else if disposition == 4 || disposition == 5 {
				wantOutcome = storage.Reset
			}
			if plan.outcome != wantOutcome || plan.file.Create != !present || !reflect.DeepEqual(plan.file.Guards, &name.Guards) || plan.file.Target != name.Condition {
				t.Fatalf("lost atomic selection: %+v", plan)
			}
			if present {
				version, ok := plan.file.ExpectedMetadata[windowsMetadataKey]
				if !ok || len(version) != 0 {
					t.Fatal("observed absence was not a metadata condition")
				}
			}
			if wantOutcome == storage.Opened && !plan.file.Initial.OnCreate.Empty() {
				t.Fatal("existing open adopted requested creation metadata")
			}
		}
	}
}

func TestCreateAttributesAreConditionalAndPreserveReadonlyAdmission(t *testing.T) {
	name := createTestName(t, true, storage.NodeRegular)
	name.Attr.Metadata = metadataTestValue(t, windowsMetadata{Attributes: dosHidden | dosSystem})
	request := createTestRequest()
	request.Disposition = 4
	request.DesiredAccess |= fileWriteData
	if _, err := buildCreatePlan(request, name); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("omitted hidden/system: %v", err)
	}
	request.Attributes = dosHidden | dosSystem
	plan, err := buildCreatePlan(request, name)
	if err != nil {
		t.Fatal(err)
	}
	value := plan.file.Initial.OnReset.Metadata[windowsMetadataKey]
	decoded, err := decodeWindowsMetadata(map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{7}, Data: value}})
	if err != nil || decoded.Attributes != dosHidden|dosSystem|dosArchive {
		t.Fatalf("reset attributes: %+v %v", decoded, err)
	}
	if !reflect.DeepEqual(plan.file.ExpectedMetadata[windowsMetadataKey], name.Attr.Metadata[windowsMetadataKey].Version) {
		t.Fatal("lost namespace predicate")
	}
	name.Attr.Metadata[windowsMetadataKey].Version[0] ^= 0xff
	if reflect.DeepEqual(plan.file.ExpectedMetadata[windowsMetadataKey], name.Attr.Metadata[windowsMetadataKey].Version) {
		t.Fatal("predicate aliases observation")
	}
	name.Attr.Metadata = metadataTestValue(t, windowsMetadata{Attributes: dosReadOnly})
	if _, err := buildCreatePlan(request, name); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("readonly overwrite: %v", err)
	}
	request.Disposition = 1
	request.DesiredAccess = fileReadAttributes | fileDelete
	request.Options = createDeleteOnClose
	if _, err := buildCreatePlan(request, name); createStatus(err) != 0xc0000121 {
		t.Fatalf("readonly delete intent: %v", err)
	}
	request.Disposition = 2
	request.Options = 0
	request.Attributes = dosReadOnly
	request.DesiredAccess = fileReadData | fileWriteData
	plan, err = buildCreatePlan(request, createTestName(t, false, storage.NodeRegular))
	if err != nil || plan.file == nil {
		t.Fatalf("initial readonly creation handle: %v", err)
	}
	request.Options = createDeleteOnClose
	request.DesiredAccess |= fileDelete
	if _, err := buildCreatePlan(request, createTestName(t, false, storage.NodeRegular)); createStatus(err) != 0xc0000121 {
		t.Fatalf("readonly initial delete intent: %v", err)
	}
}

func TestCreateDirectoryClaimsAndPrivateMetadataDoNotUpgradeWindowsRights(t *testing.T) {
	request := createTestRequest()
	request.DesiredAccess = 0x20 | 0x2 | 0x4 | 0x40 | fileDelete
	request.ShareAccess = 0
	name := createTestName(t, true, storage.NodeDirectory)
	plan, err := buildCreatePlan(request, name)
	if err != nil || plan.node == nil {
		t.Fatalf("directory plan: %+v %v", plan, err)
	}
	if plan.access != request.DesiredAccess || plan.access&fileReadAttributes != 0 || plan.node.MetadataAccess != storage.ReadMetadata {
		t.Fatal("private metadata changed Windows rights")
	}
	if plan.node.Use.Uses != storage.ReadData|storage.WriteData|storage.DeleteName || plan.node.Use.Deny != storage.AllUses {
		t.Fatalf("explicit directory categories: %+v", plan.node.Use)
	}
	request.DesiredAccess = fileReadData
	plan, err = buildCreatePlan(request, name)
	if err != nil || plan.node.Use.Uses != storage.ReadEntries {
		t.Fatalf("enumeration category: %+v %v", plan, err)
	}
	request.DesiredAccess = 0x40
	plan, err = buildCreatePlan(request, name)
	if err != nil || plan.node.Use.Uses != 0 {
		t.Fatalf("DELETE_CHILD entered sharing mask: %+v %v", plan, err)
	}
	request.DesiredAccess = fileReadAttributes | fileWriteAttributes | fileDelete
	request.Options = createDeleteOnClose
	plan, err = buildCreatePlan(request, name)
	if err != nil || plan.node.MetadataAccess != storage.ReadMetadata|storage.WriteMetadata || plan.node.CloseIntent.Condition != storage.UnlinkIfEmpty {
		t.Fatalf("directory intent: %+v %v", plan, err)
	}
	if !reflect.DeepEqual(plan.node.CloseIntent.Guards, &name.Guards) {
		t.Fatal("delete intent lost guards")
	}
	name.Root = true
	name.RootID = name.Attr.ID
	name.DirectoryRequired = true
	name.Target = storage.ChildName{}
	request.Options = 0
	request.Disposition = 3
	plan, err = buildCreatePlan(request, name)
	if err != nil || plan.node.Create {
		t.Fatalf("root open: %+v %v", plan, err)
	}
	request.Options = createDeleteOnClose
	if _, err := buildCreatePlan(request, name); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("root delete: %v", err)
	}
}

func TestCreateKnownSemanticRefusalsPrecedeNativeEffect(t *testing.T) {
	request := createTestRequest()
	for _, test := range []struct {
		name   string
		change func(*wire.CreateRequest)
		want   error
	}{
		{"maximum allowed", func(r *wire.CreateRequest) { r.DesiredAccess = 0x2000000 }, syscall.EOPNOTSUPP},
		{"unknown disposition", func(r *wire.CreateRequest) { r.Disposition = 6 }, syscall.EINVAL},
		{"unknown share", func(r *wire.CreateRequest) { r.ShareAccess = 8 }, syscall.EINVAL},
		{"impersonation", func(r *wire.CreateRequest) { r.Impersonation = 4 }, syscall.EINVAL},
		{"oplock", func(r *wire.CreateRequest) { r.OplockLevel = 3 }, syscall.EINVAL},
		{"unbuffered", func(r *wire.CreateRequest) { r.Options = 8 }, syscall.EOPNOTSUPP},
		{"contradictory kind", func(r *wire.CreateRequest) { r.Options = createDirectory | createNonDirectory }, syscall.EINVAL},
		{"delete rights", func(r *wire.CreateRequest) { r.Options = createDeleteOnClose }, syscall.EACCES},
		{"undefined attributes", func(r *wire.CreateRequest) { r.Attributes = 0x80000000 }, syscall.EINVAL},
		{"reconnect", func(r *wire.CreateRequest) { r.Contexts = []wire.CreateContext{{Name: []byte("DH2C")}} }, syscall.EOPNOTSUPP},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := request
			test.change(&r)
			called := false
			c := &connection{server: &Server{config: testConfig()}}
			_, err := c.createOpen(t.Context(), nil, r, func(context.Context, storage.FileStorage, storage.FileSession, string, Limits, func(context.Context, storage.Operation) error) (resolvedName, error) {
				called = true
				return resolvedName{}, nil
			})
			if !errors.Is(err, test.want) || called {
				t.Fatalf("semantic refusal=%v resolverCalled=%v", err, called)
			}
		})
	}
	ignored := request
	ignored.Options = 0x10 | 0x20 | 0x100 | 0x400 | 0x10000 | 0x20000 | 0x800000 | 0x4 | 0x800
	ignored.SecurityFlags = 0xff
	ignored.Attributes = 0x800 | 0x100 | dosDirectory | dosNormal | dosHidden
	if _, err := validateCreateRequest(ignored); err != nil {
		t.Fatalf("ignored options/security flags: %v", err)
	}
	for _, level := range []byte{0, 1, 8, 9, 0xff} {
		r := request
		r.OplockLevel = level
		r.Contexts = []wire.CreateContext{{Name: []byte("RqLs"), Data: []byte{1}}, {Name: []byte("DH2Q")}, {Name: []byte("QFid")}, {Name: []byte("MxAc")}, {Name: []byte("AlSi")}}
		if _, err := validateCreateRequest(r); err != nil {
			t.Fatalf("declined optional cache requests: %v", err)
		}
	}
	if got, err := expandCreateAccess(0xe0000000); err != nil || got != 0x1201bf {
		t.Fatalf("generic access: %x %v", got, err)
	}
	if _, err := expandCreateAccess(0x10000000); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unimplemented security grants: %v", err)
	}
}

func TestCreateSelectedObjectAndHistoricalFactsAreRequired(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*wire.CreateRequest, *resolvedName)
		want   error
	}{
		{"inconsistent absence", func(_ *wire.CreateRequest, n *resolvedName) { n.Condition.State = storage.Absent }, syscall.EIO},
		{"wrong identity", func(_ *wire.CreateRequest, n *resolvedName) { n.Condition.NodeID++ }, syscall.EIO},
		{"symlink", func(_ *wire.CreateRequest, n *resolvedName) { n.Attr.Kind = storage.NodeSymlink }, syscall.EOPNOTSUPP},
		{"directory required", func(r *wire.CreateRequest, _ *resolvedName) { r.Options = createDirectory }, syscall.ENOTDIR},
		{"not directory", func(r *wire.CreateRequest, n *resolvedName) {
			r.Options = createNonDirectory
			n.Attr.Kind = storage.NodeDirectory
		}, syscall.EISDIR},
		{"directory overwrite", func(r *wire.CreateRequest, n *resolvedName) { r.Disposition = 4; n.Attr.Kind = storage.NodeDirectory }, syscall.EINVAL},
		{"metadata overwrite", func(r *wire.CreateRequest, _ *resolvedName) { r.Disposition = 4; r.DesiredAccess = fileReadAttributes }, syscall.EOPNOTSUPP},
		{"missing history", func(_ *wire.CreateRequest, n *resolvedName) { n.Attr.BirthTime = nil }, syscall.EOPNOTSUPP},
		{"negative size", func(_ *wire.CreateRequest, n *resolvedName) { n.Attr.Size = -1 }, syscall.EIO},
		{"bad metadata", func(_ *wire.CreateRequest, n *resolvedName) {
			n.Attr.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}}}
		}, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := createTestRequest()
			n := createTestName(t, true, storage.NodeRegular)
			test.change(&r, &n)
			if _, err := buildCreatePlan(r, n); !errors.Is(err, test.want) {
				t.Fatalf("plan: %v", err)
			}
		})
	}
	r := createTestRequest()
	r.Disposition = 2
	r.Options = createDirectory
	plan, err := buildCreatePlan(r, createTestName(t, false, storage.NodeDirectory))
	if err != nil || plan.node == nil || plan.node.Kind != storage.NodeDirectory {
		t.Fatalf("directory create: %+v %v", plan, err)
	}
}

func TestCreateAuthorizationCarriesBackendIntentAndExactOperations(t *testing.T) {
	registry := handleTestRegistry()
	tr := registry.tree
	c := &connection{server: tr.export.server}
	tr.export.share.Volume = "trusted"
	for _, test := range []struct{ root, data, reset, intent bool }{{}, {root: true}, {data: true}, {data: true, reset: true}, {intent: true}} {
		r := createTestRequest()
		r.DesiredAccess = fileDelete
		if test.data {
			r.DesiredAccess |= fileWriteData
		}
		if test.reset {
			r.Disposition = 4
		}
		if test.intent {
			r.Options = createDeleteOnClose
		}
		n := createTestName(t, true, storage.NodeRegular)
		if test.root {
			n.Attr.Kind = storage.NodeDirectory
			n.Root = true
			n.RootID = n.Attr.ID
		}
		plan, err := buildCreatePlan(r, n)
		if err != nil {
			t.Fatal(err)
		}
		var calls []authz.AccessRequest
		c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error { calls = append(calls, request); return nil })
		if err := c.authorizeCreate(t.Context(), tr, plan); err != nil {
			t.Fatal(err)
		}
		want := storage.OpFileOpenChildRef
		if test.root {
			want = storage.OpFileOpenNodeRef
		}
		if test.data {
			want = storage.OpFileOpenAt
		}
		if calls[0].Operation != want || calls[0].Volume != "trusted" || calls[0].Open.Write != test.data || calls[0].Open.Read == test.data {
			t.Fatalf("intent: %+v", calls)
		}
		if test.intent && calls[len(calls)-1].Operation != storage.OpFileSetPendingUnlink {
			t.Fatalf("missing intent policy: %+v", calls)
		}
		for deny := range calls {
			index := 0
			c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
				current := index
				index++
				if current == deny {
					return authz.ErrDenied
				}
				return nil
			})
			if err := c.authorizeCreate(t.Context(), tr, plan); !errors.Is(err, authz.ErrDenied) || index != deny+1 {
				t.Fatalf("denial%d: %v calls%d", deny, err, index)
			}
		}
	}
}

func TestCreateWireErrorsAndResponseBudget(t *testing.T) {
	diagnostic := createFailure(statusIO, syscall.EACCES)
	if diagnostic.Error() != syscall.EACCES.Error() || !errors.Is(diagnostic, syscall.EACCES) {
		t.Fatal("CREATE status wrapper lost its original diagnostic")
	}
	if got := createStatus(errors.Join(storage.ErrUseConflict, syscall.EIO)); got != statusIO {
		t.Fatalf("unknown failure became a sharing refusal: %x", got)
	}
	c := &connection{server: &Server{config: testConfig()}}
	if _, status := c.create(t.Context(), nil, sessionRequest(wire.Create, []byte{1})); status != statusInvalid {
		t.Fatalf("malformed=%x", status)
	}
	body := make([]byte, 56)
	binary.LittleEndian.PutUint16(body, 57)
	binary.LittleEndian.PutUint32(body[40:], 8)
	if _, status := c.create(t.Context(), nil, sessionRequest(wire.Create, body)); status != statusUnsupported {
		t.Fatalf("decoded unsupported=%x", status)
	}
	if got := responseBudget(wire.Request{Header: wire.Header{Command: wire.Create}}); got < 64+len(wire.CreateResponseBody(wire.CreateResult{})) {
		t.Fatalf("CREATE pre-effect budget=%d", got)
	}
	for _, test := range []struct {
		err    error
		status uint32
	}{{nil, 0}, {storage.ErrUseConflict, 0xc0000043}, {storage.ErrPendingDelete, 0xc0000056}, {syscall.EEXIST, 0xc0000035}, {syscall.EISDIR, 0xc00000ba}, {syscall.EACCES, statusDenied}, {createFailure(statusIO, syscall.EACCES), statusIO}} {
		if got := createStatus(test.err); got != test.status {
			t.Fatalf("%v: %x", test.err, got)
		}
	}
}

func TestCreateContextsRespectRequiredRepliesAndIgnoreRules(t *testing.T) {
	for _, contexts := range [][]wire.CreateContext{
		{{Name: []byte("DHnQ")}},
		{{Name: []byte("DH2Q")}, {Name: []byte("RqLs"), Data: []byte{1}}},
		{{Name: []byte("AlSi"), Data: []byte{1}}},
		{{Name: []byte("\x93\xad\x25\x50\x9c\xb4\x11\xe7\xb4\x23\x83\xde\x96\x8b\xcd\x7c")}},
	} {
		if err := validateCreateContexts(contexts); err != nil {
			t.Fatal(err)
		}
	}
	for _, contexts := range [][]wire.CreateContext{
		{{Name: []byte("DH2Q")}, {Name: []byte("DHnQ")}},
		{{Name: []byte("DH2Q")}, {Name: []byte("DHnC")}},
		{{Name: []byte("DH2Q")}, {Name: []byte("DH2C")}},
		{{Name: []byte("QFid"), Data: []byte{1}}},
		{{Name: []byte("MxAc"), Data: []byte{1}}},
	} {
		if err := validateCreateContexts(contexts); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("context validation: %v", err)
		}
	}
	if err := validateCreateContexts([]wire.CreateContext{{Name: []byte("future")}}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unknown context: %v", err)
	}
	many := make([]wire.CreateContext, 1000)
	for i := range many {
		many[i].Name = []byte("QFid")
	}
	if err := validateCreateContexts(many); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("response contexts escaped pre-effect frame bound: %v", err)
	}

	stamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(stamp, 13)
	contexts := []wire.CreateContext{{Name: []byte("RqLs")}, {Name: []byte("QFid")}, {Name: []byte("MxAc")}, {Name: []byte("MxAc"), Data: stamp}}
	body := createResponseContexts(wire.CreateResponseBody(wire.CreateResult{}), contexts, 123, 456, 13)
	if binary.LittleEndian.Uint32(body[80:]) != 152 || int(binary.LittleEndian.Uint32(body[84:])) != len(body)-88 {
		t.Fatal("response context envelope")
	}
	first := body[88:]
	if string(first[16:20]) != "QFid" || binary.LittleEndian.Uint32(first) != 56 || binary.LittleEndian.Uint16(first[10:]) != 24 || binary.LittleEndian.Uint32(first[12:]) != 32 || binary.LittleEndian.Uint64(first[24:]) != 123 || binary.LittleEndian.Uint64(first[32:]) != 456 {
		t.Fatalf("identity response: %x", first)
	}
	second := first[56:]
	if string(second[16:20]) != "MxAc" || binary.LittleEndian.Uint32(second[24:]) != statusUnsupported || binary.LittleEndian.Uint32(second[28:]) != 0 {
		t.Fatal("maximal access fabricated success")
	}
	third := second[32:]
	if binary.LittleEndian.Uint32(third) != 0 || binary.LittleEndian.Uint32(third[24:]) != 0xc0000073 || binary.LittleEndian.Uint32(third[28:]) != 0 {
		t.Fatal("matching change timestamp did not produce NONE_MAPPED")
	}
	plain := createResponseContexts(wire.CreateResponseBody(wire.CreateResult{}), []wire.CreateContext{{Name: []byte("AlSi")}}, 123, 456, 13)
	if len(plain) != 88 || binary.LittleEndian.Uint32(plain[80:]) != 0 {
		t.Fatal("ignored allocation produced reservation response")
	}
	for _, name := range []string{"ExtA", "SecD"} {
		r := createTestRequest()
		r.Contexts = []wire.CreateContext{{Name: []byte(name), Data: []byte{1}}}
		if _, err := buildCreatePlan(r, createTestName(t, true, storage.NodeRegular)); err != nil {
			t.Fatalf("existing open must ignore%s: %v", name, err)
		}
		r.Disposition = 2
		_, err := buildCreatePlan(r, createTestName(t, false, storage.NodeRegular))
		want := uint32(statusUnsupported)
		if name == "ExtA" {
			want = 0xc000004f
		}
		if createStatus(err) != want {
			t.Fatalf("new %s: %x", name, createStatus(err))
		}
	}
	if createStatus(errPathMissing) != 0xc000003a || createStatus(errNameInvalid) != 0xc0000033 || createStatus(namespaceFailure(syscall.ENOENT)) != statusIO {
		t.Fatal("namespace phase proof was lost")
	}
}

func TestCreateOnlyGuardedFinalAbsenceReturnsNoSuchFile(t *testing.T) {
	for _, disposition := range []uint32{1, 4} {
		request := createTestRequest()
		request.Disposition = disposition
		_, err := buildCreatePlan(request, createTestName(t, false, storage.NodeRegular))
		if !errors.Is(err, syscall.ENOENT) || createStatus(err) != 0xc000000f {
			t.Fatalf("guarded final absence disposition%d: %x %v", disposition, createStatus(err), err)
		}
	}
	if createStatus(errPathMissing) != 0xc000003a {
		t.Fatal("intermediate absence lost its phase")
	}
	if createStatus(namespaceFailure(syscall.ENOENT)) != statusIO {
		t.Fatal("backend absence was treated as proof")
	}
}
