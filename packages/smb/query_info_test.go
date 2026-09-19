package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type queryTestReference struct {
	attr                                 storage.Attr
	state                                storage.ReferenceState
	statErr, stateErr, checkErr          error
	stats, states, checks, loads, closes atomic.Int32
	beforeStat                           func()
	beforeState                          func()
}

func (r *queryTestReference) capture(ctx context.Context, attr storage.Attr) error {
	length, err := storage.MetadataSize(attr.Metadata)
	if err != nil {
		return err
	}
	attr.Metadata = nil
	if err := storage.CheckAttrResultBudget(ctx, attr, int64(length)); err != nil {
		return err
	}
	r.loads.Add(1)
	return nil
}

func (r *queryTestReference) Stat(ctx context.Context) (storage.Attr, error) {
	r.stats.Add(1)
	if r.beforeStat != nil {
		r.beforeStat()
	}
	if r.statErr != nil {
		return r.attr, r.statErr
	}
	if err := r.capture(ctx, r.attr); err != nil {
		return storage.Attr{}, err
	}
	return r.attr.Clone(), nil
}

func (*queryTestReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}

func (r *queryTestReference) Close(context.Context) error { r.closes.Add(1); return nil }
func (r *queryTestReference) CheckReferenceState() error  { r.checks.Add(1); return r.checkErr }
func (r *queryTestReference) State(ctx context.Context) (storage.ReferenceState, error) {
	r.states.Add(1)
	if r.beforeState != nil {
		r.beforeState()
	}
	if r.stateErr != nil {
		return r.state, r.stateErr
	}
	if err := r.capture(ctx, r.state.Attr); err != nil {
		return storage.ReferenceState{}, err
	}
	result := r.state
	result.Attr = result.Attr.Clone()
	return result, nil
}

type queryTestBackend struct {
	storage.FileStorage
	space storage.Space
	err   error
	calls atomic.Int32
}

func (b *queryTestBackend) Space(context.Context) (storage.Space, error) {
	b.calls.Add(1)
	return b.space, b.err
}

type queryTestFixture struct {
	connection *connection
	tree       *tree
	handle     *fileHandle
	id         wire.FileID
	reference  *queryTestReference
	backend    *queryTestBackend
}

func newQueryTestFixture(t *testing.T, granted uint32) queryTestFixture {
	t.Helper()
	registry := handleTestRegistry()
	attr := informationTestAttr(t)
	reference := &queryTestReference{attr: attr, state: storage.ReferenceState{Attr: attr.Clone()}}
	backend := &queryTestBackend{space: storage.Space{Total: 4097, Used: 513, Avail: 1025}}
	registry.tree.export.share = Share{Name: "alias", Volume: "canonical-volume", Backend: backend}
	reservation := handleTestReserve(t, registry)
	reservation.attachNode(storage.NodeOpenResult{Reference: reference, Attr: attr, Outcome: storage.Opened})
	id, err := reservation.install(granted, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := registry.close(context.Background()); err != nil {
			t.Errorf("close query fixture: %v", err)
		}
	})
	return queryTestFixture{connection: &connection{server: registry.tree.export.server}, tree: registry.tree, handle: registry.get(id), id: id, reference: reference, backend: backend}
}

func queryTestRequest(id wire.FileID, kind, class byte, capacity uint32) wire.Request {
	body := make([]byte, 40)
	binary.LittleEndian.PutUint16(body, 41)
	body[2], body[3] = kind, class
	binary.LittleEndian.PutUint32(body[4:], capacity)
	copy(body[24:], id[:])
	packet := requestPacket(wire.Header{Command: wire.QueryInfo}, body)
	return wire.Request{Header: wire.Header{Command: wire.QueryInfo}, Packet: packet, Body: packet[64:]}
}

func queryTestData(t *testing.T, body []byte, length int) []byte {
	t.Helper()
	if len(body) != length+8 || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint16(body[2:]) != 72 || binary.LittleEndian.Uint32(body[4:]) != uint32(length) {
		t.Fatalf("QUERY_INFO envelope = %x, want %d data bytes", body, length)
	}
	return body[8:]
}

func TestQueryInfoSelectsCapturedFactsWithoutUniversalStat(t *testing.T) {
	for _, test := range []struct {
		class         byte
		length        int
		stats, states int32
		granted       uint32
	}{
		{4, 40, 1, 0, fileReadAttributes}, {5, 24, 0, 1, 0},
		{6, 8, 0, 0, 0}, {7, 4, 0, 0, 0}, {8, 4, 0, 0, fileDelete},
		{34, 56, 1, 0, fileReadAttributes}, {35, 8, 1, 0, fileReadAttributes}, {59, 24, 0, 0, 0},
	} {
		t.Run("class_"+strconv.Itoa(int(test.class)), func(t *testing.T) {
			fixture := newQueryTestFixture(t, test.granted)
			if test.stats == 0 {
				fixture.reference.statErr = syscall.EIO
			}
			fixture.reference.state.Attr.Size = 513
			fixture.reference.state.Attr.BirthTime, fixture.reference.state.Attr.ChangeTime = nil, nil
			if test.class == 35 {
				fixture.reference.attr.BirthTime, fixture.reference.attr.ChangeTime = nil, nil
			}
			body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, test.class, uint32(test.length)))
			if status != statusOK {
				t.Fatalf("file class %d status = %x", test.class, status)
			}
			data := queryTestData(t, body, test.length)
			if fixture.reference.stats.Load() != test.stats || fixture.reference.states.Load() != test.states || fixture.backend.calls.Load() != 0 {
				t.Fatalf("class %d observed wrong source: Stat %d, State %d, Space %d", test.class, fixture.reference.stats.Load(), fixture.reference.states.Load(), fixture.backend.calls.Load())
			}
			switch test.class {
			case 4:
				if binary.LittleEndian.Uint64(data) != 116444736000000000 || binary.LittleEndian.Uint32(data[32:]) != dosHidden|dosArchive {
					t.Fatalf("basic capture = %x", data)
				}
			case 5:
				if binary.LittleEndian.Uint64(data) != 1024 || binary.LittleEndian.Uint64(data[8:]) != 513 || binary.LittleEndian.Uint32(data[16:]) != 1 || data[20] != 0 || data[21] != 0 {
					t.Fatalf("state capture = %x", data)
				}
			case 6:
				if binary.LittleEndian.Uint64(data) != fixture.handle.nodeID {
					t.Fatal("internal identity came from another source")
				}
			case 7:
				if binary.LittleEndian.Uint32(data) != 0 {
					t.Fatal("internal metadata became exposed EA data")
				}
			case 8:
				if binary.LittleEndian.Uint32(data) != fileDelete {
					t.Fatal("access query changed the granted mask")
				}
			case 34:
				if binary.LittleEndian.Uint64(data[32:]) != 512 || binary.LittleEndian.Uint64(data[40:]) != 257 {
					t.Fatalf("network size capture = %x", data)
				}
			case 35:
				if binary.LittleEndian.Uint32(data) != dosHidden|dosArchive || binary.LittleEndian.Uint32(data[4:]) != 0 {
					t.Fatalf("attribute tag = %x", data)
				}
			case 59:
				if binary.LittleEndian.Uint64(data) != 0x3893fa5fcb470ad2 || binary.LittleEndian.Uint64(data[8:]) != fixture.handle.nodeID || !bytes.Equal(data[16:], make([]byte, 8)) {
					t.Fatalf("virtual file identity = %x", data)
				}
			}
		})
	}
}

func TestQueryInfoStandardUsesOneAuthoritativeState(t *testing.T) {
	for _, test := range []struct {
		name              string
		detached, pending bool
		directory         bool
	}{
		{name: "linked"}, {name: "pending", pending: true}, {name: "detached", detached: true},
		{name: "detached pending", detached: true, pending: true}, {name: "directory", directory: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newQueryTestFixture(t, 0)
			fixture.reference.statErr = syscall.EIO
			fixture.reference.state.Detached, fixture.reference.state.PendingUnlink = test.detached, test.pending
			if test.directory {
				fixture.reference.state.Attr.Kind, fixture.reference.state.Attr.Size = storage.NodeDirectory, -1
			}
			body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, 5, 24))
			if status != statusOK {
				t.Fatalf("standard query = %x", status)
			}
			data := queryTestData(t, body, 24)
			links, pending := uint32(1), byte(0)
			if test.detached || test.pending {
				links, pending = 0, 1
			}
			if binary.LittleEndian.Uint32(data[16:]) != links || data[20] != pending || (data[21] != 0) != test.directory || fixture.reference.states.Load() != 1 || fixture.reference.stats.Load() != 0 {
				t.Fatalf("standard state projection = %x", data)
			}
			if test.directory && (binary.LittleEndian.Uint64(data) != 0 || binary.LittleEndian.Uint64(data[8:]) != 0) {
				t.Fatalf("directory used unspecified Attr.Size = %x", data)
			}
		})
	}
}

func TestQueryInfoRejectsFixedUndersizeBeforeObservation(t *testing.T) {
	fixture := newQueryTestFixture(t, fileReadAttributes)
	var decisions atomic.Int32
	fixture.connection.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		decisions.Add(1)
		return nil
	})
	for _, test := range []struct {
		kind, class byte
		length      uint32
	}{
		{1, 4, 40}, {1, 5, 24}, {1, 6, 8}, {1, 7, 4}, {1, 8, 4}, {1, 34, 56}, {1, 35, 8}, {1, 59, 24},
		{2, 3, 24}, {2, 4, 8}, {2, 5, 12}, {2, 7, 32},
	} {
		for _, capacity := range []uint32{0, test.length - 1} {
			body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, test.kind, test.class, capacity))
			want := []byte{9, 0, 1, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
			if status != statusQueryLengthMismatch || !bytes.Equal(body, want) {
				t.Fatalf("undersized query %d/%d capacity %d = %x, %x", test.kind, test.class, capacity, body, status)
			}
		}
	}
	for _, test := range []struct{ kind, class byte }{{1, 14}, {1, 16}, {1, 18}, {1, 48}, {2, 1}, {2, 11}, {3, 4}} {
		if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, test.kind, test.class, 1024)); body != nil || status != statusUnsupported {
			t.Fatalf("unsupported query %d/%d = %x, %x", test.kind, test.class, body, status)
		}
	}
	if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(wire.FileID{}, 1, 6, 8)); body != nil || status != statusFileClosed {
		t.Fatalf("missing retained handle = %x, %x", body, status)
	}
	if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, 6, math.MaxUint32)); body != nil || status != statusInvalid {
		t.Fatalf("query above negotiated capacity = %x, %x", body, status)
	}
	if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, wire.Request{}); body != nil || status != statusInvalid {
		t.Fatalf("malformed query = %x, %x", body, status)
	}
	if fixture.reference.stats.Load() != 0 || fixture.reference.states.Load() != 0 || fixture.backend.calls.Load() != 0 || decisions.Load() != 0 {
		t.Fatal("invalid fixed query reached backend observation or host admission")
	}
}

func TestQueryInfoChecksRequestedRightsAndCurrentHostAuthorization(t *testing.T) {
	fixture := newQueryTestFixture(t, fileReadData)
	principal := Principal{SID: "query-principal"}
	ctx := WithPrincipal(t.Context(), principal)
	var decision error
	var requests []authz.AccessRequest
	fixture.connection.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
		got, ok := PrincipalFromContext(ctx)
		if !ok || got != principal || request.Volume != "canonical-volume" || request.Open != (storage.OpenAccess{}) {
			return errors.New("query lost trusted authorization context")
		}
		requests = append(requests, request)
		return decision
	})
	query := queryTestRequest(fixture.id, 1, 4, 40)
	if body, status := fixture.connection.queryHandle(ctx, fixture.tree, query); body != nil || status != statusDenied || len(requests) != 0 {
		t.Fatalf("READ_DATA substituted for READ_ATTRIBUTES: %x, %x", body, status)
	}
	fixture.handle.grantedAccess = fileReadAttributes
	for _, test := range []struct {
		err    error
		status uint32
	}{{authz.ErrDenied, statusDenied}, {errors.New("policy unavailable"), statusIO}, {nil, statusOK}, {authz.ErrDenied, statusDenied}} {
		decision = test.err
		body, status := fixture.connection.queryHandle(ctx, fixture.tree, query)
		if status != test.status || status != statusOK && body != nil {
			t.Fatalf("current policy query = %x, %x; want %x", body, status, test.status)
		}
	}
	if fixture.reference.stats.Load() != 1 || len(requests) != 4 {
		t.Fatalf("authorization decisions were cached: decisions %d, observations %d", len(requests), fixture.reference.stats.Load())
	}
	decision = nil
	for _, test := range []struct {
		kind, class byte
		capacity    uint32
		operation   storage.Operation
	}{
		{1, 5, 24, storage.OpFileState}, {1, 6, 8, storage.OpFileStat}, {1, 7, 4, storage.OpFileStat}, {1, 8, 4, storage.OpFileStat}, {1, 59, 24, storage.OpFileStat},
		{2, 4, 8, storage.OpVolumeSpace}, {2, 5, 28, storage.OpVolumeSpace},
	} {
		if _, status := fixture.connection.queryHandle(ctx, fixture.tree, queryTestRequest(fixture.id, test.kind, test.class, test.capacity)); status != statusOK || requests[len(requests)-1].Operation != test.operation {
			t.Fatalf("query authorization %d/%d = %x, operation %s", test.kind, test.class, status, requests[len(requests)-1].Operation)
		}
	}
}

func TestQueryInfoRejectsUnknownOrContradictoryCapturedFacts(t *testing.T) {
	for _, test := range []struct {
		name   string
		class  byte
		change func(*queryTestFixture)
		status uint32
	}{
		{"wrong identity", 4, func(f *queryTestFixture) { f.reference.attr.ID++ }, statusIO},
		{"invalid kind", 4, func(f *queryTestFixture) { f.reference.attr.Kind = 0 }, statusIO},
		{"negative size", 4, func(f *queryTestFixture) { f.reference.attr.Size = -1 }, statusIO},
		{"unknown birth", 4, func(f *queryTestFixture) { f.reference.attr.BirthTime = nil }, statusUnsupported},
		{"allocation overflow", 34, func(f *queryTestFixture) { f.reference.attr.Size = math.MaxInt64 }, statusIO},
		{"populated Stat with error", 4, func(f *queryTestFixture) {
			f.reference.statErr = errors.Join(storage.ErrConditionConflict, syscall.EIO)
		}, statusIO},
		{"populated State with error", 5, func(f *queryTestFixture) { f.reference.stateErr = syscall.EIO }, statusIO},
		{"State check failure", 5, func(f *queryTestFixture) { f.reference.checkErr = syscall.EIO }, statusIO},
		{"State unsupported", 5, func(f *queryTestFixture) { f.handle.reference = struct{ storage.NodeReference }{f.reference} }, statusUnsupported},
		{"wrong State identity", 5, func(f *queryTestFixture) { f.reference.state.Attr.ID++ }, statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newQueryTestFixture(t, fileReadAttributes)
			test.change(&fixture)
			body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, test.class, 128))
			if body != nil || status != test.status {
				t.Fatalf("bad query capture = %x, %x; want %x", body, status, test.status)
			}
			if (test.name == "State check failure" || test.name == "State unsupported") && fixture.reference.states.Load() != 0 {
				t.Fatal("State ran despite unavailable capability")
			}
		})
	}
}

func queryTestResultCharge(t *testing.T, state bool) int64 {
	t.Helper()
	charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	charge += 512
	if state {
		charge += storage.MaxLinkTargetBytes + storage.MaxObservationTokenBytes
	}
	return charge
}

func TestQueryInfoReservesCapturedResultsBeforeObservation(t *testing.T) {
	for _, class := range []byte{4, 5, 34, 35} {
		t.Run("class_"+strconv.Itoa(int(class)), func(t *testing.T) {
			fixture := newQueryTestFixture(t, fileReadAttributes)
			fixture.reference.attr.Metadata["test.large"] = storage.OpaquePayload{Version: []byte{1}, Data: bytes.Repeat([]byte{7}, storage.MaxMetadataValueBytes)}
			fixture.reference.state.Attr = fixture.reference.attr.Clone()
			charge := queryTestResultCharge(t, class == 5)
			fixture.tree.files.limits.MaxDirectoryBytes = charge - 1
			query := queryTestRequest(fixture.id, 1, class, 128)
			if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, query); body != nil || status != statusResources || fixture.reference.stats.Load() != 0 || fixture.reference.states.Load() != 0 || fixture.reference.loads.Load() != 0 {
				t.Fatalf("depleted result pool reached observation: %x, %x, loads %d", body, status, fixture.reference.loads.Load())
			}
			handleTestAccounting(t, fixture.tree.files, 1, 0, 1, 0)
			fixture.tree.files.limits.MaxDirectoryBytes = charge
			fixture.connection.server.config.Limits.MaxIOBytes = 65536
			fixture.connection.server.config.Limits.MaxFrameBytes = 131072
			checkCharge := func() { handleTestAccounting(t, fixture.tree.files, 1, 0, 1, charge) }
			fixture.reference.beforeStat, fixture.reference.beforeState = checkCharge, checkCharge
			if _, status := fixture.connection.queryHandle(t.Context(), fixture.tree, query); status != statusOK || fixture.reference.loads.Load() != 1 {
				t.Fatalf("valid metadata with exact result capacity = %x, loads %d", status, fixture.reference.loads.Load())
			}
			handleTestAccounting(t, fixture.tree.files, 1, 0, 1, 0)
			callbackCalls := 0
			ctx := storage.WithAttrResultBudget(t.Context(), func(storage.Attr, int64) error { callbackCalls++; return syscall.EFBIG })
			if body, status := fixture.connection.queryHandle(ctx, fixture.tree, query); body != nil || status != statusResources || callbackCalls != 1 || fixture.reference.loads.Load() != 1 {
				t.Fatalf("incoming producer callback was replaced: %x, %x, calls %d", body, status, callbackCalls)
			}
			handleTestAccounting(t, fixture.tree.files, 1, 0, 1, 0)
		})
	}
}

func TestQueryInfoSharesOpenResponseCapacityWithoutChargingIdentityQueries(t *testing.T) {
	fixture := newQueryTestFixture(t, fileReadAttributes)
	registry := fixture.tree.files
	registry.limits.MaxOpens = 2
	registry.limits.MaxDirectoryBytes = queryTestResultCharge(t, false)
	held, err := registry.reserveResponse()
	if err != nil {
		t.Fatal(err)
	}
	held.attachNode(storage.NodeOpenResult{})
	defer func() {
		held.releaseResponse()
		if err := held.finish(context.Background()); err != nil {
			t.Errorf("finish held query-pool reservation: %v", err)
		}
	}()
	charge := held.charge
	if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, 4, 40)); body != nil || status != statusResources || fixture.reference.stats.Load() != 0 {
		t.Fatalf("query borrowed occupied open-response capacity: %x, %x", body, status)
	}
	for _, test := range []struct {
		kind, class byte
		length      uint32
	}{{1, 6, 8}, {1, 7, 4}, {1, 8, 4}, {1, 59, 24}, {2, 3, 24}, {2, 4, 8}, {2, 5, 28}, {2, 7, 32}} {
		if _, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, test.kind, test.class, test.length)); status != statusOK {
			t.Fatalf("non-attribute query %d/%d consumed result capacity: %x", test.kind, test.class, status)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if body, status := fixture.connection.queryHandle(ctx, fixture.tree, queryTestRequest(fixture.id, 1, 4, 40)); body != nil || status != statusCancelled {
		t.Fatalf("canceled query = %x, %x", body, status)
	}
	handleTestAccounting(t, registry, 2, 1, 1, charge)
	held.releaseResponse()
	if err := held.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, 4, 40)); status != statusOK || fixture.reference.stats.Load() != 1 {
		t.Fatalf("released shared capacity remained unavailable: %x", status)
	}
	handleTestAccounting(t, registry, 1, 0, 1, 0)
}

func TestQueryInfoFilesystemUsesMeasuredSpaceAndKnownInterfaceFacts(t *testing.T) {
	for _, test := range []struct {
		class  byte
		length int
		calls  int32
	}{
		{3, 24, 1}, {7, 32, 1}, {4, 8, 0}, {5, 28, 0},
	} {
		t.Run("class_"+strconv.Itoa(int(test.class)), func(t *testing.T) {
			fixture := newQueryTestFixture(t, 0)
			fixture.reference.statErr, fixture.reference.stateErr = syscall.EIO, syscall.EIO
			if test.calls == 0 {
				fixture.backend.err = syscall.ENOSYS
			}
			body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 2, test.class, uint32(test.length)))
			if status != statusOK {
				t.Fatalf("filesystem class %d = %x", test.class, status)
			}
			data := queryTestData(t, body, test.length)
			if fixture.backend.calls.Load() != test.calls || fixture.reference.stats.Load() != 0 || fixture.reference.states.Load() != 0 {
				t.Fatal("filesystem query used another source or repeated Space")
			}
			switch test.class {
			case 3, 7:
				if binary.LittleEndian.Uint64(data) != 8 || binary.LittleEndian.Uint64(data[8:]) != 2 {
					t.Fatalf("measured virtual space = %x", data)
				}
				if test.class == 7 && binary.LittleEndian.Uint64(data[16:]) != 7 {
					t.Fatal("caller availability replaced actual virtual availability")
				}
			case 4:
				if binary.LittleEndian.Uint32(data) != 7 || binary.LittleEndian.Uint32(data[4:]) != 0x10 {
					t.Fatalf("virtual device = %x", data)
				}
			case 5:
				if binary.LittleEndian.Uint32(data) != 0x6 || binary.LittleEndian.Uint32(data[4:]) != 255 || string(data[12:]) != string(wire.EncodeUTF16("REMOTEFS")) {
					t.Fatalf("virtual filesystem attributes = %x", data)
				}
			}
		})
	}
	for _, test := range []struct {
		name   string
		err    error
		space  storage.Space
		status uint32
	}{
		{"unavailable", syscall.ENOSYS, storage.Space{Total: 4096, Avail: 4096}, statusUnsupported},
		{"error with populated space", syscall.EIO, storage.Space{Total: 4096, Avail: 4096}, statusIO},
		{"incoherent space", nil, storage.Space{Total: 4096, Used: 4000, Avail: 4096}, statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newQueryTestFixture(t, 0)
			fixture.backend.space, fixture.backend.err = test.space, test.err
			if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 2, 7, 32)); body != nil || status != test.status || fixture.backend.calls.Load() != 1 {
				t.Fatalf("failed measured query = %x, %x, calls %d", body, status, fixture.backend.calls.Load())
			}
		})
	}
	fixture := newQueryTestFixture(t, 0)
	for _, capacity := range []uint32{12, 13, 15, 27} {
		body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 2, 5, capacity))
		copied := int((capacity - 12) &^ 1)
		if status != statusQueryBufferOverflow {
			t.Fatalf("truncated filesystem name status = %x", status)
		}
		data := queryTestData(t, body, 12+copied)
		if binary.LittleEndian.Uint32(data[8:]) != 16 || !bytes.Equal(data[12:], wire.EncodeUTF16("REMOTEFS")[:copied]) {
			t.Fatalf("truncated filesystem name = %x", data)
		}
	}
}

func TestQueryInfoIgnoresNonApplicableInputFields(t *testing.T) {
	fixture := newQueryTestFixture(t, 0)
	for _, test := range []struct {
		kind, class byte
		capacity    uint32
	}{{1, 6, 8}, {2, 4, 8}} {
		request := queryTestRequest(fixture.id, test.kind, test.class, test.capacity)
		binary.LittleEndian.PutUint16(request.Body[8:], math.MaxUint16)
		binary.LittleEndian.PutUint32(request.Body[12:], 99)
		binary.LittleEndian.PutUint32(request.Body[16:], math.MaxUint32)
		binary.LittleEndian.PutUint32(request.Body[20:], math.MaxUint32)
		if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, request); status != statusOK || len(body) != 16 {
			t.Fatalf("ignored query input %d/%d = %x, %x", test.kind, test.class, body, status)
		}
	}
	if fixture.reference.stats.Load() != 0 || fixture.reference.states.Load() != 0 || fixture.backend.calls.Load() != 0 {
		t.Fatal("ignored input fields changed the information source")
	}
}

func TestQueryInfoBorrowKeepsResponseWorkOwnedDuringClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newQueryTestFixture(t, fileReadAttributes)
		entered, resume := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var workers sync.WaitGroup
		defer func() { release(); workers.Wait() }()
		fixture.reference.beforeStat = func() { close(entered); <-resume }
		type response struct {
			body   []byte
			status uint32
		}
		queried := make(chan response, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, 4, 40))
			queried <- response{body, status}
		}()
		select {
		case <-entered:
		case result := <-queried:
			t.Fatalf("query returned before its observation: %x", result.status)
		}
		closed := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			closed <- fixture.tree.files.closeID(t.Context(), fixture.id, fixture.handle)
		}()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("close released an active query: %v", err)
		default:
		}
		if body, status := fixture.connection.queryHandle(t.Context(), fixture.tree, queryTestRequest(fixture.id, 1, 6, 8)); body != nil || status != statusFileClosed {
			t.Fatalf("retiring query handle accepted another query: %x, %x", body, status)
		}
		handleTestAccounting(t, fixture.tree.files, 1, 0, 1, queryTestResultCharge(t, false))
		release()
		result := <-queried
		if result.status != statusOK {
			t.Fatalf("admitted query did not complete: %x", result.status)
		}
		queryTestData(t, result.body, 40)
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
	})
}
