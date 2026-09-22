package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type queryInfoReference struct {
	attr                        storage.Attr
	state                       storage.ReferenceState
	statErr, stateErr, checkErr error
	stats, states, checks       atomic.Int32
}

func (r *queryInfoReference) Stat(context.Context) (storage.Attr, error) {
	r.stats.Add(1)
	return r.attr.Clone(), r.statErr
}
func (*queryInfoReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*queryInfoReference) Close(context.Context) error { return nil }
func (*queryInfoReference) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	return storage.ReferenceCloseResult{Released: true}, nil
}
func (*queryInfoReference) CheckScopedReference() error { return nil }
func (*queryInfoReference) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "query"}, nil
}
func (r *queryInfoReference) CheckReferenceState() error {
	r.checks.Add(1)
	return r.checkErr
}
func (r *queryInfoReference) State(context.Context) (storage.ReferenceState, error) {
	r.states.Add(1)
	result := r.state
	result.Attr = result.Attr.Clone()
	return result, r.stateErr
}

type queryInfoBackend struct {
	storage.FileStorage
	space storage.Space
	err   error
	calls atomic.Int32
}

func (b *queryInfoBackend) Space(context.Context) (storage.Space, error) {
	b.calls.Add(1)
	return b.space, b.err
}

type queryInfoFixture struct {
	connection *connection
	tree       *tree
	handle     *fileHandle
	id         wire.FileID
	reference  *queryInfoReference
	backend    *queryInfoBackend
}

func newQueryInfoFixture(t *testing.T, granted uint32) queryInfoFixture {
	t.Helper()
	registry := newHandleTestRegistry(t, 2)
	attr := informationTestAttr(t)
	reference := &queryInfoReference{attr: attr, state: storage.ReferenceState{Attr: attr.Clone()}}
	backend := &queryInfoBackend{space: storage.Space{Total: 4097, Used: 513, Avail: 1025}}
	registry.tree.export.share = endpointShare("alias", "canonical-volume", backend)
	reservation, err := registry.reserve()
	if err != nil {
		t.Fatal(err)
	}
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
	return queryInfoFixture{
		connection: &connection{server: registry.tree.export.server}, tree: registry.tree,
		handle: registry.get(id), id: id, reference: reference, backend: backend,
	}
}

func queryInfoRequest(id wire.FileID, kind, class byte, capacity uint32) wire.Request {
	body := make([]byte, 40)
	binary.LittleEndian.PutUint16(body, 41)
	body[2], body[3] = kind, class
	binary.LittleEndian.PutUint32(body[4:], capacity)
	copy(body[24:], id[:])
	packet := make([]byte, wire.HeaderSize+len(body))
	copy(packet[wire.HeaderSize:], body)
	return wire.Request{Header: wire.Header{Command: wire.QueryInfo}, Packet: packet, Body: packet[wire.HeaderSize:]}
}

func queryInfoData(t *testing.T, body []byte, length int) []byte {
	t.Helper()
	if len(body) != length+8 || binary.LittleEndian.Uint16(body) != 9 ||
		binary.LittleEndian.Uint16(body[2:]) != wire.HeaderSize+8 || binary.LittleEndian.Uint32(body[4:]) != uint32(length) {
		t.Fatalf("QUERY_INFO envelope = %x", body)
	}
	return body[8:]
}

func TestQueryInfoUsesOnlyTheFactsRequiredByEachFileClass(t *testing.T) {
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
		t.Run(strconv.Itoa(int(test.class)), func(t *testing.T) {
			fixture := newQueryInfoFixture(t, test.granted)
			fixture.reference.state.Attr.Size = 513
			fixture.reference.state.Attr.BirthTime = nil
			fixture.reference.state.Attr.ChangeTime = nil
			body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 1, test.class, uint32(test.length)))
			if status != statusOK {
				t.Fatalf("file class %d status = %#x", test.class, status)
			}
			data := queryInfoData(t, body, test.length)
			if fixture.reference.stats.Load() != test.stats || fixture.reference.states.Load() != test.states || fixture.backend.calls.Load() != 0 {
				t.Fatalf("sources: Stat=%d State=%d Space=%d", fixture.reference.stats.Load(), fixture.reference.states.Load(), fixture.backend.calls.Load())
			}
			switch test.class {
			case 4:
				if binary.LittleEndian.Uint32(data[32:]) != dosHidden|dosArchive {
					t.Fatalf("basic information = %x", data)
				}
			case 5:
				if binary.LittleEndian.Uint64(data) != 1024 || binary.LittleEndian.Uint64(data[8:]) != 513 || binary.LittleEndian.Uint32(data[16:]) != 1 {
					t.Fatalf("standard information = %x", data)
				}
			case 6:
				if binary.LittleEndian.Uint64(data) != fixture.handle.nodeID {
					t.Fatalf("internal information = %x", data)
				}
			case 7:
				if !bytes.Equal(data, make([]byte, 4)) {
					t.Fatalf("EA information = %x", data)
				}
			case 8:
				if binary.LittleEndian.Uint32(data) != fileDelete {
					t.Fatalf("access information = %x", data)
				}
			case 59:
				if binary.LittleEndian.Uint64(data) != virtualVolumeSerial("canonical-volume") || binary.LittleEndian.Uint64(data[8:]) != fixture.handle.nodeID {
					t.Fatalf("file ID information = %x", data)
				}
			}
		})
	}
}

func TestQueryInfoStandardUsesOneAuthoritativeState(t *testing.T) {
	for _, test := range []struct {
		name              string
		detached, pending bool
		links             uint32
	}{
		{"linked", false, false, 1},
		{"pending", false, true, 0},
		{"detached", true, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newQueryInfoFixture(t, 0)
			fixture.reference.state.Detached = test.detached
			fixture.reference.state.PendingUnlink = test.pending
			body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 1, 5, 24))
			if status != statusOK {
				t.Fatalf("standard query status = %#x", status)
			}
			data := queryInfoData(t, body, 24)
			if binary.LittleEndian.Uint32(data[16:]) != test.links || (data[20] != 0) != (test.detached || test.pending) || fixture.reference.states.Load() != 1 {
				t.Fatalf("standard state = %x", data)
			}
		})
	}
}

func TestQueryInfoRejectsUnsupportedAndUndersizedRequestsBeforeObservation(t *testing.T) {
	fixture := newQueryInfoFixture(t, fileReadAttributes)
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
		body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, test.kind, test.class, test.length-1))
		if status != statusInfoLengthMismatch || len(body) != 16 || body[2] != 1 {
			t.Fatalf("undersized %d/%d = %x, %#x", test.kind, test.class, body, status)
		}
	}
	for _, test := range []struct{ kind, class byte }{{1, 9}, {1, 14}, {1, 18}, {2, 1}, {3, 4}} {
		body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, test.kind, test.class, 1024))
		if body != nil || status != statusUnsupported {
			t.Fatalf("unsupported %d/%d = %x, %#x", test.kind, test.class, body, status)
		}
	}
	body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 1, 6, math.MaxUint32))
	if body != nil || status != statusInvalid {
		t.Fatalf("oversized output = %x, %#x", body, status)
	}
	if fixture.reference.stats.Load() != 0 || fixture.reference.states.Load() != 0 || fixture.backend.calls.Load() != 0 || decisions.Load() != 0 {
		t.Fatal("invalid query reached authorization or backend observation")
	}
}

func TestQueryInfoChecksGrantedAndCurrentHostAuthorization(t *testing.T) {
	fixture := newQueryInfoFixture(t, fileReadData)
	request := queryInfoRequest(fixture.id, 1, 4, 40)
	if body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, request); body != nil || status != statusDenied {
		t.Fatalf("READ_DATA substituted for READ_ATTRIBUTES: %x, %#x", body, status)
	}
	fixture.handle.grantedAccess = fileReadAttributes
	decision := authz.ErrDenied
	var operations []storage.Operation
	fixture.connection.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		operations = append(operations, request.Operation)
		return decision
	})
	if body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, request); body != nil || status != statusDenied {
		t.Fatalf("denied query = %x, %#x", body, status)
	}
	decision = errors.New("policy unavailable")
	if body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, request); body != nil || status != statusIO {
		t.Fatalf("unknown authorization = %x, %#x", body, status)
	}
	decision = nil
	if _, status := fixture.connection.queryInfo(t.Context(), fixture.tree, request); status != statusOK {
		t.Fatalf("allowed query status = %#x", status)
	}
	if !reflectOperations(operations, []storage.Operation{storage.OpFileStat, storage.OpFileStat, storage.OpFileStat}) || fixture.reference.stats.Load() != 1 {
		t.Fatalf("operations=%v stats=%d", operations, fixture.reference.stats.Load())
	}
}

func reflectOperations(got, want []storage.Operation) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func TestQueryInfoFilesystemUsesMeasuredAndStaticFacts(t *testing.T) {
	for _, test := range []struct {
		class  byte
		length int
		calls  int32
	}{
		{3, 24, 1}, {7, 32, 1}, {4, 8, 0}, {5, 28, 0},
	} {
		t.Run(strconv.Itoa(int(test.class)), func(t *testing.T) {
			fixture := newQueryInfoFixture(t, 0)
			body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 2, test.class, uint32(test.length)))
			if status != statusOK {
				t.Fatalf("filesystem class %d status = %#x", test.class, status)
			}
			data := queryInfoData(t, body, test.length)
			if fixture.backend.calls.Load() != test.calls {
				t.Fatalf("Space calls = %d", fixture.backend.calls.Load())
			}
			if test.class == 3 || test.class == 7 {
				if binary.LittleEndian.Uint64(data) != 8 || binary.LittleEndian.Uint64(data[8:]) != 2 {
					t.Fatalf("space information = %x", data)
				}
			}
		})
	}
	fixture := newQueryInfoFixture(t, 0)
	for _, capacity := range []uint32{12, 13, 15, 27} {
		body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 2, 5, capacity))
		if status != statusBufferOverflow {
			t.Fatalf("filesystem name capacity %d status = %#x", capacity, status)
		}
		data := queryInfoData(t, body, 12+int((capacity-12)&^1))
		if binary.LittleEndian.Uint32(data[8:]) != uint32(len(wire.EncodeUTF16("REMOTEFS"))) {
			t.Fatalf("filesystem name = %x", data)
		}
	}
}

func TestQueryInfoCancellationAndResultAdmissionPrecedeBackend(t *testing.T) {
	fixture := newQueryInfoFixture(t, fileReadAttributes)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if body, status := fixture.connection.queryInfo(ctx, fixture.tree, queryInfoRequest(fixture.id, 1, 4, 40)); body != nil || status != statusCancelled {
		t.Fatalf("canceled query = %x, %#x", body, status)
	}
	fixture.tree.files.limits.MaxOpenResultBytes = maxOpenResultCharge() - 1
	if body, status := fixture.connection.queryInfo(t.Context(), fixture.tree, queryInfoRequest(fixture.id, 1, 4, 40)); body != nil || status != statusResources {
		t.Fatalf("unbudgeted query = %x, %#x", body, status)
	}
	if fixture.reference.stats.Load() != 0 || fixture.reference.states.Load() != 0 {
		t.Fatal("canceled or unbudgeted query reached the backend")
	}
}
