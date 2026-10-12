package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type volumeInfoSession struct {
	*infoAllocationSession
	created      time.Time
	createdErr   error
	createdCalls int
}

func (s *volumeInfoSession) VolumeCreatedAt(context.Context) (time.Time, error) {
	s.createdCalls++
	return s.created, s.createdErr
}

type volumeInfoBackend struct {
	*endpointStorage
	space     storage.Space
	err       error
	calls     int
	principal Principal
	after     func()
}

func (b *volumeInfoBackend) Space(ctx context.Context) (storage.Space, error) {
	b.calls++
	b.principal, _ = PrincipalFromContext(ctx)
	if b.after != nil {
		b.after()
	}
	if err := ctx.Err(); err != nil {
		return storage.Space{}, err
	}
	return b.space, b.err
}

type volumeInfoNode struct {
	storage.NodeReference
	probe *infoFileProbe
}

func (n *volumeInfoNode) ReferenceNodeID() (uint64, error) { return n.probe.ReferenceNodeID() }
func (*volumeInfoNode) CheckReferenceState() error         { return nil }
func (n *volumeInfoNode) State(ctx context.Context, p storage.StateProjection) (storage.ReferenceState, error) {
	return n.probe.State(ctx, p)
}

type volumeCommandFixture struct {
	c       *connection
	s       *session
	tree    *tree
	h       *fileHandle
	file    *infoFileProbe
	native  *volumeInfoSession
	backend *volumeInfoBackend
}

func newVolumeCommandFixture(t *testing.T) volumeCommandFixture {
	t.Helper()
	file := validInfoFileProbe()
	c, s, tree, h := newInfoCommandFixture(t, file, 0)
	native := &volumeInfoSession{infoAllocationSession: tree.authority.raw.(*infoAllocationSession), created: time.Unix(1700000000, 100)}
	backend := &volumeInfoBackend{endpointStorage: &endpointStorage{}, space: storage.Space{Total: 16384, Used: 4096, Avail: 4096}}
	tree.authority.raw = native
	tree.export.share.Backend = backend
	label := "volume-😀"
	tree.export.share.VolumeLabel = &label
	serial, err := stableVolumeSerial(h.identity.Backend.Volume)
	if err != nil {
		t.Fatal(err)
	}
	tree.export.volumeSerial = serial
	s.principal = testPrincipal("0123456789abcdef")
	return volumeCommandFixture{c, s, tree, h, file, native, backend}
}

func volumeInfoRequestForTest(id wire.FileID, class byte, length uint32) wire.Request {
	request := queryInfoRequestForTest(id, class, length)
	request.Body[2] = wire.InfoFilesystem
	return request
}

func (f volumeCommandFixture) query(ctx context.Context, class byte, length uint32) ([]byte, uint32) {
	return f.c.queryInfo(ctx, f.s, f.tree, volumeInfoRequestForTest(f.h.id, class, length))
}

func TestVolumeInformationZeroAccessReferencesAndFacts(t *testing.T) {
	for _, node := range []bool{false, true} {
		for _, class := range []byte{1, 3, 4, 5, 7} {
			t.Run(fmt.Sprintf("node-%t/class-%d", node, class), func(t *testing.T) {
				f := newVolumeCommandFixture(t)
				if node {
					f.file.info.Attr.Kind, f.h.nodeKind = storage.NodeDirectory, storage.NodeDirectory
					f.h.node, f.h.file = &volumeInfoNode{probe: f.file}, nil
				}
				body, status := f.query(t.Context(), class, 128)
				if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != uint32(len(body)-8) || f.file.infos.Load() != 1 || f.file.stats.Load() != 0 {
					t.Fatalf("query status=%#x body=%x state/stat=%d/%d", status, body, f.file.infos.Load(), f.file.stats.Load())
				}
				data := body[8:]
				switch class {
				case 1:
					created, _ := toFiletime(f.native.created)
					if binary.LittleEndian.Uint64(data) != created || binary.LittleEndian.Uint32(data[8:]) != uint32(f.tree.export.volumeSerial) || data[16] != 0 || data[17] != 0 {
						t.Fatalf("Volume facts=%x", data)
					}
				case 3:
					if binary.LittleEndian.Uint64(data) != 4 || binary.LittleEndian.Uint64(data[8:]) != 1 || binary.LittleEndian.Uint32(data[16:]) != 1 || binary.LittleEndian.Uint32(data[20:]) != 4096 {
						t.Fatalf("Size facts=%x", data)
					}
				case 4:
					if binary.LittleEndian.Uint32(data) != 7 || binary.LittleEndian.Uint32(data[4:]) != 0x10 {
						t.Fatalf("Device facts=%x", data)
					}
				case 5:
					if binary.LittleEndian.Uint32(data) != 6 || binary.LittleEndian.Uint32(data[4:]) != maxNameUnits || binary.LittleEndian.Uint32(data[8:]) != 2*uint32(len(volumeFileSystemName)) {
						t.Fatalf("Attribute facts=%x", data)
					}
				case 7:
					if binary.LittleEndian.Uint64(data) != 4 || binary.LittleEndian.Uint64(data[8:]) != 1 || binary.LittleEndian.Uint64(data[16:]) != 3 || binary.LittleEndian.Uint32(data[24:]) != 1 || binary.LittleEndian.Uint32(data[28:]) != 4096 {
						t.Fatalf("FullSize facts=%x", data)
					}
				}
				if f.backend.calls != 0 && !f.backend.principal.SameIdentity(f.s.principal) {
					t.Fatal("Space lost the current session principal")
				}
			})
		}
	}
}

func TestVolumeInformationPartialRepliesKeepFullLengthsAndRawPrefix(t *testing.T) {
	for _, class := range []byte{1, 5} {
		f := newVolumeCommandFixture(t)
		label := "aa😀" + strings.Repeat("a", 28)
		f.tree.export.share.VolumeLabel = &label
		full, status := f.query(t.Context(), class, 128)
		if status != statusOK {
			t.Fatalf("full class %d status=%#x", class, status)
		}
		minimum, _ := volumeInfoLengths(class)
		for length := minimum; length <= len(full)-8; length++ {
			body, status := f.query(t.Context(), class, uint32(length))
			wantStatus := statusBufferOverflow
			if length == len(full)-8 {
				wantStatus = statusOK
			}
			if status != wantStatus || !bytes.Equal(body[8:], full[8:8+length]) || binary.LittleEndian.Uint32(body[4:]) != uint32(length) {
				t.Fatalf("class %d length %d status=%#x body=%x; full=%x", class, length, status, body, full)
			}
		}
	}
	f := newVolumeCommandFixture(t)
	empty := ""
	f.tree.export.share.VolumeLabel = &empty
	if body, status := f.query(t.Context(), 1, 24); status != statusOK || len(body) != 26 || binary.LittleEndian.Uint32(body[20:]) != 0 {
		t.Fatalf("known empty label status=%#x body=%x", status, body)
	}
}

func TestVolumeInformationRejectsShortAndUnsupportedBeforeAuthorityWork(t *testing.T) {
	for _, class := range []byte{1, 3, 4, 5, 7} {
		minimum, _ := volumeInfoLengths(class)
		for _, length := range []uint32{0, uint32(minimum - 1)} {
			f := newVolumeCommandFixture(t)
			body, status := f.query(t.Context(), class, length)
			if status != statusInfoLengthMismatch || !bytes.Equal(body, wire.InfoLengthMismatchErrorBody()) || f.file.infos.Load() != 0 || f.native.createdCalls != 0 || f.native.units != 0 || f.backend.calls != 0 {
				t.Fatalf("short class %d length %d status=%#x body=%x", class, length, status, body)
			}
		}
	}
	for _, class := range []byte{0, 2, 6, 8, 9, 10, 11, 12, 255} {
		f := newVolumeCommandFixture(t)
		want := statusUnsupported
		if class == 0 || class > 11 {
			want = statusInvalidInfoClass
		}
		if body, status := f.query(t.Context(), class, 128); status != want || body != nil || f.file.infos.Load() != 0 {
			t.Fatalf("unsupported class %d status=%#x body=%x", class, status, body)
		}
	}
}

func TestVolumeInformationCapacityReadsCurrentIndependentValues(t *testing.T) {
	f := newVolumeCommandFixture(t)
	for _, space := range []storage.Space{
		{Total: 16384, Used: 4096, Avail: 4096},
		{Total: 24576, Used: 8192, Avail: 8192},
	} {
		f.backend.space = space
		body, status := f.query(t.Context(), 7, 32)
		if status != statusOK || binary.LittleEndian.Uint64(body[8:]) != uint64(space.Total/4096) ||
			binary.LittleEndian.Uint64(body[16:]) != uint64(space.Avail/4096) || binary.LittleEndian.Uint64(body[24:]) != uint64((space.Total-space.Used)/4096) {
			t.Fatalf("current capacity %+v status=%#x body=%x", space, status, body)
		}
	}
	if f.backend.calls != 2 {
		t.Fatalf("current Space captures=%d", f.backend.calls)
	}
	f.backend.space = storage.Space{Total: 16384, Used: 1, Avail: 4096}
	if _, status := f.query(t.Context(), 3, 24); status != statusOK {
		t.Fatalf("Size rejected unencoded Used: %#x", status)
	}
	if _, status := f.query(t.Context(), 7, 32); status != statusIO {
		t.Fatalf("FullSize accepted inexact actual available: %#x", status)
	}
}

func TestVolumeInformationUnavailableAndInvalidCapacityIsClassLocal(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*volumeCommandFixture)
		want  uint32
	}{
		{"zero-unit", func(f *volumeCommandFixture) { f.native.unit = 0 }, statusIO},
		{"unit-overflow", func(f *volumeCommandFixture) { f.native.unit = math.MaxUint32 + 1 }, statusIO},
		{"missing-unit", func(f *volumeCommandFixture) { f.native.err = syscall.EOPNOTSUPP }, statusIO},
		{"unit-source-failure", func(f *volumeCommandFixture) { f.native.err = syscall.ENOENT }, statusIO},
		{"allocation-check-failure", func(f *volumeCommandFixture) { f.native.checkErr = syscall.EIO }, statusIO},
		{"missing-capacity", func(f *volumeCommandFixture) { f.backend.err = syscall.ENOSYS }, statusIO},
		{"capacity-denied", func(f *volumeCommandFixture) { f.backend.err = authz.ErrDenied }, statusDenied},
		{"negative-total", func(f *volumeCommandFixture) { f.backend.space.Total = -1 }, statusIO},
		{"negative-used", func(f *volumeCommandFixture) { f.backend.space.Used = -1 }, statusIO},
		{"negative-available", func(f *volumeCommandFixture) { f.backend.space.Avail = -1 }, statusIO},
		{"incoherent-available", func(f *volumeCommandFixture) { f.backend.space.Avail = f.backend.space.Total }, statusIO},
		{"overquota", func(f *volumeCommandFixture) { f.backend.space = storage.Space{Total: 4096, Used: 8192} }, statusIO},
		{"inexact-total", func(f *volumeCommandFixture) { f.backend.space.Total++ }, statusIO},
		{"inexact-available", func(f *volumeCommandFixture) { f.backend.space.Avail++ }, statusIO},
	} {
		for _, class := range []byte{3, 7} {
			t.Run(fmt.Sprintf("%s/class-%d", test.name, class), func(t *testing.T) {
				f := newVolumeCommandFixture(t)
				test.setup(&f)
				if body, status := f.query(t.Context(), class, 32); status != test.want || body != nil {
					t.Fatalf("capacity status=%#x body=%x", status, body)
				}
				if _, status := f.query(t.Context(), 4, 8); status != statusOK {
					t.Fatalf("capacity failure fenced Device: %#x", status)
				}
			})
		}
	}
}

func TestVolumeInformationCreationFailuresAndMissingLabelAreLocal(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*volumeCommandFixture)
		want  uint32
	}{
		{"missing-label", func(f *volumeCommandFixture) { f.tree.export.share.VolumeLabel = nil }, statusUnsupported},
		{"missing-created", func(f *volumeCommandFixture) { f.native.createdErr = syscall.EOPNOTSUPP }, statusUnsupported},
		{"missing-getter", func(f *volumeCommandFixture) { f.tree.authority.raw = f.native.infoAllocationSession }, statusUnsupported},
		{"creation-failed", func(f *volumeCommandFixture) { f.native.createdErr = syscall.EIO }, statusIO},
		{"creation-denied", func(f *volumeCommandFixture) { f.native.createdErr = authz.ErrDenied }, statusDenied},
		{"creation-before-filetime", func(f *volumeCommandFixture) { f.native.created = time.Unix(-filetimeUnixOffset-1, 0) }, statusIO},
		{"creation-overflow", func(f *volumeCommandFixture) {
			f.native.created = time.Unix(math.MaxInt64/10000000-filetimeUnixOffset+1, 0)
		}, statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newVolumeCommandFixture(t)
			test.setup(&f)
			if body, status := f.query(t.Context(), 1, 128); status != test.want || body != nil {
				t.Fatalf("Volume status=%#x body=%x", status, body)
			}
			if _, status := f.query(t.Context(), 3, 24); status != statusOK {
				t.Fatalf("creation failure fenced Size: %#x", status)
			}
		})
	}
}

func TestVolumeInformationRechecksPolicyScopeAndCurrentAuthority(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*volumeCommandFixture)
		want  uint32
	}{
		{"state-denied", func(f *volumeCommandFixture) {
			f.c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
		}, statusDenied},
		{"space-denied", func(f *volumeCommandFixture) {
			f.c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
				if r.Operation == storage.OpVolumeSpace {
					return authz.ErrDenied
				}
				return nil
			})
		}, statusDenied},
		{"space-revoked-after", func(f *volumeCommandFixture) {
			f.backend.after = func() {
				f.c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
			}
		}, statusDenied},
		{"identity-after", func(f *volumeCommandFixture) {
			f.backend.after = func() { f.h.identity.SessionEpoch = "changed" }
		}, statusSessionDeleted},
		{"state-failed", func(f *volumeCommandFixture) { f.file.err = syscall.EIO }, statusIO},
		{"wrong-object", func(f *volumeCommandFixture) { f.file.info.Attr.ID++ }, statusIO},
		{"wrong-kind", func(f *volumeCommandFixture) { f.file.info.Attr.Kind = storage.NodeDirectory }, statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newVolumeCommandFixture(t)
			test.setup(&f)
			if body, status := f.query(t.Context(), 3, 24); status != test.want || body != nil {
				t.Fatalf("live query status=%#x body=%x", status, body)
			}
		})
	}
	f := newVolumeCommandFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	f.backend.after = cancel
	if body, status := f.query(ctx, 3, 24); status != statusCancelled || body != nil {
		t.Fatalf("canceled capacity status=%#x body=%x", status, body)
	}
}

func TestPublishVolumeLabelDistinguishesUnknownAndCopiesKnownValues(t *testing.T) {
	for _, label := range []string{"", "数据😀", strings.Repeat("a", 32), strings.Repeat("😀", 16)} {
		server, err := New(endpointConfig())
		if err != nil {
			t.Fatal(err)
		}
		value := label
		export, err := server.Publish(Share{Name: "Data", Volume: "host-volume", BackendVolume: "test-volume", RootNodeID: 1, Backend: &endpointStorage{}, VolumeLabel: &value})
		if err != nil {
			t.Fatalf("publish label %q: %v", label, err)
		}
		value = "changed"
		if export.share.VolumeLabel == nil || *export.share.VolumeLabel != label {
			t.Fatalf("published label changed: %v", export.share.VolumeLabel)
		}
		if err := export.Unpublish(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	for _, label := range []string{string([]byte{0xff}), "a\x00b", strings.Repeat("a", 33), strings.Repeat("😀", 17), strings.Repeat("a", 129)} {
		server, err := New(endpointConfig())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := server.Publish(Share{Name: "Data", Volume: "host-volume", BackendVolume: "test-volume", RootNodeID: 1, Backend: &endpointStorage{}, VolumeLabel: &label}); err != ErrConfig || len(server.exports) != 0 {
			t.Fatalf("invalid label %q admitted: %v", label, err)
		}
	}
}

func TestVolumeInformationResponseBudgetCoversCompleteBoundedReply(t *testing.T) {
	for _, class := range []byte{1, 3, 4, 5, 7} {
		for _, length := range []uint32{0, 8, 12, 23, 24, 25, 32, 82, math.MaxUint32} {
			request := volumeInfoRequestForTest(wire.FileID{1}, class, length)
			_, maximum := volumeInfoLengths(class)
			want := max(80, (72+min(int(length), maximum)+7)&^7)
			if got := responseBudget(request); got != want || got > 160 {
				t.Fatalf("class %d length %d response budget=%d want=%d", class, length, got, want)
			}
		}
	}
	if got := responseBudget(volumeInfoRequestForTest(wire.FileID{1}, 1, 82)); got != 160 {
		t.Fatalf("maximum label response budget=%d", got)
	}
	f := newVolumeCommandFixture(t)
	if body, status := f.query(t.Context(), 3, uint32(f.c.server.config.Limits.MaxIOBytes+1)); status != statusInvalid || body != nil || f.file.infos.Load() != 0 {
		t.Fatalf("oversized declaration status=%#x body=%x", status, body)
	}
}

func TestVolumeInformationRelatedWarningAllowsOnlyNormalFilesystemReply(t *testing.T) {
	request := volumeInfoRequestForTest(wire.FileID{1}, 1, 24)
	body, err := wire.QueryInfoResponseBody(make([]byte, 24))
	if err != nil {
		t.Fatal(err)
	}
	if relatedOperationFailed(request, statusBufferOverflow, body) {
		t.Fatal("normal partial volume reply suppresses following related CLOSE")
	}
	for _, test := range []struct {
		request wire.Request
		status  uint32
		body    []byte
	}{
		{request, statusIO, body},
		{request, statusBufferOverflow, wire.ErrorResponseBody()},
		{request, statusBufferOverflow, body[:7]},
		{queryInfoRequestForTest(wire.FileID{1}, wire.FileBasicInformation, 40), statusBufferOverflow, body},
		{wire.Request{Header: wire.Header{Command: wire.Read}}, statusBufferOverflow, body},
	} {
		if !relatedOperationFailed(test.request, test.status, test.body) {
			t.Fatalf("non-volume-partial failure continued: command=%d status=%#x body=%x", test.request.Header.Command, test.status, test.body)
		}
	}
	if relatedOperationFailed(request, statusOK, body) {
		t.Fatal("success suppressed following operation")
	}
}
