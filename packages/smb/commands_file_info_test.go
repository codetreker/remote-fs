package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func queryInfoRequestForTest(id wire.FileID, class byte, length uint32) wire.Request {
	packet := make([]byte, 64+41)
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 41)
	body[2], body[3] = wire.InfoFile, class
	binary.LittleEndian.PutUint32(body[4:], length)
	copy(body[24:40], id[:])
	return wire.Request{Header: wire.Header{Command: wire.QueryInfo}, Body: body, Packet: packet}
}

func setInfoRequestForTest(id wire.FileID, class byte, data []byte) wire.Request {
	packet := make([]byte, 64+max(33, 32+len(data)))
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 33)
	body[2], body[3] = wire.InfoFile, class
	binary.LittleEndian.PutUint32(body[4:], uint32(len(data)))
	binary.LittleEndian.PutUint16(body[8:], 96)
	copy(body[16:32], id[:])
	copy(body[32:], data)
	return wire.Request{Header: wire.Header{Command: wire.SetInfo}, Body: body, Packet: packet}
}

func eofInfoForTest(size int64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(size))
	return data
}

type infoFileProbe struct {
	handleFileStub
	attr         storage.Attr
	info         storage.ReferenceState
	err          error
	stats, infos atomic.Int32
}

func (*infoFileProbe) ReferenceNodeID() (uint64, error) { return 7, nil }
func (*infoFileProbe) CheckReferenceState() error       { return nil }
func (f *infoFileProbe) State(_ context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	if projection != storage.StateScalars {
		return storage.ReferenceState{}, syscall.EACCES
	}
	f.infos.Add(1)
	return f.info, f.err
}
func (f *infoFileProbe) Stat(context.Context) (storage.Attr, error) {
	f.stats.Add(1)
	return f.attr, f.err
}

type infoAllocationSession struct {
	*endpointFileSession
	unit          uint64
	checkErr, err error
	units         int
	afterUnit     func()
}

func (s *infoAllocationSession) CheckAllocationReporting() error { return s.checkErr }
func (s *infoAllocationSession) AllocationUnitBytes(context.Context) (uint64, error) {
	s.units++
	if s.afterUnit != nil {
		s.afterUnit()
	}
	return s.unit, s.err
}

func newInfoCommandFixture(t *testing.T, file *infoFileProbe, access uint32) (*connection, *session, *tree, *fileHandle) {
	t.Helper()
	c, s, tree, h := newIOCommandFixture(t, &ioFileProbe{}, access)
	identity := storage.FileSessionIdentityResult{Backend: endpointBackendIdentity(), SessionEpoch: "endpoint"}
	tree.authority.identity, h.identity = identity, identity
	tree.authority.raw = &infoAllocationSession{endpointFileSession: newEndpointFileSession(), unit: 4096}
	h.file, h.nodeKind = file, storage.NodeRegular
	return c, s, tree, h
}

func validInfoFileProbe() *infoFileProbe {
	return &infoFileProbe{
		attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096,
			AccessTime: time.Unix(1700000000, 0), ModTime: time.Unix(1700000001, 0)},
		info: storage.ReferenceState{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096}},
	}
}

func TestFileInformationRightsAndShortBuffers(t *testing.T) {
	for _, class := range []struct {
		class      byte
		size       uint32
		attributes bool
	}{
		{wire.FileBasicInformation, 40, true}, {wire.FileStandardInformation, 24, false},
		{wire.FileInternalInformation, 8, false}, {wire.FileNetworkOpenInformation, 56, true}, {wire.FileIdInformation, 24, false},
	} {
		for _, access := range []uint32{0, accessReadAttr} {
			t.Run(fmt.Sprintf("class-%d/access-%x", class.class, access), func(t *testing.T) {
				file := validInfoFileProbe()
				c, s, tree, h := newInfoCommandFixture(t, file, access)
				body, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, class.class, class.size))
				if class.attributes && access == 0 {
					if status != statusDenied || file.stats.Load()+file.infos.Load() != 0 {
						t.Fatalf("rights status=%#x calls=%d/%d", status, file.stats.Load(), file.infos.Load())
					}
				} else if status != statusOK || len(body) != int(class.size)+8 || binary.LittleEndian.Uint32(body[4:8]) != class.size {
					t.Fatalf("query status=%#x body=%x", status, body)
				}
			})
		}
		for _, length := range []uint32{0, class.size - 1} {
			t.Run(fmt.Sprintf("class-%d/length-%d", class.class, length), func(t *testing.T) {
				file := validInfoFileProbe()
				c, s, tree, h := newInfoCommandFixture(t, file, accessReadAttr)
				body, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, class.class, length))
				if status != 0xc0000004 || len(body) != 16 || binary.LittleEndian.Uint16(body) != 9 || body[2] != 1 || binary.LittleEndian.Uint32(body[4:8]) != 8 {
					t.Fatalf("short buffer status=%#x body=%x", status, body)
				}
				if file.stats.Load()+file.infos.Load() != 0 {
					t.Fatalf("short buffer dispatched backing: %d/%d", file.stats.Load(), file.infos.Load())
				}
			})
		}
	}
}

func TestFileInformationRejectsContradictoryFacts(t *testing.T) {
	for _, test := range []struct {
		name   string
		class  byte
		change func(*infoFileProbe, *tree)
	}{
		{"scalar-wrong-object", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.ID++ }},
		{"scalar-leaks-metadata", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.Metadata = map[string]storage.OpaquePayload{} }},
		{"scalar-leaks-times", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.ModTime = time.Now() }},
		{"scalar-leaks-link", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.info.LinkTarget = []byte("target") }},
		{"scalar-leaks-generation", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.info.PendingGeneration = []byte("private") }},
		{"scalar-wrong-kind", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.Kind = storage.NodeDirectory }},
		{"scalar-negative-eof", wire.FileStandardInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.Size = -1 }},
		{"scalar-unknown-allocation", wire.FileStandardInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.AllocationKnown = false; f.info.Attr.AllocationSize = 0 }},
		{"scalar-misaligned-allocation", wire.FileStandardInformation, func(f *infoFileProbe, _ *tree) { f.info.Attr.AllocationSize = 4095 }},
		{"attr-wrong-object", wire.FileBasicInformation, func(f *infoFileProbe, _ *tree) { f.attr.ID++ }},
		{"attr-wrong-kind", wire.FileBasicInformation, func(f *infoFileProbe, _ *tree) { f.attr.Kind = storage.NodeDirectory }},
		{"attr-negative-eof", wire.FileNetworkOpenInformation, func(f *infoFileProbe, _ *tree) { f.attr.Size = -1 }},
		{"attr-unknown-allocation", wire.FileNetworkOpenInformation, func(f *infoFileProbe, _ *tree) { f.attr.AllocationKnown = false; f.attr.AllocationSize = 0 }},
		{"attr-misaligned-allocation", wire.FileNetworkOpenInformation, func(f *infoFileProbe, _ *tree) { f.attr.AllocationSize = 1 }},
		{"malformed-windows-metadata", wire.FileBasicInformation, func(f *infoFileProbe, _ *tree) {
			f.attr.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("1"), Data: []byte("bad")}}
		}},
		{"time-before-epoch", wire.FileBasicInformation, func(f *infoFileProbe, _ *tree) { f.attr.ModTime = time.Unix(-filetimeUnixOffset-1, 0) }},
		{"allocation-zero-unit", wire.FileNetworkOpenInformation, func(_ *infoFileProbe, tree *tree) {
			tree.authority.raw.(*infoAllocationSession).unit = 0
		}},
		{"allocation-error-with-data", wire.FileStandardInformation, func(_ *infoFileProbe, tree *tree) {
			tree.authority.raw.(*infoAllocationSession).err = syscall.EIO
		}},
		{"backing-error-with-data", wire.FileInternalInformation, func(f *infoFileProbe, _ *tree) { f.err = syscall.EIO }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := validInfoFileProbe()
			c, s, tree, h := newInfoCommandFixture(t, f, accessReadAttr)
			test.change(f, tree)
			body, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, test.class, 56))
			if status != statusIO || len(body) != 0 {
				t.Fatalf("invalid facts status=%#x body=%x", status, body)
			}
		})
	}
}

func TestFileInformationMissingAllocationUnitIsClassLocal(t *testing.T) {
	f := validInfoFileProbe()
	c, s, tree, h := newInfoCommandFixture(t, f, accessReadAttr)
	tree.authority.raw.(*infoAllocationSession).checkErr = syscall.EOPNOTSUPP
	for _, class := range []byte{wire.FileBasicInformation, wire.FileInternalInformation, wire.FileIdInformation} {
		if _, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, class, 56)); status != statusOK {
			t.Fatalf("allocation-independent class %d status=%#x", class, status)
		}
	}
	for _, class := range []byte{wire.FileStandardInformation, wire.FileNetworkOpenInformation} {
		if _, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, class, 56)); status != statusUnsupported {
			t.Fatalf("allocation-dependent class %d status=%#x", class, status)
		}
	}
}

func TestFileInfoMalformedAndUnsupportedRequestsHaveNoBackingEffects(t *testing.T) {
	for _, test := range []struct {
		name    string
		request func(wire.FileID) wire.Request
		status  uint32
	}{
		{"query-unknown-class", func(id wire.FileID) wire.Request { return queryInfoRequestForTest(id, 0xff, 56) }, statusUnsupported},
		{"query-volume", func(id wire.FileID) wire.Request {
			r := queryInfoRequestForTest(id, wire.FileBasicInformation, 40)
			r.Body[2] = wire.InfoFilesystem
			return r
		}, statusUnsupported},
		{"set-short-basic", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, make([]byte, 39))
		}, statusInvalid},
		{"set-long-eof", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileEndOfFileInformation, make([]byte, 9))
		}, statusInvalid},
		{"set-negative-eof", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileEndOfFileInformation, eofInfoForTest(-1))
		}, statusInvalid},
		{"set-change-time", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{ChangeTime: 1}))
		}, statusUnsupported},
		{"set-negative-time", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{CreationTime: -3}))
		}, statusInvalid},
		{"set-suppress-time", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{LastAccessTime: -1}))
		}, statusUnsupported},
		{"set-restore-time", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{LastWriteTime: -2}))
		}, statusUnsupported},
		{"set-mixed-normal", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosNormal | dosHidden}))
		}, statusInvalid},
		{"set-directory-on-file", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosDirectory}))
		}, statusInvalid},
		{"set-unsupported-attribute", func(id wire.FileID) wire.Request {
			return setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: math.MaxUint32 &^ dosDirectory}))
		}, statusUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := validInfoFileProbe()
			c, s, tree, h := newInfoCommandFixture(t, f, accessReadAttr|accessWriteAttr|accessWriteData)
			r := test.request(h.id)
			var body []byte
			var status uint32
			if r.Header.Command == wire.QueryInfo {
				body, status = c.queryFileInfo(t.Context(), s, tree, r)
			} else {
				body, status = c.setFileInfo(t.Context(), s, tree, r)
			}
			if status != test.status || len(body) != 0 || f.stats.Load()+f.infos.Load() != 0 {
				t.Fatalf("rejected request status=%#x want=%#x body=%x calls=%d/%d", status, test.status, body, f.stats.Load(), f.infos.Load())
			}
		})
	}
}

func TestFileInfoDoesNotHideJoinedBackingFailure(t *testing.T) {
	f := validInfoFileProbe()
	f.err = errors.Join(syscall.ENOSYS, syscall.EIO)
	c, s, tree, h := newInfoCommandFixture(t, f, 0)
	if body, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, wire.FileInternalInformation, 8)); status != statusIO || len(body) != 0 {
		t.Fatalf("joined backing failure status=%#x body=%x", status, body)
	}
}

func TestFileInformationIgnoresNonconsumedQueryFields(t *testing.T) {
	f := validInfoFileProbe()
	c, s, tree, h := newInfoCommandFixture(t, f, accessReadAttr)
	request := queryInfoRequestForTest(h.id, wire.FileBasicInformation, 40)
	binary.LittleEndian.PutUint16(request.Body[8:], math.MaxUint16)
	binary.LittleEndian.PutUint32(request.Body[12:], math.MaxUint32)
	binary.LittleEndian.PutUint32(request.Body[16:], math.MaxUint32)
	binary.LittleEndian.PutUint32(request.Body[20:], math.MaxUint32)
	if body, status := c.queryFileInfo(t.Context(), s, tree, request); status != statusOK || len(body) != 48 || f.stats.Load() != 1 {
		t.Fatalf("ignored query fields status=%#x body=%x calls=%d", status, body, f.stats.Load())
	}
}

func TestFileInfoCreditsAndResponseBudgetsUseDeclaredBufferLengths(t *testing.T) {
	for _, input := range []uint32{0, 65536, 65537, 1 << 20} {
		for _, output := range []uint32{0, 40, 65536, 65537, 1 << 20} {
			request := queryInfoRequestForTest(wire.FileID{1}, wire.FileBasicInformation, output)
			binary.LittleEndian.PutUint32(request.Body[12:], input)
			want := max(1, int((uint64(max(input, output))+65535)/65536))
			if got := requiredCredits(request); got != want {
				t.Fatalf("QUERY buffers %d/%d credits=%d want=%d", input, output, got, want)
			}
			if got := responseBudget(request); got != max(80, (72+int(output)+7)&^7) {
				t.Fatalf("QUERY output=%d response budget=%d", output, got)
			}
		}
	}
	if got := responseBudget(setInfoRequestForTest(wire.FileID{1}, wire.FileBasicInformation, make([]byte, 40))); got != 80 {
		t.Fatalf("SET budget=%d", got)
	}
	if got := responseBudget(wire.Request{Header: wire.Header{Command: wire.QueryInfo}}); got != 80 {
		t.Fatalf("malformed QUERY error budget=%d", got)
	}
}

func TestFileInformationDirectorySizeSentinelDoesNotEnterEOFProjection(t *testing.T) {
	f := validInfoFileProbe()
	f.attr.Kind, f.attr.Size, f.attr.AllocationSize = storage.NodeDirectory, -1, 0
	c, s, tree, h := newInfoCommandFixture(t, f, accessReadAttr)
	h.nodeKind = storage.NodeDirectory
	for _, class := range []byte{wire.FileBasicInformation, wire.FileNetworkOpenInformation} {
		body, status := c.queryFileInfo(t.Context(), s, tree, queryInfoRequestForTest(h.id, class, 56))
		if status != statusOK {
			t.Fatalf("directory class %d status=%#x", class, status)
		}
		if class == wire.FileNetworkOpenInformation && binary.LittleEndian.Uint64(body[48:]) != 0 {
			t.Fatalf("directory sentinel projected as EOF: %x", body)
		}
	}
}

func TestFileInformationAllocationUnitChecksOwningSessionAndCurrentPolicy(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(context.CancelFunc, *connection, *tree, *infoAllocationSession)
		want  error
		calls int
	}{
		{"substituted-before", func(_ context.CancelFunc, _ *connection, _ *tree, reporter *infoAllocationSession) {
			reporter.status.Epoch = "substituted"
		}, syscall.ESTALE, 0},
		{"substituted-after", func(_ context.CancelFunc, _ *connection, _ *tree, reporter *infoAllocationSession) {
			reporter.afterUnit = func() { reporter.status.Epoch = "substituted" }
		}, syscall.ESTALE, 1},
		{"retired-after", func(_ context.CancelFunc, _ *connection, tree *tree, reporter *infoAllocationSession) {
			reporter.afterUnit = func() { tree.authority.closed = true }
		}, syscall.ESTALE, 1},
		{"canceled-after", func(cancel context.CancelFunc, _ *connection, _ *tree, reporter *infoAllocationSession) {
			reporter.afterUnit = cancel
		}, context.Canceled, 1},
		{"denied-before", func(_ context.CancelFunc, c *connection, _ *tree, _ *infoAllocationSession) {
			c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				if request.Operation == storage.OpFileAllocationUnit {
					return authz.ErrDenied
				}
				return nil
			})
		}, authz.ErrDenied, 0},
		{"revoked-after", func(_ context.CancelFunc, c *connection, _ *tree, reporter *infoAllocationSession) {
			reporter.afterUnit = func() {
				c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
					if request.Operation == storage.OpFileAllocationUnit {
						return authz.ErrDenied
					}
					return nil
				})
			}
		}, authz.ErrDenied, 1},
		{"checker-error", func(_ context.CancelFunc, _ *connection, _ *tree, reporter *infoAllocationSession) {
			reporter.checkErr = syscall.EIO
		}, syscall.EIO, 0},
		{"unit-error", func(_ context.CancelFunc, _ *connection, _ *tree, reporter *infoAllocationSession) {
			reporter.err = syscall.EIO
		}, syscall.EIO, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, s, tree, h := newInfoCommandFixture(t, validInfoFileProbe(), accessReadAttr)
			reporter := tree.authority.raw.(*infoAllocationSession)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			test.setup(cancel, c, tree, reporter)
			err := c.checkInformationAllocation(ctx, s, tree, h, true, 4096)
			if !errors.Is(err, test.want) || reporter.units != test.calls {
				t.Fatalf("allocation unit err=%v calls=%d; want %v/%d", err, reporter.units, test.want, test.calls)
			}
		})
	}
}
