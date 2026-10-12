//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	"net/http/httptest"
)

func volumeQueryForTest(id wire.FileID, class byte, length uint32) wire.Request {
	request := queryInfoRequestForTest(id, class, length)
	request.Body[2] = wire.InfoFilesystem
	return request
}

func queryVolumeForTest(t *testing.T, f realSMBFixture, id wire.FileID, class byte, length uint32, wantStatus uint32) []byte {
	t.Helper()
	body, status := f.connection.queryInfo(t.Context(), f.session, f.tree, volumeQueryForTest(id, class, length))
	if status != wantStatus {
		t.Fatalf("volume class=%d length=%d status=%#x want=%#x body=%x", class, length, status, wantStatus, body)
	}
	if status != statusOK && status != statusBufferOverflow {
		return nil
	}
	if len(body) < 8 || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint16(body[2:]) != 72 || binary.LittleEndian.Uint32(body[4:]) != uint32(len(body)-8) {
		t.Fatalf("volume QUERY_INFO envelope=%x", body)
	}
	return body[8:]
}

func enrollVolumeFactsForTest(t *testing.T, f realSMBFixture, label string) {
	t.Helper()
	f.tree.export.share.VolumeLabel = &label
	serial, err := stableVolumeSerial(f.tree.authority.identity.Backend.Volume)
	if err != nil {
		t.Fatal(err)
	}
	f.tree.export.volumeSerial = serial
}

func TestVolumeInformationRealFactsAndCurrentSpace(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		label := "Data💾"
		enrollVolumeFactsForTest(t, f, label)
		if err := f.volume.Create(t.Context(), "file"); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessReadData|accessWriteData)
		created, err := storage.VolumeCreatedAt(t.Context(), f.raw)
		if err != nil {
			t.Fatal(err)
		}
		ticks, err := toFiletime(created)
		if err != nil {
			t.Fatal(err)
		}
		volume := queryVolumeForTest(t, f, id, wire.FileFsVolumeInformation, 82, statusOK)
		if len(volume) != 18+len(wire.EncodeUTF16(label)) || binary.LittleEndian.Uint64(volume) != ticks || binary.LittleEndian.Uint32(volume[8:]) != uint32(f.tree.export.volumeSerial) || binary.LittleEndian.Uint32(volume[12:]) != uint32(len(wire.EncodeUTF16(label))) || volume[16] != 0 || volume[17] != 0 || !bytes.Equal(volume[18:], wire.EncodeUTF16(label)) {
			t.Fatalf("native volume facts=%x creation=%v", volume, created)
		}
		for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation} {
			data := queryVolumeForTest(t, f, id, class, 32, statusOK)
			if binary.LittleEndian.Uint64(data) != 256 || binary.LittleEndian.Uint64(data[8:]) != 256 {
				t.Fatalf("empty native capacity class=%d data=%x", class, data)
			}
		}
		requireIOWrite(t, f, id, 0, string(bytes.Repeat([]byte{'x'}, 4097)))
		for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation} {
			data := queryVolumeForTest(t, f, id, class, 32, statusOK)
			if binary.LittleEndian.Uint64(data[8:]) != 254 {
				t.Fatalf("next query reused capacity before WRITE class=%d data=%x", class, data)
			}
		}
		setInfoForTest(t, f, id, wire.FileEndOfFileInformation, eofInfoForTest(0))
		full := queryVolumeForTest(t, f, id, wire.FileFsFullSizeInformation, 32, statusOK)
		if len(full) != 32 || binary.LittleEndian.Uint64(full) != 256 || binary.LittleEndian.Uint64(full[8:]) != 256 || binary.LittleEndian.Uint64(full[16:]) != 256 || binary.LittleEndian.Uint32(full[24:]) != 1 || binary.LittleEndian.Uint32(full[28:]) != 4096 {
			t.Fatalf("released native capacity=%x", full)
		}
		device := queryVolumeForTest(t, f, id, wire.FileFsDeviceInformation, 8, statusOK)
		if len(device) != 8 || binary.LittleEndian.Uint32(device) != 7 || binary.LittleEndian.Uint32(device[4:]) != 0x10 {
			t.Fatalf("Device advertised unsupported facts=%x", device)
		}
		attribute := queryVolumeForTest(t, f, id, wire.FileFsAttributeInformation, 128, statusOK)
		if len(attribute) <= 12 || binary.LittleEndian.Uint32(attribute) != 6 || binary.LittleEndian.Uint32(attribute[4:]) != 255 || int(binary.LittleEndian.Uint32(attribute[8:])) != len(attribute)-12 {
			t.Fatalf("Attribute advertised unsupported capabilities=%x", attribute)
		}
		closeFixtureHandle(t, f, id)
	})
}

type volumeSpaceFailureForTest struct {
	storage.FileStorage
	space storage.Space
	err   error
}

func (s volumeSpaceFailureForTest) Space(context.Context) (storage.Space, error) {
	return s.space, s.err
}

func TestVolumeFullSizeSeparatesCallerAvailableFromActualFree(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		enrollVolumeFactsForTest(t, f, "")
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", 0)
		space, err := f.volume.Space(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		space.Avail = 8192
		f.tree.export.share.Backend = volumeSpaceFailureForTest{FileStorage: f.tree.export.share.Backend, space: space}
		size := queryVolumeForTest(t, f, id, wire.FileFsSizeInformation, 24, statusOK)
		full := queryVolumeForTest(t, f, id, wire.FileFsFullSizeInformation, 32, statusOK)
		if binary.LittleEndian.Uint64(size[8:]) != 2 || binary.LittleEndian.Uint64(full[8:]) != 2 || binary.LittleEndian.Uint64(full[16:]) != uint64((space.Total-space.Used)/4096) {
			t.Fatalf("caller and actual free collapsed: Size=%x FullSize=%x Space=%+v", size, full, space)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestVolumeQueriesPermitMetadataZeroHandlesAndRequireCurrentAuthorization(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		enrollVolumeFactsForTest(t, f, "data")
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		f.session.principal = testPrincipal("0123456789abcdef")
		for _, access := range []uint32{0, accessReadData, accessWriteData, accessWriteAttr} {
			id := openInfoFileForTest(t, f, "file", access)
			h := f.tree.findFileHandle(id)
			if access == 0 || access == accessWriteAttr {
				if h.file != nil || h.node == nil {
					t.Fatal("metadata-zero open did not retain a NodeReference")
				}
				if _, err := h.node.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
					t.Fatalf("volume query gained metadata read permission: %v", err)
				}
			}
			for _, class := range []byte{wire.FileFsVolumeInformation, wire.FileFsSizeInformation, wire.FileFsFullSizeInformation, wire.FileFsDeviceInformation, wire.FileFsAttributeInformation} {
				queryVolumeForTest(t, f, id, class, 128, statusOK)
			}
			seen := 0
			f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
				p, ok := PrincipalFromContext(ctx)
				if !ok || !p.SameIdentity(f.session.principal) {
					t.Fatal("volume query lost the current authenticated principal")
				}
				seen++
				return authz.ErrDenied
			})
			for _, class := range []byte{wire.FileFsVolumeInformation, wire.FileFsSizeInformation, wire.FileFsFullSizeInformation, wire.FileFsDeviceInformation, wire.FileFsAttributeInformation} {
				queryVolumeForTest(t, f, id, class, 128, statusDenied)
			}
			if seen != 5 {
				t.Fatalf("cached facts bypassed current authorization: %d decisions", seen)
			}
			f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
				p, ok := PrincipalFromContext(ctx)
				if !ok || !p.SameIdentity(f.session.principal) {
					t.Fatal("class-local Space authorization lost the authenticated principal")
				}
				if request.Operation == storage.OpVolumeSpace {
					return authz.ErrDenied
				}
				return nil
			})
			for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation} {
				queryVolumeForTest(t, f, id, class, 128, statusDenied)
			}
			for _, class := range []byte{wire.FileFsVolumeInformation, wire.FileFsDeviceInformation, wire.FileFsAttributeInformation} {
				queryVolumeForTest(t, f, id, class, 128, statusOK)
			}
			f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
			closeFixtureHandle(t, f, id)
		}
	})
}

type volumeFactFailureForTest struct {
	storage.FileSession
	allocation storage.AllocationReporting
	unitErr    error
	createdErr error
}

func (s volumeFactFailureForTest) CheckAllocationReporting() error {
	return s.allocation.CheckAllocationReporting()
}

func (s volumeFactFailureForTest) AllocationUnitBytes(ctx context.Context) (uint64, error) {
	if s.unitErr != nil {
		return 0, s.unitErr
	}
	return s.allocation.AllocationUnitBytes(ctx)
}

func (s volumeFactFailureForTest) VolumeCreatedAt(ctx context.Context) (time.Time, error) {
	if s.createdErr != nil {
		return time.Time{}, s.createdErr
	}
	return storage.VolumeCreatedAt(ctx, s.FileSession)
}

func TestVolumeOptionalFactFailuresRemainClassLocal(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		enrollVolumeFactsForTest(t, f, "data")
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessReadData|accessWriteData)
		backend, raw := f.tree.export.share.Backend, f.tree.authority.raw
		for _, sourceErr := range []error{syscall.ENOSYS, syscall.EOPNOTSUPP, syscall.EIO} {
			f.tree.export.share.Backend = volumeSpaceFailureForTest{FileStorage: backend, err: sourceErr}
			for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation} {
				queryVolumeForTest(t, f, id, class, 32, statusIO)
			}
			queryVolumeForTest(t, f, id, wire.FileFsDeviceInformation, 8, statusOK)
		}
		f.tree.export.share.Backend = backend
		for _, sourceErr := range []error{syscall.ENOSYS, syscall.EOPNOTSUPP, syscall.EIO} {
			f.tree.authority.raw = volumeFactFailureForTest{FileSession: raw, allocation: raw.(storage.AllocationReporting), unitErr: sourceErr}
			for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation} {
				queryVolumeForTest(t, f, id, class, 32, statusIO)
			}
			queryVolumeForTest(t, f, id, wire.FileFsAttributeInformation, 128, statusOK)
		}
		f.tree.authority.raw = volumeFactFailureForTest{FileSession: raw, allocation: raw.(storage.AllocationReporting), createdErr: syscall.EIO}
		queryVolumeForTest(t, f, id, wire.FileFsVolumeInformation, 128, statusIO)
		queryVolumeForTest(t, f, id, wire.FileFsSizeInformation, 24, statusOK)
		f.tree.authority.raw = struct {
			storage.FileSession
			storage.AllocationReporting
		}{raw, raw.(storage.AllocationReporting)}
		queryVolumeForTest(t, f, id, wire.FileFsVolumeInformation, 128, statusUnsupported)
		for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation, wire.FileFsDeviceInformation, wire.FileFsAttributeInformation} {
			queryVolumeForTest(t, f, id, class, 128, statusOK)
		}
		f.tree.authority.raw = raw
		f.tree.export.share.VolumeLabel = nil
		queryVolumeForTest(t, f, id, wire.FileFsVolumeInformation, 128, statusUnsupported)
		queryVolumeForTest(t, f, id, wire.FileFsAttributeInformation, 128, statusOK)
		body, status := f.connection.readFile(t.Context(), f.session, f.tree, readRequestForTest(id, 0, 4, 0))
		if status != statusOK || !bytes.Equal(body[16:], []byte("body")) {
			t.Fatalf("optional facts disrupted the same file/tree READ: status=%#x body=%x", status, body)
		}
		setInfoForTest(t, f, id, wire.FileEndOfFileInformation, eofInfoForTest(1))
		closeFixtureHandle(t, f, id)
	})
}

func TestVolumeQueriesKeepOriginalReferenceAcrossRenameAndUnlink(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		enrollVolumeFactsForTest(t, f, "")
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", 0)
		volume := queryVolumeForTest(t, f, id, wire.FileFsVolumeInformation, 24, statusOK)
		if len(volume) != 18 || binary.LittleEndian.Uint32(volume[12:]) != 0 {
			t.Fatalf("empty label fabricated six bytes of padding: %x", volume)
		}
		if err := f.volume.Rename(t.Context(), "file", "renamed"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Remove(t.Context(), "renamed"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "file", []byte("replacement")); err != nil {
			t.Fatal(err)
		}
		if got := queryVolumeForTest(t, f, id, wire.FileFsVolumeInformation, 24, statusOK); !bytes.Equal(got, volume) {
			t.Fatalf("volume context rebound original reference: %x -> %x", volume, got)
		}
		full := queryVolumeForTest(t, f, id, wire.FileFsFullSizeInformation, 32, statusOK)
		if binary.LittleEndian.Uint64(full[16:]) != 254 {
			t.Fatalf("detached original allocation disappeared from current Space: %x", full)
		}
		closeFixtureHandle(t, f, id)
	})
}

func newOverQuotaVolumeFixtureForTest(t *testing.T, transport string) realSMBFixture {
	t.Helper()
	config := sqlite.LockingConfig{Database: filepath.Join(t.TempDir(), "quota.db"), Volume: "files", Allowance: 8192, SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true}
	meta, err := sqlite.OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	objects := memory.New()
	volume := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := volume.Write(t.Context(), "file", bytes.Repeat([]byte{'q'}, 4097)); err != nil {
		t.Fatal(err)
	}
	if err := volume.Close(); err != nil {
		t.Fatal(err)
	}
	config.Allowance, config.Initialize = 4096, false
	meta, err = sqlite.OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	volume = objectstore.New(objects, meta)
	var backend storage.FileStorage = volume
	if transport == "http" {
		options := httprest.DefaultHandlerOptions()
		options.Files = httprest.DefaultFileLimits()
		options.Files.Session = fixtureFileSessionOptions()
		handler, err := httprest.NewHandlerWithOptions(volume, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(func() {
			server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := handler.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		backend, err = httprest.Dial(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
	}
	raw, err := backend.NewFileSession(t.Context(), fixtureFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := raw.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{config: Config{Limits: DefaultLimits(), Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}, nameComparer: testNameCompare}
	export := &Export{server: server, share: Share{Volume: "files", Backend: backend, BackendVolume: identity.Backend.Volume, RootNodeID: identity.Backend.RootNodeID}}
	authority := &authoritySession{raw: raw, identity: identity, export: export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	session := &session{id: 21}
	return realSMBFixture{volume: volume, raw: raw, state: state, server: server, tree: &tree{kind: volumeTree, export: export, authority: authority, sessionID: session.id}, session: session, connection: &connection{server: server}}
}

func TestVolumeOverQuotaFailsCapacityWhileSameHandleReadsAndReleases(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			f := newOverQuotaVolumeFixtureForTest(t, transport)
			enrollVolumeFactsForTest(t, f, "data")
			space, err := f.volume.Space(t.Context())
			if err != nil || space != (storage.Space{Total: 4096, Used: 8192, Avail: 0}) {
				t.Fatalf("native over-quota precondition=%+v error=%v", space, err)
			}
			id := openInfoFileForTest(t, f, "file", accessReadData|accessWriteData)
			for _, class := range []byte{wire.FileFsSizeInformation, wire.FileFsFullSizeInformation} {
				queryVolumeForTest(t, f, id, class, 32, statusIO)
			}
			body, status := f.connection.readFile(t.Context(), f.session, f.tree, readRequestForTest(id, 0, 3, 0))
			if status != statusOK || string(body[16:]) != "qqq" {
				t.Fatalf("capacity error fenced readable same file: status=%#x data=%x", status, body)
			}
			setInfoForTest(t, f, id, wire.FileEndOfFileInformation, eofInfoForTest(1))
			full := queryVolumeForTest(t, f, id, wire.FileFsFullSizeInformation, 32, statusOK)
			if binary.LittleEndian.Uint64(full) != 1 || binary.LittleEndian.Uint64(full[8:]) != 0 || binary.LittleEndian.Uint64(full[16:]) != 0 {
				t.Fatalf("native shortening did not recover exact capacity: %x", full)
			}
			closeFixtureHandle(t, f, id)
		})
	}
}
