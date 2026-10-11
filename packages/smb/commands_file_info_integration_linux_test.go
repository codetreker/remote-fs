//go:build linux

package smb

import (
	"bytes"
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

func fileInfoFixturesForTest(t *testing.T, run func(*testing.T, realSMBFixture)) {
	t.Helper()
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			fixture.tree.sessionID = fixture.session.id
			run(t, fixture)
		})
	}
}

func queryInfoForTest(t *testing.T, fixture realSMBFixture, id wire.FileID, class byte) []byte {
	t.Helper()
	body, status := fixture.connection.queryFileInfo(t.Context(), fixture.session, fixture.tree, queryInfoRequestForTest(id, class, 56))
	if status != statusOK || len(body) < 8 || binary.LittleEndian.Uint16(body) != 9 || binary.LittleEndian.Uint16(body[2:4]) != 72 || binary.LittleEndian.Uint32(body[4:8]) != uint32(len(body)-8) {
		t.Fatalf("QUERY_INFO class=%d status=%#x body=%x", class, status, body)
	}
	return body[8:]
}

func setInfoForTest(t *testing.T, fixture realSMBFixture, id wire.FileID, class byte, data []byte) {
	t.Helper()
	ctx, finish := ioResponseContextForTest(t.Context())
	body, status := fixture.connection.setFileInfo(ctx, fixture.session, fixture.tree, setInfoRequestForTest(id, class, data))
	finish()
	if status != statusOK || !bytes.Equal(body, []byte{2, 0}) {
		t.Fatalf("SET_INFO class=%d status=%#x body=%x", class, status, body)
	}
}

func openInfoFileForTest(t *testing.T, fixture realSMBFixture, name string, access uint32) wire.FileID {
	t.Helper()
	_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(name, 1, access))
	if status != statusOK {
		t.Fatalf("CREATE %s access=%x status=%#x", name, access, status)
	}
	return id
}

func TestFileInformationRealReferencesKeepObjectFactsThroughReplacement(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("original")); err != nil {
			t.Fatal(err)
		}
		seed := seedIOWindowsMetadata(t, f, "file", dosHidden|dosSystem)
		id := openInfoFileForTest(t, f, "file", accessReadAttr|accessWriteData)
		internal := queryInfoForTest(t, f, id, wire.FileInternalInformation)
		fileID := queryInfoForTest(t, f, id, wire.FileIdInformation)
		if binary.LittleEndian.Uint64(internal) != seed.ID || binary.LittleEndian.Uint64(fileID[8:]) != seed.ID || binary.LittleEndian.Uint64(fileID) == 0 {
			t.Fatalf("identity internal=%x fileID=%x seed=%d", internal, fileID, seed.ID)
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
		replacement, err := f.volume.Stat(t.Context(), "file")
		if err != nil || replacement.ID == seed.ID {
			t.Fatalf("replacement=%+v err=%v", replacement, err)
		}
		requireIOWrite(t, f, id, 0, "old")
		if data, err := f.volume.Read(t.Context(), "file"); err != nil || string(data) != "replacement" {
			t.Fatalf("retained modification rebound path: %q %v", data, err)
		}
		if got := queryInfoForTest(t, f, id, wire.FileInternalInformation); !bytes.Equal(got, internal) {
			t.Fatalf("Internal changed: %x -> %x", internal, got)
		}
		if got := queryInfoForTest(t, f, id, wire.FileIdInformation); !bytes.Equal(got, fileID) {
			t.Fatalf("FileId changed: %x -> %x", fileID, got)
		}
		standard := queryInfoForTest(t, f, id, wire.FileStandardInformation)
		if binary.LittleEndian.Uint64(standard) != 4096 || binary.LittleEndian.Uint64(standard[8:]) != 8 || binary.LittleEndian.Uint32(standard[16:]) != 0 || standard[21] != 0 {
			t.Fatalf("detached Standard=%x", standard)
		}
		basic := queryInfoForTest(t, f, id, wire.FileBasicInformation)
		if binary.LittleEndian.Uint32(basic[32:]) != dosHidden|dosSystem|dosArchive {
			t.Fatalf("Basic attrs=%x", basic)
		}
		network := queryInfoForTest(t, f, id, wire.FileNetworkOpenInformation)
		if !bytes.Equal(network[:32], basic[:32]) || binary.LittleEndian.Uint64(network[32:]) != 4096 || binary.LittleEndian.Uint64(network[40:]) != 8 || binary.LittleEndian.Uint32(network[48:]) != dosHidden|dosSystem|dosArchive {
			t.Fatalf("NetworkOpen=%x Basic=%x", network, basic)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInformationRealScalarQueriesDoNotGrantReadAttributes(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessWriteAttr)
		h := f.tree.findFileHandle(id)
		if h.file != nil {
			t.Fatal("metadata-only open exposed byte File")
		}
		if _, err := h.node.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("metadata-only Stat gained access: %v", err)
		}
		if _, err := h.node.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("metadata-only full state gained access: %v", err)
		}
		state, err := h.node.State(t.Context(), storage.StateScalars)
		if err != nil || state.CheckProjection(h.nodeID, storage.StateScalars) != nil {
			t.Fatalf("live scalar projection failed: %+v %v", state, err)
		}
		for _, class := range []byte{wire.FileStandardInformation, wire.FileInternalInformation, wire.FileIdInformation} {
			queryInfoForTest(t, f, id, class)
		}
		for _, class := range []byte{wire.FileBasicInformation, wire.FileNetworkOpenInformation} {
			if _, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, class, 56)); status != statusDenied {
				t.Fatalf("attributes class=%d status=%#x", class, status)
			}
		}
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden}))
		if _, err := h.node.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("setter granted Stat: %v", err)
		}
		if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileEndOfFileInformation, eofInfoForTest(0))); status != statusDenied {
			t.Fatalf("metadata-only EOF status=%#x", status)
		}
		closeFixtureHandle(t, f, id)
		id = openInfoFileForTest(t, f, "file", 0)
		h = f.tree.findFileHandle(id)
		if _, err := h.node.State(t.Context(), storage.StateFull); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("zero-access full state gained access: %v", err)
		}
		for _, class := range []byte{wire.FileStandardInformation, wire.FileInternalInformation, wire.FileIdInformation} {
			queryInfoForTest(t, f, id, class)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInformationBasicAndEOFCommitRealFacts(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("abcdef")); err != nil {
			t.Fatal(err)
		}
		seed := seedIOWindowsMetadata(t, f, "file", dosHidden|dosSystem)
		id := openInfoFileForTest(t, f, "file", accessReadAttr|accessWriteAttr|accessWriteData)
		birth, access, write := time.Unix(1700000000, 100), time.Unix(1700000001, 200), time.Unix(1700000002, 300)
		bt, _ := toFiletime(birth)
		at, _ := toFiletime(access)
		wt, _ := toFiletime(write)
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{CreationTime: int64(bt), LastAccessTime: int64(at), LastWriteTime: int64(wt), Attributes: dosArchive}))
		got, err := f.volume.Stat(t.Context(), "file")
		if err != nil || got.BirthTime == nil || !got.BirthTime.Equal(birth) || !got.AccessTime.Equal(access) || !got.ModTime.Equal(write) || got.ID != seed.ID {
			t.Fatalf("combined attr=%+v err=%v", got, err)
		}
		basic := queryInfoForTest(t, f, id, wire.FileBasicInformation)
		if binary.LittleEndian.Uint64(basic) != bt || binary.LittleEndian.Uint64(basic[8:]) != at || binary.LittleEndian.Uint64(basic[16:]) != wt || binary.LittleEndian.Uint32(basic[32:]) != dosArchive {
			t.Fatalf("Basic=%x", basic)
		}
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{}))
		if preserved := queryInfoForTest(t, f, id, wire.FileBasicInformation); !bytes.Equal(preserved[:24], basic[:24]) || binary.LittleEndian.Uint32(preserved[32:]) != dosArchive {
			t.Fatalf("zero KEEP changed fields: %x -> %x", basic, preserved)
		}
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosNormal}))
		if normal := queryInfoForTest(t, f, id, wire.FileBasicInformation); binary.LittleEndian.Uint32(normal[32:]) != dosNormal {
			t.Fatalf("NORMAL did not clear flags: %x", normal)
		}
		setInfoForTest(t, f, id, wire.FileEndOfFileInformation, eofInfoForTest(3))
		requireIOContents(t, f, "file", "abc")
		setInfoForTest(t, f, id, wire.FileEndOfFileInformation, eofInfoForTest(8))
		attr := requireIOContents(t, f, "file", "abc\x00\x00\x00\x00\x00")
		meta, err := decodeWindowsMetadata(attr.Metadata)
		if err != nil || meta.Attributes != dosArchive {
			t.Fatalf("EOF ARCHIVE metadata=%+v err=%v", meta, err)
		}
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosReadOnly | dosHidden}))
		before, err := f.volume.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileEndOfFileInformation, eofInfoForTest(1))); status != statusDenied {
			t.Fatalf("READONLY EOF status=%#x", status)
		}
		after, err := f.volume.Stat(t.Context(), "file")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("rejected EOF changed facts: before=%+v after=%+v err=%v", before, after, err)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInformationDirectoryUsesMetadataReference(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "dir"); err != nil {
			t.Fatal(err)
		}
		request := createRequestForTest("dir", 1, accessReadAttr|accessWriteAttr)
		binary.LittleEndian.PutUint32(request.Body[40:], 1)
		_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
		if status != statusOK {
			t.Fatalf("directory CREATE=%#x", status)
		}
		h := f.tree.findFileHandle(id)
		if h.file != nil || h.node == nil {
			t.Fatal("directory gained byte API")
		}
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosDirectory | dosHidden | dosSystem}))
		basic := queryInfoForTest(t, f, id, wire.FileBasicInformation)
		standard := queryInfoForTest(t, f, id, wire.FileStandardInformation)
		if binary.LittleEndian.Uint32(basic[32:]) != dosDirectory|dosHidden|dosSystem || binary.LittleEndian.Uint64(standard[8:]) != 0 || standard[21] != 1 {
			t.Fatalf("directory Basic=%x Standard=%x", basic, standard)
		}
		queryInfoForTest(t, f, id, wire.FileInternalInformation)
		queryInfoForTest(t, f, id, wire.FileIdInformation)
		queryInfoForTest(t, f, id, wire.FileNetworkOpenInformation)
		attr, err := f.volume.Stat(t.Context(), "dir")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := decodeWindowsMetadata(attr.Metadata)
		if err != nil || meta.Attributes != dosHidden|dosSystem {
			t.Fatalf("derived DIRECTORY was persisted: %+v %v", meta, err)
		}
		if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileEndOfFileInformation, eofInfoForTest(0))); status != statusDenied {
			t.Fatalf("directory EOF=%#x", status)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInformationReauthorizesEveryLiveObservationAndMutation(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessReadAttr|accessWriteAttr|accessWriteData)
		before, err := f.volume.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		for _, denied := range []storage.Operation{storage.OpFileState, storage.OpFileStat, storage.OpFileAllocationUnit, storage.OpFileSetAttr, storage.OpFileSetMetadata, storage.OpFileObserveMetadata, storage.OpFileTruncate} {
			f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				if request.Operation == denied {
					return syscall.EACCES
				}
				return nil
			})
			var status uint32
			switch denied {
			case storage.OpFileState:
				_, status = f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileInternalInformation, 8))
			case storage.OpFileStat:
				_, status = f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileBasicInformation, 40))
			case storage.OpFileAllocationUnit:
				_, status = f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileStandardInformation, 24))
			case storage.OpFileSetAttr, storage.OpFileSetMetadata, storage.OpFileObserveMetadata:
				_, status = f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden})))
			default:
				_, status = f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileEndOfFileInformation, eofInfoForTest(1)))
			}
			if status != statusDenied {
				t.Fatalf("revoked %s status=%#x", denied, status)
			}
		}
		after, err := f.volume.Stat(t.Context(), "file")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("revoked operations changed node: before=%+v after=%+v err=%v", before, after, err)
		}
		f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
		closeFixtureHandle(t, f, id)
		for _, class := range []byte{wire.FileBasicInformation, wire.FileStandardInformation, wire.FileInternalInformation, wire.FileNetworkOpenInformation, wire.FileIdInformation} {
			if _, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, class, 56)); status != statusInvalidHandle {
				t.Fatalf("closed query class=%d status=%#x", class, status)
			}
		}
	})
}

func TestFileInfoHTTPUnknownSetKeepsExactActionAndRecoversTypedResult(t *testing.T) {
	for _, class := range []byte{wire.FileBasicInformation, wire.FileEndOfFileInformation} {
		t.Run(map[byte]string{wire.FileBasicInformation: "basic", wire.FileEndOfFileInformation: "eof"}[class], func(t *testing.T) {
			f, drop := newDroppedWriteSMBFixture(t)
			if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
				t.Fatal(err)
			}
			id := openInfoFileForTest(t, f, "file", accessReadAttr|accessWriteAttr|accessWriteData)
			data := eofInfoForTest(2)
			if class == wire.FileBasicInformation {
				data = wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden})
			}
			ctx, finish := ioResponseContextForTest(t.Context())
			_, status := f.connection.setFileInfo(ctx, f.session, f.tree, setInfoRequestForTest(id, class, data))
			finish()
			if status != statusIO {
				t.Fatalf("lost SET response=%#x", status)
			}
			h := f.tree.findFileHandle(id)
			if h.pendingWrite == nil {
				t.Fatal("unknown SET released its original owner")
			}
			drop.mu.Lock()
			if len(drop.actions) == 0 {
				drop.mu.Unlock()
				t.Fatal("no original mutation captured")
			}
			original := drop.actions[0]
			for _, action := range drop.actions {
				if action != original {
					drop.mu.Unlock()
					t.Fatal("unknown SET switched action")
				}
			}
			drop.drop = false
			drop.mu.Unlock()
			query, queryStatus := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileBasicInformation, 40))
			if queryStatus != statusOK {
				t.Fatalf("recovery QUERY=%#x", queryStatus)
			}
			basic := query[8:]
			if h.pendingWrite.pending() {
				t.Fatal("Completed typed replay retained pending execution")
			}
			attr, err := f.volume.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			if class == wire.FileBasicInformation {
				if binary.LittleEndian.Uint32(basic[32:]) != dosHidden || attr.Size != 4 {
					t.Fatalf("recovered Basic=%x attr=%+v", basic, attr)
				}
			} else {
				if attr.Size != 2 || binary.LittleEndian.Uint32(basic[32:]) != dosArchive {
					t.Fatalf("recovered EOF=%x attr=%+v", basic, attr)
				}
			}
			if err := f.tree.closeFileHandle(t.Context(), h); !errors.Is(err, syscall.EIO) || f.tree.findFileHandle(id) != nil {
				t.Fatalf("recovered original failure CLOSE=%v retained=%t", err, f.tree.findFileHandle(id) != nil)
			}
			statusFacts := f.server.Status()
			if len(statusFacts.WriteFailures) != 1 || !statusFacts.WriteFailures[0].PayloadReleased || !statusFacts.WriteFailures[0].ChainSettled || !statusFacts.WriteFailures[0].Terminal || !h.released {
				t.Fatalf("settled original failure diagnostics=%+v", statusFacts)
			}
		})
	}
}

func TestFileInformationRootKeepsHiddenSystemBits(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		seedIOWindowsMetadata(t, f, "", dosHidden|dosSystem|dosArchive)
		request := createRequestForTest("", 1, accessReadAttr|accessWriteAttr)
		binary.LittleEndian.PutUint32(request.Body[40:], 1)
		_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
		if status != statusOK {
			t.Fatalf("root CREATE=%#x", status)
		}
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosNormal}))
		if basic := queryInfoForTest(t, f, id, wire.FileBasicInformation); binary.LittleEndian.Uint32(basic[32:]) != dosDirectory|dosHidden|dosSystem {
			t.Fatalf("root cleared retained HIDDEN/SYSTEM: %x", basic)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInformationQueriesObserveExternalChangesAfterOpen(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("old")); err != nil {
			t.Fatal(err)
		}
		seedIOWindowsMetadata(t, f, "file", dosHidden)
		id := openInfoFileForTest(t, f, "file", accessReadAttr)
		before := queryInfoForTest(t, f, id, wire.FileStandardInformation)
		if binary.LittleEndian.Uint64(before[8:]) != 3 {
			t.Fatalf("initial EOF=%x", before)
		}
		if err := f.volume.Write(t.Context(), "file", bytes.Repeat([]byte{'x'}, 4097)); err != nil {
			t.Fatal(err)
		}
		attr, err := f.volume.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosSystem})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.raw.(storage.MetadataAccess).SetMetadata(t.Context(), attr.ID, windowsMetadataKey, attr.Metadata[windowsMetadataKey].Version, data); err != nil {
			t.Fatal(err)
		}
		standard := queryInfoForTest(t, f, id, wire.FileStandardInformation)
		basic := queryInfoForTest(t, f, id, wire.FileBasicInformation)
		network := queryInfoForTest(t, f, id, wire.FileNetworkOpenInformation)
		if binary.LittleEndian.Uint64(standard) != 8192 || binary.LittleEndian.Uint64(standard[8:]) != 4097 || binary.LittleEndian.Uint64(network[32:]) != 8192 || binary.LittleEndian.Uint64(network[40:]) != 4097 || binary.LittleEndian.Uint32(basic[32:]) != dosSystem || binary.LittleEndian.Uint32(network[48:]) != dosSystem {
			t.Fatalf("query reused stale facts: Standard=%x Basic=%x NetworkOpen=%x attr=%+v", standard, basic, network, attr)
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInfoRejectsMutationOwnerExhaustionBeforeEffects(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessReadAttr|accessWriteAttr|accessWriteData)
		before, err := f.volume.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		f.server.config.Limits.MaxRetainedWriteBytes = 1
		for _, class := range []byte{wire.FileBasicInformation, wire.FileEndOfFileInformation} {
			data := eofInfoForTest(1)
			if class == wire.FileBasicInformation {
				data = wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden})
			}
			if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, class, data)); status != statusResources {
				t.Fatalf("owner exhaustion class=%d status=%#x", class, status)
			}
			after, err := f.volume.Stat(t.Context(), "file")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("owner exhaustion changed node: before=%+v after=%+v err=%v", before, after, err)
			}
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInfoObservationDoesNotAuthorizeMetadataMutation(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		var observe, deniedWrite bool
		f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpFileObserveMetadata {
				if request.MetadataObservation == nil || request.MetadataObservation.Purpose != storage.MetadataObservationNamespace ||
					request.MetadataObservation.Namespace != windowsMetadataKey || request.MetadataObservation.MaxPayloadBytes != 8 ||
					len(request.ContentMetadataEffects) != 0 {
					t.Fatalf("incorrect namespace observation policy: %+v", request)
				}
				observe = true
				request.MetadataObservation.Namespace = "mutated"
			}
			if request.Operation == storage.OpFileSetMetadata {
				if request.MetadataObservation != nil {
					t.Fatal("metadata mutation inherited observation authority")
				}
				deniedWrite = true
				return syscall.EACCES
			}
			return nil
		})
		id := openInfoFileForTest(t, f, "file", accessWriteAttr)
		before, err := f.volume.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden}))); status != statusDenied || !deniedWrite {
			t.Fatalf("observation allowance authorized metadata mutation: status=%#x deny=%t", status, deniedWrite)
		}
		after, err := f.volume.Stat(t.Context(), "file")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("unauthorized attributes committed: before=%+v after=%+v err=%v", before, after, err)
		}
		observe = false
		instant := time.Unix(1700000000, 0)
		ticks, _ := toFiletime(instant)
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{LastWriteTime: int64(ticks)}))
		if observe {
			t.Fatal("time-only change observed a metadata namespace")
		}
		closeFixtureHandle(t, f, id)
	})
}

func TestFileInfoObservationPurposeChecksCurrentHostPolicy(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessWriteAttr|accessWriteData)
		h := f.tree.findFileHandle(id)
		allowed := storage.MetadataObservationNamespace
		f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpFileObserveMetadata {
				if request.MetadataObservation == nil || request.MetadataObservation.Purpose != allowed {
					return authz.ErrDenied
				}
				request.MetadataObservation.Namespace = "policy.changed"
				request.MetadataObservation.MaxPayloadBytes = 0
			}
			return nil
		})
		setInfoForTest(t, f, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosHidden}))
		before, err := f.volume.Stat(t.Context(), "file")
		if err != nil {
			t.Fatal(err)
		}
		if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileEndOfFileInformation, eofInfoForTest(1))); status != statusDenied {
			t.Fatalf("namespace approval granted content observation: %#x", status)
		}
		after, err := f.volume.Stat(t.Context(), "file")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("denied content observation changed authority: %+v %v", after, err)
		}
		allowed = storage.MetadataObservationContentEffect
		requireIOWrite(t, f, id, 0, "good")
		before = requireIOContents(t, f, "file", "good")
		if _, status := f.connection.setFileInfo(t.Context(), f.session, f.tree, setInfoRequestForTest(id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosSystem}))); status != statusDenied {
			t.Fatalf("content approval granted namespace observation: %#x", status)
		}
		after, err = f.volume.Stat(t.Context(), "file")
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("denied namespace observation changed authority: %+v %v", after, err)
		}
		if _, err := h.file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("conditional observers granted Stat: %v", err)
		}
		closeFixtureHandle(t, f, id)
	})
}
