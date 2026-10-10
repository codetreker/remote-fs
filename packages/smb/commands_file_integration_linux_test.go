//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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
)

func createRequestForTest(name string, disposition, access uint32) wire.Request {
	encodedName := wire.EncodeUTF16(name)
	packet := make([]byte, 64+max(57, 56+len(encodedName)))
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 57)
	binary.LittleEndian.PutUint32(body[4:], 2)
	binary.LittleEndian.PutUint32(body[24:], access)
	binary.LittleEndian.PutUint32(body[32:], 7)
	binary.LittleEndian.PutUint32(body[36:], disposition)
	binary.LittleEndian.PutUint16(body[44:], 120)
	binary.LittleEndian.PutUint16(body[46:], uint16(len(encodedName)))
	copy(packet[120:], encodedName)
	return wire.Request{Header: wire.Header{Command: wire.Create}, Body: body, Packet: packet}
}

type realSMBFixture struct {
	volume     *objectstore.Storage
	raw        storage.FileSession
	state      storage.FileSessionStatus
	server     *Server
	tree       *tree
	session    *session
	connection *connection
}

// These fixtures exercise commands without starting the endpoint renewal worker.
// Their finite lease covers the instrumented matrices; retirement tests fence
// their authority explicitly.
func fixtureFileSessionOptions() storage.FileSessionOptions {
	options := storage.DefaultFileSessionOptions()
	options.Lease = 5 * time.Minute
	return options
}

func newRealSMBFixture(t *testing.T) realSMBFixture {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "files.db"), Volume: "files", Allowance: 1 << 20,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	raw, err := volume.NewFileSession(t.Context(), fixtureFileSessionOptions())
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
	server := &Server{config: Config{Limits: DefaultLimits(), Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}, nameComparer: testNameCompare}
	identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	export := &Export{server: server, share: Share{Volume: "files", Backend: volume, BackendVolume: identity.Backend.Volume, RootNodeID: identity.Backend.RootNodeID}}
	authority := &authoritySession{raw: raw, identity: identity, export: export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	tree := &tree{kind: volumeTree, export: export, authority: authority}
	s := &session{id: 21}
	c := &connection{server: server}
	return realSMBFixture{volume: volume, raw: raw, state: state, server: server, tree: tree, session: s, connection: c}
}

func TestCreateOpensRealAuthorityAndSharesObjectIdentity(t *testing.T) {
	fixture := newRealSMBFixture(t)
	volume, raw, state, server, tree, s, c := fixture.volume, fixture.raw, fixture.state, fixture.server, fixture.tree, fixture.session, fixture.connection
	first, status, firstID := c.createFile(t.Context(), s, tree, createRequestForTest("file", 3, accessReadData|accessReadAttr))
	if status != statusOK || len(first) != 88 || firstID == (wire.FileID{}) || binary.LittleEndian.Uint32(first[4:]) != uint32(storage.Created) || binary.LittleEndian.Uint32(first[56:])&dosArchive == 0 {
		t.Fatalf("created file: status=%#x body=%v id=%x", status, first, firstID)
	}
	second, status, secondID := c.createFile(t.Context(), s, tree, createRequestForTest("file", 1, accessReadData|accessReadAttr))
	if status != statusOK || secondID == firstID || binary.LittleEndian.Uint32(second[4:]) != uint32(storage.Opened) {
		t.Fatalf("opened file: status=%#x body=%v id=%x", status, second, secondID)
	}
	firstAttr, err := tree.findFileHandle(firstID).file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	secondAttr, err := tree.findFileHandle(secondID).file.Stat(t.Context())
	if err != nil || firstAttr.ID != secondAttr.ID {
		t.Fatalf("same-name opens did not retain object identity: first=%+v second=%+v err=%v", firstAttr, secondAttr, err)
	}
	if _, status, _ := c.createFile(t.Context(), s, tree, createRequestForTest("missing", 1, accessReadData)); status != statusNameNotFound {
		t.Fatalf("missing OPEN status = %#x", status)
	}
	if _, status, _ := c.createFile(t.Context(), s, tree, createRequestForTest("file", 2, accessReadData)); status != statusNameCollision {
		t.Fatalf("exclusive existing CREATE status = %#x", status)
	}
	conflicting := createRequestForTest("file", 1, accessReadData)
	binary.LittleEndian.PutUint32(conflicting.Body[32:], 0)
	if _, status, _ := c.createFile(t.Context(), s, tree, conflicting); status != statusSharingViolation {
		t.Fatalf("conflicting share status = %#x", status)
	}
	server.config.Limits.MaxHandles = 2
	if _, status, _ := c.createFile(t.Context(), s, tree, createRequestForTest("another", 3, accessReadData)); status != statusResources {
		t.Fatalf("full handle table status = %#x", status)
	}
	if _, err := volume.Stat(t.Context(), "another"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("capacity refusal created a node: %v", err)
	}
	for _, id := range []wire.FileID{firstID, secondID} {
		body := make([]byte, 24)
		binary.LittleEndian.PutUint16(body, 24)
		copy(body[8:], id[:])
		if _, status := c.closeFile(t.Context(), s, tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{}); status != statusOK {
			t.Fatalf("close %x: %#x", id, status)
		}
	}
	selected, err := resolveCreateName(t.Context(), tree, "budget")
	if err != nil || selected.Condition.State != storage.Absent {
		t.Fatalf("select absent child: %+v %v", selected, err)
	}
	action, err := storage.NewFileActionID(state.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	budgetFailure := errors.New("allocation fact unavailable")
	bounded := storage.WithBoundedAttrResult(t.Context(), 4096, func(storage.Attr, int64) error { return budgetFailure })
	opened, err := raw.(storage.AtomicFileOpener).OpenAt(bounded, selected.Selection, storage.OpenAtOptions{
		Read: true, Create: true, Target: selected.Condition, Action: action,
		Use: storage.UseClaim{Uses: storage.ReadData}, Existing: storage.Keep,
	})
	if !errors.Is(err, budgetFailure) || opened.File != nil {
		t.Fatalf("budget admission: %+v %v", opened, err)
	}
	if _, err := volume.Stat(t.Context(), "budget"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed result budget committed CREATE: %v", err)
	}
}

func TestCreateDirectoryAndMetadataOnlyReferences(t *testing.T) {
	fixture := newRealSMBFixture(t)
	request := createRequestForTest("folder", 2, accessReadData)
	binary.LittleEndian.PutUint32(request.Body[40:], createDirectory)
	body, status, directoryID := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request)
	if status != statusOK || directoryID == (wire.FileID{}) || binary.LittleEndian.Uint32(body[56:])&dosDirectory == 0 {
		t.Fatalf("directory CREATE: status=%#x body=%v", status, body)
	}
	directory := fixture.tree.findFileHandle(directoryID)
	if directory == nil || directory.node == nil || directory.file != nil {
		t.Fatal("directory did not retain a NodeReference")
	}
	if err := fixture.volume.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	response, status, metadataID := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("file", 1, accessReadAttr))
	if status != statusOK || len(response) != 88 || metadataID == (wire.FileID{}) {
		t.Fatalf("metadata-only OPEN: status=%#x body=%v", status, response)
	}
	metadata := fixture.tree.findFileHandle(metadataID)
	if metadata == nil || metadata.node == nil || metadata.file != nil || metadata.access != accessReadAttr {
		t.Fatal("metadata-only open granted the wrong reference or access")
	}
	for _, id := range []wire.FileID{directoryID, metadataID} {
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		copy(closeBody[8:], id[:])
		if _, status := fixture.connection.closeFile(t.Context(), fixture.session, fixture.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeBody}, wire.FileID{}); status != statusOK {
			t.Fatalf("close %x: %#x", id, status)
		}
	}
}

func TestTreeDisconnectDrainsItsHandlesWithoutClosingSiblingAuthority(t *testing.T) {
	fixture := newRealSMBFixture(t)
	firstTree := fixture.tree
	firstTree.id, firstTree.sessionID, firstTree.done = 1, fixture.session.id, make(chan struct{})
	secondTree := &tree{kind: volumeTree, id: 2, sessionID: fixture.session.id, export: firstTree.export, authority: firstTree.authority, done: make(chan struct{})}
	firstTree.export.refs, firstTree.export.trees = 2, 2
	firstTree.authority.refs = 2
	firstTree.authority.done = make(chan struct{})
	close(firstTree.authority.done)
	fixture.connection.pending = make(map[uint64]*pendingRequest)
	_, status, firstID := fixture.connection.createFile(t.Context(), fixture.session, firstTree, createRequestForTest("file", 3, accessReadData))
	if status != statusOK {
		t.Fatalf("first tree CREATE: %#x", status)
	}
	if err := fixture.connection.closeTreeContext(t.Context(), firstTree); err != nil {
		t.Fatal(err)
	}
	if !firstTree.closed || firstTree.findFileHandle(firstID) != nil || firstTree.authority.isClosed() {
		t.Fatal("first tree cleanup closed shared authority or retained its handle")
	}
	_, status, secondID := fixture.connection.createFile(t.Context(), fixture.session, secondTree, createRequestForTest("file", 1, accessReadData))
	if status != statusOK || secondID == firstID {
		t.Fatalf("sibling tree CREATE: %#x id=%x", status, secondID)
	}
	if err := fixture.connection.closeTreeContext(t.Context(), secondTree); err != nil {
		t.Fatal(err)
	}
	if !secondTree.closed || !secondTree.authority.isClosed() || secondTree.findFileHandle(secondID) != nil {
		t.Fatal("last tree did not retire authority and handles")
	}
}

func TestShareClaimOrdersAcrossFileSessions(t *testing.T) {
	fixture := newRealSMBFixture(t)
	firstRequest := createRequestForTest("shared", 3, accessReadData)
	binary.LittleEndian.PutUint32(firstRequest.Body[32:], 0)
	_, status, firstID := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, firstRequest)
	if status != statusOK {
		t.Fatalf("exclusive-share CREATE: %#x", status)
	}
	otherRaw, err := fixture.volume.NewFileSession(t.Context(), fixtureFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := otherRaw.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	otherState, err := otherRaw.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	otherIdentity, err := otherRaw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	otherAuthority := &authoritySession{raw: otherRaw, identity: otherIdentity, export: fixture.tree.export, actionEpoch: otherState.ActionEpoch, deadline: time.Now().Add(otherState.Remaining)}
	otherTree := &tree{kind: volumeTree, export: fixture.tree.export, authority: otherAuthority}
	otherSession := &session{id: 22}
	_, status, _ = fixture.connection.createFile(t.Context(), otherSession, otherTree, createRequestForTest("shared", 1, accessReadData))
	if status != statusSharingViolation {
		t.Fatalf("cross-session share conflict: %#x", status)
	}
	closeBody := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeBody, 24)
	copy(closeBody[8:], firstID[:])
	if _, status := fixture.connection.closeFile(t.Context(), fixture.session, fixture.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeBody}, wire.FileID{}); status != statusOK {
		t.Fatalf("release share claim: %#x", status)
	}
	_, status, otherID := fixture.connection.createFile(t.Context(), otherSession, otherTree, createRequestForTest("shared", 1, accessReadData))
	if status != statusOK || otherID == firstID {
		t.Fatalf("cross-session open after release: status=%#x id=%x", status, otherID)
	}
	copy(closeBody[8:], otherID[:])
	if _, status := fixture.connection.closeFile(t.Context(), otherSession, otherTree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeBody}, wire.FileID{}); status != statusOK {
		t.Fatalf("close second session: %#x", status)
	}
}

func TestCreateAuthorizationDenialHasNoNameEffect(t *testing.T) {
	fixture := newRealSMBFixture(t)
	fixture.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpFileOpenAt {
			return authz.ErrDenied
		}
		return nil
	})
	_, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("denied", 3, accessReadData))
	if status != statusDenied {
		t.Fatalf("denied CREATE status = %#x", status)
	}
	if _, err := fixture.volume.Stat(t.Context(), "denied"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("denied CREATE changed namespace: %v", err)
	}
}

func TestCreateAndCloseReportAuthoritativeAllocation(t *testing.T) {
	fixture := newRealSMBFixture(t)
	for _, test := range []struct {
		size, allocated int
	}{
		{0, 0}, {1, 4096}, {4096, 4096}, {4097, 8192},
	} {
		name := fmt.Sprintf("size-%d", test.size)
		if err := fixture.volume.Write(t.Context(), name, make([]byte, test.size)); err != nil {
			t.Fatal(err)
		}
		created, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(name, 1, accessReadData|accessReadAttr))
		if status != statusOK || len(created) != 88 || binary.LittleEndian.Uint64(created[40:]) != uint64(test.allocated) || binary.LittleEndian.Uint64(created[48:]) != uint64(test.size) {
			t.Fatalf("CREATE size=%d: status=%#x body=%v", test.size, status, created)
		}
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		binary.LittleEndian.PutUint16(closeBody[2:], 1)
		copy(closeBody[8:], id[:])
		closed, status := fixture.connection.closeFile(t.Context(), fixture.session, fixture.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeBody}, wire.FileID{})
		if status != statusOK || len(closed) != 60 || binary.LittleEndian.Uint16(closed[2:]) != 1 || binary.LittleEndian.Uint64(closed[40:]) != uint64(test.allocated) || binary.LittleEndian.Uint64(closed[48:]) != uint64(test.size) {
			t.Fatalf("CLOSE size=%d: status=%#x body=%v", test.size, status, closed)
		}
	}
}

func TestCreateWithoutAllocationCapabilityHasNoEffect(t *testing.T) {
	fixture := newRealSMBFixture(t)
	fixture.tree.authority.raw = &noAllocationSession{FileSession: fixture.raw}
	_, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("unreported", 3, accessReadData))
	if status != statusUnsupported {
		t.Fatalf("missing allocation capability status = %#x", status)
	}
	if _, err := fixture.volume.Stat(t.Context(), "unreported"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missing allocation capability changed namespace: %v", err)
	}
}

func newHTTPSMBFixture(t *testing.T) realSMBFixture {
	t.Helper()
	fixture := newRealSMBFixture(t)
	handlerOptions := httprest.DefaultHandlerOptions()
	handlerOptions.Files = httprest.DefaultFileLimits()
	handlerOptions.Files.Session = fixtureFileSessionOptions()
	handler, err := httprest.NewHandlerWithOptions(fixture.volume, nil, handlerOptions)
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
	client, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := client.NewFileSession(t.Context(), fixtureFileSessionOptions())
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
	fixture.raw, fixture.state = raw, state
	fixture.tree.export.share.Backend = client
	fixture.tree.authority = &authoritySession{raw: raw, identity: identity, export: fixture.tree.export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	return fixture
}

func closeFixtureHandle(t *testing.T, fixture realSMBFixture, id wire.FileID) {
	t.Helper()
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	copy(body[8:], id[:])
	if _, status := fixture.connection.closeFile(t.Context(), fixture.session, fixture.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{}); status != statusOK {
		t.Fatalf("CLOSE status=%#x", status)
	}
}

func TestCreateDispositionsPreserveAtomicResultsDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			for _, test := range []struct {
				disposition uint32
				existing    bool
				outcome     storage.OpenOutcome
				status      uint32
				size        uint64
			}{
				{1, false, 0, statusNameNotFound, 0}, {1, true, storage.Opened, statusOK, 4},
				{2, false, storage.Created, statusOK, 0}, {2, true, 0, statusNameCollision, 4},
				{3, false, storage.Created, statusOK, 0}, {3, true, storage.Opened, statusOK, 4},
				{4, false, 0, statusNameNotFound, 0}, {4, true, storage.Reset, statusOK, 0},
				{5, false, storage.Created, statusOK, 0}, {5, true, storage.Reset, statusOK, 0},
			} {
				name := fmt.Sprintf("d%d-%v", test.disposition, test.existing)
				var before storage.Attr
				if test.existing {
					if err := fixture.volume.Write(t.Context(), name, []byte("body")); err != nil {
						t.Fatal(err)
					}
					var err error
					before, err = fixture.volume.Stat(t.Context(), name)
					if err != nil {
						t.Fatal(err)
					}
				}
				body, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(name, test.disposition, accessReadData|accessWriteData))
				if status != test.status {
					t.Fatalf("%s status=%#x want=%#x", name, status, test.status)
				}
				if status != statusOK {
					continue
				}
				if binary.LittleEndian.Uint32(body[4:]) != uint32(test.outcome) || binary.LittleEndian.Uint64(body[48:]) != test.size {
					t.Fatalf("%s result=%v", name, body)
				}
				attr, err := fixture.volume.Stat(t.Context(), name)
				if err != nil {
					t.Fatal(err)
				}
				if test.existing && attr.ID != before.ID {
					t.Fatalf("%s replaced identity %d with %d", name, before.ID, attr.ID)
				}
				if attr.Size != int64(test.size) {
					t.Fatalf("%s size=%d", name, attr.Size)
				}
				closeFixtureHandle(t, fixture, id)
			}
		})
	}
}

func TestCreateRightsAreExactOnRetainedDirectAndHTTPReferences(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			for _, test := range []struct {
				name                                           string
				access                                         uint32
				node, read, write, metadataRead, metadataWrite bool
			}{
				{"execute", accessExecute, true, false, false, false, false},
				{"execute-write", accessExecute | accessWriteData, false, false, true, false, false},
				{"read-attr-write", accessReadData | accessWriteAttr, false, true, false, false, true},
				{"write-attr-read", accessWriteData | accessReadAttr, false, false, true, true, false},
				{"delete", accessDelete, true, false, false, false, false},
				{"attr-read", accessReadAttr, true, false, false, true, false},
				{"attr-write", accessWriteAttr, true, false, false, false, true},
			} {
				t.Run(test.name, func(t *testing.T) {
					_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(test.name, 2, test.access))
					if status != statusOK {
						t.Fatalf("CREATE status=%#x", status)
					}
					handle := fixture.tree.findFileHandle(id)
					if handle == nil || (handle.node != nil) != test.node || (handle.file != nil) == test.node {
						t.Fatalf("wrong reference: %+v", handle)
					}
					check := func(operation string, err error, allowed bool) {
						t.Helper()
						if allowed && err != nil || !allowed && !errors.Is(err, syscall.EBADF) {
							t.Fatalf("%s allowed=%v err=%v", operation, allowed, err)
						}
					}
					if handle.file != nil {
						_, err := handle.file.ReadAt(t.Context(), 0, 1)
						check("read", err, test.read)
						_, err = handle.file.WriteAt(t.Context(), 0, []byte("x"))
						check("write", err, test.write)
						_, err = handle.file.Stat(t.Context())
						check("metadata read", err, test.metadataRead)
						_, err = handle.file.SetAttr(t.Context(), storage.AttrChange{})
						check("metadata write", err, test.metadataWrite)
					} else {
						_, err := handle.node.Stat(t.Context())
						check("metadata read", err, test.metadataRead)
						_, err = handle.node.SetAttr(t.Context(), storage.AttrChange{})
						check("metadata write", err, test.metadataWrite)
					}
					closeFixtureHandle(t, fixture, id)
				})
			}
		})
	}
}

func TestCreateRootRulesRetainPinnedDirectoryDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			for _, disposition := range []uint32{1, 3} {
				body, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("", disposition, accessReadAttr))
				if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != uint32(storage.Opened) || binary.LittleEndian.Uint32(body[56:])&dosDirectory == 0 {
					t.Fatalf("root d=%d status=%#x body=%v", disposition, status, body)
				}
				handle := fixture.tree.findFileHandle(id)
				nodeID, err := storage.ReferenceNodeID(handle.node)
				if err != nil || nodeID != fixture.tree.authority.identity.Backend.RootNodeID {
					t.Fatalf("root identity=%d err=%v", nodeID, err)
				}
				closeFixtureHandle(t, fixture, id)
			}
			for _, test := range []struct {
				disposition, options uint32
				status               uint32
			}{
				{2, 0, statusNameCollision}, {4, 0, createStatusError(syscall.EISDIR)}, {5, 0, createStatusError(syscall.EISDIR)}, {1, createNonDirectory, createStatusError(syscall.EISDIR)},
			} {
				request := createRequestForTest("", test.disposition, accessWriteData)
				binary.LittleEndian.PutUint32(request.Body[40:], test.options)
				if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request); status != test.status {
					t.Fatalf("root d=%d options=%x status=%#x want=%#x", test.disposition, test.options, status, test.status)
				}
			}
		})
	}
}

type createSessionContract interface {
	storage.FileSession
	storage.AllocationReporting
	storage.FileSessionIdentity
	storage.StableReferenceIdentity
	storage.OpenMetadataAccess
	storage.RecoverableReferenceClose
	storage.DirectoryMetadataObserver
	storage.AtomicFileOpener
	storage.NodeReferences
	storage.FileActions
}

type createResultProbe struct {
	createSessionContract
	change      func(*storage.OpenResult)
	afterOpen   func()
	beforeOpen  func()
	returnError error
	loseFirst   bool
	calls       []storage.OpenAtOptions
	selections  []storage.ChildSelection
	last        storage.File
}

func (p *createResultProbe) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	p.calls = append(p.calls, options)
	p.selections = append(p.selections, selection.Clone())
	if p.beforeOpen != nil {
		p.beforeOpen()
	}
	result, err := p.createSessionContract.OpenAt(ctx, selection, options)
	p.last = result.File
	if err == nil {
		if p.change != nil {
			p.change(&result)
		}
		if p.afterOpen != nil {
			p.afterOpen()
		}
	}
	if p.loseFirst && len(p.calls) == 1 && err == nil {
		return storage.OpenResult{}, syscall.EIO
	}
	if err == nil && p.returnError != nil {
		err = p.returnError
	}
	return result, err
}

func TestCreateInvalidAtomicResultsRetainCleanupOwnership(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*storage.OpenResult)
	}{
		{"identity", func(result *storage.OpenResult) { result.Attr.ID++ }},
		{"allocation", func(result *storage.OpenResult) { result.Attr.AllocationKnown = false }},
		{"outcome", func(result *storage.OpenResult) { result.Outcome = storage.Replaced }},
		{"created-missing-birth", func(result *storage.OpenResult) { result.Attr.BirthTime = nil }},
		{"created-missing-change", func(result *storage.OpenResult) { result.Attr.ChangeTime = nil }},
		{"reset-missing-change", func(result *storage.OpenResult) { result.Attr.ChangeTime = nil }},
		{"kind", func(result *storage.OpenResult) {
			result.Attr.Kind = storage.NodeDirectory
			result.Attr.AllocationSize = 0
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRealSMBFixture(t)
			probe := &createResultProbe{createSessionContract: fixture.raw.(createSessionContract), change: test.change}
			fixture.tree.authority.raw = probe
			disposition, access := uint32(2), accessReadData
			if test.name == "reset-missing-change" {
				if err := fixture.volume.Write(t.Context(), "bad-result", []byte("body")); err != nil {
					t.Fatal(err)
				}
				disposition, access = 4, accessWriteData
			}
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("bad-result", disposition, access))
			if status != statusIO || id != (wire.FileID{}) {
				t.Fatalf("invalid result status=%#x id=%x", status, id)
			}
			fixture.tree.fileMu.Lock()
			retained := len(fixture.tree.handles)
			fixture.tree.fileMu.Unlock()
			if retained != 0 {
				t.Fatalf("cleaned invalid reference still counted: %d", retained)
			}
			state, err := probe.last.(storage.ReferenceCloseActions).CloseOwnerStatus(t.Context())
			if err != nil || !state.Released {
				t.Fatalf("authority retained malformed reference: %+v %v", state, err)
			}
		})
	}
}

func TestCreateOverwriteWindowsAttributesDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			name := "attributes"
			if err := fixture.volume.Write(t.Context(), name, []byte("body")); err != nil {
				t.Fatal(err)
			}
			attr, err := fixture.volume.Stat(t.Context(), name)
			if err != nil {
				t.Fatal(err)
			}
			data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosHidden | dosSystem | dosArchive})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := fixture.raw.(storage.MetadataAccess).SetMetadata(t.Context(), attr.ID, windowsMetadataKey, nil, data)
			if err != nil {
				t.Fatal(err)
			}
			for _, disposition := range []uint32{4, 5} {
				request := createRequestForTest(name, disposition, accessWriteData)
				binary.LittleEndian.PutUint32(request.Body[28:], dosNormal)
				if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request); status != statusDenied {
					t.Fatalf("hidden/system overwrite disposition %d status=%#x", disposition, status)
				}
				current, err := fixture.volume.Stat(t.Context(), name)
				if err != nil || current.Size != 4 || current.ID != attr.ID || !bytes.Equal(current.Metadata[windowsMetadataKey].Version, payload.Version) {
					t.Fatalf("denied overwrite changed object: %+v %v", current, err)
				}
			}
			request := createRequestForTest(name, 4, accessWriteData)
			binary.LittleEndian.PutUint32(request.Body[28:], dosHidden|dosSystem|dosReadOnly)
			body, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request)
			if status != statusOK || binary.LittleEndian.Uint32(body[56:]) != dosHidden|dosSystem|dosReadOnly|dosArchive || binary.LittleEndian.Uint64(body[48:]) != 0 {
				t.Fatalf("requested overwrite attrs: status=%#x body=%v", status, body)
			}
			closeFixtureHandle(t, fixture, id)
			_, status, id = fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(name, 1, accessDelete))
			if status != statusOK || fixture.tree.findFileHandle(id).node == nil {
				t.Fatalf("readonly DELETE-only OPEN status=%#x", status)
			}
			closeFixtureHandle(t, fixture, id)
			if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(name, 1, accessWriteData)); status != statusDenied {
				t.Fatalf("readonly bytewriter status=%#x", status)
			}
		})
	}
}

func TestCreateWindowsMetadataPredicatePreventsStaleReset(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			if err := fixture.volume.Write(t.Context(), "readonly-race", []byte("body")); err != nil {
				t.Fatal(err)
			}
			attr, err := fixture.volume.Stat(t.Context(), "readonly-race")
			if err != nil {
				t.Fatal(err)
			}
			raw := fixture.raw
			probe := &createResultProbe{createSessionContract: raw.(createSessionContract)}
			probe.beforeOpen = func() {
				probe.beforeOpen = nil
				data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosReadOnly})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := raw.(storage.MetadataAccess).SetMetadata(t.Context(), attr.ID, windowsMetadataKey, nil, data); err != nil {
					t.Fatal(err)
				}
			}
			fixture.tree.authority.raw = probe
			_, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("readonly-race", 4, accessWriteData))
			if status != statusRetry {
				t.Fatalf("stale predicate status=%#x", status)
			}
			current, err := fixture.volume.Stat(t.Context(), "readonly-race")
			attributes, projectionErr := projectWindowsAttributes(current)
			if err != nil || projectionErr != nil || current.Size != 4 || attributes != dosReadOnly {
				t.Fatalf("stale reset effects: %+v attrs=%x err=%v/%v", current, attributes, err, projectionErr)
			}
		})
	}
}

func TestCreateRecoveryKeepsSameTypedIntentAfterLostResultAndRevocation(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			probe := &createResultProbe{createSessionContract: fixture.raw.(createSessionContract), loseFirst: true}
			probe.afterOpen = func() {
				fixture.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
			}
			fixture.tree.authority.raw = probe
			request := createRequestForTest("lost-result", 2, accessReadData)
			binary.LittleEndian.PutUint32(request.Body[32:], 0)
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request)
			if status != statusIO || id != (wire.FileID{}) || len(probe.calls) != 2 || !reflect.DeepEqual(probe.calls[0], probe.calls[1]) || !reflect.DeepEqual(probe.selections[0], probe.selections[1]) {
				t.Fatalf("lost result recovery status=%#x id=%x calls=%+v", status, id, probe.calls)
			}
			if err := fixture.volume.Write(t.Context(), "lost-result", []byte("after cleanup")); err != nil {
				t.Fatalf("revocation blocked accepted share cleanup: %v", err)
			}
			current, err := fixture.volume.Stat(t.Context(), "lost-result")
			if err != nil || current.ID == 0 {
				t.Fatalf("original creation absent: %+v %v", current, err)
			}
			fixture.tree.fileMu.Lock()
			retained := len(fixture.tree.handles)
			fixture.tree.fileMu.Unlock()
			if retained != 0 {
				t.Fatalf("cleanup retained %d owners", retained)
			}
		})
	}
}

func TestCreateResponseUsesOriginalCaptureAndErrorReferenceIsReleased(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			if err := fixture.volume.Write(t.Context(), "captured", []byte("body")); err != nil {
				t.Fatal(err)
			}
			probe := &createResultProbe{createSessionContract: fixture.raw.(createSessionContract), afterOpen: func() {
				if err := fixture.volume.Write(t.Context(), "captured", []byte("changed")); err != nil {
					t.Fatal(err)
				}
			}}
			fixture.tree.authority.raw = probe
			body, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("captured", 1, accessReadData))
			if status != statusOK || binary.LittleEndian.Uint64(body[48:]) != 4 {
				t.Fatalf("later metadata reconstructed open result: status=%#x body=%v", status, body)
			}
			closeFixtureHandle(t, fixture, id)
			probe.afterOpen = nil
			probe.returnError = syscall.EIO
			request := createRequestForTest("error-ref", 2, accessReadData)
			binary.LittleEndian.PutUint32(request.Body[32:], 0)
			if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request); status != statusIO {
				t.Fatalf("error+reference status=%#x", status)
			}
			if err := fixture.volume.Write(t.Context(), "error-ref", []byte("after cleanup")); err != nil {
				t.Fatalf("error+reference leaked share: %v", err)
			}
		})
	}
}

func siblingSMBFixture(t *testing.T, fixture realSMBFixture) realSMBFixture {
	t.Helper()
	raw, err := fixture.tree.export.share.Backend.NewFileSession(t.Context(), fixtureFileSessionOptions())
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
	sibling := fixture
	sibling.raw, sibling.state = raw, state
	sibling.session = &session{id: fixture.session.id + 1}
	sibling.tree = &tree{kind: volumeTree, export: fixture.tree.export, authority: &authoritySession{raw: raw, identity: identity, export: fixture.tree.export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}}
	return sibling
}

func nonSMBMatrixOpen(t *testing.T, raw storage.FileSession, root, nodeID uint64, access, share uint32) (func(), error) {
	t.Helper()
	state, err := raw.Status(t.Context())
	if err != nil {
		return nil, err
	}
	action, err := storage.NewFileActionID(state.ActionEpoch)
	if err != nil {
		return nil, err
	}
	intent := openIntent{read: access&accessReadData != 0, write: access&accessWriteData != 0}
	switch access {
	case accessReadData:
		intent.use.Uses = storage.ReadData
	case accessWriteData:
		intent.use.Uses = storage.WriteData
	case accessDelete:
		intent.use.Uses = storage.DeleteName
	}
	if access&accessReadAttr != 0 {
		intent.metadata = storage.ReadMetadata
	}
	if access&accessWriteEA != 0 {
		intent.metadata = storage.WriteMetadata
	}
	intent.metadataOnly = !intent.read && !intent.write
	if intent.use.Uses != 0 {
		if share&1 == 0 {
			intent.use.Deny |= storage.ReadData | storage.ReadEntries
		}
		if share&2 == 0 {
			intent.use.Deny |= storage.WriteData
		}
		if share&4 == 0 {
			intent.use.Deny |= storage.DeleteName
		}
	}
	selection := storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root}, RawLeaf: []byte("matrix")}}
	target := storage.ChildCondition{State: storage.SameNode, NodeID: nodeID}
	if intent.metadataOnly {
		result, err := raw.(storage.NodeReferences).OpenChildRef(t.Context(), selection, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: target, Action: action, Use: intent.use, MetadataAccess: intent.metadata})
		if err != nil {
			return nil, err
		}
		return func() {
			if err := result.Reference.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		}, nil
	}
	result, err := raw.(storage.AtomicFileOpener).OpenAt(t.Context(), selection, storage.OpenAtOptions{Read: intent.read, Write: intent.write, MetadataAccess: intent.metadata, Target: target, Action: action, Use: intent.use, Existing: storage.Keep})
	if err != nil {
		return nil, err
	}
	return func() {
		if err := result.File.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}, nil
}

func TestCreateShareMatrixAcrossTwoSMBSessionsAndNonSMBDirectAndHTTP(t *testing.T) {
	classes := []struct {
		name             string
		access, shareBit uint32
	}{{"read", accessReadData, 1}, {"write", accessWriteData, 2}, {"delete", accessDelete, 4}}
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			first := factory(t)
			second := siblingSMBFixture(t, first)
			if err := first.volume.Write(t.Context(), "matrix", []byte("body")); err != nil {
				t.Fatal(err)
			}
			attr, err := first.volume.Stat(t.Context(), "matrix")
			if err != nil {
				t.Fatal(err)
			}
			external, err := first.volume.NewFileSession(t.Context(), fixtureFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := external.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			root := first.tree.authority.identity.Backend.RootNodeID
			for _, incumbent := range classes {
				for share := uint32(0); share < 8; share++ {
					for _, applicant := range classes {
						label := fmt.Sprintf("%s/share%d/%s", incumbent.name, share, applicant.name)
						request := createRequestForTest("matrix", 1, incumbent.access)
						binary.LittleEndian.PutUint32(request.Body[32:], share)
						_, status, firstID := first.connection.createFile(t.Context(), first.session, first.tree, request)
						if status != statusOK {
							t.Fatalf("%s incumbent status=%#x", label, status)
						}
						expected := statusOK
						if share&applicant.shareBit == 0 {
							expected = statusSharingViolation
						}
						_, status, secondID := second.connection.createFile(t.Context(), second.session, second.tree, createRequestForTest("matrix", 1, applicant.access))
						if status != expected {
							t.Fatalf("%s secondSMB status=%#x want=%#x", label, status, expected)
						}
						if status == statusOK {
							closeFixtureHandle(t, second, secondID)
						}
						externalClose, err := nonSMBMatrixOpen(t, external, root, attr.ID, applicant.access, 7)
						if expected == statusOK && err != nil || expected == statusSharingViolation && !errors.Is(err, storage.ErrUseConflict) {
							t.Fatalf("%s external admission err=%v", label, err)
						}
						if externalClose != nil {
							externalClose()
						}
						closeFixtureHandle(t, first, firstID)
						externalClose, err = nonSMBMatrixOpen(t, external, root, attr.ID, incumbent.access, share)
						if err != nil {
							t.Fatalf("%s external incumbent err=%v", label, err)
						}
						for _, endpoint := range []realSMBFixture{first, second} {
							_, status, id := endpoint.connection.createFile(t.Context(), endpoint.session, endpoint.tree, createRequestForTest("matrix", 1, applicant.access))
							if status != expected {
								t.Fatalf("%s reverseSMB session%d status=%#x want=%#x", label, endpoint.session.id, status, expected)
							}
							if status == statusOK {
								closeFixtureHandle(t, endpoint, id)
							}
						}
						externalClose()
						current, err := first.volume.Stat(t.Context(), "matrix")
						if err != nil || current.ID != attr.ID || current.Size != 4 {
							t.Fatalf("%s changed object: %+v %v", label, current, err)
						}
					}
				}
			}
		})
	}
}

func TestCreateMetadataOnlyZeroShareDoesNotDenyDataDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			first := factory(t)
			second := siblingSMBFixture(t, first)
			if err := first.volume.Write(t.Context(), "matrix", []byte("body")); err != nil {
				t.Fatal(err)
			}
			attr, err := first.volume.Stat(t.Context(), "matrix")
			if err != nil {
				t.Fatal(err)
			}
			external, err := first.volume.NewFileSession(t.Context(), fixtureFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := external.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			root := first.tree.authority.identity.Backend.RootNodeID
			for _, access := range []uint32{accessReadAttr, accessWriteEA, accessReadCtrl | accessSync} {
				request := createRequestForTest("matrix", 1, access)
				binary.LittleEndian.PutUint32(request.Body[32:], 0)
				_, status, metadataID := first.connection.createFile(t.Context(), first.session, first.tree, request)
				if status != statusOK {
					t.Fatalf("metadata incumbent access=%x status=%#x", access, status)
				}
				_, status, readerID := second.connection.createFile(t.Context(), second.session, second.tree, createRequestForTest("matrix", 1, accessReadData))
				if status != statusOK {
					t.Fatalf("metadata denied second reader access=%x status=%#x", access, status)
				}
				closeFixtureHandle(t, second, readerID)
				externalClose, err := nonSMBMatrixOpen(t, external, root, attr.ID, accessReadData, 7)
				if err != nil {
					t.Fatalf("metadata denied external reader: %v", err)
				}
				externalClose()
				closeFixtureHandle(t, first, metadataID)
				_, status, readerID = first.connection.createFile(t.Context(), first.session, first.tree, createRequestForTest("matrix", 1, accessReadData))
				if status != statusOK {
					t.Fatalf("reader incumbent status=%#x", status)
				}
				_, status, metadataID = second.connection.createFile(t.Context(), second.session, second.tree, request)
				if status != statusOK {
					t.Fatalf("metadata applicant access=%x status=%#x", access, status)
				}
				closeFixtureHandle(t, second, metadataID)
				externalClose, err = nonSMBMatrixOpen(t, external, root, attr.ID, access, 0)
				if err != nil {
					t.Fatalf("external metadata applicant err=%v", err)
				}
				externalClose()
				closeFixtureHandle(t, first, readerID)
			}
		})
	}
}

func TestSharedAuthorityTreeRetirementDrainsOwnHandlesDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			first := fixture.tree
			first.id, first.sessionID, first.done = 1, fixture.session.id, make(chan struct{})
			second := &tree{kind: volumeTree, id: 2, sessionID: fixture.session.id, export: first.export, authority: first.authority, done: make(chan struct{})}
			first.export.refs, first.export.trees = 2, 2
			first.authority.refs = 2
			first.authority.done = make(chan struct{})
			close(first.authority.done)
			fixture.connection.pending = make(map[uint64]*pendingRequest)
			if err := fixture.volume.Write(t.Context(), "trees", []byte("body")); err != nil {
				t.Fatal(err)
			}
			_, status, firstID := fixture.connection.createFile(t.Context(), fixture.session, first, createRequestForTest("trees", 1, accessReadData))
			if status != statusOK {
				t.Fatalf("first tree status=%#x", status)
			}
			request := createRequestForTest("trees", 1, accessReadData)
			binary.LittleEndian.PutUint32(request.Body[32:], 5)
			_, status, secondID := fixture.connection.createFile(t.Context(), fixture.session, second, request)
			if status != statusOK {
				t.Fatalf("second tree status=%#x", status)
			}
			if err := fixture.connection.closeTreeContext(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if first.findFileHandle(firstID) != nil || first.authority.isClosed() {
				t.Fatal("first tree retained own handle or closed shared authority")
			}
			result, err := second.findFileHandle(secondID).file.ReadAt(t.Context(), 0, 4)
			if err != nil || string(result.Data) != "body" {
				t.Fatalf("sibling lost retained object: %+v %v", result, err)
			}
			if err := fixture.volume.Write(t.Context(), "trees", []byte("blocked")); !errors.Is(err, storage.ErrUseConflict) {
				t.Fatalf("first tree cleanup dropped sibling share claim: %v", err)
			}
			if err := fixture.connection.closeTreeContext(t.Context(), second); err != nil {
				t.Fatal(err)
			}
			if !second.authority.isClosed() || second.findFileHandle(secondID) != nil {
				t.Fatal("last tree retained raw authority or owner")
			}
			if err := fixture.volume.Write(t.Context(), "trees", []byte("after cleanup")); err != nil {
				t.Fatalf("last tree leaked claim: %v", err)
			}
		})
	}
}

func TestCreateDelayedResultCannotPublishAfterCancellationOrRetirement(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		for _, reason := range []string{"cancel", "tree", "session", "deadline"} {
			t.Run(transport+"/"+reason, func(t *testing.T) {
				factory := newRealSMBFixture
				if transport == "http" {
					factory = newHTTPSMBFixture
				}
				fixture := factory(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				probe := &createResultProbe{createSessionContract: fixture.raw.(createSessionContract)}
				probe.afterOpen = func() {
					switch reason {
					case "cancel":
						cancel()
					case "tree":
						fixture.tree.fenceFileWork()
					case "session":
						fixture.session.mu.Lock()
						fixture.session.retired = true
						fixture.session.mu.Unlock()
					case "deadline":
						fixture.tree.authority.installMu.Lock()
						fixture.tree.authority.deadline = time.Now().Add(-time.Second)
						fixture.tree.authority.installMu.Unlock()
					}
				}
				fixture.tree.authority.raw = probe
				request := createRequestForTest("delayed", 2, accessReadData)
				binary.LittleEndian.PutUint32(request.Body[32:], 0)
				_, status, id := fixture.connection.createFile(ctx, fixture.session, fixture.tree, request)
				if status == statusOK || id != (wire.FileID{}) {
					t.Fatalf("published after %s: status=%#x id=%x", reason, status, id)
				}
				if err := fixture.volume.Write(t.Context(), "delayed", []byte("cleanup released")); err != nil {
					t.Fatalf("delayed result cleanup leaked share: %v", err)
				}
			})
		}
	}
}

func TestCreateGenericAllRejectsUnsupportedRightsBeforeEffectsDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			if err := fixture.volume.Write(t.Context(), "existing", []byte("body")); err != nil {
				t.Fatal(err)
			}
			before, err := fixture.volume.Stat(t.Context(), "existing")
			if err != nil {
				t.Fatal(err)
			}
			probe := &createResultProbe{createSessionContract: fixture.raw.(createSessionContract)}
			fixture.tree.authority.raw = probe
			for _, test := range []struct {
				name        string
				disposition uint32
			}{{"existing", 1}, {"missing", 2}, {"existing", 4}, {"", 1}} {
				_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(test.name, test.disposition, accessGenericAll))
				if status != statusUnsupported || id != (wire.FileID{}) {
					t.Fatalf("GENERIC_ALL name=%q disposition=%d status=%#x id=%x", test.name, test.disposition, status, id)
				}
			}
			if len(probe.calls) != 0 {
				t.Fatal("unsupported rights reached atomic open")
			}
			fixture.tree.fileMu.Lock()
			retained := len(fixture.tree.handles)
			fixture.tree.fileMu.Unlock()
			if retained != 0 {
				t.Fatalf("unsupported rights reserved %d owners", retained)
			}
			after, err := fixture.volume.Stat(t.Context(), "existing")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("unsupported rights changed existing metadata: before=%+v after=%+v err=%v", before, after, err)
			}
			if _, err := fixture.volume.Stat(t.Context(), "missing"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("unsupported rights created name: %v", err)
			}
		})
	}
}
