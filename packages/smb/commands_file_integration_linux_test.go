//go:build linux

package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
)

func createRequestForTest(name string, disposition, access uint32) wire.Request {
	encodedName := wire.EncodeUTF16(name)
	packet := make([]byte, 64+56+len(encodedName))
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
	raw, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
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
	export := &Export{server: server, share: Share{Volume: "files", Backend: volume}}
	authority := &authoritySession{raw: raw, export: export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	tree := &tree{kind: volumeTree, export: export, authority: authority}
	s := &session{id: 21}
	c := &connection{server: server}
	return realSMBFixture{volume: volume, raw: raw, state: state, server: server, tree: tree, session: s, connection: c}
}

func TestCreateOpensRealAuthorityAndSharesObjectIdentity(t *testing.T) {
	fixture := newRealSMBFixture(t)
	volume, raw, state, server, tree, s, c := fixture.volume, fixture.raw, fixture.state, fixture.server, fixture.tree, fixture.session, fixture.connection
	first, status, firstID := c.createFile(t.Context(), s, tree, createRequestForTest("file", 3, accessReadData))
	if status != statusOK || len(first) != 88 || firstID == (wire.FileID{}) || binary.LittleEndian.Uint32(first[4:]) != uint32(storage.Created) || binary.LittleEndian.Uint32(first[56:])&dosArchive == 0 {
		t.Fatalf("created file: status=%#x body=%v id=%x", status, first, firstID)
	}
	second, status, secondID := c.createFile(t.Context(), s, tree, createRequestForTest("file", 1, accessReadData))
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
	otherRaw, err := fixture.volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
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
	otherAuthority := &authoritySession{raw: otherRaw, export: fixture.tree.export, actionEpoch: otherState.ActionEpoch, deadline: time.Now().Add(otherState.Remaining)}
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
		created, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest(name, 1, accessReadData))
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
