//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func createContextsRequest(t *testing.T, request wire.Request, contexts ...wire.CreateContext) wire.Request {
	t.Helper()
	packet := make([]byte, (len(request.Packet)+7)&^7)
	copy(packet, request.Packet)
	start := len(packet)
	for index, context := range contexts {
		length := (24 + len(context.Data) + 7) &^ 7
		block := make([]byte, length)
		if index+1 < len(contexts) {
			binary.LittleEndian.PutUint32(block, uint32(length))
		}
		binary.LittleEndian.PutUint16(block[4:], 16)
		binary.LittleEndian.PutUint16(block[6:], uint16(len(context.Name)))
		copy(block[16:20], context.Name)
		if len(context.Data) != 0 {
			binary.LittleEndian.PutUint16(block[10:], 24)
			binary.LittleEndian.PutUint32(block[12:], uint32(len(context.Data)))
			copy(block[24:], context.Data)
		}
		packet = append(packet, block...)
	}
	if len(contexts) != 0 {
		binary.LittleEndian.PutUint32(packet[64+48:], uint32(start))
		binary.LittleEndian.PutUint32(packet[64+52:], uint32(len(packet)-start))
	}
	if err := request.Header.Encode(packet); err != nil {
		t.Fatal(err)
	}
	parsed, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: 1 << 20, MaxCommands: 1, MaxContexts: 16})
	if err != nil {
		t.Fatal(err)
	}
	return parsed[0]
}

func TestSignedCreateBackupQfidUsesOpenedIdentity(t *testing.T) {
	f, client := startRealFileProtocolServer(t)
	if err := f.volume.Mkdir(t.Context(), "folder"); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), "file", []byte("content")); err != nil {
		t.Fatal(err)
	}
	sessionID, key := authenticateProtocol(t, client)
	defer key.Destroy()
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	serial := f.server.exports["DATA"].volumeSerial
	message := uint64(4)
	for _, name := range []string{"", "folder", "file"} {
		attr, err := f.volume.Stat(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		request := createRequestForTest(name, 1, accessReadData|accessReadAttr|accessSync)
		options := createBackupIntent | createSyncNonAlert
		contexts := []wire.CreateContext{{Name: []byte("QFid")}}
		if attr.IsDir() {
			options |= createDirectory
		} else {
			request.Body[3] = 9
			contexts = append([]wire.CreateContext{{Name: []byte("DH2Q"), Data: make([]byte, 32)}}, contexts...)
		}
		binary.LittleEndian.PutUint32(request.Body[40:], options)
		request = createContextsRequest(t, request, contexts...)
		sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Create, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1, Flags: 0x30}, request.Body))
		response := readFrame(t, client)
		header, err := wire.ParseHeader(response)
		if err != nil || header.Status != statusOK || key.Verify(response) != nil || len(response) != 208 || response[66] != 0 {
			t.Fatalf("native-style CREATE %q = %+v length=%d error=%v", name, header, len(response), err)
		}
		body := response[64:]
		if binary.LittleEndian.Uint32(body[80:]) != 152 || binary.LittleEndian.Uint32(body[84:]) != 56 ||
			!bytes.Equal(body[104:108], []byte("QFid")) || binary.LittleEndian.Uint32(body[100:]) != 32 ||
			binary.LittleEndian.Uint64(body[112:]) != attr.ID || binary.LittleEndian.Uint64(body[120:]) != serial ||
			!bytes.Equal(body[128:144], make([]byte, 16)) {
			t.Fatalf("CREATE %q reported invented disk identity: body=%x attr=%+v serial=%#x", name, body, attr, serial)
		}
		var id wire.FileID
		copy(id[:], body[64:80])
		message++
		query := queryInfoRequestForTest(id, wire.FileIdInformation, 24)
		sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.QueryInfo, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, query.Body))
		response = readFrame(t, client)
		header, err = wire.ParseHeader(response)
		if err != nil || header.Status != statusOK || key.Verify(response) != nil || binary.LittleEndian.Uint64(response[72:]) != serial || binary.LittleEndian.Uint64(response[80:]) != attr.ID {
			t.Fatalf("QFid diverged from FileIdInformation: %x, %v", response, err)
		}
		message++
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		copy(closeBody[8:], id[:])
		sendFrame(t, client, signedRequest(t, key, wire.Header{Command: wire.Close, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, closeBody))
		response = readFrame(t, client)
		header, err = wire.ParseHeader(response)
		if err != nil || header.Status != statusOK || key.Verify(response) != nil || f.server.Status().Handles != 0 {
			t.Fatalf("QFid close retained ownership: header=%+v error=%v status=%+v", header, err, f.server.Status())
		}
		message++
	}
}

func TestCreateBackupQfidPreservesAuthorizationAndRejectsMalformedBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name     string
		contexts []wire.CreateContext
		denied   bool
		status   uint32
	}{
		{name: "host denied", contexts: []wire.CreateContext{{Name: []byte("QFid")}}, denied: true, status: statusDenied},
		{name: "nonempty QFid", contexts: []wire.CreateContext{{Name: []byte("QFid"), Data: []byte{1}}}, status: statusInvalid},
		{name: "duplicate QFid", contexts: []wire.CreateContext{{Name: []byte("QFid")}, {Name: []byte("QFid")}}, status: statusInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRealSMBFixture(t)
			calls := 0
			f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
				calls++
				return authz.ErrDenied
			})
			request := createRequestForTest("refused", 2, accessReadData)
			binary.LittleEndian.PutUint32(request.Body[40:], createBackupIntent)
			request = createContextsRequest(t, request, test.contexts...)
			body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
			if status != test.status || body != nil || id != (wire.FileID{}) || (calls != 0) != test.denied || len(f.tree.handles) != 0 {
				t.Fatalf("refused CREATE: status=%#x body=%x id=%x authorization=%d handles=%d", status, body, id, calls, len(f.tree.handles))
			}
			if _, err := f.volume.Stat(t.Context(), "refused"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("refused CREATE changed namespace: %v", err)
			}
		})
	}
}

func TestSignedCreateQfidCompoundBudgetRejectsBeforeEffects(t *testing.T) {
	f := newRealSMBFixture(t)
	// After an eight-byte ECHO response, 359 leaves 207 bytes for CREATE
	// while reserving the existing 80-byte minimum for the final command.
	f.server.config.Limits.MaxFrameBytes = 359
	f.server.sessions = newSessionRegistry()
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	c := newConnection(f.server, left)
	defer c.cancel()
	c.negotiated = true
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	defer key.Destroy()
	f.session.signer, f.session.principal = key, testPrincipal("0123456789abcdef")
	registerSession(t, f.server, c, f.session)
	f.session.trees[f.tree.id] = f.tree
	request := createRequestForTest("budget", 2, accessReadData)
	binary.LittleEndian.PutUint32(request.Body[40:], createBackupIntent)
	request = createContextsRequest(t, request, wire.CreateContext{Name: []byte("QFid")})
	headers := []wire.Header{
		{Command: wire.Echo, MessageID: 1, SessionID: f.session.id, Credits: 1},
		{Command: wire.Create, MessageID: 2, SessionID: f.session.id, TreeID: f.tree.id, Credits: 1},
		{Command: wire.Echo, MessageID: 3, SessionID: f.session.id, Credits: 1},
	}
	packet := compoundRequest(t, key, headers, [][]byte{wire.EmptyResponseBody(), request.Body, wire.EmptyResponseBody()}, false)
	requests, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: 359, MaxCommands: 3, MaxContexts: 16})
	if err != nil {
		t.Fatal(err)
	}
	if responseBudget(requests[1]) != 208 {
		t.Fatalf("QFid response was underbudgeted: %d", responseBudget(requests[1]))
	}
	for _, request := range requests {
		ctx, cancel := context.WithCancel(c.ctx)
		c.pending[request.Header.MessageID] = &pendingRequest{frame: 1, ctx: ctx, cancel: cancel}
	}
	done := make(chan error, 1)
	go func() { done <- c.process(requests) }()
	_ = right.SetDeadline(time.Now().Add(2 * time.Second))
	response := readFrame(t, right)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	offset := 0
	for _, status := range []uint32{statusOK, statusResources, statusOK} {
		header, err := wire.ParseHeader(response[offset:])
		end := len(response)
		if header.NextCommand != 0 {
			end = offset + int(header.NextCommand)
		}
		if err != nil || header.Status != status || key.Verify(response[offset:end]) != nil {
			t.Fatalf("budget compound = %+v, %v", header, err)
		}
		offset = end
	}
	if _, err := f.volume.Stat(t.Context(), "budget"); !errors.Is(err, syscall.ENOENT) || len(f.tree.handles) != 0 {
		t.Fatalf("QFid budget rejection occurred after effects: stat=%v handles=%d", err, len(f.tree.handles))
	}
}
