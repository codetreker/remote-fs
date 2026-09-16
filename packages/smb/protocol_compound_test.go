package smb

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type protocolFile struct {
	*sessionFile
	reads, closes, writes atomic.Int32
}

func (f *protocolFile) ReadAt(ctx context.Context, offset int64, n int) (storage.FileRead, error) {
	f.reads.Add(1)
	return f.commandFile.ReadAt(ctx, offset, n)
}
func (f *protocolFile) Close(ctx context.Context, id windowsActionID) (windowsActionResult, error) {
	f.closes.Add(1)
	return f.commandFile.Close(ctx, id)
}
func (f *protocolFile) WriteAt(ctx context.Context, offset int64, b []byte, id windowsActionID) (windowsActionResult, error) {
	f.writes.Add(1)
	return f.commandFile.WriteAt(ctx, offset, b, id)
}

func compoundProtocol(t *testing.T, pending bool) (net.Conn, uint64, uint32, *signing.Session, *protocolFile) {
	t.Helper()
	s, c := startProtocolServer(t)
	f := &protocolFile{sessionFile: &sessionFile{commandFile: &commandFile{attr: windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 3}}, NameInfo: windowsNameInfo{State: windowsNameLinked, Path: "file"}}, data: []byte("abc"), result: windowsActionResult{State: windowsActionCompleted}}, pending: pending}}
	ws := &backendSession{commandSession: &commandSession{}, cancelState: windowsActionCancelled}
	ws.open = func(r windowsOpenRequest) (windowsOpenResult, error) {
		if r.Kind == windowsDirectory {
			root := &commandFile{attr: windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}}}
			return windowsOpenResult{File: root, Attr: root.attr}, nil
		}
		return windowsOpenResult{File: f, Attr: f.attr, CreateAction: windowsOpened}, nil
	}
	if _, err := s.publish(Share{Name: "work", Volume: "trusted", Changes: testNotifySource(testNotifyStream())}, &sessionBackend{session: ws}); err != nil {
		t.Fatal(err)
	}
	sid, key := authenticateProtocol(t, c)
	name := wire.EncodeUTF16("\\\\127.0.0.1\\work")
	b := make([]byte, 8+len(name))
	smbLE.PutUint16(b, 9)
	smbLE.PutUint16(b[4:], 72)
	smbLE.PutUint16(b[6:], uint16(len(name)))
	copy(b[8:], name)
	p := requestPacket(wire.Header{Command: wire.TreeConnect, MessageID: 3, SessionID: sid, Credits: 1}, b)
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response := readFrame(t, c)
	h, _ := wire.ParseHeader(response)
	if h.Status != 0 {
		t.Fatal(h.Status)
	}
	return c, sid, h.TreeID, key, f
}

func sendCompound(t *testing.T, c net.Conn, key *signing.Session, sid uint64, tree uint32, first uint64, requests ...wire.Request) {
	t.Helper()
	var packet []byte
	for i, r := range requests {
		h := r.Header
		h.MessageID = first + uint64(i)
		h.SessionID = sid
		h.TreeID = tree
		h.Credits = 1
		body := append([]byte(nil), r.Body...)
		if i > 0 {
			h.Flags |= wire.FlagRelated
			h.SessionID = ^uint64(0)
			h.TreeID = ^uint32(0)
		}
		if i+1 < len(requests) {
			body = append(body, make([]byte, (-(64+len(body)))&7)...)
			h.NextCommand = uint32(64 + len(body))
		}
		p := requestPacket(h, body)
		if err := key.Sign(p); err != nil {
			t.Fatal(err)
		}
		packet = append(packet, p...)
	}
	sendFrame(t, c, packet)
}

func responseHeaders(t *testing.T, key *signing.Session, packet []byte) []wire.Header {
	t.Helper()
	var headers []wire.Header
	for len(packet) > 0 {
		h, err := wire.ParseHeader(packet)
		if err != nil {
			t.Fatal(err)
		}
		n := len(packet)
		if h.NextCommand != 0 {
			n = int(h.NextCommand)
		}
		if err := key.Verify(packet[:n]); err != nil {
			t.Fatal(err)
		}
		if len(headers) > 0 && h.Flags&wire.FlagRelated == 0 {
			t.Fatal("compound response lost RELATED")
		}
		headers = append(headers, h)
		packet = packet[n:]
	}
	return headers
}

func TestPendingCompoundPreservesPrefixAndCancelsDependentOperations(t *testing.T) {
	c, sid, tree, key, f := compoundProtocol(t, true)
	lock := make([]byte, 48)
	smbLE.PutUint16(lock, 48)
	smbLE.PutUint16(lock[2:], 1)
	for i := 8; i < 24; i++ {
		lock[i] = 255
	}
	smbLE.PutUint64(lock[32:], 10)
	smbLE.PutUint32(lock[40:], wire.LockExclusive)
	sendCompound(t, c, key, sid, tree, 4, createCommand("file", 3, 1), fileRequest(wire.Lock, lock), readCommand(wire.FileID{99}, 0, 3))
	for _, message := range []uint64{5, 6} {
		p := readFrame(t, c)
		h, _ := wire.ParseHeader(p)
		if h.MessageID != message || h.Status != statusPending || h.AsyncID != message+1 || h.Credits != 1 {
			t.Fatalf("interim %+v", h)
		}
	}
	cancel := requestPacket(wire.Header{Command: wire.Cancel, MessageID: 0, SessionID: sid, Flags: wire.FlagAsync, AsyncID: 6}, wire.EmptyResponseBody())
	_ = key.Sign(cancel)
	sendFrame(t, c, cancel)
	packet := readFrame(t, c)
	headers := responseHeaders(t, key, packet)
	if len(headers) != 3 || headers[0].Status != 0 || headers[0].Flags&wire.FlagAsync != 0 {
		t.Fatalf("prefix %+v", headers)
	}
	for i := 1; i < 3; i++ {
		if headers[i].Status != statusCancelled || headers[i].Flags&wire.FlagAsync == 0 || headers[i].Credits != 0 || headers[i].AsyncID != uint64(i+5) {
			t.Fatalf("dependent %+v", headers[i])
		}
	}
	if f.reads.Load() != 0 {
		t.Fatal("cancelled dependent READ reached backend")
	}
}

func TestRelatedCloseUsesPreviousWriteFileID(t *testing.T) {
	c, sid, tree, key, f := compoundProtocol(t, false)
	r := createCommand("file", 3, 1)
	p := requestPacket(wire.Header{Command: wire.Create, MessageID: 4, SessionID: sid, TreeID: tree, Credits: 1}, r.Body)
	_ = key.Sign(p)
	sendFrame(t, c, p)
	opened := readFrame(t, c)
	h, _ := wire.ParseHeader(opened)
	if h.Status != 0 {
		t.Fatal(h.Status)
	}
	var id wire.FileID
	copy(id[:], opened[128:144])
	closeBody := make([]byte, 24)
	smbLE.PutUint16(closeBody, 24)
	closeBody[8] = 222
	sendCompound(t, c, key, sid, tree, 5, writeCommand(id, []byte("new")), fileRequest(wire.Close, closeBody))
	headers := responseHeaders(t, key, readFrame(t, c))
	if len(headers) != 2 || headers[0].Status != 0 || headers[1].Status != 0 || f.writes.Load() != 1 || f.closes.Load() != 1 {
		t.Fatalf("WRITE/CLOSE %+v writes%d closes%d", headers, f.writes.Load(), f.closes.Load())
	}
}
