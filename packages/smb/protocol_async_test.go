package smb

import (
	"io/fs"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestProtocolAsyncCancelAndResourceRejectionKeepConnectionUsable(t *testing.T) {
	config := testConfig()
	config.Limits.MaxRequests = 1
	config.Limits.MaxCompound = 1
	s, c := startConfiguredServer(t, config)
	f := &sessionFile{commandFile: &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}, NameInfo: storage.WindowsNameInfo{State: storage.WindowsNameRoot}}}, pending: true}
	ws := &backendSession{commandSession: &commandSession{}, cancelState: storage.WindowsActionCancelled}
	ws.open = func(storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		return storage.WindowsOpenResult{File: f, Attr: f.attr, CreateAction: storage.WindowsOpened}, nil
	}
	if _, err := s.Publish(Share{Name: "work", Volume: "trusted", Backend: &sessionBackend{session: ws}, Changes: testNotifySource(testNotifyStream())}); err != nil {
		t.Fatal(err)
	}
	session, key := authenticateProtocol(t, c)
	name := wire.EncodeUTF16("\\\\127.0.0.1\\work")
	body := make([]byte, 8+len(name))
	smbLE.PutUint16(body, 9)
	smbLE.PutUint16(body[4:], 72)
	smbLE.PutUint16(body[6:], uint16(len(name)))
	copy(body[8:], name)
	p := requestPacket(wire.Header{Command: wire.TreeConnect, MessageID: 3, SessionID: session, Credits: 1}, body)
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response := readFrame(t, c)
	h, _ := wire.ParseHeader(response)
	if h.Status != 0 {
		t.Fatalf("tree %x", h.Status)
	}
	tree := h.TreeID
	r := createCommand("", 1, 1)
	p = requestPacket(wire.Header{Command: wire.Create, MessageID: 4, SessionID: session, TreeID: tree, Credits: 1}, r.Body)
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Status != 0 {
		t.Fatalf("open %x", h.Status)
	}
	var id wire.FileID
	copy(id[:], response[128:144])
	body = make([]byte, 48)
	smbLE.PutUint16(body, 48)
	smbLE.PutUint16(body[2:], 1)
	copy(body[8:], id[:])
	smbLE.PutUint64(body[32:], 10)
	smbLE.PutUint32(body[40:], wire.LockExclusive)
	p = requestPacket(wire.Header{Command: wire.Lock, MessageID: 5, SessionID: session, TreeID: tree, Credits: 1}, body)
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Status != statusPending || h.Flags&wire.FlagAsync == 0 || h.Credits != 1 {
		t.Fatalf("pending %+v", h)
	}
	p = requestPacket(wire.Header{Command: wire.Echo, MessageID: 6, SessionID: session, Credits: 1}, wire.EmptyResponseBody())
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Status != statusResources || key.Verify(response) != nil {
		t.Fatalf("capacity %+v", h)
	}
	p = requestPacket(wire.Header{Command: wire.Cancel, MessageID: 0, SessionID: session, Flags: wire.FlagAsync, AsyncID: 6}, wire.EmptyResponseBody())
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Command != wire.Lock || h.Status != statusCancelled || h.Credits != 0 || h.AsyncID != 6 || key.Verify(response) != nil {
		t.Fatalf("final %+v", h)
	}
	p = requestPacket(wire.Header{Command: wire.Echo, MessageID: 7, SessionID: session, Credits: 1}, wire.EmptyResponseBody())
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Status != 0 {
		t.Fatalf("echo after cancel %+v", h)
	}
}

func TestProtocolReauthenticationKeepsExistingSigningKey(t *testing.T) {
	_, c := startProtocolServer(t)
	session, key := authenticateProtocol(t, c)
	for i, token := range []string{"initial", "proof"} {
		p := setupPacket(uint64(3+i), session, token)
		_ = key.Sign(p)
		sendFrame(t, c, p)
		response := readFrame(t, c)
		h, _ := wire.ParseHeader(response)
		want := statusMoreProcessing
		if i == 1 {
			want = 0
		}
		if h.Status != want || key.Verify(response) != nil {
			t.Fatalf("reauth %+v", h)
		}
	}
	p := requestPacket(wire.Header{Command: wire.Echo, MessageID: 5, SessionID: session, Credits: 1}, wire.EmptyResponseBody())
	_ = key.Sign(p)
	sendFrame(t, c, p)
	response := readFrame(t, c)
	h, _ := wire.ParseHeader(response)
	if h.Status != 0 || key.Verify(response) != nil {
		t.Fatalf("post-reauth %+v", h)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteCreditChargeExcludesProtocolHeader(t *testing.T) {
	r := writeCommand(wire.FileID{1}, make([]byte, 65536))
	if got := requiredCredits(r); got != 1 {
		t.Fatalf("64 KiB requires %d credits", got)
	}
	r = writeCommand(wire.FileID{1}, make([]byte, 65537))
	if got := requiredCredits(r); got != 2 {
		t.Fatalf("64 KiB + 1 requires %d credits", got)
	}
}
