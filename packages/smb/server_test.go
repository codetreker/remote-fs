package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type testAuthenticator struct{ closed atomic.Int32 }
type testAuthentication struct {
	owner *testAuthenticator
	step  int
}

func (a *testAuthenticator) Begin(context.Context) (Authentication, error) {
	return &testAuthentication{owner: a}, nil
}
func (a *testAuthentication) Step(_ context.Context, token []byte) (AuthenticationResult, error) {
	a.step++
	if a.step == 1 && string(token) == "initial" {
		return AuthenticationResult{Continue: true, Token: []byte("challenge")}, nil
	}
	if a.step == 2 && string(token) == "proof" {
		return AuthenticationResult{Principal: Principal{SID: "S-1-5-21-1", Name: "test"}, SessionKey: []byte("0123456789abcdef")}, nil
	}
	return AuthenticationResult{}, errors.New("bad proof")
}
func (a *testAuthentication) Close() error { a.owner.closed.Add(1); return nil }

func testConfig() Config {
	return Config{Authenticator: &testAuthenticator{}, Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }), Limits: DefaultLimits()}
}

func TestConfigurationAndIdleShutdown(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	c := testConfig()
	c.Limits.MaxFrameBytes = 1
	if _, err := New(c); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	s, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !s.Status().Stopped {
		t.Fatal("not stopped")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "a/b", "a\\b", " a", "a:", "\x00"} {
		if _, err := shareKey(name); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	if a, _ := shareKey("Work"); a != "WORK" {
		t.Fatal(a)
	}
}

func startProtocolServer(t *testing.T) (*Server, net.Conn) {
	t.Helper()
	return startConfiguredServer(t, testConfig())
}

func startConfiguredServer(t *testing.T, config Config) (*Server, net.Conn) {
	t.Helper()
	s, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(context.Background(), l) }()
	c, err := net.Dial("tcp4", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Error("Serve did not drain")
		}
	})
	return s, c
}

func sendFrame(t *testing.T, c net.Conn, packet []byte) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(packet)))
	if _, err := c.Write(append(prefix[:], packet...)); err != nil {
		t.Fatal(err)
	}
}
func readFrame(t *testing.T, c net.Conn) []byte {
	t.Helper()
	var prefix [4]byte
	if _, err := io.ReadFull(c, prefix[:]); err != nil {
		t.Fatal(err)
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n > 2<<20 {
		t.Fatal("oversize response")
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(c, p); err != nil {
		t.Fatal(err)
	}
	return p
}
func requestPacket(h wire.Header, b []byte) []byte {
	p := make([]byte, 64+len(b))
	_ = h.Encode(p)
	copy(p[64:], b)
	return p
}

func negotiatePacket() []byte {
	b := make([]byte, 54)
	binary.LittleEndian.PutUint16(b, 36)
	binary.LittleEndian.PutUint16(b[2:], 1)
	binary.LittleEndian.PutUint16(b[4:], 3)
	binary.LittleEndian.PutUint32(b[28:], 104)
	binary.LittleEndian.PutUint16(b[32:], 1)
	binary.LittleEndian.PutUint16(b[36:], wire.Dialect311)
	binary.LittleEndian.PutUint16(b[40:], 1)
	binary.LittleEndian.PutUint16(b[42:], 6)
	binary.LittleEndian.PutUint16(b[48:], 1)
	binary.LittleEndian.PutUint16(b[52:], 1)
	return requestPacket(wire.Header{Command: wire.Negotiate, Credits: 32}, b)
}
func setupPacket(id, session uint64, token string) []byte {
	b := make([]byte, 24+len(token))
	binary.LittleEndian.PutUint16(b, 25)
	b[3] = 3
	binary.LittleEndian.PutUint16(b[12:], 88)
	binary.LittleEndian.PutUint16(b[14:], uint16(len(token)))
	copy(b[24:], token)
	return requestPacket(wire.Header{Command: wire.SessionSetup, MessageID: id, SessionID: session, Credits: 1}, b)
}

func authenticateProtocol(t *testing.T, c net.Conn) (uint64, *signing.Session) {
	t.Helper()
	return authenticateProtocolAt(t, c, 0)
}

func authenticateProtocolAt(t *testing.T, c net.Conn, firstMessage uint64) (uint64, *signing.Session) {
	t.Helper()
	p := negotiatePacket()
	binary.LittleEndian.PutUint64(p[24:], firstMessage)
	sendFrame(t, c, p)
	response := readFrame(t, c)
	h, err := wire.ParseHeader(response)
	if err != nil || h.Status != 0 || len(response) < 70 || binary.LittleEndian.Uint16(response[68:]) != wire.Dialect311 || binary.LittleEndian.Uint16(response[66:]) != 3 {
		t.Fatalf("negotiate: %x %v", h.Status, err)
	}
	hash := signing.Preauth([64]byte{}, p)
	hash = signing.Preauth(hash, response)
	p = setupPacket(firstMessage+1, 0, "initial")
	hash = signing.Preauth(hash, p)
	sendFrame(t, c, p)
	response = readFrame(t, c)
	hash = signing.Preauth(hash, response)
	h, _ = wire.ParseHeader(response)
	if h.Status != statusMoreProcessing || h.SessionID == 0 {
		t.Fatalf("challenge: %+v", h)
	}
	id := h.SessionID
	p = setupPacket(firstMessage+2, id, "proof")
	hash = signing.Preauth(hash, p)
	key, err := signing.NewSession(hash, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Status != 0 || key.Verify(response) != nil {
		t.Fatalf("authenticated reply: %x", h.Status)
	}
	return id, key
}

func TestProtocolSigningAndCreditWindow(t *testing.T) {
	s, c := startProtocolServer(t)
	id, key := authenticateProtocol(t, c)
	for _, message := range []uint64{5, 3, 4} {
		p := requestPacket(wire.Header{Command: wire.Echo, MessageID: message, SessionID: id, Credits: 1}, wire.EmptyResponseBody())
		if err := key.Sign(p); err != nil {
			t.Fatal(err)
		}
		sendFrame(t, c, p)
		response := readFrame(t, c)
		h, _ := wire.ParseHeader(response)
		if h.Status != 0 || h.MessageID != message || key.Verify(response) != nil {
			t.Fatalf("echo %+v", h)
		}
	}
	if s.config.Authenticator.(*testAuthenticator).closed.Load() != 1 {
		t.Fatal("native security context retained")
	}
	p := requestPacket(wire.Header{Command: wire.Echo, MessageID: 6, SessionID: id, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, c, p)
	response := readFrame(t, c)
	h, _ := wire.ParseHeader(response)
	if h.Status != statusDenied {
		t.Fatalf("unsigned accepted %x", h.Status)
	}
}

func TestProtocolDuplicateMessageDoesNotRunTwice(t *testing.T) {
	_, c := startProtocolServer(t)
	id, key := authenticateProtocol(t, c)
	p := requestPacket(wire.Header{Command: wire.Echo, MessageID: 3, SessionID: id, Credits: 1}, wire.EmptyResponseBody())
	_ = key.Sign(p)
	sendFrame(t, c, p)
	_ = readFrame(t, c)
	sendFrame(t, c, p)
	var prefix [4]byte
	if _, err := io.ReadFull(c, prefix[:]); err == nil {
		t.Fatal("duplicate message accepted")
	}
}

func TestProtocolRejectsOversizeBeforeAllocation(t *testing.T) {
	_, c := startProtocolServer(t)
	_, err := c.Write([]byte{0, 0xff, 0xff, 0xff})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := c.Read(b[:]); err == nil {
		t.Fatal("oversize remained open")
	}
}

func multiProtocolPacket() []byte {
	dialects := []byte("\x02NT LM 0.12\x00\x02SMB 2.002\x00\x02SMB 2.???\x00")
	packet := make([]byte, 35+len(dialects))
	copy(packet, "\xffSMB")
	packet[4] = 0x72
	packet[9] = 0x18
	binary.LittleEndian.PutUint16(packet[10:], 0xc853)
	binary.LittleEndian.PutUint16(packet[33:], uint16(len(dialects)))
	copy(packet[35:], dialects)
	return packet
}

func TestProtocolMultiProtocolBootstrapSignsOnlyFinalTranscript(t *testing.T) {
	_, c := startProtocolServer(t)
	packet := multiProtocolPacket()
	if len(packet)+4 != 73 {
		t.Fatal("unexpected Windows multi-protocol preface size")
	}
	sendFrame(t, c, packet)
	response := readFrame(t, c)
	h, err := wire.ParseHeader(response)
	if err != nil || h.Command != wire.Negotiate || h.Status != 0 || h.MessageID != 0 || h.Credits != 1 || h.Flags != wire.FlagResponse || len(response) != 128 {
		t.Fatalf("bootstrap header=%+v length=%d error=%v", h, len(response), err)
	}
	if binary.LittleEndian.Uint16(response[68:]) != 0x02ff || binary.LittleEndian.Uint16(response[66:]) != 3 || binary.LittleEndian.Uint16(response[120:]) != 128 || binary.LittleEndian.Uint16(response[122:]) != 0 {
		t.Fatal("wildcard response changed the dialect, signing, or empty-token contract")
	}
	id, key := authenticateProtocolAt(t, c, 1)
	p := requestPacket(wire.Header{Command: wire.Echo, MessageID: 4, SessionID: id, Credits: 1}, wire.EmptyResponseBody())
	if err := key.Sign(p); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, c, p)
	response = readFrame(t, c)
	h, _ = wire.ParseHeader(response)
	if h.Status != 0 || key.Verify(response) != nil {
		t.Fatalf("post-bootstrap signed echo=%+v", h)
	}
}

func TestProtocolMultiProtocolBootstrapRejectsInvalidTransitions(t *testing.T) {
	for _, name := range []string{"other SMB1 command", "repeated bootstrap", "session before real negotiate", "old final dialect", "bootstrap after authenticated SMB2"} {
		t.Run(name, func(t *testing.T) {
			_, c := startProtocolServer(t)
			p := multiProtocolPacket()
			switch name {
			case "other SMB1 command":
				p[4] = 0x75
			case "bootstrap after authenticated SMB2":
				_, _ = authenticateProtocol(t, c)
			default:
				sendFrame(t, c, p)
				_ = readFrame(t, c)
				if name == "session before real negotiate" {
					p = setupPacket(1, 0, "initial")
				} else if name == "old final dialect" {
					p = negotiatePacket()
					binary.LittleEndian.PutUint64(p[24:], 1)
					binary.LittleEndian.PutUint16(p[100:], wire.Dialect302)
				}
			}
			sendFrame(t, c, p)
			if name == "old final dialect" {
				h, err := wire.ParseHeader(readFrame(t, c))
				if err != nil || h.Status != statusUnsupported {
					t.Fatalf("older final dialect accepted: %+v %v", h, err)
				}
				return
			}
			var prefix [4]byte
			if _, err := io.ReadFull(c, prefix[:]); err == nil {
				t.Fatal("invalid bootstrap transition returned a response")
			}
		})
	}
}
