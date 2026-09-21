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
	"github.com/codetreker/remote-fs/packages/storage"
)

type protocolAuthenticator struct {
	begins atomic.Int32
	closed atomic.Int32
}

type protocolAuthentication struct {
	owner *protocolAuthenticator
	step  int
}

func (a *protocolAuthenticator) Begin(context.Context) (Authentication, error) {
	a.begins.Add(1)
	return &protocolAuthentication{owner: a}, nil
}

func (a *protocolAuthentication) Step(_ context.Context, token []byte) (AuthenticationResult, error) {
	a.step++
	if a.step == 1 && string(token) == "initial" {
		return AuthenticationResult{Continue: true, Token: []byte("challenge")}, nil
	}
	if a.step == 2 && string(token) == "proof" {
		return AuthenticationResult{
			Principal: testPrincipal("0123456789abcdef"), SessionKey: []byte("0123456789abcdef"),
			ExpiresAt: time.Now().Add(time.Hour),
		}, nil
	}
	return AuthenticationResult{}, errors.New("bad proof")
}

func (a *protocolAuthentication) Close() error {
	a.owner.closed.Add(1)
	return nil
}

func startProtocolServer(t *testing.T, limits Limits) (*Server, *endpointStorage, net.Conn) {
	t.Helper()
	authenticator := &protocolAuthenticator{}
	config := Config{
		Authenticator:     authenticator,
		AuthorizeIdentity: IdentityAuthorizerFunc(func(context.Context, Principal) error { return nil }),
		Authorize: authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
			principal, ok := PrincipalFromContext(ctx)
			if !ok || !principal.SameIdentity(testPrincipal("0123456789abcdef")) {
				return authz.ErrDenied
			}
			return nil
		}),
		Limits: limits,
	}
	return startConfiguredProtocolServer(t, config)
}

func startConfiguredProtocolServer(t *testing.T, config Config) (*Server, *endpointStorage, net.Conn) {
	t.Helper()
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	backend := &endpointStorage{session: newEndpointFileSession()}
	if _, err := server.Publish(Share{Name: "data", Volume: "volume", Backend: backend}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background(), listener) }()
	client, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
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
	return server, backend, client
}

func sendFrame(t *testing.T, connection net.Conn, packet []byte) {
	t.Helper()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4+len(packet))
	binary.BigEndian.PutUint32(frame, uint32(len(packet)))
	copy(frame[4:], packet)
	if _, err := connection.Write(frame); err != nil {
		t.Fatal(err)
	}
}

func readFrame(t *testing.T, connection net.Conn) []byte {
	t.Helper()
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		t.Fatal(err)
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length > 2<<20 {
		t.Fatalf("oversized response: %d", length)
	}
	packet := make([]byte, length)
	if _, err := io.ReadFull(connection, packet); err != nil {
		t.Fatal(err)
	}
	return packet
}

func requestPacket(header wire.Header, body []byte) []byte {
	packet := make([]byte, wire.HeaderSize+len(body))
	_ = header.Encode(packet)
	copy(packet[wire.HeaderSize:], body)
	return packet
}

func negotiatePacket() []byte {
	body := make([]byte, 54)
	binary.LittleEndian.PutUint16(body, 36)
	binary.LittleEndian.PutUint16(body[2:], 1)
	binary.LittleEndian.PutUint16(body[4:], 3)
	binary.LittleEndian.PutUint32(body[28:], 104)
	binary.LittleEndian.PutUint16(body[32:], 1)
	binary.LittleEndian.PutUint16(body[36:], wire.Dialect311)
	binary.LittleEndian.PutUint16(body[40:], wire.ContextPreauthIntegrity)
	binary.LittleEndian.PutUint16(body[42:], 6)
	binary.LittleEndian.PutUint16(body[48:], 1)
	binary.LittleEndian.PutUint16(body[52:], wire.HashSHA512)
	return requestPacket(wire.Header{Command: wire.Negotiate, Credits: 32}, body)
}

func negotiatePacketWithSigningContext() []byte {
	body := make([]byte, 68)
	binary.LittleEndian.PutUint16(body, 36)
	binary.LittleEndian.PutUint16(body[2:], 1)
	binary.LittleEndian.PutUint16(body[4:], 3)
	binary.LittleEndian.PutUint32(body[28:], 104)
	binary.LittleEndian.PutUint16(body[32:], 2)
	binary.LittleEndian.PutUint16(body[36:], wire.Dialect311)
	binary.LittleEndian.PutUint16(body[40:], wire.ContextPreauthIntegrity)
	binary.LittleEndian.PutUint16(body[42:], 6)
	binary.LittleEndian.PutUint16(body[48:], 1)
	binary.LittleEndian.PutUint16(body[52:], wire.HashSHA512)
	binary.LittleEndian.PutUint16(body[56:], wire.ContextSigning)
	binary.LittleEndian.PutUint16(body[58:], 4)
	binary.LittleEndian.PutUint16(body[64:], 1)
	binary.LittleEndian.PutUint16(body[66:], wire.SigningAESCMAC)
	return requestPacket(wire.Header{Command: wire.Negotiate, Credits: 32}, body)
}

func TestNegotiateOnlyEchoesRequestedSigningContext(t *testing.T) {
	for _, test := range []struct {
		name       string
		packet     []byte
		contexts   uint16
		secondType uint16
	}{
		{name: "default CMAC", packet: negotiatePacket(), contexts: 1},
		{name: "explicit CMAC", packet: negotiatePacketWithSigningContext(), contexts: 2, secondType: wire.ContextSigning},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, connection := testConnection(t, DefaultLimits())
			requests, err := wire.ParseFrame(test.packet, wire.Limits{MaxBytes: 1 << 20, MaxCommands: 1, MaxContexts: 4})
			if err != nil {
				t.Fatal(err)
			}
			body, status := connection.negotiate(requests[0])
			if status != statusOK || binary.LittleEndian.Uint16(body[6:8]) != test.contexts {
				t.Fatalf("negotiate status=%#x contexts=%d", status, binary.LittleEndian.Uint16(body[6:8]))
			}
			if test.secondType != 0 {
				offset := int(binary.LittleEndian.Uint32(body[60:64])) - wire.HeaderSize
				firstLength := int(binary.LittleEndian.Uint16(body[offset+2 : offset+4]))
				offset = (offset + 8 + firstLength + 7) &^ 7
				if got := binary.LittleEndian.Uint16(body[offset : offset+2]); got != test.secondType {
					t.Fatalf("second response context = %#x", got)
				}
			}
		})
	}
}

func setupPacket(message, session uint64, token string) []byte {
	body := make([]byte, 24+len(token))
	binary.LittleEndian.PutUint16(body, 25)
	body[3] = 3
	binary.LittleEndian.PutUint16(body[12:], 88)
	binary.LittleEndian.PutUint16(body[14:], uint16(len(token)))
	copy(body[24:], token)
	return requestPacket(wire.Header{Command: wire.SessionSetup, MessageID: message, SessionID: session, Credits: 1}, body)
}

func treeConnectPacket(message, session uint64, path string) []byte {
	encoded := wire.EncodeUTF16(path)
	body := make([]byte, 8+len(encoded))
	binary.LittleEndian.PutUint16(body, 9)
	binary.LittleEndian.PutUint16(body[4:], 72)
	binary.LittleEndian.PutUint16(body[6:], uint16(len(encoded)))
	copy(body[8:], encoded)
	return requestPacket(wire.Header{Command: wire.TreeConnect, MessageID: message, SessionID: session, Credits: 1}, body)
}

func authenticateProtocol(t *testing.T, connection net.Conn) (uint64, *signing.Session) {
	t.Helper()
	packet := negotiatePacket()
	sendFrame(t, connection, packet)
	response := readFrame(t, connection)
	header, err := wire.ParseHeader(response)
	if err != nil || header.Status != statusOK || len(response) < 92 ||
		binary.LittleEndian.Uint16(response[68:]) != wire.Dialect311 || binary.LittleEndian.Uint16(response[66:]) != 3 {
		t.Fatalf("negotiate header=%+v length=%d error=%v", header, len(response), err)
	}
	hash := signing.Preauth([64]byte{}, packet)
	hash = signing.Preauth(hash, response)
	packet = setupPacket(1, 0, "initial")
	hash = signing.Preauth(hash, packet)
	sendFrame(t, connection, packet)
	response = readFrame(t, connection)
	hash = signing.Preauth(hash, response)
	header, _ = wire.ParseHeader(response)
	if header.Status != statusMoreProcessing || header.SessionID == 0 {
		t.Fatalf("challenge = %+v", header)
	}
	packet = setupPacket(2, header.SessionID, "proof")
	hash = signing.Preauth(hash, packet)
	key, err := signing.NewSession(hash, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	sendFrame(t, connection, packet)
	response = readFrame(t, connection)
	final, err := wire.ParseHeader(response)
	if err != nil || final.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("authenticated response = %+v, %v", final, err)
	}
	return header.SessionID, key
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

func TestMultiProtocolBootstrapAdvertisesOnlySMB2Negotiation(t *testing.T) {
	_, _, connection := startProtocolServer(t, DefaultLimits())
	sendFrame(t, connection, multiProtocolPacket())
	response := readFrame(t, connection)
	header, err := wire.ParseHeader(response)
	if err != nil || header.Command != wire.Negotiate || header.Status != statusOK ||
		header.MessageID != 0 || header.Credits != 1 || header.Flags != wire.FlagResponse || len(response) != 128 {
		t.Fatalf("bootstrap header=%+v length=%d error=%v", header, len(response), err)
	}
	if binary.LittleEndian.Uint16(response[68:]) != wire.DialectWildcard ||
		binary.LittleEndian.Uint16(response[66:]) != 3 || binary.LittleEndian.Uint16(response[120:]) != 128 ||
		binary.LittleEndian.Uint16(response[122:]) != 0 {
		t.Fatal("bootstrap response changed the wildcard or signing-only contract")
	}
}

func TestNegotiateStateViolationsFailClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		flags  uint32
		repeat bool
	}{
		{name: "async", flags: wire.FlagAsync},
		{name: "replay", flags: wire.FlagReplay},
		{name: "DFS", flags: wire.FlagDFS},
		{name: "repeated", repeat: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, connection := startProtocolServer(t, DefaultLimits())
			if test.repeat {
				sendFrame(t, connection, negotiatePacket())
				_ = readFrame(t, connection)
			}
			packet := negotiatePacket()
			binary.LittleEndian.PutUint32(packet[16:20], test.flags)
			if test.repeat {
				binary.LittleEndian.PutUint64(packet[24:32], 1)
			}
			sendFrame(t, connection, packet)
			var one [1]byte
			if _, err := connection.Read(one[:]); err == nil {
				t.Fatal("invalid NEGOTIATE received a response")
			}
		})
	}
}

func signedRequest(t *testing.T, key *signing.Session, header wire.Header, body []byte) []byte {
	t.Helper()
	packet := requestPacket(header, body)
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	return packet
}

func connectProtocolTree(t *testing.T, connection net.Conn, key *signing.Session, sessionID, message uint64, share string) uint32 {
	t.Helper()
	packet := treeConnectPacket(message, sessionID, `\\localhost\`+share)
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, connection, packet)
	response := readFrame(t, connection)
	header, _ := wire.ParseHeader(response)
	if header.Status != statusOK || header.TreeID == 0 || key.Verify(response) != nil {
		t.Fatalf("tree connect = %+v", header)
	}
	return header.TreeID
}

func compoundRequest(t *testing.T, key *signing.Session, headers []wire.Header, bodies [][]byte, related bool) []byte {
	t.Helper()
	var frame []byte
	for index := range headers {
		body := append([]byte(nil), bodies[index]...)
		if index+1 < len(headers) {
			padding := (-(wire.HeaderSize + len(body))) & 7
			body = append(body, make([]byte, padding)...)
			headers[index].NextCommand = uint32(wire.HeaderSize + len(body))
		}
		if related && index > 0 {
			headers[index].Flags |= wire.FlagRelated
		}
		packet := requestPacket(headers[index], body)
		if err := key.Sign(packet); err != nil {
			t.Fatal(err)
		}
		frame = append(frame, packet...)
	}
	return frame
}

func TestHandshakeCommandsInAnyCompoundPositionFailBeforeDispatch(t *testing.T) {
	for _, command := range []uint16{wire.Negotiate, wire.SessionSetup} {
		for _, position := range []int{1, 2} {
			for _, related := range []bool{false, true} {
				name := map[uint16]string{wire.Negotiate: "negotiate", wire.SessionSetup: "session-setup"}[command]
				style := "unrelated"
				if related {
					style = "related"
				}
				t.Run(name+"-position-"+string(rune('1'+position))+"-"+style, func(t *testing.T) {
					_, backend, connection := startProtocolServer(t, DefaultLimits())
					sessionID, key := authenticateProtocol(t, connection)
					commands := []uint16{wire.TreeConnect, command}
					bodies := [][]byte{treeConnectPacket(0, 0, `\\localhost\data`)[wire.HeaderSize:], nil}
					if command == wire.Negotiate {
						bodies[1] = negotiatePacket()[wire.HeaderSize:]
					} else {
						bodies[1] = setupPacket(0, 0, "initial")[wire.HeaderSize:]
					}
					if position == 2 {
						commands = []uint16{wire.Echo, wire.TreeConnect, command}
						bodies = [][]byte{wire.EmptyResponseBody(), bodies[0], bodies[1]}
					}
					headers := make([]wire.Header, len(commands))
					for index, current := range commands {
						headers[index] = wire.Header{Command: current, MessageID: uint64(3 + index), SessionID: sessionID, Credits: 1}
					}
					packet := compoundRequest(t, key, headers, bodies, related)
					sendFrame(t, connection, packet)
					var one [1]byte
					if _, err := connection.Read(one[:]); err == nil {
						t.Fatal("handshake compound received a response")
					}
					if opens := backend.sessionOpens.Load(); opens != 0 {
						t.Fatalf("handshake compound dispatched %d tree opens", opens)
					}
				})
			}
		}
	}
}

func TestAuthenticatedControlTranscriptAndUnsupportedCommands(t *testing.T) {
	server, backend, connection := startProtocolServer(t, DefaultLimits())
	sessionID, key := authenticateProtocol(t, connection)

	echo := signedRequest(t, key, wire.Header{Command: wire.Echo, MessageID: 3, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, echo)
	response := readFrame(t, connection)
	header, _ := wire.ParseHeader(response)
	if header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("echo = %+v", header)
	}

	connect := treeConnectPacket(4, sessionID, `\\localhost\data`)
	if err := key.Sign(connect); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, connection, connect)
	response = readFrame(t, connection)
	header, _ = wire.ParseHeader(response)
	if header.Status != statusOK || header.TreeID == 0 || key.Verify(response) != nil {
		t.Fatalf("tree connect = %+v", header)
	}
	treeID := header.TreeID

	for index, command := range []uint16{wire.Create, wire.Close, wire.Flush, wire.Read, wire.Write, wire.Lock, wire.QueryDirectory, wire.ChangeNotify, wire.QueryInfo, wire.SetInfo} {
		packet := signedRequest(t, key, wire.Header{Command: command, MessageID: uint64(5 + index), SessionID: sessionID, TreeID: treeID, Credits: 1}, []byte{2, 0})
		sendFrame(t, connection, packet)
		response = readFrame(t, connection)
		header, _ = wire.ParseHeader(response)
		if header.Status != statusUnsupported || key.Verify(response) != nil {
			t.Fatalf("command %d = %+v", command, header)
		}
	}
	if calls := backend.dataCalls.Load(); calls != 0 {
		t.Fatalf("unsupported commands reached backend %d times", calls)
	}

	message := uint64(15)
	disconnect := signedRequest(t, key, wire.Header{Command: wire.TreeDisconnect, MessageID: message, SessionID: sessionID, TreeID: treeID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, disconnect)
	response = readFrame(t, connection)
	header, _ = wire.ParseHeader(response)
	if header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("tree disconnect = %+v", header)
	}
	message++
	logoff := signedRequest(t, key, wire.Header{Command: wire.Logoff, MessageID: message, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, logoff)
	response = readFrame(t, connection)
	header, _ = wire.ParseHeader(response)
	if header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("logoff = %+v", header)
	}
	deadline := time.Now().Add(time.Second)
	for server.Status().Sessions != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := server.Status(); status.Sessions != 0 || status.Trees != 0 {
		t.Fatalf("retained control state = %+v", status)
	}
}

func TestUnsignedAndTamperedPostAuthenticationRequestsAreRejected(t *testing.T) {
	for _, test := range []struct {
		name           string
		mutate         func([]byte)
		responseSigned bool
	}{
		{name: "unsigned", mutate: func([]byte) {}},
		{name: "tampered", mutate: func(packet []byte) { packet[len(packet)-1] ^= 1 }, responseSigned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, connection := startProtocolServer(t, DefaultLimits())
			sessionID, key := authenticateProtocol(t, connection)
			packet := requestPacket(wire.Header{Command: wire.Echo, MessageID: 3, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
			if test.name == "tampered" {
				if err := key.Sign(packet); err != nil {
					t.Fatal(err)
				}
			}
			test.mutate(packet)
			sendFrame(t, connection, packet)
			response := readFrame(t, connection)
			header, _ := wire.ParseHeader(response)
			if header.Status != statusDenied || (header.Flags&wire.FlagSigned != 0) != test.responseSigned {
				t.Fatalf("response = %+v", header)
			}
			if test.responseSigned && key.Verify(response) != nil {
				t.Fatal("tampered request denial was not signed")
			}
		})
	}
}

func TestTamperedReauthenticationGetsSignedDenial(t *testing.T) {
	_, _, connection := startProtocolServer(t, DefaultLimits())
	sessionID, key := authenticateProtocol(t, connection)
	packet := signedRequest(t, key, wire.Header{Command: wire.SessionSetup, MessageID: 3, SessionID: sessionID, Credits: 1}, setupPacket(0, 0, "initial")[wire.HeaderSize:])
	packet[len(packet)-1] ^= 1
	sendFrame(t, connection, packet)
	response := readFrame(t, connection)
	header, _ := wire.ParseHeader(response)
	if header.Status != statusDenied || key.Verify(response) != nil {
		t.Fatalf("tampered reauthentication = %+v", header)
	}
	echo := signedRequest(t, key, wire.Header{Command: wire.Echo, MessageID: 4, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, echo)
	response = readFrame(t, connection)
	header, _ = wire.ParseHeader(response)
	if header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("old session after tampered reauthentication = %+v", header)
	}
}

func TestSignedMalformedReauthenticationGetsSignedInvalidParameter(t *testing.T) {
	_, _, connection := startProtocolServer(t, DefaultLimits())
	sessionID, key := authenticateProtocol(t, connection)
	packet := setupPacket(3, sessionID, "initial")
	binary.LittleEndian.PutUint16(packet[wire.HeaderSize+12:], wire.HeaderSize+8)
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, connection, packet)
	response := readFrame(t, connection)
	header, _ := wire.ParseHeader(response)
	if header.Status != statusInvalid || key.Verify(response) != nil {
		t.Fatalf("malformed reauthentication = %+v", header)
	}
}

func TestSessionlessEchoSucceedsBeforeAndAfterAuthentication(t *testing.T) {
	t.Run("after negotiate", func(t *testing.T) {
		_, _, connection := startProtocolServer(t, DefaultLimits())
		sendFrame(t, connection, negotiatePacket())
		_ = readFrame(t, connection)
		packet := requestPacket(wire.Header{Command: wire.Echo, MessageID: 1, Credits: 1}, wire.EmptyResponseBody())
		sendFrame(t, connection, packet)
		response := readFrame(t, connection)
		header, _ := wire.ParseHeader(response)
		if header.Status != statusOK || header.Flags&wire.FlagSigned != 0 {
			t.Fatalf("sessionless echo = %+v", header)
		}
	})
	t.Run("with another session established", func(t *testing.T) {
		_, _, connection := startProtocolServer(t, DefaultLimits())
		_, _ = authenticateProtocol(t, connection)
		packet := requestPacket(wire.Header{Command: wire.Echo, MessageID: 3, Credits: 1}, wire.EmptyResponseBody())
		sendFrame(t, connection, packet)
		response := readFrame(t, connection)
		header, _ := wire.ParseHeader(response)
		if header.Status != statusOK || header.Flags&wire.FlagSigned != 0 {
			t.Fatalf("sessionless echo = %+v", header)
		}
	})
}

func TestSessionlessEchoDoesNotBypassHeaderFlagValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		flags  uint32
		status uint32
	}{
		{name: "replay", flags: wire.FlagReplay, status: statusUnsupported},
		{name: "DFS", flags: wire.FlagDFS, status: statusUnsupported},
		{name: "signed without session", flags: wire.FlagSigned, status: statusInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, connection := startProtocolServer(t, DefaultLimits())
			sendFrame(t, connection, negotiatePacket())
			_ = readFrame(t, connection)
			packet := requestPacket(wire.Header{Command: wire.Echo, MessageID: 1, Credits: 1, Flags: test.flags}, wire.EmptyResponseBody())
			sendFrame(t, connection, packet)
			response := readFrame(t, connection)
			header, _ := wire.ParseHeader(response)
			if header.Status != test.status || header.Flags&wire.FlagSigned != 0 {
				t.Fatalf("response = %+v", header)
			}
		})
	}
}

func TestInitialSessionSetupRejectsForbiddenFlagsBeforeAuthentication(t *testing.T) {
	for _, test := range []struct {
		name   string
		flags  uint32
		status uint32
	}{
		{name: "signed", flags: wire.FlagSigned, status: statusInvalid},
		{name: "replay", flags: wire.FlagReplay, status: statusUnsupported},
		{name: "DFS", flags: wire.FlagDFS, status: statusUnsupported},
		{name: "async", flags: wire.FlagAsync, status: statusInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, _, connection := startProtocolServer(t, DefaultLimits())
			sendFrame(t, connection, negotiatePacket())
			_ = readFrame(t, connection)
			packet := setupPacket(1, 0, "initial")
			binary.LittleEndian.PutUint32(packet[16:20], test.flags)
			sendFrame(t, connection, packet)
			response := readFrame(t, connection)
			header, _ := wire.ParseHeader(response)
			if header.Status != test.status || header.SessionID != 0 {
				t.Fatalf("response = %+v", header)
			}
			authenticator := server.config.Authenticator.(*protocolAuthenticator)
			if authenticator.begins.Load() != 0 || server.Status().Sessions != 0 {
				t.Fatalf("forbidden setup reached authentication: begins=%d status=%+v", authenticator.begins.Load(), server.Status())
			}
		})
	}
}

func TestCompoundResponsesPreserveRequestRelatedStyle(t *testing.T) {
	for _, related := range []bool{false, true} {
		name := "unrelated"
		if related {
			name = "related"
		}
		t.Run(name, func(t *testing.T) {
			_, _, connection := startProtocolServer(t, DefaultLimits())
			sessionID, key := authenticateProtocol(t, connection)
			headers := []wire.Header{
				{Command: wire.Echo, MessageID: 3, SessionID: sessionID, Credits: 1},
				{Command: wire.Echo, MessageID: 4, SessionID: sessionID, Credits: 1},
			}
			packet := compoundRequest(t, key, headers, [][]byte{wire.EmptyResponseBody(), wire.EmptyResponseBody()}, related)
			sendFrame(t, connection, packet)
			response := readFrame(t, connection)
			first, err := wire.ParseHeader(response)
			if err != nil || first.NextCommand == 0 || first.Flags&wire.FlagRelated != 0 {
				t.Fatalf("first response = %+v, %v", first, err)
			}
			offset := int(first.NextCommand)
			second, err := wire.ParseHeader(response[offset:])
			if err != nil || (second.Flags&wire.FlagRelated != 0) != related {
				t.Fatalf("second response = %+v, %v", second, err)
			}
			if key.Verify(response[:offset]) != nil || key.Verify(response[offset:]) != nil {
				t.Fatal("compound response signature failed")
			}
		})
	}
}

func TestDirectTCPFrameBoundsCloseTheConnection(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxIOBytes = 65536
	limits.MaxFrameBytes = 131072
	for _, test := range []struct {
		name   string
		prefix [4]byte
		body   []byte
	}{
		{name: "nonzero transport type", prefix: [4]byte{1, 0, 0, 64}},
		{name: "below direct minimum", prefix: [4]byte{0, 0, 0, 34}},
		{name: "short SMB2 header", prefix: [4]byte{0, 0, 0, 63}, body: make([]byte, 63)},
		{name: "exact maximum malformed", prefix: [4]byte{0, 2, 0, 0}, body: make([]byte, 131072)},
		{name: "over maximum before body", prefix: [4]byte{0, 2, 0, 1}},
		{name: "short body EOF", prefix: [4]byte{0, 0, 0, 64}, body: make([]byte, 10)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, connection := startProtocolServer(t, limits)
			_ = connection.SetDeadline(time.Now().Add(time.Second))
			if _, err := connection.Write(test.prefix[:]); err != nil {
				t.Fatal(err)
			}
			if len(test.body) != 0 {
				if _, err := connection.Write(test.body); err != nil {
					t.Fatal(err)
				}
			}
			if test.name == "short body EOF" {
				if tcp, ok := connection.(*net.TCPConn); ok {
					_ = tcp.CloseWrite()
				}
			}
			var one [1]byte
			if _, err := connection.Read(one[:]); err == nil {
				t.Fatal("invalid frame kept the connection open")
			}
		})
	}
}

func TestNegotiatedRequestBoundsAreEnforcedBeforeDispatch(t *testing.T) {
	t.Run("MaxIO plus envelope exact", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxIOBytes = 65536
		limits.MaxFrameBytes = 131072
		_, backend, connection := startProtocolServer(t, limits)
		sessionID, key := authenticateProtocol(t, connection)
		treeID := connectProtocolTree(t, connection, key, sessionID, 3, "IPC$")
		body := make([]byte, limits.MaxIOBytes+requestEnvelopeBytes-wire.HeaderSize)
		packet := signedRequest(t, key, wire.Header{
			Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1, CreditCharge: 2,
		}, body)
		sendFrame(t, connection, packet)
		response := readFrame(t, connection)
		header, _ := wire.ParseHeader(response)
		if header.Status != statusUnsupported || key.Verify(response) != nil {
			t.Fatalf("exact request boundary = %+v", header)
		}
		if backend.dataCalls.Load() != 0 {
			t.Fatal("exact unsupported request reached backing")
		}
	})
	t.Run("MaxIO plus envelope plus one", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxIOBytes = 65536
		limits.MaxFrameBytes = 131072
		_, backend, connection := startProtocolServer(t, limits)
		sessionID, key := authenticateProtocol(t, connection)
		treeID := connectProtocolTree(t, connection, key, sessionID, 3, "IPC$")
		body := make([]byte, limits.MaxIOBytes+requestEnvelopeBytes-wire.HeaderSize+1)
		packet := signedRequest(t, key, wire.Header{
			Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1, CreditCharge: 2,
		}, body)
		sendFrame(t, connection, packet)
		var one [1]byte
		if _, err := connection.Read(one[:]); err == nil {
			t.Fatal("request one byte above negotiated receive bound remained open")
		}
		if backend.dataCalls.Load() != 0 || backend.sessionOpens.Load() != 0 {
			t.Fatal("over-limit request reached backing")
		}
	})
	t.Run("control envelope and credit charge", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			length int
			charge uint16
		}{
			{name: "oversized", length: maxControlFrameBytes + 1, charge: 1},
			{name: "multicredit", length: wire.HeaderSize + len(wire.EmptyResponseBody()), charge: 2},
		} {
			t.Run(test.name, func(t *testing.T) {
				_, backend, connection := startProtocolServer(t, DefaultLimits())
				sessionID, key := authenticateProtocol(t, connection)
				treeID := connectProtocolTree(t, connection, key, sessionID, 3, "IPC$")
				body := make([]byte, test.length-wire.HeaderSize)
				copy(body, wire.EmptyResponseBody())
				packet := signedRequest(t, key, wire.Header{
					Command: wire.Echo, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1, CreditCharge: test.charge,
				}, body)
				sendFrame(t, connection, packet)
				var one [1]byte
				if _, err := connection.Read(one[:]); err == nil {
					t.Fatal("invalid control request remained open")
				}
				if backend.dataCalls.Load() != 0 {
					t.Fatal("invalid control request reached backing")
				}
			})
		}
	})
}

func TestRequestLimitRejectsMaxPlusOneAndRecovers(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRequests = 1
	limits.MaxCompound = 1
	entered := make(chan struct{})
	release := make(chan struct{})
	authenticator := &protocolAuthenticator{}
	config := Config{
		Authenticator:     authenticator,
		AuthorizeIdentity: IdentityAuthorizerFunc(func(context.Context, Principal) error { return nil }),
		Authorize: authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
			principal, ok := PrincipalFromContext(ctx)
			if !ok || !principal.SameIdentity(testPrincipal("0123456789abcdef")) {
				return authz.ErrDenied
			}
			if request.Operation == storage.OpFileSessionOpen {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}),
		Limits: limits,
	}
	server, _, connection := startConfiguredProtocolServer(t, config)
	sessionID, key := authenticateProtocol(t, connection)
	connect := treeConnectPacket(3, sessionID, `\\localhost\data`)
	if err := key.Sign(connect); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, connection, connect)
	<-entered
	if state := server.Status(); state.PendingRequests != 1 {
		t.Fatalf("pending request status = %+v", state)
	}
	echo := signedRequest(t, key, wire.Header{Command: wire.Echo, MessageID: 4, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, echo)
	response := readFrame(t, connection)
	header, _ := wire.ParseHeader(response)
	if header.MessageID != 4 || header.Status != statusResources || key.Verify(response) != nil {
		t.Fatalf("max+1 response = %+v", header)
	}
	close(release)
	response = readFrame(t, connection)
	header, _ = wire.ParseHeader(response)
	if header.MessageID != 3 || header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("admitted response = %+v", header)
	}
	if state := server.Status(); state.PendingRequests != 0 {
		t.Fatalf("completed request remained pending: %+v", state)
	}
	echo = signedRequest(t, key, wire.Header{Command: wire.Echo, MessageID: 5, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, echo)
	response = readFrame(t, connection)
	header, _ = wire.ParseHeader(response)
	if header.MessageID != 5 || header.Status != statusOK || key.Verify(response) != nil {
		t.Fatalf("reused request capacity = %+v", header)
	}
}

func TestCancelIsNotImplementedByTheEndpointSlice(t *testing.T) {
	_, _, connection := startProtocolServer(t, DefaultLimits())
	sessionID, key := authenticateProtocol(t, connection)
	packet := signedRequest(t, key, wire.Header{Command: wire.Cancel, MessageID: 3, SessionID: sessionID, Credits: 1}, wire.EmptyResponseBody())
	sendFrame(t, connection, packet)
	var one [1]byte
	if _, err := connection.Read(one[:]); err == nil {
		t.Fatal("CANCEL received a fabricated response")
	}
}
