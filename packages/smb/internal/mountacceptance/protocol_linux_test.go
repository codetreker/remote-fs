//go:build linux && rfs_mountacceptance

package mountacceptance

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const sessionSecret = "0123456789abcdef"

var principal = smb.Principal{SID: "S-1-5-21-1", LogonSession: "0123456789abcdef"}

type authenticator struct{}
type authentication struct{ step int }

func (authenticator) Begin(context.Context) (smb.Authentication, error) {
	return &authentication{}, nil
}

func (a *authentication) Step(_ context.Context, token []byte) (smb.AuthenticationResult, error) {
	a.step++
	if a.step == 1 && string(token) == "initial" {
		return smb.AuthenticationResult{Continue: true, Token: []byte("challenge")}, nil
	}
	if a.step == 2 && string(token) == "proof" {
		return smb.AuthenticationResult{Principal: principal, SessionKey: []byte(sessionSecret), ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	return smb.AuthenticationResult{}, errors.New("invalid acceptance authentication exchange")
}

func (*authentication) Close() error { return nil }

type protocolClient struct {
	connection net.Conn
	key        *signing.Session
	session    uint64
	tree       uint32
	message    uint64
}

func startSMB(t *testing.T, backend storage.FileStorage) *protocolClient {
	t.Helper()
	return startSMBObserved(t, backend, nil)
}

func startSMBObserved(t *testing.T, backend storage.FileStorage, observe func(*smb.Server)) *protocolClient {
	t.Helper()
	server, err := smb.New(smb.Config{
		Authenticator: authenticator{},
		AuthorizeIdentity: smb.IdentityAuthorizerFunc(func(_ context.Context, p smb.Principal) error {
			if !p.SameIdentity(principal) {
				return authz.ErrDenied
			}
			return nil
		}),
		Authorize: authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
			p, ok := smb.PrincipalFromContext(ctx)
			if !ok || !p.SameIdentity(principal) {
				return authz.ErrDenied
			}
			return nil
		}),
		Limits: smb.DefaultLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if observe != nil {
		observe(server)
	}
	identity, err := backend.(storage.BackendIdentity).BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Publish(smb.Share{Name: "data", Volume: "joint-acceptance", BackendVolume: identity.Volume, RootNodeID: identity.RootNodeID, Backend: backend}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background(), listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown SMB: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve SMB: %v", err)
			}
		case <-ctx.Done():
			t.Error("SMB server did not drain")
		}
	})
	connection, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	c := &protocolClient{connection: connection, message: 4}
	c.authenticate(t)
	name := wire.EncodeUTF16(`\\localhost\data`)
	body := make([]byte, 8+len(name))
	binary.LittleEndian.PutUint16(body, 9)
	binary.LittleEndian.PutUint16(body[4:], 72)
	binary.LittleEndian.PutUint16(body[6:], uint16(len(name)))
	copy(body[8:], name)
	packet := packet(wire.Header{Command: wire.TreeConnect, MessageID: 3, SessionID: c.session, Credits: 1}, body)
	if err := c.key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	response := c.exchange(t, packet)
	header := checkedHeader(t, response, wire.TreeConnect, 3, 0)
	if err := c.key.Verify(response); err != nil || header.TreeID == 0 {
		t.Fatalf("TREE_CONNECT signature=%v tree=%d", err, header.TreeID)
	}
	c.tree = header.TreeID
	return c
}

func packet(header wire.Header, body []byte) []byte {
	result := make([]byte, wire.HeaderSize+len(body))
	if err := header.Encode(result); err != nil {
		panic(err)
	}
	copy(result[wire.HeaderSize:], body)
	return result
}

func (c *protocolClient) exchange(t *testing.T, request []byte) []byte {
	t.Helper()
	if err := c.connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 4+len(request))
	binary.BigEndian.PutUint32(frame, uint32(len(request)))
	copy(frame[4:], request)
	for len(frame) > 0 {
		n, err := c.connection.Write(frame)
		if err != nil {
			t.Fatal(err)
		}
		frame = frame[n:]
	}
	var prefix [4]byte
	if _, err := io.ReadFull(c.connection, prefix[:]); err != nil {
		t.Fatal(err)
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size < wire.HeaderSize || size > 2<<20 {
		t.Fatalf("invalid SMB response length %d", size)
	}
	response := make([]byte, size)
	if _, err := io.ReadFull(c.connection, response); err != nil {
		t.Fatal(err)
	}
	return response
}

func checkedHeader(t *testing.T, response []byte, command uint16, message uint64, status uint32) wire.Header {
	t.Helper()
	header, err := wire.ParseHeader(response)
	if err != nil || header.Command != command || header.MessageID != message || header.Status != status {
		t.Fatalf("SMB response header=%+v error=%v, want command=%d message=%d status=%#x", header, err, command, message, status)
	}
	return header
}

func (c *protocolClient) authenticate(t *testing.T) {
	t.Helper()
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
	request := packet(wire.Header{Command: wire.Negotiate, Credits: 32}, body)
	response := c.exchange(t, request)
	checkedHeader(t, response, wire.Negotiate, 0, 0)
	if len(response) < 92 || binary.LittleEndian.Uint16(response[68:]) != wire.Dialect311 {
		t.Fatal("SMB 3.1.1 was not negotiated")
	}
	hash := signing.Preauth(signing.Preauth([64]byte{}, request), response)
	setup := func(message, session uint64, token string) []byte {
		body := make([]byte, 24+len(token))
		binary.LittleEndian.PutUint16(body, 25)
		body[3] = 3
		binary.LittleEndian.PutUint16(body[12:], 88)
		binary.LittleEndian.PutUint16(body[14:], uint16(len(token)))
		copy(body[24:], token)
		return packet(wire.Header{Command: wire.SessionSetup, MessageID: message, SessionID: session, Credits: 1}, body)
	}
	request = setup(1, 0, "initial")
	hash = signing.Preauth(hash, request)
	response = c.exchange(t, request)
	header := checkedHeader(t, response, wire.SessionSetup, 1, 0xc0000016)
	if header.SessionID == 0 {
		t.Fatal("authentication challenge has no session")
	}
	c.session = header.SessionID
	hash = signing.Preauth(hash, response)
	request = setup(2, c.session, "proof")
	hash = signing.Preauth(hash, request)
	key, err := signing.NewSession(hash, []byte(sessionSecret))
	if err != nil {
		t.Fatal(err)
	}
	c.key = key
	t.Cleanup(key.Destroy)
	response = c.exchange(t, request)
	checkedHeader(t, response, wire.SessionSetup, 2, 0)
	if err := key.Verify(response); err != nil {
		t.Fatal(err)
	}
}

func (c *protocolClient) call(t *testing.T, command uint16, body []byte, status uint32) []byte {
	t.Helper()
	message := c.message
	c.message++
	request := packet(wire.Header{Command: command, MessageID: message, SessionID: c.session, TreeID: c.tree, Credits: 1}, body)
	if err := c.key.Sign(request); err != nil {
		t.Fatal(err)
	}
	response := c.exchange(t, request)
	header := checkedHeader(t, response, command, message, status)
	if header.SessionID != c.session || header.TreeID != c.tree || c.key.Verify(response) != nil {
		t.Fatal("response did not retain signed SMB session/tree")
	}
	return response[wire.HeaderSize:]
}

func (c *protocolClient) open(t *testing.T, name string) wire.FileID {
	t.Helper()
	encoded := wire.EncodeUTF16(name)
	body := make([]byte, max(57, 56+len(encoded)))
	binary.LittleEndian.PutUint16(body, 57)
	binary.LittleEndian.PutUint32(body[4:], 2)
	binary.LittleEndian.PutUint32(body[24:], 0x00000001|0x00000002|0x00000080|0x00000100)
	binary.LittleEndian.PutUint32(body[32:], 7)
	binary.LittleEndian.PutUint32(body[36:], 1)
	binary.LittleEndian.PutUint16(body[44:], 120)
	binary.LittleEndian.PutUint16(body[46:], uint16(len(encoded)))
	copy(body[56:], encoded)
	result := c.call(t, wire.Create, body, 0)
	if len(result) != 88 || binary.LittleEndian.Uint16(result) != 89 {
		t.Fatalf("invalid CREATE response %x", result)
	}
	var id wire.FileID
	copy(id[:], result[64:80])
	t.Cleanup(func() {
		body := make([]byte, 24)
		binary.LittleEndian.PutUint16(body, 24)
		copy(body[8:], id[:])
		c.call(t, wire.Close, body, 0)
	})
	return id
}

func (c *protocolClient) query(t *testing.T, id wire.FileID, class byte) []byte {
	t.Helper()
	body := make([]byte, 41)
	binary.LittleEndian.PutUint16(body, 41)
	body[2], body[3] = wire.InfoFile, class
	binary.LittleEndian.PutUint32(body[4:], 56)
	copy(body[24:40], id[:])
	result := c.call(t, wire.QueryInfo, body, 0)
	if len(result) < 8 || binary.LittleEndian.Uint16(result) != 9 || binary.LittleEndian.Uint16(result[2:]) != 72 || binary.LittleEndian.Uint32(result[4:]) != uint32(len(result)-8) {
		t.Fatalf("invalid QUERY_INFO response %x", result)
	}
	return result[8:]
}

func (c *protocolClient) set(t *testing.T, id wire.FileID, class byte, data []byte, status uint32) {
	t.Helper()
	body := make([]byte, max(33, 32+len(data)))
	binary.LittleEndian.PutUint16(body, 33)
	body[2], body[3] = wire.InfoFile, class
	binary.LittleEndian.PutUint32(body[4:], uint32(len(data)))
	binary.LittleEndian.PutUint16(body[8:], 96)
	copy(body[16:32], id[:])
	copy(body[32:], data)
	result := c.call(t, wire.SetInfo, body, status)
	if status == 0 && (len(result) != 2 || binary.LittleEndian.Uint16(result) != 2) {
		t.Fatalf("invalid SET_INFO response %x", result)
	}
}

func (c *protocolClient) read(t *testing.T, id wire.FileID, size int) []byte {
	t.Helper()
	body := make([]byte, 49)
	binary.LittleEndian.PutUint16(body, 49)
	binary.LittleEndian.PutUint32(body[4:], uint32(size))
	copy(body[16:32], id[:])
	result := c.call(t, wire.Read, body, 0)
	if len(result) < 16 || binary.LittleEndian.Uint16(result) != 17 || result[2] != 80 || binary.LittleEndian.Uint32(result[4:]) != uint32(len(result)-16) {
		t.Fatalf("invalid READ response %x", result)
	}
	return result[16:]
}
