package smb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func controlConnectRequest(name string) wire.Request {
	path := wire.EncodeUTF16(`\\127.0.0.1\` + name)
	body := make([]byte, 8+len(path))
	smbLE.PutUint16(body, 9)
	smbLE.PutUint16(body[4:], 72)
	smbLE.PutUint16(body[6:], uint16(len(path)))
	copy(body[8:], path)
	return fileRequest(wire.TreeConnect, body)
}

func TestControlTreeAdmissionIsolationAndQuota(t *testing.T) {
	c, s, volume, _, _, _ := testConnection(t)
	var authorized atomic.Int32
	c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		authorized.Add(1)
		return authz.ErrDenied
	})
	c.server.config.Limits.MaxTrees = 2
	r := signedRequest(t, s, controlConnectRequest("iPc$"))
	h := r.Header
	r.Packet[48] ^= 1
	if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusDenied || len(s.trees) != 1 {
		t.Fatal("unsigned control connection bypassed session authentication")
	}
	r.Packet[48] ^= 1
	body, status, _ := c.dispatch(t.Context(), r, r, &h)
	if status != 0 || len(body) != 16 || body[2] != 2 || h.TreeID == 1 {
		t.Fatalf("control connect status=%x tree=%d body=%x", status, h.TreeID, body)
	}
	id := h.TreeID
	control := s.trees[id]
	if control.kind != controlTree || control.export != nil || control.files != nil || control.authority != nil || control.session != nil || len(s.authorities) != 0 || volume.export.refs != 1 || authorized.Load() != 0 {
		t.Fatal("control tree acquired volume resources or authorization")
	}
	if _, status, _ = c.dispatch(t.Context(), r, r, &h); status != statusResources {
		t.Fatalf("control tree escaped shared capacity: %x", status)
	}
	for _, command := range []uint16{wire.Create, wire.IOCTL, wire.QueryDirectory, wire.Read} {
		request := fileRequest(command, wire.EmptyResponseBody())
		if command == wire.IOCTL {
			request = ioctlCommand(0x001401fc, nil)
		}
		r = signedRequest(t, s, request)
		r.Header.TreeID = id
		r.Packet = requestPacket(r.Header, r.Body)
		_ = s.signer.Sign(r.Packet)
		h = r.Header
		if _, status, _ = c.dispatch(t.Context(), r, r, &h); status != statusUnsupported {
			t.Fatalf("unsupported control command %d returned %x", command, status)
		}
	}
	other := &session{id: 2, signer: s.signer, principal: s.principal, trees: make(map[uint32]*tree)}
	c.sessions[2] = other
	r = signedRequest(t, other, fileRequest(wire.TreeDisconnect, wire.EmptyResponseBody()))
	r.Header.TreeID = id
	r.Packet = requestPacket(r.Header, r.Body)
	_ = s.signer.Sign(r.Packet)
	h = r.Header
	if _, status, _ = c.dispatch(t.Context(), r, r, &h); status != statusNetworkDeleted || s.trees[id] != control {
		t.Fatal("control tree crossed session ownership")
	}
	delete(c.sessions, 2)
	r.Header.SessionID = s.id
	r.Packet = requestPacket(r.Header, r.Body)
	_ = s.signer.Sign(r.Packet)
	h = r.Header
	if _, status, _ = c.dispatch(t.Context(), r, r, &h); status != 0 || s.trees[id] != nil || !control.closed {
		t.Fatalf("control disconnect status=%x", status)
	}
	if err := c.closeTree(control); err != nil {
		t.Fatal(err)
	}
	if _, status, _ = c.dispatch(t.Context(), r, r, &h); status != statusNetworkDeleted {
		t.Fatal("retired control tree remained usable")
	}
	if authorized.Load() != 0 {
		t.Fatal("control operations issued volume authorization requests")
	}
	r = signedRequest(t, s, controlConnectRequest("work"))
	h = r.Header
	if _, status, _ = c.dispatch(t.Context(), r, r, &h); status != statusDenied || authorized.Load() != 1 {
		t.Fatal("data share authorization changed")
	}
	if _, err := c.server.Publish(Share{Name: "iPc$", Volume: "v", Backend: &sessionBackend{stateErr: errors.New("backend must not be reached")}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("reserved control share could be published: %v", err)
	}
}

func TestControlTreeTransportLifecycle(t *testing.T) {
	for _, end := range []string{"logoff", "disconnect", "shutdown", "validate negotiate"} {
		t.Run(end, func(t *testing.T) {
			server, transport := startProtocolServer(t)
			sessionID, signer := authenticateProtocol(t, transport)
			r := controlConnectRequest("IPC$")
			r.Header.MessageID = 3
			r.Header.SessionID = sessionID
			p := requestPacket(r.Header, r.Body)
			_ = signer.Sign(p)
			sendFrame(t, transport, p)
			header, err := wire.ParseHeader(readFrame(t, transport))
			if err != nil || header.Status != 0 {
				t.Fatalf("control connect=%+v %v", header, err)
			}
			server.mu.Lock()
			var connection *connection
			for c := range server.connections {
				connection = c
			}
			server.mu.Unlock()
			connection.mu.Lock()
			s := connection.sessions[sessionID]
			connection.mu.Unlock()
			s.mu.Lock()
			tree := s.trees[header.TreeID]
			s.mu.Unlock()
			if end == "logoff" || end == "validate negotiate" {
				request := fileRequest(wire.Logoff, wire.EmptyResponseBody())
				if end == "validate negotiate" {
					request = ioctlCommand(0x00140204, nil)
				}
				request.Header.MessageID = 4
				request.Header.SessionID = sessionID
				request.Header.TreeID = header.TreeID
				p = requestPacket(request.Header, request.Body)
				_ = signer.Sign(p)
				sendFrame(t, transport, p)
				if end == "logoff" {
					response := readFrame(t, transport)
					h, err := wire.ParseHeader(response)
					if err != nil || h.Status != 0 || signer.Verify(response) != nil {
						t.Fatalf("control logoff=%+v %v", h, err)
					}
				} else {
					var prefix [4]byte
					if _, err := transport.Read(prefix[:]); err == nil {
						t.Fatal("SMB311 validate-negotiate returned a response")
					}
				}
			} else if end == "disconnect" {
				_ = transport.Close()
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			tree.closeMu.Lock()
			closed := tree.closed
			tree.closeMu.Unlock()
			s.mu.Lock()
			remaining := len(s.trees)
			s.mu.Unlock()
			if !closed || remaining != 0 || server.Status().Connections != 0 || server.Status().RetainedConnections != 0 {
				t.Fatal("control tree outlived session or transport")
			}
		})
	}
}
