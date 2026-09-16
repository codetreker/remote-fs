package smb

import (
	"context"
	"encoding/binary"
	"math"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func controlConnectRequest(name string) wire.Request {
	path := wire.EncodeUTF16(`\\127.0.0.1\` + name)
	body := make([]byte, 8+len(path))
	binary.LittleEndian.PutUint16(body, 9)
	binary.LittleEndian.PutUint16(body[4:], 72)
	binary.LittleEndian.PutUint16(body[6:], uint16(len(path)))
	copy(body[8:], path)
	return sessionRequest(wire.TreeConnect, body)
}

func controlIOCTLRequest(code uint32) wire.Request {
	body := make([]byte, 56)
	binary.LittleEndian.PutUint16(body, 57)
	binary.LittleEndian.PutUint32(body[4:], code)
	return sessionRequest(wire.IOCTL, body)
}

func TestControlTreeAdmissionIsolationAndQuota(t *testing.T) {
	c, s := registryAuthenticatedConnection(t)
	c.server.config.Limits.MaxTrees = 1
	c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error {
		t.Error("IPC acquired volume authorization")
		return authz.ErrDenied
	})
	r := sessionSignedRequest(t, s, 0, controlConnectRequest("iPc$"))
	h := r.Header
	body, status, _ := c.dispatch(t.Context(), r, r, &h)
	if status != statusOK || len(body) != 16 || body[2] != 2 {
		t.Fatalf("IPC connect: %x %x", status, body)
	}
	id := h.TreeID
	tr := s.trees[id]
	if tr.export != nil || tr.authority != nil || tr.files != nil || len(s.authorities) != 0 {
		t.Fatal("IPC acquired a volume")
	}
	if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusResources {
		t.Fatalf("quota: %x", status)
	}
	for _, command := range []uint16{wire.Create, wire.Read, wire.OplockBreak, wire.IOCTL} {
		request := sessionRequest(command, wire.EmptyResponseBody())
		if command == wire.IOCTL {
			request = controlIOCTLRequest(0x001401fc)
		}
		r = sessionSignedRequest(t, s, id, request)
		h = r.Header
		if _, status, key := c.dispatch(t.Context(), r, r, &h); status != statusUnsupported || key != s.signer {
			t.Fatalf("unsupported command %d: %x", command, status)
		}
	}
	r = sessionSignedRequest(t, s, id+1, sessionRequest(wire.TreeDisconnect, wire.EmptyResponseBody()))
	h = r.Header
	if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusNetworkDeleted {
		t.Fatalf("unknown tree: %x", status)
	}
	r = sessionSignedRequest(t, s, id, sessionRequest(wire.TreeDisconnect, wire.EmptyResponseBody()))
	h = r.Header
	if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusOK || s.trees[id] != nil {
		t.Fatalf("disconnect: %x", status)
	}
	if err := c.closeTree(tr); err != nil {
		t.Fatal(err)
	}
	c.nextTree = math.MaxUint32
	if _, status := c.connectControl(s, &h); status != statusResources {
		t.Fatalf("tree ID overflow: %x", status)
	}
}

func TestTreeConnectRejectsInvalidUNC(t *testing.T) {
	c, s := registryAuthenticatedConnection(t)
	for _, path := range []string{"IPC$", `\IPC$`, `\\\IPC$`, `\\host\IPC$\child`, `\\host\`} {
		name := wire.EncodeUTF16(path)
		body := make([]byte, 8+len(name))
		binary.LittleEndian.PutUint16(body, 9)
		binary.LittleEndian.PutUint16(body[4:], 72)
		binary.LittleEndian.PutUint16(body[6:], uint16(len(name)))
		copy(body[8:], name)
		r := sessionSignedRequest(t, s, 0, sessionRequest(wire.TreeConnect, body))
		h := r.Header
		if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusBadNetworkName {
			t.Fatalf("UNC %q: %x", path, status)
		}
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
				request := sessionRequest(wire.Logoff, wire.EmptyResponseBody())
				if end == "validate negotiate" {
					request = controlIOCTLRequest(0x00140204)
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

func TestVolumeCloseUsesRetainedReferenceWithoutMetadata(t *testing.T) {
	c, s := registryAuthenticatedConnection(t)
	var deny, fail atomic.Bool
	c.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
		if _, ok := PrincipalFromContext(ctx); !ok {
			return authz.ErrDenied
		}
		if deny.Load() && request.Operation == storage.OpFileClose {
			return authz.ErrDenied
		}
		return nil
	})
	raw := newAuthorityTestSession()
	export, err := c.server.Publish(Share{Name: "volume", Volume: "trusted", Backend: &authorityTestBackend{raw: raw}})
	if err != nil {
		t.Fatal(err)
	}
	tree := authorityTestConnect(t, c, s, export)
	for _, flags := range []uint16{0, 1} {
		ref := &handleTestReference{closeFn: func(context.Context, int32) error {
			if fail.Load() {
				return syscall.EIO
			}
			return nil
		}}
		reservation := handleTestReserve(t, tree.files)
		reservation.attachNode(storage.NodeOpenResult{Reference: ref, Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular}, Outcome: storage.Opened})
		id, err := reservation.install(0x10000, 7)
		if err != nil {
			t.Fatal(err)
		}
		if err := reservation.finish(t.Context()); err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 24)
		binary.LittleEndian.PutUint16(body, 24)
		binary.LittleEndian.PutUint16(body[2:], flags)
		copy(body[8:], id[:])
		call := func(b []byte) ([]byte, uint32) {
			r := sessionSignedRequest(t, s, tree.id, sessionRequest(wire.Close, b))
			h := r.Header
			reply, status, key := c.dispatch(t.Context(), r, r, &h)
			if key != s.signer {
				t.Fatal("CLOSE lost signer")
			}
			return reply, status
		}
		invalid := append([]byte(nil), body...)
		binary.LittleEndian.PutUint16(invalid[2:], 2)
		if _, status := call(invalid); status != statusInvalid {
			t.Fatalf("flags: %x", status)
		}
		deny.Store(true)
		if _, status := call(body); status != statusDenied || ref.closeCalls.Load() != 0 {
			t.Fatalf("denied close: %x", status)
		}
		deny.Store(false)
		fail.Store(true)
		if _, status := call(body); status != statusIO || tree.files.get(id) == nil {
			t.Fatalf("failed close: %x", status)
		}
		fail.Store(false)
		reply, status := call(body)
		if status != statusOK || len(reply) != 60 || binary.LittleEndian.Uint16(reply[2:]) != 0 || ref.attributeCalls.Load() != 0 || ref.closeCalls.Load() != 2 {
			t.Fatalf("close status=%x reply=%x attrs=%d closes=%d", status, reply, ref.attributeCalls.Load(), ref.closeCalls.Load())
		}
		if _, status := call(body); status != 0xc0000008 {
			t.Fatalf("retired file: %x", status)
		}
	}
	r := sessionSignedRequest(t, s, tree.id, sessionRequest(wire.Read, wire.EmptyResponseBody()))
	h := r.Header
	if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusUnsupported {
		t.Fatalf("futurecommand: %x", status)
	}
}
