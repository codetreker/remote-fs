package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func sessionRequest(command uint16, body []byte) wire.Request {
	packet := requestPacket(wire.Header{Command: command, Credits: 1}, body)
	h, _ := wire.ParseHeader(packet)
	return wire.Request{Header: h, Packet: packet, Body: packet[64:]}
}

func sessionSignedRequest(t *testing.T, s *session, tree uint32, r wire.Request) wire.Request {
	t.Helper()
	r.Header.SessionID, r.Header.TreeID = s.id, tree
	r.Packet = requestPacket(r.Header, r.Body)
	if err := s.signer.Sign(r.Packet); err != nil {
		t.Fatal(err)
	}
	r.Header, _ = wire.ParseHeader(r.Packet)
	r.Body = r.Packet[64:]
	return r
}

func TestReauthenticationPreservesExistingSigningKeyAndIdentity(t *testing.T) {
	for _, sid := range []string{"S-1-5-21-1", "S-1-5-21-2"} {
		t.Run(sid, func(t *testing.T) {
			c, s := registryAuthenticatedConnection(t)
			original := s.signer
			c.server.config.Authenticator = &registryAuthenticator{sid: sid}
			_, status, key := registrySetup(t, t.Context(), c, s.id, 10, 0, 0, 0, "initial")
			if status != statusMoreProcessing || key != original {
				t.Fatalf("reauthentication challenge: %x", status)
			}
			_, status, key = registrySetup(t, t.Context(), c, s.id, 11, 0, 0, 0, "proof")
			want := statusOK
			if sid != s.principal.SID {
				want = statusDenied
			}
			if status != want || key != original || s.signer != original || s.principal.SID != "S-1-5-21-1" {
				t.Fatalf("reauthentication identity/key changed: %x", status)
			}
			r := sessionSignedRequest(t, s, 0, sessionRequest(wire.Echo, wire.EmptyResponseBody()))
			h := r.Header
			if _, status, key := c.dispatch(t.Context(), r, r, &h); status != statusOK || key != original {
				t.Fatalf("old key unusable: %x", status)
			}
		})
	}
}

func TestSessionDispatchRejectsUnsignedAndRetiredRequests(t *testing.T) {
	c, s := registryAuthenticatedConnection(t)
	r := sessionSignedRequest(t, s, 0, sessionRequest(wire.Echo, wire.EmptyResponseBody()))
	h := r.Header
	r.Packet[48] ^= 1
	if _, status, _ := c.dispatch(t.Context(), r, r, &h); status != statusDenied {
		t.Fatalf("bad signature: %x", status)
	}
	r.Packet[48] ^= 1
	s.mu.Lock()
	s.retired = true
	s.mu.Unlock()
	if _, status, key := c.dispatch(t.Context(), r, r, &h); status != statusSessionDeleted || key != s.signer {
		t.Fatalf("retired request: %x", status)
	}
}

type failingCloseAuthenticator struct {
	fail   atomic.Bool
	closes atomic.Int32
}
type failingCloseAuthentication struct{ owner *failingCloseAuthenticator }

func (a *failingCloseAuthenticator) Begin(context.Context) (Authentication, error) {
	return &failingCloseAuthentication{owner: a}, nil
}
func (a *failingCloseAuthentication) Step(context.Context, []byte) (AuthenticationResult, error) {
	return AuthenticationResult{}, errors.New("proof rejected")
}
func (a *failingCloseAuthentication) Close() error {
	a.owner.closes.Add(1)
	if a.owner.fail.Load() {
		return errors.New("security context close failed")
	}
	return nil
}

func TestAuthenticationCloseFailureRetainsGlobalCapacity(t *testing.T) {
	config := testConfig()
	config.Authenticator = &registryAuthenticator{sid: "S-1-5-21-1", immediate: true}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	c := registryConnection(t, server)
	if _, status, _ := registrySetup(t, t.Context(), c, 0, 1, 0, 0, 0, "proof"); status != statusOK {
		t.Fatalf("authentication: %x", status)
	}
	fault := &failingCloseAuthenticator{}
	fault.fail.Store(true)
	c.server.config.Authenticator = fault
	c.server.config.Limits.MaxSessions = 2
	next := registryConnection(t, c.server)
	h, status, _ := registrySetup(t, t.Context(), next, 0, 10, 0, 0, 0, "invalid")
	if status != statusDenied || c.server.sessions.get(h.SessionID).session == nil {
		t.Fatalf("lost failed security context: %x", status)
	}
	t.Cleanup(func() { fault.fail.Store(false) })
	third := registryConnection(t, c.server)
	if _, status, _ := registrySetup(t, t.Context(), third, 0, 1, 0, 0, 0, "invalid"); status != statusResources {
		t.Fatalf("capacity: %x", status)
	}
	fault.fail.Store(false)
	next.cleanup()
	if c.server.sessions.get(h.SessionID).session != nil || fault.closes.Load() < 2 {
		t.Fatal("security context was not retried")
	}
	c.cleanup()
	third.cleanup()
	historical := server.cleanupErr
	if historical == nil {
		t.Fatal("cleanup failure was not retained")
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatalf("confirmed cleanup retry: %v", err)
	}
	if server.cleanupErr != historical || server.Status().CleanupFailures == 0 {
		t.Fatal("lost historical diagnostics")
	}
	if state := server.Status(); !state.Stopped || state.Sessions != 0 || state.Connections != 0 {
		t.Fatalf("cleanup did not finish: %+v", state)
	}

}

func TestSessionSetupBoundsAndExpiry(t *testing.T) {
	t.Run("expired exchange", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, _ := registryAuthenticatedConnection(t)
			authenticator := &registryAuthenticator{sid: "S-1-5-21-1"}
			c.server.config.Authenticator = authenticator
			c.server.config.Limits.MaxSessions = 2
			h, status, _ := registrySetup(t, t.Context(), c, 0, 10, 0, 0, 0, "initial")
			if status != statusMoreProcessing || c.server.Status().Sessions != 2 {
				t.Fatalf("challenge: %x, state=%+v", status, c.server.Status())
			}
			expiring := c.server.sessions.get(h.SessionID).session
			if expiring == nil {
				t.Fatal("challenge has no registered session")
			}
			time.Sleep(c.server.config.Limits.HandshakeTimeout)
			synctest.Wait()
			select {
			case <-expiring.authDone:
			default:
				t.Fatal("expired authentication watcher has not retired")
			}
			c.mu.Lock()
			retained := c.sessions[h.SessionID] != nil
			c.mu.Unlock()
			if retained || c.server.sessions.get(h.SessionID).session != nil || c.server.Status().Sessions != 1 || authenticator.closed.Load() != 1 {
				t.Fatalf("expired exchange retained ownership: local=%v state=%+v closed=%d", retained, c.server.Status(), authenticator.closed.Load())
			}
			if _, status, key := registrySetup(t, t.Context(), c, h.SessionID, 11, 0, 0, 0, "proof"); status != statusSessionDeleted || key != nil {
				t.Fatalf("expired exchange: %x", status)
			}
			next, status, _ := registrySetup(t, t.Context(), c, 0, 12, 0, 0, 0, "initial")
			if status != statusMoreProcessing || next.SessionID == h.SessionID || c.server.Status().Sessions != 2 || authenticator.closed.Load() != 1 {
				t.Fatalf("reclaimed authentication slot: %x, state=%+v closed=%d", status, c.server.Status(), authenticator.closed.Load())
			}
		})
	})
	t.Run("invalid SID", func(t *testing.T) {
		c, _ := registryAuthenticatedConnection(t)
		for _, sid := range []string{"", "S-2-5-1", "S-1-281474976710656-1", "S-1-5-4294967296"} {
			c.server.config.Authenticator = &registryAuthenticator{sid: sid, immediate: true}
			if _, status, key := registrySetup(t, t.Context(), c, 0, 12, 0, 0, 0, "proof"); status != statusDenied || key != nil {
				t.Fatalf("invalid SID %q: %x", sid, status)
			}
		}
	})
}

func TestSessionSetupRequiresOldSignature(t *testing.T) {
	c, s := registryAuthenticatedConnection(t)
	packet := setupPacket(10, s.id, "proof")
	h, _ := wire.ParseHeader(packet)
	r := wire.Request{Header: h, Packet: packet, Body: packet[64:]}
	if _, status, key := c.sessionSetup(t.Context(), r, &h); status != statusDenied || key != nil {
		t.Fatalf("unsigned reauthentication: %x", status)
	}
	if s.signer == nil {
		t.Fatal("unsigned request removed authenticated session")
	}
}

func TestSessionCapabilitiesExcludeLeasing(t *testing.T) {
	c, _ := registryAuthenticatedConnection(t)
	c.negotiated = false
	p := negotiatePacket()
	requests, err := wire.ParseFrame(p, wire.Limits{MaxBytes: c.server.config.Limits.MaxFrameBytes, MaxCommands: c.server.config.Limits.MaxCompound, MaxContexts: c.server.config.Limits.MaxContexts})
	if err != nil {
		t.Fatal(err)
	}
	body, status := c.negotiate(requests[0])
	if status != statusOK || binary.LittleEndian.Uint32(body[24:]) != 4 || binary.LittleEndian.Uint16(body[2:]) != 3 {
		t.Fatalf("capabilities/security: %x %x", status, body)
	}
	if _, status = c.negotiate(requests[0]); status != statusInvalid {
		t.Fatalf("duplicate negotiate: %x", status)
	}
}
