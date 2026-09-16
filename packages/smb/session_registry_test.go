package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type registryAuthenticator struct {
	sid       string
	initial   string
	immediate bool
	proof     func(context.Context) error
	closed    atomic.Int32
}

type registryAuthentication struct {
	owner   *registryAuthenticator
	started bool
}

func (a *registryAuthenticator) Begin(context.Context) (Authentication, error) {
	return &registryAuthentication{owner: a}, nil
}

func (a *registryAuthentication) Step(ctx context.Context, token []byte) (AuthenticationResult, error) {
	initial := a.owner.initial
	if initial == "" {
		initial = "initial"
	}
	if !a.owner.immediate && !a.started && string(token) == initial {
		a.started = true
		return AuthenticationResult{Continue: true, Token: []byte("challenge")}, nil
	}
	if string(token) != "proof" {
		return AuthenticationResult{}, errors.New("invalid proof")
	}
	if a.owner.proof != nil {
		if err := a.owner.proof(ctx); err != nil {
			return AuthenticationResult{}, err
		}
	}
	return AuthenticationResult{Principal: Principal{SID: a.owner.sid}, SessionKey: []byte("0123456789abcdef")}, nil
}

func (a *registryAuthentication) Close() error { a.owner.closed.Add(1); return nil }

func registryConnection(t *testing.T, server *Server) *connection {
	t.Helper()
	left, right := net.Pipe()
	c := newConnection(server, left)
	c.negotiated = true
	server.mu.Lock()
	server.connections[c] = struct{}{}
	server.mu.Unlock()
	t.Cleanup(func() { _ = right.Close(); c.cleanup() })
	return c
}

func registrySetup(t *testing.T, ctx context.Context, c *connection, sessionID, messageID, previous uint64, channel uint32, flags byte, token string) (wire.Header, uint32, *signing.Session) {
	t.Helper()
	packet := setupPacket(messageID, sessionID, token)
	packet[66] = flags
	packet[67] = 2
	binary.LittleEndian.PutUint32(packet[68:72], 1)
	binary.LittleEndian.PutUint32(packet[72:76], channel)
	binary.LittleEndian.PutUint64(packet[80:88], previous)
	c.mu.Lock()
	s := c.sessions[sessionID]
	c.mu.Unlock()
	if s != nil {
		s.identityMu.RLock()
		key := s.signer
		s.identityMu.RUnlock()
		if key != nil {
			if err := key.Sign(packet); err != nil {
				t.Fatal(err)
			}
		}
	}
	header, err := wire.ParseHeader(packet)
	if err != nil {
		t.Fatal(err)
	}
	r := wire.Request{Header: header, Packet: packet, Body: packet[64:]}
	_, status, key := c.sessionSetup(ctx, r, &header)
	return header, status, key
}

func registryAuthenticatedConnection(t *testing.T) (*connection, *session) {
	t.Helper()
	config := testConfig()
	config.Authenticator = &registryAuthenticator{sid: "S-1-5-21-1", immediate: true}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	c := registryConnection(t, server)
	h, status, _ := registrySetup(t, t.Context(), c, 0, 1, 0, 0, 0, "proof")
	if status != statusOK {
		t.Fatalf("initial authentication: %x", status)
	}
	return c, c.sessions[h.SessionID]
}

func TestSessionIDsAreGlobalAndReservedValuesNeverAllocate(t *testing.T) {
	first, original := registryAuthenticatedConnection(t)
	first.server.config.Authenticator = &registryAuthenticator{sid: original.principal.SID}
	second, third := registryConnection(t, first.server), registryConnection(t, first.server)
	a, status, _ := registrySetup(t, t.Context(), second, 0, 1, 0, math.MaxUint32, 0, "initial")
	if status != statusMoreProcessing {
		t.Fatalf("reserved Channel changed handshake: %x", status)
	}
	b, status, _ := registrySetup(t, t.Context(), third, 0, 1, 0, 0, 0, "initial")
	if status != statusMoreProcessing {
		t.Fatalf("third handshake: %x", status)
	}
	if a.SessionID == b.SessionID || a.SessionID == original.id || b.SessionID == original.id || a.SessionID == 0 || b.SessionID == math.MaxUint64 {
		t.Fatalf("nonunique IDs: %d %d %d", original.id, a.SessionID, b.SessionID)
	}
	r := newSessionRegistry()
	r.nextID = math.MaxUint64 - 2
	last := &session{}
	if err := r.add(second, last); err != nil || last.id != math.MaxUint64-1 {
		t.Fatalf("last ID: %d %v", last.id, err)
	}
	if err := r.add(third, &session{}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("ID exhaustion: %v", err)
	}
}

func TestSessionBindingRemainsUnsupported(t *testing.T) {
	c, _ := registryAuthenticatedConnection(t)
	for _, test := range []struct {
		flags byte
		want  uint32
	}{{1, statusRequestNotAccepted}, {2, statusInvalid}, {3, statusInvalid}} {
		_, status, _ := registrySetup(t, t.Context(), c, 0, 99, 0, 0, test.flags, "initial")
		if status != test.want {
			t.Fatalf("flags %x = %x, want %x", test.flags, status, test.want)
		}
	}
}

func TestPreviousSessionCancellationDoesNotExemptAnotherConnectionsFrame(t *testing.T) {
	old, previous := registryAuthenticatedConnection(t)
	oldKey := previous.signer
	current := registryConnection(t, old.server)
	old.server.config.Authenticator = &registryAuthenticator{sid: previous.principal.SID, immediate: true}
	readContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	old.mu.Lock()
	old.pending[77] = &pendingRequest{frame: 77, command: wire.Read, sessionID: previous.id, ctx: readContext, cancel: cancel}
	old.mu.Unlock()
	ctx := context.WithValue(t.Context(), pendingFrameKey{}, requestFrame{connection: current, id: 77})
	_, status, key := registrySetup(t, ctx, current, 0, 77, previous.id, 0, 0, "proof")
	if status != statusOK || key == nil {
		t.Fatalf("replacement setup: %x", status)
	}
	select {
	case <-readContext.Done():
	default:
		t.Fatal("same frame number on another connection escaped cancellation")
	}
	packet := requestPacket(wire.Header{Command: wire.Read, SessionID: previous.id}, wire.EmptyResponseBody())
	if err := oldKey.Sign(packet); err != nil {
		t.Fatalf("old reply lost its signing key before frame retirement: %v", err)
	}
	old.retireRequests([]wire.Request{{Header: wire.Header{MessageID: 77}}})
	if err := oldKey.Sign(packet); !errors.Is(err, signing.ErrDestroyed) {
		t.Fatalf("old key survived frame retirement: %v", err)
	}
	if old.server.sessions.get(previous.id).session != nil {
		t.Fatal("old global entry survived final frame")
	}
}

func TestFirstAuthenticationFrameOwnsItsNewSessionSigningLifetime(t *testing.T) {
	base, _ := registryAuthenticatedConnection(t)
	base.server.config.Authenticator = &registryAuthenticator{sid: "S-1-5-21-1", immediate: true}
	first := registryConnection(t, base.server)
	first.mu.Lock()
	first.pending[10] = &pendingRequest{frame: 10, command: wire.SessionSetup, ctx: t.Context(), cancel: func() {}}
	first.mu.Unlock()
	header, status, key := registrySetup(t, t.Context(), first, 0, 10, 0, 0, 0, "proof")
	if status != statusOK || key == nil {
		t.Fatalf("first setup: %x", status)
	}
	second := registryConnection(t, base.server)
	if _, status, _ := registrySetup(t, t.Context(), second, 0, 20, header.SessionID, 0, 0, "proof"); status != statusOK {
		t.Fatalf("replacement setup: %x", status)
	}
	packet := wire.EncodeResponse(wire.Header{Command: wire.SessionSetup, SessionID: header.SessionID, MessageID: 10}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatalf("first successful authentication response could not be signed: %v", err)
	}
	first.retireRequests([]wire.Request{{Header: wire.Header{MessageID: 10}}})
	if err := key.Sign(packet); !errors.Is(err, signing.ErrDestroyed) {
		t.Fatalf("completed first frame retained key: %v", err)
	}
}

func TestConcurrentPreviousSessionFinalizationDoesNotNestAuthenticationLocks(t *testing.T) {
	left, leftSession := registryAuthenticatedConnection(t)
	left.server.config.Authenticator = &registryAuthenticator{sid: leftSession.principal.SID, immediate: true}
	right := registryConnection(t, left.server)
	rightHeader, status, _ := registrySetup(t, t.Context(), right, 0, 20, 0, 0, 0, "proof")
	if status != statusOK {
		t.Fatalf("right initial session: %x", status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ready, release := make(chan struct{}, 2), make(chan struct{})
	left.server.config.Authenticator = &registryAuthenticator{sid: leftSession.principal.SID, immediate: true, proof: func(ctx context.Context) error {
		ready <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	results := make(chan uint32, 2)
	go func() {
		_, status, _ := registrySetup(t, ctx, left, leftSession.id, 30, rightHeader.SessionID, 0, 0, "proof")
		results <- status
	}()
	go func() {
		_, status, _ := registrySetup(t, ctx, right, rightHeader.SessionID, 40, leftSession.id, 0, 0, "proof")
		results <- status
	}()
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("both authentications did not reach final proof")
		}
	}
	close(release)
	for range 2 {
		select {
		case status := <-results:
			if status != statusOK && status != statusSessionDeleted {
				t.Fatalf("concurrent finalization = %x", status)
			}
		case <-ctx.Done():
			t.Fatal("mutual previous-session retirement deadlocked authentication locks")
		}
	}
	if left.server.sessions.get(leftSession.id).session != nil && left.server.sessions.get(rightHeader.SessionID).session != nil {
		t.Fatal("both old sessions escaped same-user retirement")
	}
}

func TestRestartedServerAuthenticatesPreviousIDOneWithoutRestoringIt(t *testing.T) {
	config := testConfig()
	initial := strings.Repeat("i", 62)
	config.Authenticator = &registryAuthenticator{sid: "S-1-5-21-1", initial: initial}
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	c := registryConnection(t, server)
	header, status, _ := registrySetup(t, t.Context(), c, 0, 1, 1, 0, 0, initial)
	if status != statusMoreProcessing || header.SessionID != 1 {
		t.Fatalf("restarted server rejected native initial setup: id=%d status=%x", header.SessionID, status)
	}
	_, status, key := registrySetup(t, t.Context(), c, header.SessionID, 2, 1, 0, 0, "proof")
	if status != statusOK || key == nil {
		t.Fatalf("self-ID on final authentication: %x", status)
	}
	owner := server.sessions.get(header.SessionID)
	if owner.session == nil || owner.connection != c {
		t.Fatal("new authenticated session was retired as its own predecessor")
	}
	if len(owner.session.trees) != 0 || len(owner.session.authorities) != 0 {
		t.Fatal("old handles or authority sessions were restored")
	}
}

func TestGlobalSessionQuotaIncludesRetiringFrames(t *testing.T) {
	old, previous := registryAuthenticatedConnection(t)
	old.server.config.Limits.MaxSessions = 2
	current := registryConnection(t, old.server)
	old.pending[77] = &pendingRequest{frame: 77, command: wire.Read, sessionID: previous.id, ctx: t.Context(), cancel: func() {}}
	header, status, _ := registrySetup(t, t.Context(), current, 0, 78, previous.id, 0, 0, "proof")
	if status != statusOK {
		t.Fatalf("replace = %x", status)
	}
	third := registryConnection(t, old.server)
	if _, status, _ := registrySetup(t, t.Context(), third, 0, 1, 0, 0, 0, "proof"); status != statusResources {
		t.Fatalf("retained old response escaped server quota: %x", status)
	}
	if old.server.sessions.get(previous.id).session != previous {
		t.Fatal("previous owner disappeared before its response")
	}
	old.retireRequests([]wire.Request{{Header: wire.Header{MessageID: 77}}})
	if _, status, _ := registrySetup(t, t.Context(), third, 0, 2, 0, 0, 0, "proof"); status != statusOK {
		t.Fatalf("completed response retained server quota: %x", status)
	}
	if old.server.sessions.get(header.SessionID).session == nil {
		t.Fatal("replacement owner was removed")
	}
}

func TestFailedAuthenticationRetainsItsFrameCharge(t *testing.T) {
	base, _ := registryAuthenticatedConnection(t)
	base.server.config.Limits.MaxSessions = 2
	first := registryConnection(t, base.server)
	first.pending[99] = &pendingRequest{frame: 99, command: wire.SessionSetup, ctx: t.Context(), cancel: func() {}}
	h, status, _ := registrySetup(t, t.Context(), first, 0, 99, 0, 0, 0, "invalid")
	if status != statusDenied {
		t.Fatalf("bad authentication = %x", status)
	}
	if base.server.sessions.get(h.SessionID).session == nil {
		t.Fatal("failed authentication released a pending response's charge")
	}
	second := registryConnection(t, base.server)
	if _, status, _ := registrySetup(t, t.Context(), second, 0, 1, 0, 0, 0, "proof"); status != statusResources {
		t.Fatalf("capacity = %x", status)
	}
	first.retireRequests([]wire.Request{{Header: wire.Header{MessageID: 99}}})
	if _, status, _ := registrySetup(t, t.Context(), second, 0, 2, 0, 0, 0, "proof"); status != statusOK {
		t.Fatalf("capacity after frame = %x", status)
	}
}

func TestPreviousSessionAuthenticationIdentityRules(t *testing.T) {
	for _, tc := range []struct {
		name, sid, token string
		previous         uint64
		retire           bool
	}{
		{"same user", "S-1-5-21-1", "proof", 1, true},
		{"different user", "S-1-5-21-2", "proof", 1, false},
		{"unknown identifier", "S-1-5-21-1", "proof", 999, false},
		{"failed proof", "S-1-5-21-1", "invalid", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, previous := registryAuthenticatedConnection(t)
			old.server.config.Authenticator = &registryAuthenticator{sid: tc.sid}
			current := registryConnection(t, old.server)
			h, status, _ := registrySetup(t, t.Context(), current, 0, 10, tc.previous, 0, 0, "initial")
			if status != statusMoreProcessing || old.server.sessions.get(previous.id).session != previous {
				t.Fatalf("premature retirement: %x", status)
			}
			_, status, key := registrySetup(t, t.Context(), current, h.SessionID, 11, tc.previous, 0, 0, tc.token)
			if tc.token == "proof" {
				if status != statusOK || key == nil {
					t.Fatalf("authentication: %x", status)
				}
				if len(current.sessions[h.SessionID].trees) != 0 || len(current.sessions[h.SessionID].authorities) != 0 {
					t.Fatal("new session inherited old resources")
				}
			} else if status != statusDenied || key != nil {
				t.Fatalf("failed proof: %x", status)
			}
			if (old.server.sessions.get(previous.id).session == nil) != tc.retire {
				t.Fatal("incorrect previous-session retirement")
			}
		})
	}
}

func TestPreviousSessionRetirementUsesVerifiedPrincipal(t *testing.T) {
	const sid = "S-1-5-21-31"
	raw := newAuthorityTestSession()
	var decisions atomic.Int32
	old, s, export := authorityTestConnection(t, &authorityTestBackend{raw: raw}, func(c *Config) {
		c.Authenticator = &registryAuthenticator{sid: sid}
		c.Authorize = authz.AuthorizerFunc(func(ctx context.Context, _ authz.AccessRequest) error {
			principal, ok := PrincipalFromContext(ctx)
			if !ok || principal.SID != sid {
				return authz.ErrDenied
			}
			decisions.Add(1)
			return nil
		})
	})
	authorityTestConnect(t, old, s, export)
	next := registryConnection(t, old.server)
	h, status, _ := registrySetup(t, t.Context(), next, 0, 70, s.id, 0, 0, "initial")
	if status != statusMoreProcessing {
		t.Fatalf("initial status %x", status)
	}
	_, status, _ = registrySetup(t, t.Context(), next, h.SessionID, 71, s.id, 0, 0, "proof")
	if status != statusOK || raw.closes.Load() != 1 || decisions.Load() < 3 {
		t.Fatalf("retirement status=%x closes=%d policy=%d", status, raw.closes.Load(), decisions.Load())
	}
	s.mu.Lock()
	clean := s.cleaned
	s.mu.Unlock()
	if !clean {
		t.Fatal("old volume session retained")
	}
}
