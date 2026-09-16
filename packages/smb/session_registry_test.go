package smb

import (
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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
	smbLE.PutUint32(packet[68:72], 1)
	smbLE.PutUint32(packet[72:76], channel)
	smbLE.PutUint64(packet[80:88], previous)
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

func TestSessionIDsAreGlobalAndReservedValuesNeverAllocate(t *testing.T) {
	first, original, _, _, _, _ := testConnection(t)
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
	c, _, _, _, _, _ := testConnection(t)
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

func TestPreviousSessionIsHandledOnlyAfterSuccessfulCurrentAuthentication(t *testing.T) {
	for _, test := range []struct {
		name     string
		sid      string
		previous func(old, current uint64) uint64
		proof    string
		retire   bool
	}{
		{"unknown", "S-1-5-21-1", func(uint64, uint64) uint64 { return 9000 }, "proof", false},
		{"same_user", "S-1-5-21-1", func(old, _ uint64) uint64 { return old }, "proof", true},
		{"different_user", "S-1-5-21-2", func(old, _ uint64) uint64 { return old }, "proof", false},
		{"current_session", "S-1-5-21-1", func(_, current uint64) uint64 { return current }, "proof", false},
		{"failed_authentication", "S-1-5-21-1", func(old, _ uint64) uint64 { return old }, "invalid", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			old, previous, tr, _, backend, _ := testConnection(t)
			old.server.config.Authenticator = &registryAuthenticator{sid: test.sid}
			current := registryConnection(t, old.server)
			header, status, _ := registrySetup(t, t.Context(), current, 0, 10, previous.id, 77, 0, "initial")
			if status != statusMoreProcessing || backend.closed != 0 {
				t.Fatalf("pre-authentication retirement: %x closes=%d", status, backend.closed)
			}
			id := header.SessionID
			_, status, key := registrySetup(t, t.Context(), current, id, 11, test.previous(previous.id, id), 77, 0, test.proof)
			if test.proof != "proof" {
				if status != statusDenied || key != nil || old.server.sessions.get(id).session != nil {
					t.Fatalf("failed auth retained new session: %x", status)
				}
			} else {
				if status != statusOK || key == nil {
					t.Fatalf("authenticated setup: %x", status)
				}
				owner := old.server.sessions.get(id)
				if owner.connection != current || owner.session == nil {
					t.Fatal("new session missing from global index")
				}
				request := signedRequest(t, owner.session, readCommand(wire.FileID{1}, 0, 1))
				h := request.Header
				_, got, _ := current.dispatch(t.Context(), request, request, &h)
				if got != statusNetworkDeleted {
					t.Fatalf("new session inherited old tree/handle: %x", got)
				}
			}
			if got := old.server.sessions.get(previous.id).session; (got == nil) != test.retire {
				t.Fatalf("old session retirement = %v, want %v", got == nil, test.retire)
			}
			wantCloses := 0
			if test.retire {
				wantCloses = 1
			}
			if backend.closed != wantCloses {
				t.Fatalf("old authority closes=%d, want %d", backend.closed, wantCloses)
			}
			if test.retire {
				if err := tr.export.Unpublish(t.Context()); err != nil {
					t.Fatalf("old authority remained pinned: %v", err)
				}
			}
		})
	}
}

func TestPreviousSessionCancellationDoesNotExemptAnotherConnectionsFrame(t *testing.T) {
	old, previous, _, _, _, _ := testConnection(t)
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
	base, _, _, _, _, _ := testConnection(t)
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
	left, leftSession, _, _, _, _ := testConnection(t)
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

func TestDisconnectedSessionCannotActivateAfterPreviousCleanup(t *testing.T) {
	old, previous, tr, _, _, _ := testConnection(t)
	old.server.config.Authenticator = &registryAuthenticator{sid: previous.principal.SID, immediate: true}
	current := registryConnection(t, old.server)
	gate := &cleanupDrainSession{windowsSession: tr.session, entered: make(chan struct{}), release: make(chan struct{})}
	tr.session = gate
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	result := make(chan uint32, 1)
	go func() {
		_, status, _ := registrySetup(t, ctx, current, 0, 90, previous.id, 0, 0, "proof")
		result <- status
	}()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("previous authority cleanup did not begin")
	}
	current.cleanup()
	close(gate.release)
	select {
	case status := <-result:
		if status != statusSessionDeleted {
			t.Fatalf("late authentication activated after disconnect: %x", status)
		}
	case <-ctx.Done():
		t.Fatal("finalization did not finish after previous cleanup")
	}
	current.mu.Lock()
	remaining := len(current.sessions)
	current.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("disconnected connection revived %d sessions", remaining)
	}
	old.server.sessions.mu.Lock()
	for _, owner := range old.server.sessions.owners {
		if owner.connection == current {
			old.server.sessions.mu.Unlock()
			t.Fatal("disconnected session remained globally reachable")
		}
	}
	old.server.sessions.mu.Unlock()
}

func TestPreviousAuthorityCleanupFailurePreventsNewSessionActivation(t *testing.T) {
	old, previous, tr, _, backend, _ := testConnection(t)
	fault := &cleanupAuthority{windowsSession: backend}
	fault.closeFails.Store(true)
	tr.session = fault
	t.Cleanup(func() { fault.closeFails.Store(false) })
	old.server.config.Authenticator = &registryAuthenticator{sid: previous.principal.SID, immediate: true}
	current := registryConnection(t, old.server)
	header, status, key := registrySetup(t, t.Context(), current, 0, 99, previous.id, 0, 0, "proof")
	if status != statusIO || key != nil {
		t.Fatalf("cleanup failure activated session: %x key=%v", status, key != nil)
	}
	if old.server.sessions.get(header.SessionID).session != nil {
		t.Fatal("failed replacement remained globally usable")
	}
	if old.server.sessions.get(previous.id).session != previous || fault.closes.Load() != 1 {
		t.Fatal("failed old authority cleanup lost its owner")
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
