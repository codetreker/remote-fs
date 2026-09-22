package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func testPrincipal(session string) Principal {
	return Principal{SID: "S-1-5-21-1", LogonSession: LogonSessionID(session), Name: "diagnostic"}
}

func testConnection(t *testing.T, limits Limits) (*Server, *connection) {
	t.Helper()
	config := endpointConfig()
	config.Limits = limits
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	connection := newConnection(server, left)
	server.mu.Lock()
	server.connections[connection] = struct{}{}
	server.mu.Unlock()
	return server, connection
}

func registerSession(t *testing.T, server *Server, connection *connection, session *session) {
	t.Helper()
	if session.trees == nil {
		session.trees = make(map[uint32]*tree)
	}
	if session.cleanedDone == nil {
		session.cleanedDone = make(chan struct{})
	}
	if err := server.sessions.add(connection, session); err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	connection.sessions[session.id] = session
	connection.mu.Unlock()
}

func TestPrincipalComparisonIncludesLogonSession(t *testing.T) {
	first := testPrincipal("0123456789abcdef")
	if !samePrincipalIdentity(first, Principal{SID: first.SID, LogonSession: first.LogonSession, Name: "changed"}) {
		t.Fatal("display-name change altered identity")
	}
	if samePrincipalIdentity(first, testPrincipal("fedcba9876543210")) {
		t.Fatal("different logon sessions became one previous-session owner")
	}
}

type immediateIdentityAuthenticator struct{ principal Principal }

func (a immediateIdentityAuthenticator) Begin(context.Context) (Authentication, error) {
	return &immediateIdentityAuthentication{principal: a.principal}, nil
}

type immediateIdentityAuthentication struct{ principal Principal }

func (a *immediateIdentityAuthentication) Step(context.Context, []byte) (AuthenticationResult, error) {
	return AuthenticationResult{
		Principal: a.principal, SessionKey: []byte("0123456789abcdef"),
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

func (*immediateIdentityAuthentication) Close() error { return nil }

type blockedBeginAuthenticator struct {
	entered chan struct{}
	release chan struct{}
}

func (a *blockedBeginAuthenticator) Begin(ctx context.Context) (Authentication, error) {
	close(a.entered)
	select {
	case <-a.release:
		return nil, errors.New("authentication released")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func startBlockedReauthentication(
	t *testing.T,
	server *Server,
	connection *connection,
	s *session,
) (release func(), done <-chan uint32) {
	t.Helper()
	blocked := &blockedBeginAuthenticator{entered: make(chan struct{}), release: make(chan struct{})}
	server.config.Authenticator = blocked
	s.identityMu.RLock()
	key := s.signer
	s.identityMu.RUnlock()
	request := signedParsedRequest(t, key, setupPacket(1, s.id, "proof"))
	result := make(chan uint32, 1)
	go func() {
		header := request.Header
		_, status, _ := connection.sessionSetup(context.Background(), request, &header)
		result <- status
	}()
	<-blocked.entered
	var once sync.Once
	release = func() { once.Do(func() { close(blocked.release) }) }
	t.Cleanup(release)
	return release, result
}

func TestReauthenticationRejectsSameSIDFromDifferentLogonSession(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	original := testPrincipal("0123456789abcdef")
	s := &session{signer: key, principal: original, trees: make(map[uint32]*tree), identityDeadline: time.Now().Add(time.Hour)}
	registerSession(t, server, connection, s)
	server.config.Authenticator = immediateIdentityAuthenticator{principal: testPrincipal("fedcba9876543210")}
	request := signedParsedRequest(t, key, setupPacket(1, s.id, "proof"))
	header := request.Header
	if _, status, signer := connection.sessionSetup(t.Context(), request, &header); status != statusDenied || signer != key {
		t.Fatalf("cross-logon reauthentication = %#x signer=%p", status, signer)
	}
	s.identityMu.RLock()
	principal := s.principal
	s.identityMu.RUnlock()
	if !principal.SameIdentity(original) {
		t.Fatalf("rejected authentication changed identity: %+v", principal)
	}
}

func TestIdentityAdmissionRejectsInitialSessionWithoutRetainingCapacity(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy error
		status uint32
	}{
		{name: "denied", policy: ErrIdentityDenied, status: statusDenied},
		{name: "policy fault", policy: errors.New("identity policy unavailable"), status: statusIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.MaxSessions = 1
			server, connection := testConnection(t, limits)
			server.config.Authenticator = immediateIdentityAuthenticator{principal: testPrincipal("0123456789abcdef")}
			server.config.AuthorizeIdentity = IdentityAuthorizerFunc(func(context.Context, Principal) error { return test.policy })
			request := setupRequest(t, 1, 0, "proof")
			header := request.Header
			if _, status, signer := connection.sessionSetup(t.Context(), request, &header); status != test.status || signer != nil {
				t.Fatalf("identity admission = %#x signer=%p", status, signer)
			}
			if state := server.Status(); state.Sessions != 0 || state.Trees != 0 {
				t.Fatalf("rejected identity retained resources: %+v", state)
			}
			server.config.AuthorizeIdentity = IdentityAuthorizerFunc(func(context.Context, Principal) error { return nil })
			request = setupRequest(t, 2, 0, "proof")
			header = request.Header
			if _, status, signer := connection.sessionSetup(t.Context(), request, &header); status != statusOK || signer == nil {
				t.Fatalf("reused identity capacity = %#x signer=%p", status, signer)
			}
			accepted := server.sessions.get(header.SessionID).session
			if accepted == nil {
				t.Fatal("accepted identity has no session owner")
			}
			if err := connection.cleanSession(WithPrincipal(t.Context(), testPrincipal("0123456789abcdef")), accepted, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRejectedReauthenticationPreservesExistingSession(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	s := &session{signer: key, principal: principal, trees: make(map[uint32]*tree), identityDeadline: time.Now().Add(time.Hour)}
	registerSession(t, server, connection, s)
	server.config.Authenticator = immediateIdentityAuthenticator{principal: principal}
	server.config.AuthorizeIdentity = IdentityAuthorizerFunc(func(context.Context, Principal) error { return ErrIdentityDenied })
	request := signedParsedRequest(t, key, setupPacket(1, s.id, "proof"))
	header := request.Header
	if _, status, signer := connection.sessionSetup(t.Context(), request, &header); status != statusDenied || signer != key {
		t.Fatalf("reauthentication policy = %#x signer=%p", status, signer)
	}
	if owner := server.sessions.get(s.id); owner.session != s {
		t.Fatal("rejected reauthentication retired established session")
	}
	packet := wire.EncodeResponse(wire.Header{}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatalf("rejected reauthentication destroyed old signer: %v", err)
	}
}

func TestPreviousSessionRetirementRequiresExactIdentity(t *testing.T) {
	server, currentConnection := testConnection(t, DefaultLimits())
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	oldConnection := newConnection(server, left)
	server.mu.Lock()
	server.connections[oldConnection] = struct{}{}
	server.mu.Unlock()
	old := &session{principal: testPrincipal("0123456789abcdef"), trees: make(map[uint32]*tree)}
	registerSession(t, server, oldConnection, old)
	current := &session{principal: old.principal, trees: make(map[uint32]*tree)}
	registerSession(t, server, currentConnection, current)
	if err := currentConnection.retirePreviousSession(t.Context(), current, old.id,
		testPrincipal("fedcba9876543210")); err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(old.id); owner.session != old {
		t.Fatal("different logon session retired the previous owner")
	}
	if err := currentConnection.retirePreviousSession(t.Context(), current, old.id, old.principal); err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(old.id); owner.session != nil {
		t.Fatal("exact identity did not retire previous session")
	}
	if err := currentConnection.retirePreviousSession(t.Context(), current, 0, old.principal); err != nil {
		t.Fatal(err)
	}
}

func setupRequest(t *testing.T, message, sessionID uint64, token string) wire.Request {
	t.Helper()
	requests, err := wire.ParseFrame(setupPacket(message, sessionID, token), wire.Limits{
		MaxBytes: 1 << 20, MaxCommands: 1, MaxContexts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return requests[0]
}

func TestIncompletePrimaryAuthenticationExpiresThroughWatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := DefaultLimits()
		limits.HandshakeTimeout = 5 * time.Second
		server, connection := testConnection(t, limits)
		server.config.Authenticator = &protocolAuthenticator{}
		request := setupRequest(t, 1, 0, "initial")
		header := request.Header
		_, status, signer := connection.sessionSetup(t.Context(), request, &header)
		if status != statusMoreProcessing || signer != nil || header.SessionID == 0 {
			t.Fatalf("challenge = %#x session=%d signer=%v", status, header.SessionID, signer)
		}
		expiring := server.sessions.get(header.SessionID).session
		if expiring == nil {
			t.Fatal("challenge session was not registered")
		}
		time.Sleep(limits.HandshakeTimeout)
		synctest.Wait()
		select {
		case <-expiring.authDone:
		default:
			t.Fatal("authentication watcher did not finish")
		}
		if owner := server.sessions.get(header.SessionID); owner.session != nil {
			t.Fatal("expired authentication retained global capacity")
		}
	})
}

func TestAbandonedSecondaryAuthenticationPreservesEstablishedSession(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	authentication := &endpointAuthentication{}
	s := &session{
		signer: key, principal: testPrincipal("0123456789abcdef"), auth: authentication,
		authArmed: true, authGeneration: 4, authDeadline: time.Now().Add(-time.Second),
		identityDeadline: time.Now().Add(time.Hour),
	}
	registerSession(t, server, connection, s)
	if connection.expireAuthentication(s, 4) {
		t.Fatal("secondary timeout retired the established session")
	}
	if !authentication.closed || authentication.closes != 1 || s.retired || s.auth != nil {
		t.Fatalf("secondary timeout state: closed=%v closes=%d retired=%v auth=%v", authentication.closed, authentication.closes, s.retired, s.auth)
	}
	packet := wire.EncodeResponse(wire.Header{}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatalf("established signer was lost: %v", err)
	}
}

func TestAuthenticationCloseFailureRetainsGlobalSessionCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxSessions = 1
	server, connection := testConnection(t, limits)
	cause := errors.New("native close failed")
	authentication := &endpointAuthentication{closeErr: cause}
	s := &session{auth: authentication, authArmed: true, authGeneration: 1, authDeadline: time.Now().Add(-time.Second)}
	registerSession(t, server, connection, s)
	if !connection.expireAuthentication(s, 1) {
		t.Fatal("initial authentication timeout did not retire its session")
	}
	connection.finishSessionRetirement(s)
	if owner := server.sessions.get(s.id); owner.session != s {
		t.Fatal("failed native cleanup released global capacity")
	}
	if err := server.sessions.add(connection, &session{}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("capacity after failed close = %v", err)
	}
	authentication.closeErr = nil
	if err := connection.cleanSession(t.Context(), s, false); err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(s.id); owner.session != nil {
		t.Fatal("successful retry retained global capacity")
	}
}

func signedParsedRequest(t *testing.T, key *signing.Session, packet []byte) wire.Request {
	t.Helper()
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	requests, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: 1 << 20, MaxCommands: 1, MaxContexts: 1})
	if err != nil {
		t.Fatal(err)
	}
	return requests[0]
}

func TestNativeIdentityExpiryFencesWorkAndAllowsReauthentication(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	s := &session{signer: key, principal: testPrincipal("0123456789abcdef"), identityDeadline: time.Now().Add(-time.Second)}
	registerSession(t, server, connection, s)
	connection.negotiated = true
	if connection.expireAuthentication(s, 0) {
		t.Fatal("identity expiry retired the SMB session")
	}
	if owner := server.sessions.get(s.id); owner.session != s {
		t.Fatal("expired identity lost its reauthentication owner")
	}
	if status := server.Status(); status.ExpiredSessions != 1 {
		t.Fatalf("expired identity was not observable: %+v", status)
	}
	echo := signedParsedRequest(t, key, requestPacket(wire.Header{Command: wire.Echo, MessageID: 1, SessionID: s.id}, wire.EmptyResponseBody()))
	header := echo.Header
	if _, status, signer := connection.dispatch(t.Context(), echo, echo, &header); status != statusNetworkSessionExpired || signer != key {
		t.Fatalf("expired ordinary request = %#x signer=%p", status, signer)
	}
	server.config.Authenticator = &protocolAuthenticator{}
	first := signedParsedRequest(t, key, setupPacket(2, s.id, "initial"))
	header = first.Header
	if _, status, signer := connection.sessionSetup(t.Context(), first, &header); status != statusMoreProcessing || signer != key {
		t.Fatalf("expired reauthentication challenge = %#x signer=%p", status, signer)
	}
	second := signedParsedRequest(t, key, setupPacket(3, s.id, "proof"))
	header = second.Header
	if _, status, signer := connection.sessionSetup(t.Context(), second, &header); status != statusOK || signer != key {
		t.Fatalf("expired reauthentication completion = %#x signer=%p", status, signer)
	}
	if status := server.Status(); status.ExpiredSessions != 0 {
		t.Fatalf("reauthenticated identity remained expired: %+v", status)
	}
	echo = signedParsedRequest(t, key, requestPacket(wire.Header{Command: wire.Echo, MessageID: 4, SessionID: s.id}, wire.EmptyResponseBody()))
	header = echo.Header
	if _, status, signer := connection.dispatch(t.Context(), echo, echo, &header); status != statusOK || signer != key {
		t.Fatalf("reauthenticated request = %#x signer=%p", status, signer)
	}
}

func TestStatusDoesNotWaitForBlockedAuthenticationProvider(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	s := &session{trees: make(map[uint32]*tree)}
	s.identityExpired.Store(true)
	registerSession(t, server, connection, s)
	s.authMu.Lock()
	defer s.authMu.Unlock()
	done := make(chan Status, 1)
	go func() { done <- server.Status() }()
	select {
	case status := <-done:
		if status.ExpiredSessions != 1 {
			t.Fatalf("status = %+v", status)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Status waited for a blocked authentication provider")
	}
}

func TestSessionCleanupCompletionWaitIsBounded(t *testing.T) {
	already := &session{cleaned: true}
	if err := already.waitCleaned(t.Context()); err != nil {
		t.Fatal(err)
	}
	waiting := &session{cleanedDone: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waiting.waitCleaned(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- waiting.waitCleaned(context.Background()) }()
	waiting.mu.Lock()
	waiting.cleaned = true
	close(waiting.cleanedDone)
	waiting.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRetirementKeepsSignerUntilFinalResponseFrameCompletes(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	s := &session{signer: key, principal: testPrincipal("0123456789abcdef")}
	registerSession(t, server, connection, s)
	frame := requestFrame{connection: connection, id: 17}
	ctx := context.WithValue(t.Context(), pendingFrameKey{}, frame)
	connection.mu.Lock()
	connection.retainSessionFrameLocked(s, ctx, 17)
	connection.mu.Unlock()
	s.mu.Lock()
	s.retired = true
	s.mu.Unlock()
	connection.finishSessionRetirement(s)
	if owner := server.sessions.get(s.id); owner.session != s {
		t.Fatal("session disappeared before its final response")
	}
	packet := wire.EncodeResponse(wire.Header{}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatalf("final response lost signer: %v", err)
	}
	connection.mu.Lock()
	connection.retireResponseFrameLocked(frame)
	connection.mu.Unlock()
	if owner := server.sessions.get(s.id); owner.session != nil {
		t.Fatal("completed response retained its session")
	}
	if err := key.Verify(packet); !errors.Is(err, signing.ErrDestroyed) {
		t.Fatalf("completed response signer = %v", err)
	}
}

func TestVolumeTreeSharesOneAuthoritySessionAndReleasesIt(t *testing.T) {
	var operations []storage.Operation
	config := endpointConfig()
	config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
		principal, ok := PrincipalFromContext(ctx)
		if !ok || !principal.SameIdentity(testPrincipal("0123456789abcdef")) {
			return authz.ErrDenied
		}
		operations = append(operations, request.Operation)
		return nil
	})
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	backend := &endpointStorage{session: newEndpointFileSession()}
	export, err := server.Publish(endpointShare("data", "volume", backend))
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := newConnection(server, left)
	principal := testPrincipal("0123456789abcdef")
	s := &session{principal: principal, trees: make(map[uint32]*tree)}
	header := wire.Header{}
	if _, status := connection.connectVolume(WithPrincipal(t.Context(), principal), s, "DATA", &header); status != statusOK {
		t.Fatalf("tree connect status = %#x", status)
	}
	if header.TreeID == 0 || len(s.trees) != 1 || len(s.authorities) != 1 {
		t.Fatalf("tree state: id=%d trees=%d authorities=%d", header.TreeID, len(s.trees), len(s.authorities))
	}
	tree := s.trees[header.TreeID]
	if err := connection.closeTreeAuthorized(WithPrincipal(t.Context(), principal), tree); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	delete(s.trees, tree.id)
	if err := connection.closeOrphansLocked(t.Context(), s, export); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	s.mu.Unlock()
	if backend.session.closes != 1 {
		t.Fatalf("file session closes = %d", backend.session.closes)
	}
	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []storage.Operation{storage.OpFileSessionOpen, storage.OpFileStatus, storage.OpFileListDeleteIntents, storage.OpFileSessionClose}
	if len(operations) != len(want) {
		t.Fatalf("authorization operations = %v", operations)
	}
	for index := range want {
		if operations[index] != want[index] {
			t.Fatalf("authorization operations = %v", operations)
		}
	}
}

func TestSessionAndTreeLimitsReuseExactCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxSessions = 1
	limits.MaxTrees = 1
	server, connection := testConnection(t, limits)
	first := &session{trees: make(map[uint32]*tree)}
	registerSession(t, server, connection, first)
	if err := server.sessions.add(connection, &session{}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("session max+1 = %v", err)
	}
	header := wire.Header{}
	if _, status := connection.connectControl(first, &header); status != statusOK {
		t.Fatalf("first tree = %#x", status)
	}
	if _, status := connection.connectControl(first, &wire.Header{}); status != statusResources {
		t.Fatalf("tree max+1 = %#x", status)
	}
	first.mu.Lock()
	tree := first.trees[header.TreeID]
	first.mu.Unlock()
	if err := connection.closeTree(tree); err != nil {
		t.Fatal(err)
	}
	first.mu.Lock()
	delete(first.trees, tree.id)
	first.mu.Unlock()
	if _, status := connection.connectControl(first, &wire.Header{}); status != statusOK {
		t.Fatalf("tree capacity did not recover: %#x", status)
	}
	first.mu.Lock()
	for _, tree := range first.trees {
		_ = connection.closeTree(tree)
		delete(first.trees, tree.id)
	}
	first.retired = true
	first.mu.Unlock()
	connection.finishSessionRetirement(first)
	if err := server.sessions.add(connection, &session{}); err != nil {
		t.Fatalf("session capacity did not recover: %v", err)
	}
}

type blockingEndpointStorage struct {
	endpointStorage
	entered chan struct{}
	release chan struct{}
}

func (s *blockingEndpointStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.endpointStorage.NewFileSession(ctx, options)
}

func TestLogoffRacingTreeConnectCannotInstallLateAuthority(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	backend := &blockingEndpointStorage{
		endpointStorage: endpointStorage{session: newEndpointFileSession()},
		entered:         make(chan struct{}), release: make(chan struct{}),
	}
	export, err := server.Publish(endpointShare("data", "volume", backend))
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	s := &session{principal: principal, trees: make(map[uint32]*tree)}
	registerSession(t, server, connection, s)
	connectDone := make(chan uint32, 1)
	go func() {
		_, status := connection.connectVolume(WithPrincipal(context.Background(), principal), s, export.key, &wire.Header{})
		connectDone <- status
	}()
	<-backend.entered
	logoffDone := make(chan error, 1)
	go func() { logoffDone <- connection.cleanSession(WithPrincipal(context.Background(), principal), s, true) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		retired := s.retired
		s.mu.Unlock()
		if retired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("logoff did not fence the session")
		}
		time.Sleep(time.Millisecond)
	}
	close(backend.release)
	if status := <-connectDone; status != statusSessionDeleted {
		t.Fatalf("late tree connect = %#x", status)
	}
	if err := <-logoffDone; err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(s.id); owner.session != nil {
		t.Fatal("logoff race retained global session")
	}
	s.mu.Lock()
	trees, authorities := len(s.trees), len(s.authorities)
	s.mu.Unlock()
	server.mu.Lock()
	refs := export.refs
	server.mu.Unlock()
	if trees != 0 || authorities != 0 || refs != 0 || backend.session.closes != 1 {
		t.Fatalf("late tree leak: trees=%d authorities=%d refs=%d closes=%d", trees, authorities, refs, backend.session.closes)
	}
}

func TestLogoffWaitsForPreviousSessionFinalization(t *testing.T) {
	server, currentConnection := testConnection(t, DefaultLimits())
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	oldConnection := newConnection(server, left)
	server.mu.Lock()
	server.connections[oldConnection] = struct{}{}
	server.mu.Unlock()
	closeEntered, closeRelease := make(chan struct{}), make(chan struct{})
	backend := &endpointStorage{session: newEndpointFileSession()}
	backend.session.closeEntered, backend.session.closeRelease = closeEntered, closeRelease
	export, err := server.Publish(endpointShare("data", "volume", backend))
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	old := &session{principal: principal, trees: make(map[uint32]*tree)}
	registerSession(t, server, oldConnection, old)
	if _, status := oldConnection.connectVolume(WithPrincipal(t.Context(), principal), old, export.key, &wire.Header{}); status != statusOK {
		t.Fatalf("old tree connect = %#x", status)
	}
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	current := &session{signer: key, principal: principal, trees: make(map[uint32]*tree), identityDeadline: time.Now().Add(time.Hour)}
	registerSession(t, server, currentConnection, current)
	server.config.Authenticator = immediateIdentityAuthenticator{principal: principal}
	requestPacket := setupPacket(1, current.id, "proof")
	binary.LittleEndian.PutUint64(requestPacket[wire.HeaderSize+16:], old.id)
	request := signedParsedRequest(t, key, requestPacket)
	reauthenticated := make(chan uint32, 1)
	go func() {
		header := request.Header
		_, status, _ := currentConnection.sessionSetup(context.Background(), request, &header)
		reauthenticated <- status
	}()
	<-closeEntered
	loggedOff := make(chan error, 1)
	go func() {
		loggedOff <- currentConnection.cleanSession(WithPrincipal(context.Background(), principal), current, true)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		current.mu.Lock()
		retired := current.retired
		current.mu.Unlock()
		if retired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("LOGOFF did not publish retirement")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-loggedOff:
		t.Fatalf("LOGOFF returned while session finalization remained blocked: %v", err)
	default:
	}
	close(closeRelease)
	if status := <-reauthenticated; status != statusSessionDeleted {
		t.Fatalf("late reauthentication = %#x", status)
	}
	if err := <-loggedOff; err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(current.id); owner.session != nil {
		t.Fatal("LOGOFF left current session capacity owned")
	}
}

func TestShutdownRetainsFailedCleanupAndRetries(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	cause := errors.New("close outcome unknown")
	backend := &endpointStorage{session: newEndpointFileSession()}
	backend.session.closeErr = cause
	export, err := server.Publish(endpointShare("data", "volume", backend))
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	s := &session{principal: principal, trees: make(map[uint32]*tree)}
	registerSession(t, server, connection, s)
	if _, status := connection.connectVolume(WithPrincipal(t.Context(), principal), s, export.key, &wire.Header{}); status != statusOK {
		t.Fatalf("tree connect = %#x", status)
	}
	connection.mu.Lock()
	connection.disconnected = true
	connection.mu.Unlock()
	if err := server.Shutdown(t.Context()); !errors.Is(err, cause) {
		t.Fatalf("first shutdown = %v", err)
	}
	if state := server.Status(); !state.Stopping || state.Stopped || state.Sessions != 1 || state.Connections != 1 || state.RetainedConnections != 1 || state.Trees != 1 || state.CleanupFailures == 0 {
		t.Fatalf("failed cleanup lost ownership: %+v", state)
	}
	backend.session.closeErr = nil
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := server.Status(); state.Sessions != 0 || state.Connections != 0 || state.RetainedConnections != 0 || state.Trees != 0 || state.Exports != 0 || state.StoppingExports != 0 {
		t.Fatalf("retry did not settle ownership: %+v", state)
	}
}

func TestUnpublishRefusesAnIdleLiveTreeWithoutChangingIt(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	backend := &endpointStorage{session: newEndpointFileSession()}
	export, err := server.Publish(endpointShare("data", "volume", backend))
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	s := &session{principal: principal, trees: make(map[uint32]*tree)}
	registerSession(t, server, connection, s)
	if _, status := connection.connectVolume(WithPrincipal(t.Context(), principal), s, export.key, &wire.Header{}); status != statusOK {
		t.Fatalf("tree connect = %#x", status)
	}
	if err := export.Unpublish(t.Context()); !errors.Is(err, ErrBusy) {
		t.Fatalf("unpublish with live tree = %v", err)
	}
	if state := server.Status(); state.Exports != 1 || state.StoppingExports != 0 || state.Trees != 1 || state.FencedAuthorities != 0 {
		t.Fatalf("busy unpublish changed ownership: %+v", state)
	}
	if backend.session.closes != 0 {
		t.Fatal("busy unpublish started cleanup")
	}
}

func TestUnpublishUnusedExportDoesNotJoinUnrelatedConnectionCleanup(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	target, err := server.Publish(endpointShare("target", "target", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	closeEntered := make(chan struct{})
	closeRelease := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-closeRelease:
		default:
			close(closeRelease)
		}
	})
	unrelatedBackend := &endpointStorage{session: newEndpointFileSession()}
	unrelatedBackend.session.closeEntered = closeEntered
	unrelatedBackend.session.closeRelease = closeRelease
	unrelated, err := server.Publish(endpointShare("unrelated", "unrelated", unrelatedBackend))
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	s := &session{principal: principal, trees: make(map[uint32]*tree)}
	registerSession(t, server, connection, s)
	if _, status := connection.connectVolume(WithPrincipal(t.Context(), principal), s, unrelated.key, &wire.Header{}); status != statusOK {
		t.Fatalf("unrelated tree connect = %#x", status)
	}
	connection.mu.Lock()
	connection.disconnected = true
	connection.mu.Unlock()
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- connection.retryDisconnected(context.Background()) }()
	<-closeEntered

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	unpublishDone := make(chan error, 1)
	go func() { unpublishDone <- target.Unpublish(ctx) }()
	select {
	case err := <-unpublishDone:
		if err != nil {
			t.Fatalf("unused export unpublish = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(closeRelease)
		<-cleanupDone
		t.Fatal("unused export unpublish waited behind unrelated connection cleanup")
	}
	if state := server.Status(); state.Exports != 1 {
		t.Fatalf("unused export remained published: %+v", state)
	}
	close(closeRelease)
	if err := <-cleanupDone; err != nil {
		t.Fatal(err)
	}
}

func TestUnpublishOwnedExportDoesNotWaitForUnrelatedAuthentication(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	export, err := server.Publish(endpointShare("target", "target", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	done := make(chan struct{})
	close(done)
	authority := &authoritySession{
		export: export, principal: testPrincipal("0123456789abcdef"), orphan: true,
		ready: ready, done: done,
	}
	owner := &session{
		trees:       make(map[uint32]*tree),
		authorities: map[*Export]*authoritySession{export: authority},
	}
	registerSession(t, server, connection, owner)
	server.mu.Lock()
	export.refs++
	server.mu.Unlock()

	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	unrelated := &session{
		signer: key, principal: testPrincipal("0123456789abcdef"),
		trees: make(map[uint32]*tree), identityDeadline: time.Now().Add(time.Hour),
	}
	registerSession(t, server, connection, unrelated)
	release, authenticationDone := startBlockedReauthentication(t, server, connection, unrelated)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	unpublishDone := make(chan error, 1)
	go func() { unpublishDone <- export.Unpublish(ctx) }()
	select {
	case err := <-unpublishDone:
		if err != nil {
			release()
			t.Fatalf("unpublish with unrelated authentication = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		release()
		<-authenticationDone
		t.Fatal("unpublish waited for unrelated authentication")
	}
	if state := server.Status(); state.Exports != 0 || state.StoppingExports != 0 {
		release()
		t.Fatalf("unpublish retained target export: %+v", state)
	}
	release()
	if status := <-authenticationDone; status != statusDenied {
		t.Fatalf("released authentication = %#x", status)
	}
}

func TestUnpublishRetiredOwnerAuthenticationWaitHonorsContext(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	export, err := server.Publish(endpointShare("target", "target", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	done := make(chan struct{})
	close(done)
	authority := &authoritySession{
		export: export, principal: testPrincipal("0123456789abcdef"), orphan: true,
		ready: ready, done: done,
	}
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	s := &session{
		signer: key, principal: testPrincipal("0123456789abcdef"),
		trees: map[uint32]*tree{}, authorities: map[*Export]*authoritySession{export: authority},
		identityDeadline: time.Now().Add(time.Hour),
	}
	registerSession(t, server, connection, s)
	server.mu.Lock()
	export.refs++
	server.mu.Unlock()
	release, authenticationDone := startBlockedReauthentication(t, server, connection, s)
	connection.retireSessionRequests(s, requestFrame{})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	unpublishDone := make(chan error, 1)
	go func() { unpublishDone <- export.Unpublish(ctx) }()
	select {
	case err := <-unpublishDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			release()
			t.Fatalf("unpublish authentication wait = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		release()
		<-authenticationDone
		t.Fatal("unpublish exceeded its authentication wait deadline")
	}
	if state := server.Status(); state.Exports != 1 || state.StoppingExports != 1 {
		release()
		t.Fatalf("timed-out authentication wait lost export ownership: %+v", state)
	}

	release()
	if status := <-authenticationDone; status != statusDenied {
		t.Fatalf("released authentication = %#x", status)
	}
	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(s.id); owner.session != nil {
		t.Fatal("completed authentication retained retired session capacity")
	}
	if state := server.Status(); state.Exports != 0 || state.StoppingExports != 0 || state.Sessions != 0 {
		t.Fatalf("retry retained retired ownership: %+v", state)
	}
}

func TestUnpublishRetryCompletesRetirementAfterNativeCloseDeadline(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxSessions = 1
	server, connection := testConnection(t, limits)
	export, err := server.Publish(endpointShare("target", "target", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	done := make(chan struct{})
	close(done)
	authority := &authoritySession{
		export: export, principal: testPrincipal("0123456789abcdef"), orphan: true,
		ready: ready, done: done,
	}
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	packet := wire.EncodeResponse(wire.Header{}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	s := &session{
		retired: true, signer: key, principal: testPrincipal("0123456789abcdef"),
		trees: map[uint32]*tree{}, authorities: map[*Export]*authoritySession{export: authority},
	}
	registerSession(t, server, connection, s)
	server.mu.Lock()
	export.refs++
	server.mu.Unlock()
	connection.mu.Lock()
	connection.disconnected = true
	connection.mu.Unlock()

	s.authMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			s.authMu.Unlock()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	if err := export.Unpublish(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("retirement bookkeeping wait = %v", err)
	}
	cancel()
	if !authority.isClosed() {
		t.Fatal("deadline returned before native authority cleanup completed")
	}
	s.mu.Lock()
	retained := s.authorities[export] == authority
	s.mu.Unlock()
	if !retained {
		t.Fatal("closed authority was removed before retirement commit")
	}
	s.authMu.Unlock()
	locked = false
	s.retirementMu.Lock()
	resourcesClosed := s.resourcesClosed
	s.retirementMu.Unlock()
	if resourcesClosed {
		t.Fatal("canceled retirement bookkeeping reported resources closed")
	}
	if state := server.Status(); state.Exports != 1 || state.StoppingExports != 1 ||
		state.Sessions != 1 || state.Connections != 1 || state.RetainedConnections != 1 {
		t.Fatalf("canceled retirement lost retry ownership: %+v", state)
	}

	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if owner := server.sessions.get(s.id); owner.session != nil {
		t.Fatal("retirement retry retained global session ownership")
	}
	if err := key.Verify(packet); !errors.Is(err, signing.ErrDestroyed) {
		t.Fatalf("retirement retry retained signer: %v", err)
	}
	if state := server.Status(); state.Exports != 0 || state.StoppingExports != 0 ||
		state.Sessions != 0 || state.Connections != 0 || state.RetainedConnections != 0 {
		t.Fatalf("retirement retry retained resources: %+v", state)
	}
	replacement := &session{}
	if err := server.sessions.add(connection, replacement); err != nil {
		t.Fatalf("retirement retry retained global capacity: %v", err)
	}
	server.sessions.remove(replacement)
}

func TestUnpublishRetiredSessionKeepsOtherExportIndependent(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	target, err := server.Publish(endpointShare("target", "target", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := server.Publish(endpointShare("remaining", "remaining", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	closedChannel := func() chan struct{} {
		channel := make(chan struct{})
		close(channel)
		return channel
	}
	targetAuthority := &authoritySession{
		export: target, principal: testPrincipal("0123456789abcdef"), orphan: true,
		ready: closedChannel(), done: closedChannel(),
	}
	remainingAuthority := &authoritySession{
		export: remaining, principal: testPrincipal("0123456789abcdef"), orphan: true,
		ready: closedChannel(), done: closedChannel(),
	}
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	packet := wire.EncodeResponse(wire.Header{}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatal(err)
	}
	s := &session{
		retired: true, signer: key, principal: testPrincipal("0123456789abcdef"),
		trees: map[uint32]*tree{}, authorities: map[*Export]*authoritySession{
			target: targetAuthority, remaining: remainingAuthority,
		},
	}
	registerSession(t, server, connection, s)
	server.mu.Lock()
	target.refs++
	remaining.refs++
	server.mu.Unlock()
	connection.mu.Lock()
	connection.disconnected = true
	connection.mu.Unlock()

	s.authMu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err = target.Unpublish(ctx)
	cancel()
	s.authMu.Unlock()
	if err != nil {
		t.Fatalf("target unpublish waited for unrelated retirement: %v", err)
	}
	s.mu.Lock()
	targetOwned := s.authorities[target] != nil
	remainingOwned := s.authorities[remaining] == remainingAuthority
	s.mu.Unlock()
	if targetOwned || !remainingOwned || remainingAuthority.isClosed() {
		t.Fatalf("target cleanup changed remaining authority: target=%v remaining=%v closed=%v",
			targetOwned, remainingOwned, remainingAuthority.isClosed())
	}
	if state := server.Status(); state.Exports != 1 || state.StoppingExports != 0 ||
		state.Sessions != 1 || state.Connections != 1 || state.RetainedConnections != 1 {
		t.Fatalf("target cleanup changed unrelated ownership: %+v", state)
	}

	if err := remaining.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := key.Verify(packet); !errors.Is(err, signing.ErrDestroyed) {
		t.Fatalf("final export cleanup retained signer: %v", err)
	}
	if state := server.Status(); state.Exports != 0 || state.Sessions != 0 ||
		state.Connections != 0 || state.RetainedConnections != 0 {
		t.Fatalf("final export cleanup retained resources: %+v", state)
	}
}

func TestCloseExportCleanupSerializationHonorsContext(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	export := &Export{server: server, share: endpointShare("target", "target", &endpointStorage{})}
	authority := &authoritySession{export: export}
	s := &session{
		trees:       make(map[uint32]*tree),
		authorities: map[*Export]*authoritySession{export: authority},
	}
	registerSession(t, server, connection, s)
	if err := connection.cleanupMu.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer connection.cleanupMu.unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := connection.closeExport(ctx, export); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close export lock wait = %v", err)
	}
}

func TestUnpublishOwnedCleanupWaitHonorsContextAndCanRetry(t *testing.T) {
	server, connection := testConnection(t, DefaultLimits())
	export, err := server.Publish(endpointShare("target", "target", &endpointStorage{}))
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	close(ready)
	done := make(chan struct{})
	close(done)
	authority := &authoritySession{
		export: export, principal: testPrincipal("0123456789abcdef"), orphan: true,
		ready: ready, done: done,
	}
	s := &session{
		trees:       make(map[uint32]*tree),
		authorities: map[*Export]*authoritySession{export: authority},
	}
	registerSession(t, server, connection, s)
	server.mu.Lock()
	export.refs++
	server.mu.Unlock()
	if err := authority.treeCloseMu.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := export.Unpublish(ctx); !errors.Is(err, context.DeadlineExceeded) {
		authority.treeCloseMu.unlock()
		t.Fatalf("unpublish cleanup wait = %v", err)
	}
	if state := server.Status(); state.Exports != 1 || state.StoppingExports != 1 {
		authority.treeCloseMu.unlock()
		t.Fatalf("timed-out cleanup lost ownership: %+v", state)
	}
	authority.treeCloseMu.unlock()
	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := server.Status(); state.Exports != 0 || state.StoppingExports != 0 {
		t.Fatalf("retry did not settle export: %+v", state)
	}
}

func TestAuthorityRenewalFailureFencesAndClosesSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, connection := testConnection(t, DefaultLimits())
		failure := errors.New("renew failed")
		raw := newEndpointFileSession()
		raw.status.Remaining = 9 * time.Second
		raw.renewFn = func(_ context.Context, attempt int) (storage.FileSessionStatus, error) {
			if attempt == 1 {
				status := raw.status
				status.Revision++
				return status, nil
			}
			return storage.FileSessionStatus{}, failure
		}
		export := &Export{server: server, share: endpointShare("volume", "volume", &endpointStorage{})}
		authority := &authoritySession{
			raw: raw, export: export, principal: testPrincipal("0123456789abcdef"),
			ready: make(chan struct{}), done: make(chan struct{}), epoch: raw.status.Epoch,
			revision: raw.status.Revision, actionEpoch: raw.status.ActionEpoch,
			deadline: time.Now().Add(raw.status.Remaining),
		}
		s := &session{trees: make(map[uint32]*tree), authorities: map[*Export]*authoritySession{export: authority}}
		registerSession(t, server, connection, s)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		authority.cancel = cancel
		go authority.renew(ctx, server.config.Limits)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		raw.mu.Lock()
		renewals := raw.renewals
		raw.mu.Unlock()
		authority.installMu.RLock()
		revision := authority.revision
		authority.installMu.RUnlock()
		if renewals != 1 || revision != 2 {
			t.Fatalf("first renewal: calls=%d revision=%d", renewals, revision)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		select {
		case <-authority.done:
		default:
			t.Fatal("failed renewal did not stop the worker")
		}
		raw.mu.Lock()
		closes := raw.closes
		raw.mu.Unlock()
		if !authority.isClosed() || closes != 1 || server.Status().CleanupFailures == 0 || server.Status().FencedAuthorities != 1 {
			t.Fatalf("renewal failure state: closed=%v closes=%d status=%+v", authority.isClosed(), closes, server.Status())
		}
	})
}

func TestControlTreeDispatchAndCapacityReuse(t *testing.T) {
	_, connection := testConnection(t, DefaultLimits())
	s := &session{trees: make(map[uint32]*tree)}
	header := wire.Header{}
	if _, status := connection.connectControl(s, &header); status != statusOK {
		t.Fatalf("connect control = %#x", status)
	}
	tree := s.trees[header.TreeID]
	unsupported := setupRequest(t, 1, 0, "x")
	unsupported.Header.Command = wire.IOCTL
	if _, status := connection.control(s, tree, unsupported); status != statusUnsupported {
		t.Fatalf("control IOCTL = %#x", status)
	}
	disconnect := setupRequest(t, 2, 0, "x")
	disconnect.Header.Command = wire.TreeDisconnect
	disconnect.Body = wire.EmptyResponseBody()
	if _, status := connection.control(s, tree, disconnect); status != statusOK {
		t.Fatalf("control disconnect = %#x", status)
	}
	if len(s.trees) != 0 {
		t.Fatal("control tree was retained")
	}
}

func TestStatusErrorPreservesClosedFailureVocabulary(t *testing.T) {
	for _, test := range []struct {
		err  error
		want uint32
	}{
		{nil, statusOK}, {authz.ErrDenied, statusDenied}, {syscall.EINTR, statusCancelled},
		{ErrIdentityDenied, statusDenied},
		{syscall.EINVAL, statusInvalid}, {syscall.EACCES, statusDenied},
		{syscall.ENOMEM, statusResources}, {syscall.EOPNOTSUPP, statusUnsupported},
		{syscall.ESTALE, statusSessionDeleted}, {errors.New("unknown"), statusIO},
	} {
		if got := statusError(test.err); got != test.want {
			t.Fatalf("statusError(%v) = %#x, want %#x", test.err, got, test.want)
		}
	}
}
