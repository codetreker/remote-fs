package smb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

type expiryAuthenticator struct {
	opened, closed, closeCalls atomic.Int32
	failClose                  atomic.Bool
	closeDuringStep            atomic.Bool
	step                       func(context.Context, []byte) (AuthenticationResult, error)
}

type expiryAuthentication struct {
	owner  *expiryAuthenticator
	inStep atomic.Bool
}

func (a *expiryAuthenticator) Begin(context.Context) (Authentication, error) {
	a.opened.Add(1)
	return &expiryAuthentication{owner: a}, nil
}

func (a *expiryAuthentication) Step(ctx context.Context, token []byte) (AuthenticationResult, error) {
	a.inStep.Store(true)
	defer a.inStep.Store(false)
	if a.owner.step != nil {
		return a.owner.step(ctx, token)
	}
	if string(token) == "initial" {
		return AuthenticationResult{Continue: true, Token: []byte("challenge")}, nil
	}
	if string(token) != "proof" {
		return AuthenticationResult{}, errors.New("invalid proof")
	}
	return AuthenticationResult{Principal: Principal{SID: "S-1-5-21-1", Name: "original"}, SessionKey: []byte("0123456789abcdef")}, nil
}

func (a *expiryAuthentication) Close() error {
	a.owner.closeCalls.Add(1)
	if a.inStep.Load() {
		a.owner.closeDuringStep.Store(true)
	}
	if a.owner.failClose.Load() {
		return errors.New("authentication close failed")
	}
	a.owner.closed.Add(1)
	return nil
}

func expiryConnection(t *testing.T) (*connection, *session, *expiryAuthenticator) {
	t.Helper()
	a := &expiryAuthenticator{}
	config := testConfig()
	config.Authenticator = a
	config.Limits.MaxSessions = 2
	config.Limits.HandshakeTimeout = 30 * time.Second
	s, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	c := registryConnection(t, s)
	h, status, _ := registrySetup(t, t.Context(), c, 0, 1, 0, 0, 0, "proof")
	if status != statusOK {
		t.Fatalf("primary authentication: %#x", status)
	}
	return c, s.sessions.get(h.SessionID).session, a
}

func expiryEcho(t *testing.T, c *connection, s *session, key *signing.Session) {
	t.Helper()
	p := requestPacket(wire.Header{Command: wire.Echo, SessionID: s.id, MessageID: 90}, wire.EmptyResponseBody())
	if err := key.Sign(p); err != nil {
		t.Fatal(err)
	}
	h, err := wire.ParseHeader(p)
	if err != nil {
		t.Fatal(err)
	}
	r := wire.Request{Header: h, Packet: p, Body: p[64:]}
	body, status, replyKey := c.dispatch(t.Context(), r, r, &h)
	if status != statusOK || replyKey != key {
		t.Fatalf("original signed echo: status=%#x key=%p, want %p", status, replyKey, key)
	}
	reply := wire.EncodeResponse(h, body)
	if err := replyKey.Sign(reply); err != nil {
		t.Fatal(err)
	}
	if err := key.Verify(reply); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticationExpiryReclaimsAbandonedSecondary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, primary, a := expiryConnection(t)
		h, status, _ := registrySetup(t, t.Context(), c, 0, 2, 0, 0, 0, "initial")
		if status != statusMoreProcessing || c.server.Status().Sessions != 2 {
			t.Fatalf("secondary admission: %#x, %+v", status, c.server.Status())
		}
		secondary := c.server.sessions.get(h.SessionID).session
		time.Sleep(31 * time.Second)
		synctest.Wait()
		if c.server.sessions.get(h.SessionID).session != nil || c.server.Status().Sessions != 1 || a.closed.Load() != 2 {
			t.Fatalf("autonomous expiry retained ownership: %+v, closes=%d", c.server.Status(), a.closed.Load())
		}
		select {
		case <-secondary.authDone:
		default:
			t.Fatal("expired initial watcher survived")
		}
		primary.identityMu.RLock()
		key := primary.signer
		primary.identityMu.RUnlock()
		expiryEcho(t, c, primary, key)
		if _, status, _ := registrySetup(t, t.Context(), c, 0, 3, 0, 0, 0, "initial"); status != statusMoreProcessing {
			t.Fatalf("reclaimed capacity unavailable: %#x", status)
		}
	})
}

func TestAuthenticationExpiryPreservesReauthenticationIdentity(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		name := "closed"
		if failClose {
			name = "close_failure"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, s, a := expiryConnection(t)
				s.identityMu.RLock()
				key, principal := s.signer, s.principal
				s.identityMu.RUnlock()
				s.authMu.Lock()
				preauth := s.preauth
				s.authMu.Unlock()
				if _, status, replyKey := registrySetup(t, t.Context(), c, s.id, 2, 0, 0, 0, "initial"); status != statusMoreProcessing || replyKey != key {
					t.Fatalf("reauthentication start: %#x", status)
				}
				a.failClose.Store(failClose)
				t.Cleanup(func() { a.failClose.Store(false) })
				time.Sleep(31 * time.Second)
				synctest.Wait()
				s.authMu.Lock()
				retained, expired, armed := s.auth != nil, s.authExpired, s.authArmed
				currentPreauth := s.preauth
				s.authMu.Unlock()
				s.identityMu.RLock()
				unchanged := s.signer == key && s.principal == principal
				s.identityMu.RUnlock()
				if retained != failClose || !expired || armed || !unchanged || currentPreauth != preauth || c.server.Status().Sessions != 1 {
					t.Fatalf("expired reauthentication changed established session: retained=%v expired=%v armed=%v unchanged=%v", retained, expired, armed, unchanged)
				}
				expiryEcho(t, c, s, key)
				calls := a.closeCalls.Load()
				p := setupPacket(3, s.id, "initial")
				h, err := wire.ParseHeader(p)
				if err != nil {
					t.Fatal(err)
				}
				if _, status, _ := c.sessionSetup(t.Context(), wire.Request{Header: h, Packet: p, Body: p[64:]}, &h); status != statusDenied || a.closeCalls.Load() != calls {
					t.Fatalf("unsigned request triggered cleanup: status=%#x closes=%d", status, a.closeCalls.Load())
				}
				if failClose {
					if _, status, replyKey := registrySetup(t, t.Context(), c, s.id, 4, 0, 0, 0, "initial"); status != statusIO || replyKey != key || a.opened.Load() != 2 || a.closeCalls.Load() != calls+1 {
						t.Fatalf("failed cleanup admitted a new exchange: status=%#x opened=%d closes=%d", status, a.opened.Load(), a.closeCalls.Load())
					}
					a.failClose.Store(false)
				}
				if _, status, _ := registrySetup(t, t.Context(), c, s.id, 5, 0, 0, 0, "initial"); status != statusMoreProcessing || a.opened.Load() != 3 {
					t.Fatalf("signed cleanup retry did not start one exchange: %#x, opened=%d", status, a.opened.Load())
				}
				if _, status, replyKey := registrySetup(t, t.Context(), c, s.id, 6, 0, 0, 0, "proof"); status != statusOK || replyKey != key {
					t.Fatalf("fresh reauthentication failed: %#x", status)
				}
				expiryEcho(t, c, s, key)
			})
		})
	}
}

func TestAuthenticationExpiryFailedInitialCloseRetainsCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _, a := expiryConnection(t)
		h, status, _ := registrySetup(t, t.Context(), c, 0, 2, 0, 0, 0, "initial")
		if status != statusMoreProcessing {
			t.Fatalf("secondary admission: %#x", status)
		}
		s := c.server.sessions.get(h.SessionID).session
		a.failClose.Store(true)
		t.Cleanup(func() { a.failClose.Store(false) })
		time.Sleep(31 * time.Second)
		synctest.Wait()
		s.authMu.Lock()
		retained := s.auth != nil
		s.authMu.Unlock()
		s.mu.Lock()
		retired := s.retired
		s.mu.Unlock()
		if !retained || !retired || c.server.Status().Sessions != 2 || a.closeCalls.Load() != 2 || a.closed.Load() != 1 {
			t.Fatalf("failed close released ownership: retained=%v retired=%v status=%+v calls=%d", retained, retired, c.server.Status(), a.closeCalls.Load())
		}
		if _, status, _ := registrySetup(t, t.Context(), c, 0, 3, 0, 0, 0, "initial"); status != statusResources {
			t.Fatalf("retained context did not consume capacity: %#x", status)
		}
		a.failClose.Store(false)
		if err := c.logoff(t.Context(), s); err != nil {
			t.Fatal(err)
		}
		if c.server.Status().Sessions != 1 || a.closed.Load() != 2 || c.server.sessions.get(s.id).session != nil {
			t.Fatalf("explicit cleanup retry did not reclaim: %+v", c.server.Status())
		}
	})
}

func TestAuthenticationExpiryRejectsStaleGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := registryAuthenticatedConnection(t)
		a := &expiryAuthenticator{}
		c.server.config.Authenticator = a
		if _, status, _ := registrySetup(t, t.Context(), c, s.id, 2, 0, 0, 0, "initial"); status != statusMoreProcessing {
			t.Fatalf("start: %#x", status)
		}
		s.authMu.Lock()
		generation := s.authGeneration
		deadline := s.deadline
		s.authMu.Unlock()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, status, _ := registrySetup(t, ctx, c, s.id, 3, 0, 0, 0, "proof"); status != statusDenied {
			t.Fatalf("canceled exchange: %#x", status)
		}
		time.Sleep(time.Until(deadline) + time.Second)
		if _, status, _ := registrySetup(t, t.Context(), c, s.id, 4, 0, 0, 0, "initial"); status != statusMoreProcessing {
			t.Fatalf("next generation: %#x", status)
		}
		s.authMu.Lock()
		freshGeneration, fresh := s.authGeneration, s.auth
		watcher := s.authDone
		s.authMu.Unlock()
		if freshGeneration <= generation || fresh == nil {
			t.Fatal("new exchange did not acquire a fresh generation")
		}
		if c.expireAuthentication(s, generation) || a.closeCalls.Load() != 1 {
			t.Fatal("old expired generation affected fresh authentication")
		}
		s.authMu.Lock()
		preserved := s.auth == fresh && s.authDone == watcher && s.authArmed
		s.authMu.Unlock()
		if !preserved {
			t.Fatal("stale expiry changed fresh context or watcher")
		}
		if _, status, _ := registrySetup(t, t.Context(), c, s.id, 5, 0, 0, 0, "proof"); status != statusOK {
			t.Fatalf("fresh completion: %#x", status)
		}
		time.Sleep(c.server.config.Limits.HandshakeTimeout + time.Second)
		synctest.Wait()
		if c.expireAuthentication(s, freshGeneration) || a.closed.Load() != 2 || c.server.Status().Sessions != 1 {
			t.Fatal("completed authentication was retired by an old timer")
		}
	})
}

func TestAuthenticationExpirySerializesBlockedStepAndClose(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		name := "deadline_error"
		if lateSuccess {
			name = "late_success"
		}
		t.Run(name, func(t *testing.T) {
			// Mutex waiters are not durably blocked in synctest. The watcher
			// must contend on authMu while this provider holds Step open.
			c, primary, _ := expiryConnection(t)
			c.server.config.Limits.HandshakeTimeout = 100 * time.Millisecond
			entered, canceled, release := make(chan context.Context, 1), make(chan error, 1), make(chan struct{})
			a := &expiryAuthenticator{step: func(ctx context.Context, _ []byte) (AuthenticationResult, error) {
				entered <- ctx
				<-ctx.Done()
				canceled <- ctx.Err()
				<-release
				if lateSuccess {
					return AuthenticationResult{Principal: Principal{SID: "S-1-5-21-1"}, SessionKey: []byte("0123456789abcdef")}, nil
				}
				return AuthenticationResult{}, ctx.Err()
			}}
			c.server.config.Authenticator = a
			requestContext, cancelRequest := context.WithCancel(t.Context())
			result, done := make(chan uint32, 1), make(chan struct{})
			released := false
			defer func() {
				cancelRequest()
				if !released {
					close(release)
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("authentication request did not drain")
				}
			}()
			started := time.Now()
			go func() {
				defer close(done)
				_, status, _ := registrySetup(t, requestContext, c, 0, 2, 0, 0, 0, "blocked")
				result <- status
			}()
			var ctx context.Context
			select {
			case ctx = <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("provider did not enter Step")
			}
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Before(started.Add(c.server.config.Limits.HandshakeTimeout)) || deadline.After(time.Now().Add(c.server.config.Limits.HandshakeTimeout)) {
				t.Fatalf("provider lacks exchange deadline: %v, %v", deadline, ok)
			}
			select {
			case err := <-canceled:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("provider cancellation: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("exchange deadline did not cancel Step")
			}
			if a.closeCalls.Load() != 0 || c.server.Status().Sessions != 2 {
				t.Fatal("blocked provider was destroyed or its capacity released")
			}
			close(release)
			released = true
			select {
			case status := <-result:
				if status != statusDenied {
					t.Fatalf("expired provider result accepted: %#x", status)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("expired authentication did not finish")
			}
			if a.closeDuringStep.Load() || a.closed.Load() != 1 || c.server.Status().Sessions != 1 {
				t.Fatalf("provider cleanup order: concurrent=%v closed=%d status=%+v", a.closeDuringStep.Load(), a.closed.Load(), c.server.Status())
			}
			primary.identityMu.RLock()
			key := primary.signer
			primary.identityMu.RUnlock()
			expiryEcho(t, c, primary, key)
		})
	}
}

func TestAuthenticationExpiryWatchersDrainOnDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, primary, a := expiryConnection(t)
		h, status, _ := registrySetup(t, t.Context(), c, 0, 2, 0, 0, 0, "initial")
		if status != statusMoreProcessing {
			t.Fatalf("secondary admission: %#x", status)
		}
		secondary := c.server.sessions.get(h.SessionID).session
		c.cleanup()
		for _, s := range []*session{primary, secondary} {
			select {
			case <-s.authDone:
			default:
				t.Fatal("disconnect returned before authentication watcher exited")
			}
		}
		if c.server.Status().Sessions != 0 || a.closed.Load() != 2 {
			t.Fatalf("disconnect retained authentication: %+v, closed=%d", c.server.Status(), a.closed.Load())
		}
	})
}

func TestAuthenticationExpirySecondaryOverTCP(t *testing.T) {
	a := &expiryAuthenticator{}
	config := testConfig()
	config.Authenticator = a
	config.Limits.HandshakeTimeout = 500 * time.Millisecond
	config.Limits.MaxSessions = 2
	s, client := startConfiguredServer(t, config)
	id, key := authenticateProtocol(t, client)
	t.Cleanup(key.Destroy)
	sendFrame(t, client, setupPacket(3, 0, "initial"))
	response := readFrame(t, client)
	h, err := wire.ParseHeader(response)
	if err != nil || h.Status != statusMoreProcessing {
		t.Fatalf("secondary exchange: %+v, %v", h, err)
	}
	secondary := h.SessionID
	if s.sessions.get(secondary).session == nil || a.opened.Load() != 2 {
		t.Fatal("secondary context was not retained after challenge")
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for s.sessions.get(secondary).session != nil || s.Status().PendingRequests != 0 {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("abandoned exchange retained without another packet: %+v, closed=%d", s.Status(), a.closed.Load())
		}
	}
	if s.Status().Sessions != 1 || a.closed.Load() != 2 {
		t.Fatalf("expiry ownership: %+v, closed=%d", s.Status(), a.closed.Load())
	}
	p := requestPacket(wire.Header{Command: wire.Echo, MessageID: 4, SessionID: id, Credits: 1}, wire.EmptyResponseBody())
	if err := key.Sign(p); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, client, p)
	response = readFrame(t, client)
	h, err = wire.ParseHeader(response)
	if err != nil || h.Status != statusOK || h.MessageID != 4 || key.Verify(response) != nil {
		t.Fatalf("primary signed echo after secondary expiry: %+v, %v", h, err)
	}
	sendFrame(t, client, setupPacket(5, 0, "initial"))
	h, err = wire.ParseHeader(readFrame(t, client))
	if err != nil || h.Status != statusMoreProcessing || h.SessionID == secondary {
		t.Fatalf("expired capacity could not be reused: %+v, %v", h, err)
	}
}
