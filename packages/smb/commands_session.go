package smb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func (c *connection) dispatch(ctx context.Context, r, original wire.Request, h *wire.Header) ([]byte, uint32, *signing.Session) {
	if r.Header.Command == wire.Negotiate {
		b, status := c.negotiate(r)
		return b, status, nil
	}
	if !c.negotiated {
		return nil, statusInvalid, nil
	}
	if r.Header.Command == wire.SessionSetup {
		return c.sessionSetup(ctx, r, h)
	}
	c.mu.Lock()
	s := c.sessions[r.Header.SessionID]
	c.retainSessionFrameLocked(s, ctx, r.Header.MessageID)
	c.mu.Unlock()
	if s == nil {
		return nil, statusSessionDeleted, nil
	}
	s.identityMu.RLock()
	signer := s.signer
	principal := s.principal
	s.identityMu.RUnlock()
	if signer == nil || signer.Verify(original.Packet) != nil {
		return nil, statusDenied, nil
	}
	ctx = WithPrincipal(ctx, principal)
	if r.Header.Flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
		return nil, statusUnsupported, signer
	}
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired && r.Header.Command != wire.Logoff {
		return nil, statusSessionDeleted, signer
	}
	switch r.Header.Command {
	case wire.Echo:
		if r.Empty() != nil {
			return nil, statusInvalid, signer
		}
		return wire.EmptyResponseBody(), 0, signer
	case wire.Logoff:
		if r.Empty() != nil {
			return nil, statusInvalid, signer
		}
		if err := c.logoff(ctx, s); err != nil {
			if errors.Is(err, ErrStopped) {
				return nil, statusSessionDeleted, signer
			}
			return nil, statusError(err), signer
		}
		return wire.EmptyResponseBody(), 0, signer
	case wire.TreeConnect:
		b, status := c.treeConnect(ctx, s, r, h)
		return b, status, signer
	}
	s.mu.Lock()
	t := s.trees[r.Header.TreeID]
	s.mu.Unlock()
	if t == nil {
		return nil, statusNetworkDeleted, signer
	}
	if r.Header.Command == wire.OplockBreak {
		return nil, statusUnsupported, signer
	}
	if t.kind == controlTree {
		body, status := c.control(s, t, r)
		return body, status, signer
	}
	c.server.mu.Lock()
	if t.export.stopping {
		c.server.mu.Unlock()
		return nil, statusNetworkDeleted, signer
	}
	t.export.active++
	c.server.mu.Unlock()
	defer func() { c.server.mu.Lock(); t.export.active--; c.server.mu.Unlock() }()

	switch r.Header.Command {
	case wire.TreeDisconnect:
		if r.Empty() != nil {
			return nil, statusInvalid, signer
		}
		if err := c.closeTreeAuthorized(ctx, t); err != nil {
			return nil, statusError(err), signer
		}
		s.mu.Lock()
		delete(s.trees, t.id)
		err := c.closeOrphansLocked(ctx, s, t.export)
		s.mu.Unlock()
		if err != nil {
			return nil, statusError(err), signer
		}
		return wire.EmptyResponseBody(), statusOK, signer
	case wire.Create:
		body, status := c.create(ctx, t, r)
		return body, status, signer
	case wire.Close:
		body, status := c.closeHandle(ctx, t, r)
		return body, status, signer
	default:
		return nil, statusUnsupported, signer
	}
}

// The wildcard exchange selects SMB2 framing; the subsequent SMB 3.1.1
// NEGOTIATE alone starts the preauthentication transcript.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/bcd6d594-017b-47fc-8742-b7d847791783
func (c *connection) bootstrap() error {
	limits := c.server.config.Limits
	body, err := wire.NegotiateResponseBody(wire.Negotiation{
		SecurityMode: 3, Dialect: wire.DialectWildcard, ServerGUID: c.server.guid,
		Capabilities: 4, MaxTransactSize: uint32(limits.MaxIOBytes),
		MaxReadSize: uint32(limits.MaxIOBytes), MaxWriteSize: uint32(limits.MaxIOBytes),
		SystemTime: uint64(time.Now().UnixNano()/100 + 116444736000000000),
	})
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint16(body[56:], 128)
	c.credits = map[uint64]struct{}{1: {}}
	c.nextCredit = 2
	return c.write(wire.EncodeResponse(wire.Header{Command: wire.Negotiate, Credits: 1}, body))
}

func (c *connection) negotiate(r wire.Request) ([]byte, uint32) {
	if c.negotiated || r.Header.SessionID != 0 {
		return nil, statusInvalid
	}
	n, err := r.Negotiate()
	if err != nil {
		return nil, statusInvalid
	}
	found := false
	for _, d := range n.Dialects {
		found = found || d == wire.Dialect311
	}
	if !found {
		return nil, statusUnsupported
	}
	sha512 := false
	cmac := true
	for _, ctx := range n.Contexts {
		if ctx.Type == 1 {
			if len(ctx.Data) < 4 {
				return nil, statusInvalid
			}
			count := int(binary.LittleEndian.Uint16(ctx.Data))
			salt := int(binary.LittleEndian.Uint16(ctx.Data[2:]))
			if count < 1 || 4+count*2+salt != len(ctx.Data) {
				return nil, statusInvalid
			}
			for i := 0; i < count; i++ {
				sha512 = sha512 || binary.LittleEndian.Uint16(ctx.Data[4+i*2:]) == 1
			}
		}
		if ctx.Type == 8 {
			cmac = false
			if len(ctx.Data) < 2 {
				return nil, statusInvalid
			}
			count := int(binary.LittleEndian.Uint16(ctx.Data))
			if count < 1 || 2+count*2 != len(ctx.Data) {
				return nil, statusInvalid
			}
			for i := 0; i < count; i++ {
				cmac = cmac || binary.LittleEndian.Uint16(ctx.Data[2+i*2:]) == 1
			}
		}
	}
	if !sha512 || !cmac {
		return nil, statusUnsupported
	}
	preauth := make([]byte, 38)
	binary.LittleEndian.PutUint16(preauth, 1)
	binary.LittleEndian.PutUint16(preauth[2:], 32)
	binary.LittleEndian.PutUint16(preauth[4:], 1)
	rand.Read(preauth[6:])
	limits := c.server.config.Limits
	b, err := wire.NegotiateResponseBody(wire.Negotiation{SecurityMode: 3, Dialect: wire.Dialect311, ServerGUID: c.server.guid, Capabilities: 4, MaxTransactSize: uint32(limits.MaxIOBytes), MaxReadSize: uint32(limits.MaxIOBytes), MaxWriteSize: uint32(limits.MaxIOBytes), SystemTime: uint64(time.Now().UnixNano()/100 + 116444736000000000), Contexts: []wire.Context{{Type: 1, Data: preauth}, {Type: 8, Data: []byte{1, 0, 1, 0}}}})
	if err != nil {
		return nil, statusIO
	}
	c.clientGUID = n.ClientGUID
	c.preauth = signing.Preauth(c.preauth, r.Packet)
	c.negotiated = true
	return b, 0
}

func (c *connection) sessionSetup(ctx context.Context, r wire.Request, h *wire.Header) (body []byte, status uint32, key *signing.Session) {
	setup, err := r.SessionSetup()
	// Channel is reserved and must be ignored by the receiver; it does not
	// request multichannel binding, which has its own Flags bit.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/5a3c2c28-d6b0-48ed-b917-a86b2ca4575f
	if err != nil || setup.Flags & ^byte(1) != 0 || len(setup.Token) > c.server.config.Limits.MaxTokenBytes {
		return nil, statusInvalid, nil
	}
	if setup.Flags&1 != 0 {
		return nil, statusRequestNotAccepted, nil
	}
	c.mu.Lock()
	if c.closing || c.disconnected || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, statusSessionDeleted, nil
	}
	s := c.sessions[r.Header.SessionID]
	created := false
	if r.Header.SessionID == 0 {
		if len(c.sessions) >= c.server.config.Limits.MaxSessions {
			c.mu.Unlock()
			return nil, statusResources, nil
		}
		s = &session{preauth: c.preauth, trees: make(map[uint32]*tree), deadline: time.Now().Add(c.server.config.Limits.HandshakeTimeout)}
		if err := c.server.sessions.add(c, s); err != nil {
			c.mu.Unlock()
			return nil, statusResources, nil
		}
		c.sessions[s.id] = s
		created = true
	}
	if s != nil {
		c.retainSessionFrameLocked(s, ctx, r.Header.MessageID)
		if created {
			c.startAuthenticationWatcherLocked(s)
		}
		if pending := c.pending[r.Header.MessageID]; pending != nil {
			pending.sessionID = s.id
		}
	}
	c.mu.Unlock()
	if s == nil {
		return nil, statusSessionDeleted, nil
	}
	s.authMu.Lock()
	defer func() {
		s.authMu.Unlock()
		s.mu.Lock()
		retired := s.retired
		s.mu.Unlock()
		if retired {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.server.config.Limits.CleanupTimeout)
			c.server.cleanupFailure(s.waitAuthenticationWatcher(cleanup))
			cancel()
		}
		c.finishSessionRetirement(s)
	}()
	s.mu.Lock()
	retired := s.retired
	finalizing := s.finalizing
	s.mu.Unlock()
	if retired {
		return nil, statusSessionDeleted, nil
	}
	if finalizing {
		return nil, statusRequestNotAccepted, nil
	}
	h.SessionID = s.id
	s.identityMu.RLock()
	existing := s.signer
	previousPrincipal := s.principal
	s.identityMu.RUnlock()
	if existing != nil {
		if existing.Verify(r.Packet) != nil {
			return nil, statusDenied, nil
		}
		if s.authExpired && s.auth != nil {
			if err := s.closeAuthenticationLocked(); err != nil {
				c.server.cleanupFailure(err)
				return nil, statusIO, existing
			}
		}
		if s.auth == nil {
			if err := s.armAuthenticationLocked(c.server.config.Limits.HandshakeTimeout); err != nil {
				return nil, statusResources, existing
			}
		}
	}
	closeAttempted := false

	defer func() {
		if status == 0 || status == statusMoreProcessing {
			return
		}
		s.disarmAuthenticationLocked(true)
		if !closeAttempted {
			closeAttempted = true
			c.server.cleanupFailure(s.closeAuthenticationLocked())
		}
		if existing == nil {
			frame, _ := ctx.Value(pendingFrameKey{}).(requestFrame)
			c.retireSessionRequests(s, frame)
		}
	}()
	authContext, cancelAuthentication := s.authenticationContextLocked(ctx)
	ctx = authContext
	defer cancelAuthentication()
	if !time.Now().Before(s.deadline) || ctx.Err() != nil {
		return nil, statusDenied, existing
	}
	if existing == nil {
		s.preauth = signing.Preauth(s.preauth, r.Packet)
	}
	if s.auth == nil {
		s.auth, err = c.server.config.Authenticator.Begin(ctx)
		if err != nil || s.auth == nil {
			return nil, statusDenied, existing
		}
	}
	result, err := s.auth.Step(ctx, setup.Token)
	defer clear(result.SessionKey)
	if ctx.Err() != nil || !time.Now().Before(s.deadline) {
		return nil, statusDenied, existing
	}
	if err != nil || len(result.Token) > c.server.config.Limits.MaxTokenBytes {
		return nil, statusDenied, existing
	}
	body, err = wire.SessionSetupResponseBody(0, result.Token)
	if err != nil {
		return nil, statusInvalid, existing
	}
	if result.Continue {
		if len(result.SessionKey) != 0 || result.Principal.SID != "" {
			return nil, statusDenied, existing
		}
		if existing == nil {
			responseHeader := *h
			responseHeader.Status = statusMoreProcessing
			s.preauth = signing.Preauth(s.preauth, wire.EncodeResponse(responseHeader, body))
		}
		return body, statusMoreProcessing, existing
	}
	if result.Principal.SID == "" || len(result.SessionKey) < 16 || existing != nil && result.Principal.SID != previousPrincipal.SID {
		return nil, statusDenied, existing
	}
	if !validPrincipalSID(result.Principal.SID) {
		return nil, statusDenied, existing
	}
	if existing == nil {
		key, err = signing.NewSession(s.preauth, result.SessionKey)
		if err != nil {
			return nil, statusDenied, nil
		}
	} else {
		key = existing
	}
	closeAttempted = true
	if err := s.closeAuthenticationLocked(); err != nil {
		if existing == nil {
			key.Destroy()
		}
		return nil, statusIO, existing
	}
	if ctx.Err() != nil || !time.Now().Before(s.deadline) {
		if existing == nil {
			key.Destroy()
		}
		return nil, statusDenied, existing
	}
	s.mu.Lock()
	s.finalizing = true
	s.mu.Unlock()
	// Previous-session cleanup acquires the other session's auth lock. The
	// transition flag keeps this exchange exclusive while neither auth lock
	// nests inside another one.
	err = func() error {
		s.authMu.Unlock()
		defer s.authMu.Lock()
		return c.retirePreviousSession(ctx, s, setup.PreviousSessionID, result.Principal)
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizing = false
	c.mu.Lock()
	unavailable := s.retired || c.closing || c.disconnected || c.ctx.Err() != nil || c.sessions[s.id] != s
	c.mu.Unlock()
	if err != nil || ctx.Err() != nil || !time.Now().Before(s.deadline) || s.authExpired || unavailable {
		if existing == nil {
			key.Destroy()
		}
		if err != nil {
			return nil, statusError(err), existing
		}
		if unavailable {
			return nil, statusSessionDeleted, existing
		}
		return nil, statusDenied, existing
	}
	s.identityMu.Lock()
	s.signer = key
	s.principal = result.Principal
	s.identityMu.Unlock()
	s.disarmAuthenticationLocked(false)
	_ = c.net.SetReadDeadline(time.Time{})
	return body, 0, key
}

func (c *connection) treeConnect(ctx context.Context, s *session, r wire.Request, h *wire.Header) ([]byte, uint32) {
	path, err := r.TreePath()
	if err != nil {
		return nil, statusInvalid
	}
	parts := strings.Split(path, "\\")
	if len(parts) != 4 || parts[0] != "" || parts[1] != "" || parts[2] == "" {
		return nil, statusBadNetworkName
	}
	key, err := shareKey(parts[3])
	if err != nil {
		return nil, statusBadNetworkName
	}
	if key == "IPC$" {
		return c.connectControl(s, h)
	}
	return c.connectVolume(ctx, s, key, h)
}

func validPrincipalSID(text string) bool {
	parts := strings.Split(text, "-")
	if len(parts) < 4 || len(parts) > 18 || parts[0] != "S" || parts[1] != "1" {
		return false
	}
	if _, err := strconv.ParseUint(parts[2], 10, 48); err != nil {
		return false
	}
	for _, part := range parts[3:] {
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}
