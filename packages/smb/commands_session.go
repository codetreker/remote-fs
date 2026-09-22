package smb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func (c *connection) dispatch(ctx context.Context, request, original wire.Request, header *wire.Header) ([]byte, uint32, *signing.Session) {
	if request.Header.Command == wire.Negotiate {
		body, status := c.negotiate(request)
		return body, status, nil
	}
	if !c.negotiated {
		return nil, statusInvalid, nil
	}
	if request.Header.Command == wire.SessionSetup {
		return c.sessionSetup(ctx, request, header)
	}
	if request.Header.Command == wire.Echo && request.Header.SessionID == 0 {
		if original.Header.Flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
			return nil, statusUnsupported, nil
		}
		if request.Header.TreeID != 0 || original.Header.Flags != 0 || request.Empty() != nil {
			return nil, statusInvalid, nil
		}
		return wire.EmptyResponseBody(), statusOK, nil
	}

	c.mu.Lock()
	s := c.sessions[request.Header.SessionID]
	c.retainSessionFrameLocked(s, ctx, request.Header.MessageID)
	c.mu.Unlock()
	if s == nil {
		return nil, statusSessionDeleted, nil
	}
	s.identityMu.RLock()
	signer, principal := s.signer, s.principal
	s.identityMu.RUnlock()
	if signer == nil {
		return nil, statusDenied, nil
	}
	if original.Header.Flags&wire.FlagSigned == 0 {
		return nil, statusDenied, nil
	}
	if signer.Verify(original.Packet) != nil {
		return nil, statusDenied, signer
	}
	ctx = WithPrincipal(ctx, principal)
	if request.Header.Flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
		return nil, statusUnsupported, signer
	}
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired && request.Header.Command != wire.Logoff {
		return nil, statusSessionDeleted, signer
	}
	if request.Header.Command != wire.Logoff && s.expiredIdentity() {
		return nil, statusNetworkSessionExpired, signer
	}
	switch request.Header.Command {
	case wire.Echo:
		if request.Empty() != nil {
			return nil, statusInvalid, signer
		}
		return wire.EmptyResponseBody(), statusOK, signer
	case wire.Logoff:
		if request.Empty() != nil {
			return nil, statusInvalid, signer
		}
		if err := c.logoff(ctx, s); err != nil {
			if errors.Is(err, ErrStopped) {
				return nil, statusSessionDeleted, signer
			}
			return nil, statusError(err), signer
		}
		return wire.EmptyResponseBody(), statusOK, signer
	case wire.TreeConnect:
		body, status := c.treeConnect(ctx, s, request, header)
		return body, status, signer
	}

	s.mu.Lock()
	tree := s.trees[request.Header.TreeID]
	s.mu.Unlock()
	if tree == nil {
		return nil, statusNetworkDeleted, signer
	}
	if request.Header.Command == wire.OplockBreak {
		return nil, statusUnsupported, signer
	}
	if tree.kind == controlTree {
		body, status := c.control(s, tree, request)
		return body, status, signer
	}
	if request.Header.Command != wire.TreeDisconnect {
		release, err := tree.beginFileWork()
		if err != nil {
			return nil, fileCommandStatus(err), signer
		}
		defer release()
	}
	switch request.Header.Command {
	case wire.Create:
		body, status := c.create(ctx, tree, request)
		return body, status, signer
	case wire.Close:
		body, status := c.closeHandle(ctx, tree, request)
		return body, status, signer
	case wire.Flush:
		body, status := c.flushHandle(ctx, tree, request)
		return body, status, signer
	case wire.Read:
		body, status := c.readHandle(ctx, tree, request)
		return body, status, signer
	case wire.Write:
		body, status := c.writeHandle(ctx, tree, request)
		return body, status, signer
	case wire.QueryDirectory:
		body, status := c.queryDirectory(ctx, tree, request)
		return body, status, signer
	case wire.QueryInfo:
		body, status := c.queryInfo(ctx, tree, request)
		return body, status, signer
	case wire.TreeDisconnect:
	default:
		return nil, statusUnsupported, signer
	}
	if request.Empty() != nil {
		return nil, statusInvalid, signer
	}
	if err := c.closeTreeAuthorized(ctx, tree); err != nil {
		return nil, statusError(err), signer
	}
	s.mu.Lock()
	delete(s.trees, tree.id)
	if err := c.closeOrphansLocked(ctx, s, tree.export); err != nil {
		s.mu.Unlock()
		return nil, statusError(err), signer
	}
	s.mu.Unlock()
	return wire.EmptyResponseBody(), statusOK, signer
}

// The wildcard exchange only selects SMB2 framing. The following SMB 3.1.1
// NEGOTIATE starts the preauthentication transcript.
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

func (c *connection) negotiate(request wire.Request) ([]byte, uint32) {
	if c.negotiated || request.Header.SessionID != 0 {
		return nil, statusInvalid
	}
	negotiation, err := request.Negotiate()
	if err != nil {
		return nil, statusInvalid
	}
	found311 := false
	for _, dialect := range negotiation.Dialects {
		found311 = found311 || dialect == wire.Dialect311
	}
	if !found311 {
		return nil, statusUnsupported
	}
	sha512, cmac, signingContext := false, true, false
	for _, context := range negotiation.Contexts {
		switch context.Type {
		case 1:
			if len(context.Data) < 4 {
				return nil, statusInvalid
			}
			count := int(binary.LittleEndian.Uint16(context.Data))
			salt := int(binary.LittleEndian.Uint16(context.Data[2:]))
			if count < 1 || 4+count*2+salt != len(context.Data) {
				return nil, statusInvalid
			}
			for index := 0; index < count; index++ {
				sha512 = sha512 || binary.LittleEndian.Uint16(context.Data[4+index*2:]) == 1
			}
		case 8:
			signingContext = true
			cmac = false
			if len(context.Data) < 2 {
				return nil, statusInvalid
			}
			count := int(binary.LittleEndian.Uint16(context.Data))
			if count < 1 || 2+count*2 != len(context.Data) {
				return nil, statusInvalid
			}
			for index := 0; index < count; index++ {
				cmac = cmac || binary.LittleEndian.Uint16(context.Data[2+index*2:]) == 1
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
	if _, err := rand.Read(preauth[6:]); err != nil {
		return nil, statusIO
	}
	limits := c.server.config.Limits
	contexts := []wire.Context{{Type: wire.ContextPreauthIntegrity, Data: preauth}}
	if signingContext {
		contexts = append(contexts, wire.Context{Type: wire.ContextSigning, Data: []byte{1, 0, 1, 0}})
	}
	body, err := wire.NegotiateResponseBody(wire.Negotiation{
		SecurityMode: 3, Dialect: wire.Dialect311, ServerGUID: c.server.guid,
		Capabilities: 4, MaxTransactSize: uint32(limits.MaxIOBytes),
		MaxReadSize: uint32(limits.MaxIOBytes), MaxWriteSize: uint32(limits.MaxIOBytes),
		SystemTime: uint64(time.Now().UnixNano()/100 + 116444736000000000),
		Contexts:   contexts,
	})
	if err != nil {
		return nil, statusIO
	}
	c.clientGUID = negotiation.ClientGUID
	c.preauth = signing.Preauth(c.preauth, request.Packet)
	c.negotiated = true
	return body, statusOK
}

func (c *connection) sessionSetup(ctx context.Context, request wire.Request, header *wire.Header) (body []byte, status uint32, key *signing.Session) {
	flags := request.Header.Flags
	var verifiedSigner *signing.Session
	if request.Header.SessionID == 0 {
		if flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
			return nil, statusUnsupported, nil
		}
		if flags != 0 {
			return nil, statusInvalid, nil
		}
	} else {
		c.mu.Lock()
		existingSession := c.sessions[request.Header.SessionID]
		c.retainSessionFrameLocked(existingSession, ctx, request.Header.MessageID)
		c.mu.Unlock()
		if existingSession == nil {
			return nil, statusSessionDeleted, nil
		}
		existingSession.identityMu.RLock()
		existingSigner := existingSession.signer
		existingSession.identityMu.RUnlock()
		if existingSigner == nil {
			if flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
				return nil, statusUnsupported, nil
			}
			if flags != 0 {
				return nil, statusInvalid, nil
			}
		} else {
			if flags&wire.FlagSigned == 0 {
				return nil, statusDenied, nil
			}
			if existingSigner.Verify(request.Packet) != nil {
				return nil, statusDenied, existingSigner
			}
			if flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
				return nil, statusUnsupported, existingSigner
			}
			if flags != wire.FlagSigned {
				return nil, statusInvalid, existingSigner
			}
			verifiedSigner = existingSigner
		}
	}
	setup, err := request.SessionSetup()
	if err != nil || setup.Flags&^byte(1) != 0 || len(setup.Token) > c.server.config.Limits.MaxTokenBytes {
		return nil, statusInvalid, verifiedSigner
	}
	if setup.Flags&1 != 0 {
		return nil, statusRequestNotAccepted, verifiedSigner
	}
	c.mu.Lock()
	if c.closing || c.disconnected || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, statusSessionDeleted, verifiedSigner
	}
	s := c.sessions[request.Header.SessionID]
	created := false
	if request.Header.SessionID == 0 {
		s = &session{
			preauth: c.preauth, trees: make(map[uint32]*tree),
			authDeadline: time.Now().Add(c.server.config.Limits.HandshakeTimeout), cleanedDone: make(chan struct{}),
		}
		if err := c.server.sessions.add(c, s); err != nil {
			c.mu.Unlock()
			return nil, statusResources, nil
		}
		c.sessions[s.id] = s
		created = true
	}
	if s != nil {
		c.retainSessionFrameLocked(s, ctx, request.Header.MessageID)
		if created {
			c.startAuthenticationWatcherLocked(s)
		}
		if pending := c.pending[request.Header.MessageID]; pending != nil {
			pending.sessionID = s.id
		}
	}
	c.mu.Unlock()
	if s == nil {
		return nil, statusSessionDeleted, verifiedSigner
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
	retired, finalizing := s.retired, s.finalizing
	s.mu.Unlock()
	if retired {
		return nil, statusSessionDeleted, verifiedSigner
	}
	if finalizing {
		return nil, statusRequestNotAccepted, verifiedSigner
	}
	header.SessionID = s.id
	s.identityMu.RLock()
	existing, previousPrincipal := s.signer, s.principal
	s.identityMu.RUnlock()
	if existing != nil {
		if request.Header.Flags&wire.FlagSigned == 0 {
			return nil, statusDenied, nil
		}
		if existing.Verify(request.Packet) != nil {
			return nil, statusDenied, existing
		}
		if request.Header.Flags&(wire.FlagReplay|wire.FlagDFS) != 0 {
			return nil, statusUnsupported, existing
		}
		if request.Header.Flags != wire.FlagSigned {
			return nil, statusInvalid, existing
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
	} else if request.Header.Flags != 0 {
		return nil, statusInvalid, nil
	}
	closeAttempted := false
	defer func() {
		if status == statusOK || status == statusMoreProcessing {
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
	if !time.Now().Before(s.authDeadline) || ctx.Err() != nil {
		return nil, statusDenied, existing
	}
	if existing == nil {
		s.preauth = signing.Preauth(s.preauth, request.Packet)
	}
	if s.auth == nil {
		s.auth, err = c.server.config.Authenticator.Begin(ctx)
		if err != nil || s.auth == nil {
			return nil, statusDenied, existing
		}
	}
	result, err := s.auth.Step(ctx, setup.Token)
	defer clear(result.Token)
	defer clear(result.SessionKey)
	if ctx.Err() != nil || !time.Now().Before(s.authDeadline) {
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
		if len(result.SessionKey) != 0 || result.Principal != (Principal{}) || !result.ExpiresAt.IsZero() {
			return nil, statusDenied, existing
		}
		if existing == nil {
			responseHeader := *header
			responseHeader.Status = statusMoreProcessing
			s.preauth = signing.Preauth(s.preauth, wire.EncodeResponse(responseHeader, body))
		}
		return body, statusMoreProcessing, existing
	}
	if !result.Principal.Valid() || len(result.SessionKey) < 16 ||
		existing != nil && !samePrincipalIdentity(result.Principal, previousPrincipal) ||
		!result.ExpiresAt.IsZero() && !time.Now().Before(result.ExpiresAt) {
		return nil, statusDenied, existing
	}
	if err := c.server.config.AuthorizeIdentity.AuthorizeIdentity(ctx, result.Principal); err != nil {
		return nil, identityAuthorizationStatus(err), existing
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
	if ctx.Err() != nil || !time.Now().Before(s.authDeadline) {
		if existing == nil {
			key.Destroy()
		}
		return nil, statusDenied, existing
	}
	s.mu.Lock()
	s.finalizing = true
	s.finalizationDone = make(chan struct{})
	s.mu.Unlock()
	err = func() error {
		s.authMu.Unlock()
		defer s.authMu.Lock()
		return c.retirePreviousSession(ctx, s, setup.PreviousSessionID, result.Principal)
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizing = false
	close(s.finalizationDone)
	s.finalizationDone = nil
	c.mu.Lock()
	unavailable := s.retired || c.closing || c.disconnected || c.ctx.Err() != nil || c.sessions[s.id] != s
	c.mu.Unlock()
	if err != nil || ctx.Err() != nil || !time.Now().Before(s.authDeadline) || s.authExpired || unavailable {
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
	if err := s.setIdentityDeadlineLocked(result.ExpiresAt); err != nil {
		if existing == nil {
			key.Destroy()
		}
		return nil, statusDenied, existing
	}
	s.identityMu.Lock()
	s.signer = key
	s.principal = result.Principal
	s.identityMu.Unlock()
	s.disarmAuthenticationLocked(false)
	_ = c.net.SetReadDeadline(time.Time{})
	return body, statusOK, key
}

func (c *connection) treeConnect(ctx context.Context, s *session, request wire.Request, header *wire.Header) ([]byte, uint32) {
	path, err := request.TreePath()
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
		return c.connectControl(s, header)
	}
	return c.connectVolume(ctx, s, key, header)
}
