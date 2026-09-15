package smb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
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
		return nil, statusSessionDeleted, nil
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
				return nil, statusSessionDeleted, nil
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
	if r.Header.Command == wire.Create {
		var err error
		r, err = c.maximumAccess(ctx, t, r)
		if err != nil {
			return nil, statusError(err), signer
		}
	}
	operation := operationFor(r.Header.Command)
	if r.Header.Command == wire.SetInfo {
		info, err := r.SetInfo()
		if err != nil {
			return nil, statusInvalid, signer
		}
		switch info.Class {
		case 10:
			operation = storage.OpWindowsRename
		case 13:
			operation = storage.OpWindowsSetDeletePending
		case 19, 20:
			operation = storage.OpWindowsTruncate
		}
	}
	if r.Header.Command == wire.IOCTL {
		q, err := r.IOCTL()
		if err != nil {
			return nil, statusInvalid, signer
		}
		switch q.Code {
		case fsctlGetReparsePoint:
			operation = storage.OpWindowsReadLink
		case fsctlSetReparsePoint:
			operation = storage.OpWindowsSetLink
		}
	}
	if operation == "" {
		return nil, statusUnsupported, signer
	}
	request := authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation}
	if r.Header.Command == wire.Create {
		create, err := r.Create()
		if err != nil {
			return nil, statusInvalid, signer
		}
		intent, err := windowsIntent(create)
		if err != nil {
			return nil, statusError(err), signer
		}
		request.WindowsOpen = intent
	}
	if err := c.server.config.Authorize.Authorize(ctx, request); err != nil {
		return nil, statusError(err), signer
	}
	if r.Header.Command != wire.Close && r.Header.Command != wire.TreeDisconnect {
		if err := t.export.changes.Health(ctx); err != nil {
			return nil, statusIO, signer
		}
	}
	if r.Header.Command == wire.TreeDisconnect {
		if r.Empty() != nil {
			return nil, statusInvalid, signer
		}
		if err := c.closeTree(t); err != nil {
			return nil, statusIO, signer
		}
		s.mu.Lock()
		delete(s.trees, t.id)
		if err := c.closeOrphansLocked(ctx, s, t.export); err != nil {
			s.mu.Unlock()
			return nil, statusIO, signer
		}
		s.mu.Unlock()
		return wire.EmptyResponseBody(), 0, signer
	}
	if r.Header.Command == wire.Lock {
		b, status := c.lock(ctx, t, r, signer)
		return b, status, signer
	}
	if r.Header.Command == wire.ChangeNotify {
		b, status := c.notify(ctx, t, r, signer)
		return b, status, signer
	}
	if r.Header.Command == wire.Close {
		if id, err := r.FileID(); err == nil {
			c.cancelOpen(r.Header.SessionID, r.Header.TreeID, id, r.Header.MessageID)
			t.export.changes.remove(c.notifyKey(id))
		}
	}
	if r.Header.Command == wire.QueryInfo {
		q, err := r.QueryInfo()
		if err == nil && q.Type == 3 {
			b, status := c.securityInfo(ctx, t, r)
			return b, status, signer
		}
	}
	if r.Header.Command == wire.IOCTL {
		q, err := r.IOCTL()
		if err == nil && (q.Code == fsctlGetReparsePoint || q.Code == fsctlSetReparsePoint) {
			b, status := c.reparse(ctx, t, r)
			return b, status, signer
		}
	}
	if r.Header.Command == wire.Create {
		authorizeContext := ctx
		ctx = context.WithValue(ctx, maximalAccessKey{}, func(attr storage.WindowsAttr) (uint32, error) { return c.maximalAccess(authorizeContext, t, attr) })
	}
	b, status := t.files.handle(ctx, r)
	return b, status, signer
}

func operationFor(command uint16) storage.Operation {
	switch command {
	case wire.TreeDisconnect:
		return storage.OpWindowsSessionClose
	case wire.Create:
		return storage.OpWindowsOpen
	case wire.Close:
		return storage.OpWindowsClose
	case wire.Flush:
		return storage.OpWindowsSync
	case wire.Read:
		return storage.OpWindowsRead
	case wire.Write:
		return storage.OpWindowsWrite
	case wire.Lock:
		return storage.OpWindowsLockBatch
	case wire.QueryDirectory:
		return storage.OpWindowsList
	case wire.QueryInfo:
		return storage.OpWindowsStat
	case wire.SetInfo:
		return storage.OpWindowsSetAttr
	case wire.ChangeNotify:
		return storage.OpReplicationSubscribe
	case wire.IOCTL:
		return storage.OpWindowsStat
	}
	return ""
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
	c.preauth = signing.Preauth(c.preauth, r.Packet)
	c.negotiated = true
	return b, 0
}

func (c *connection) sessionSetup(ctx context.Context, r wire.Request, h *wire.Header) (body []byte, status uint32, key *signing.Session) {
	setup, err := r.SessionSetup()
	if err != nil || setup.Flags != 0 || setup.Channel != 0 || setup.PreviousSessionID != 0 || len(setup.Token) > c.server.config.Limits.MaxTokenBytes {
		return nil, statusInvalid, nil
	}
	c.mu.Lock()
	s := c.sessions[r.Header.SessionID]
	if r.Header.SessionID == 0 {
		if len(c.sessions) >= c.server.config.Limits.MaxSessions {
			c.mu.Unlock()
			return nil, statusResources, nil
		}
		c.nextSession++
		s = &session{id: c.nextSession, preauth: c.preauth, trees: make(map[uint32]*tree), deadline: time.Now().Add(c.server.config.Limits.HandshakeTimeout)}
		c.sessions[s.id] = s
	}
	c.mu.Unlock()
	if s == nil {
		return nil, statusSessionDeleted, nil
	}
	s.authMu.Lock()
	defer s.authMu.Unlock()
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired {
		return nil, statusSessionDeleted, nil
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
		if s.auth == nil {
			s.deadline = time.Now().Add(c.server.config.Limits.HandshakeTimeout)
		}
	}
	defer func() {
		if status != 0 && status != statusMoreProcessing {
			if s.auth != nil {
				c.server.cleanupFailure(s.auth.Close())
				s.auth = nil
			}
			if existing == nil {
				c.mu.Lock()
				delete(c.sessions, s.id)
				c.mu.Unlock()
			}
		}
	}()
	if time.Now().After(s.deadline) {
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
	if _, err := encodeSID(result.Principal.SID); err != nil {
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
	if err := s.auth.Close(); err != nil {
		if existing == nil {
			key.Destroy()
		}
		return nil, statusIO, existing
	}
	s.auth = nil
	s.identityMu.Lock()
	s.signer = key
	s.principal = result.Principal
	s.identityMu.Unlock()
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
	c.server.mu.Lock()
	e := c.server.exports[key]
	if e == nil || e.stopping {
		c.server.mu.Unlock()
		return nil, statusBadNetworkName
	}
	e.refs++
	e.active++
	c.server.mu.Unlock()
	retained := false
	defer func() {
		c.server.mu.Lock()
		e.active--
		if !retained {
			e.refs--
		}
		c.server.mu.Unlock()
	}()
	if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: e.share.Volume, Operation: storage.OpWindowsSessionOpen}); err != nil {
		return nil, statusError(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, statusSessionDeleted
	}
	if len(s.trees) >= c.server.config.Limits.MaxTrees || len(s.authorities) >= c.server.config.Limits.MaxTrees && s.authorities[e] == nil {
		return nil, statusResources
	}
	if s.authorities == nil {
		s.authorities = make(map[*Export]*authoritySession)
	}
	a := s.authorities[e]
	if a != nil && a.orphan {
		return nil, statusIO
	}
	if a != nil {
		a.mu.Lock()
		closed := a.closed
		if !closed {
			a.refs++
		}
		a.mu.Unlock()
		if closed {
			a = nil
		}
	}
	if a != nil {
		state, err := a.session.Status(ctx)
		if err != nil || state.Fenced || state.Retired || state.ActionEpoch == 0 {
			a.mu.Lock()
			a.refs--
			a.mu.Unlock()
			return nil, statusIO
		}
	}
	if a == nil {
		ws, err := e.share.Backend.NewWindowsSession(ctx, c.server.config.Limits.FileSession)
		if err != nil {
			return nil, statusError(err)
		}
		if ws == nil {
			return nil, statusIO
		}
		a = &authoritySession{session: ws, refs: 1, orphan: true}
		s.authorities[e] = a
		state, err := ws.Status(ctx)
		if err != nil || state.Fenced || state.Retired || state.ActionEpoch == 0 {
			cleanup, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
			defer cancel()
			cleanupErr := ws.Close(cleanup)
			c.server.cleanupFailure(cleanupErr)
			if cleanupErr != nil {
				retained = true
			} else {
				delete(s.authorities, e)
			}
			return nil, statusIO
		}
		a.epoch = state.ActionEpoch
		a.orphan = false
	}
	c.mu.Lock()
	c.nextTree++
	id := c.nextTree
	c.mu.Unlock()
	t := &tree{id: id, sessionID: s.id, done: make(chan struct{}), export: e, session: a.session, authority: a, files: newFileDispatcher(e.share.Backend, a.session, a.epoch, c.server.config.Limits)}
	t.files.onCleanup = c.server.cleanupFailure
	t.files.onUncertain = c.server.unconfirmedMutation
	t.files.onFence = func(delta int) { c.server.mu.Lock(); c.server.fencedTrees += delta; c.server.mu.Unlock() }
	t.files.onOpen = func(delta int) { c.server.mu.Lock(); e.opens += delta; c.server.mu.Unlock() }
	s.trees[id] = t
	h.TreeID = id
	retained = true
	c.wg.Add(1)
	principal, _ := PrincipalFromContext(ctx)
	go c.renew(t, principal)
	return wire.TreeConnectResponseBody(1, 0x30, 0, 0x001f01ff), 0
}
