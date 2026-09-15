package smb

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	statusOK             uint32 = 0
	statusPending        uint32 = 0x103
	statusMoreProcessing uint32 = 0xc0000016
	statusInvalid        uint32 = 0xc000000d
	statusDenied         uint32 = 0xc0000022
	statusUnsupported    uint32 = 0xc00000bb
	statusIO             uint32 = 0xc0000185
	statusResources      uint32 = 0xc000009a
	statusSessionDeleted uint32 = 0xc0000203
	statusNetworkDeleted uint32 = 0xc00000c9
	statusBadNetworkName uint32 = 0xc00000cc
	statusCancelled      uint32 = 0xc0000120
)

type connection struct {
	cleanupMu    sync.Mutex
	disconnected bool
	pendingWake  chan struct{}
	server       *Server
	net          net.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	writeMu      sync.Mutex
	wg           sync.WaitGroup
	sessions     map[uint64]*session
	pending      map[uint64]*pendingRequest
	nextSession  uint64
	nextTree     uint32
	credits      map[uint64]struct{}
	nextCredit   uint64
	negotiated   bool
	preauth      [64]byte
	incarnation  [16]byte
}

type pendingRequest struct {
	frame     uint64
	command   uint16
	ctx       context.Context
	cancel    context.CancelFunc
	sessionID uint64
	async     bool
	charge    uint16
	fileID    wire.FileID
	treeID    uint32
}

type pendingKey struct{}
type pendingFrameKey struct{}

type session struct {
	retirementMu    sync.Mutex
	retiringFrames  map[uint64]struct{}
	resourcesClosed bool
	retired         bool
	logoffMu        sync.Mutex
	cleaned         bool
	mu              sync.Mutex
	identityMu      sync.RWMutex
	authMu          sync.Mutex
	id              uint64
	principal       Principal
	auth            Authentication
	signer          *signing.Session
	preauth         [64]byte
	trees           map[uint32]*tree
	authorities     map[*Export]*authoritySession
	deadline        time.Time
}

type authoritySession struct {
	orphan  bool
	mu      sync.Mutex
	session storage.WindowsSession
	epoch   uint64
	refs    int
	closed  bool
}

type tree struct {
	id        uint32
	sessionID uint64
	export    *Export
	session   storage.WindowsSession
	files     *fileDispatcher
	authority *authoritySession
	closeMu   sync.Mutex
	closed    bool
	done      chan struct{}
	stopOnce  sync.Once
}

func newConnection(s *Server, n net.Conn) *connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &connection{server: s, net: n, ctx: ctx, cancel: cancel, sessions: make(map[uint64]*session), pending: make(map[uint64]*pendingRequest), pendingWake: make(chan struct{}), credits: map[uint64]struct{}{0: {}}, nextCredit: 1}
	rand.Read(c.incarnation[:])
	return c
}

func (c *connection) run() error {
	defer c.cleanup()
	_ = c.net.SetReadDeadline(time.Now().Add(c.server.config.Limits.HandshakeTimeout))
	firstPacket := true
	bootstrap := false
	for {
		var prefix [4]byte
		if _, err := io.ReadFull(c.net, prefix[:]); err != nil {
			return err
		}
		length := int(binary.BigEndian.Uint32(prefix[:]))
		limits := c.server.config.Limits
		if prefix[0] != 0 || length < 35 || length > limits.MaxFrameBytes {
			return wire.ErrMalformed
		}
		packet := make([]byte, length)
		if _, err := io.ReadFull(c.net, packet); err != nil {
			return err
		}
		initial := firstPacket
		firstPacket = false
		if string(packet[:4]) == "\xffSMB" {
			if !initial || wire.ParseMultiProtocolNegotiate(packet) != nil {
				return wire.ErrMalformed
			}
			if err := c.bootstrap(); err != nil {
				return err
			}
			bootstrap = true
			continue
		}
		requests, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: limits.MaxFrameBytes, MaxCommands: limits.MaxCompound, MaxContexts: limits.MaxContexts})
		if err != nil {
			return err
		}
		if bootstrap {
			if len(requests) != 1 || requests[0].Header.Command != wire.Negotiate {
				return wire.ErrMalformed
			}
			bootstrap = false
		}
		c.mu.Lock()
		reject := len(c.pending)+len(requests) > limits.MaxRequests
		c.mu.Unlock()
		for _, r := range requests {
			if r.Header.Command == wire.Cancel {
				if len(requests) != 1 {
					return wire.ErrMalformed
				}
				if err := c.cancelRequest(r); err != nil {
					return err
				}
				continue
			}
			c.mu.Lock()
			charge := r.Header.CreditCharge
			if charge == 0 {
				charge = 1
			}
			if r.Header.MessageID > math.MaxUint64-uint64(charge) {
				c.mu.Unlock()
				return errors.New("SMB request capacity exhausted")
			}
			if int(charge) < requiredCredits(r) {
				c.mu.Unlock()
				return wire.ErrMalformed
			}
			for j := uint64(0); j < uint64(charge); j++ {
				if _, ok := c.credits[r.Header.MessageID+j]; !ok {
					c.mu.Unlock()
					return wire.ErrMalformed
				}
			}
			for j := uint64(0); j < uint64(charge); j++ {
				delete(c.credits, r.Header.MessageID+j)
			}
			if !reject {
				requestCtx, cancel := context.WithTimeout(c.ctx, limits.RequestTimeout)
				c.pending[r.Header.MessageID] = &pendingRequest{frame: requests[0].Header.MessageID, command: r.Header.Command, ctx: requestCtx, cancel: cancel, sessionID: r.Header.SessionID, charge: charge}
			}
			c.mu.Unlock()
		}
		if len(requests) == 1 && requests[0].Header.Command == wire.Cancel {
			continue
		}
		if reject {
			if err := c.rejectRequests(requests); err != nil {
				return err
			}
			continue
		}
		// Handshake state advances in transport order; authenticated data work can
		// wait independently so CANCEL never waits behind a remote lock request.
		if requests[0].Header.Command == wire.SessionSetup && len(requests) != 1 {
			return wire.ErrMalformed
		}
		if requests[0].Header.Command == wire.Negotiate {
			if len(requests) != 1 {
				return wire.ErrMalformed
			}
			if err := c.process(requests); err != nil {
				return err
			}
			continue
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			if err := c.process(requests); err != nil {
				c.cancel()
				_ = c.net.Close()
			}
		}()
	}
}

func (c *connection) cleanup() {
	c.cancel()
	_ = c.net.Close()
	c.wg.Wait()
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	c.mu.Lock()
	c.disconnected = true
	sessions := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	for _, s := range sessions {
		s.mu.Lock()
		s.retired = true
		for _, a := range s.authorities {
			a.mu.Lock()
			if !a.closed {
				ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
				err := a.session.Close(ctx)
				cancel()
				if err == nil {
					a.closed = true
				}
				c.server.cleanupFailure(err)
			}
			a.mu.Unlock()
		}
		for id, t := range s.trees {
			if c.closeTree(t) == nil {
				delete(s.trees, id)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
		c.server.cleanupFailure(c.closeOrphansLocked(ctx, s, nil))
		cancel()
		if s.auth != nil {
			if err := s.auth.Close(); err != nil {
				c.server.cleanupFailure(err)
			} else {
				s.auth = nil
			}
		}
		s.identityMu.Lock()
		if s.signer != nil {
			s.signer.Destroy()
			s.signer = nil
		}
		s.identityMu.Unlock()
		clean := len(s.trees) == 0 && len(s.authorities) == 0 && s.auth == nil
		s.mu.Unlock()
		if clean {
			c.mu.Lock()
			if c.sessions[s.id] == s {
				delete(c.sessions, s.id)
			}
			c.mu.Unlock()
		}
	}
	c.releaseDisconnected()
}

func (c *connection) releaseDisconnected() {
	c.mu.Lock()
	clean := c.disconnected && len(c.sessions) == 0
	c.mu.Unlock()
	if clean {
		c.server.mu.Lock()
		delete(c.server.connections, c)
		c.server.mu.Unlock()
	}
}

func (c *connection) closeTree(t *tree) error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if t.closed {
		return nil
	}
	t.stopOnce.Do(func() {
		if t.done != nil {
			close(t.done)
		}
	})
	c.mu.Lock()
	for _, p := range c.pending {
		if p.treeID == t.id && p.sessionID == t.sessionID && p.command != wire.Logoff && p.command != wire.TreeDisconnect {
			p.cancel()
		}
	}
	c.mu.Unlock()
	t.files.mu.Lock()
	for id := range t.files.handles {
		t.export.changes.remove(c.notifyKey(id))
	}
	t.files.failed = true
	t.files.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
	defer cancel()
	var err error
	if t.authority == nil {
		err = t.session.Close(ctx)
	} else {
		a := t.authority
		a.mu.Lock()
		if a.closed {
			a.refs--
		} else if a.refs > 1 {
			err = t.files.closeAll(ctx)
			if err == nil {
				a.refs--
			}
		} else {
			err = a.session.Close(ctx)
			if err == nil {
				a.closed = true
				a.refs = 0
			}
		}
		a.mu.Unlock()
	}
	if err == nil {
		t.closed = true
		t.files.mu.Lock()
		n := len(t.files.handles)
		clear(t.files.handles)
		if !t.files.retired && t.files.fenced && t.files.onFence != nil {
			t.files.onFence(-1)
		}
		t.files.retired = true
		t.files.mu.Unlock()
		if n != 0 && t.files.onOpen != nil {
			t.files.onOpen(-n)
		}
	}
	c.server.mu.Lock()
	if err == nil {
		t.export.refs--
	}
	c.server.mu.Unlock()
	c.server.cleanupFailure(err)
	return err
}

func (c *connection) cancelRequest(r wire.Request) error {
	if err := r.Empty(); err != nil {
		return err
	}
	message := r.Header.MessageID
	if r.Header.Flags&wire.FlagAsync != 0 {
		if r.Header.AsyncID == 0 {
			return wire.ErrMalformed
		}
		message = r.Header.AsyncID - 1
	}
	c.mu.Lock()
	s := c.sessions[r.Header.SessionID]
	pending := c.pending[message]
	var cancel context.CancelFunc
	if pending != nil && pending.sessionID == r.Header.SessionID && (r.Header.Flags&wire.FlagAsync == 0 || pending.async) {
		cancel = pending.cancel
	}
	c.mu.Unlock()
	if s == nil {
		return wire.ErrMalformed
	}
	s.identityMu.RLock()
	signer := s.signer
	s.identityMu.RUnlock()
	if signer == nil || signer.Verify(r.Packet) != nil {
		return signing.ErrSignature
	}
	if cancel != nil {
		cancel()
	}
	return nil
}

func (c *connection) retireRequests(requests []wire.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range requests {
		if p := c.pending[r.Header.MessageID]; p != nil {
			p.cancel()
			delete(c.pending, r.Header.MessageID)
		}
	}
	if len(requests) > 0 {
		frame := requests[0].Header.MessageID
		for _, s := range c.sessions {
			s.retirementMu.Lock()
			delete(s.retiringFrames, frame)
			s.retirementMu.Unlock()
		}
	}
	c.pruneRetiredSessionsLocked()
	if c.pendingWake != nil {
		close(c.pendingWake)
	}
	c.pendingWake = make(chan struct{})
}
func (c *connection) writeFinal(packet []byte, requests []wire.Request) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.retireRequests(requests)
	return c.writeLocked(packet)
}
func (c *connection) write(packet []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeLocked(packet)
}
func (c *connection) writeLocked(packet []byte) error {
	_ = c.net.SetWriteDeadline(time.Now().Add(c.server.config.Limits.RequestTimeout))
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(packet)))
	if _, err := c.net.Write(prefix[:]); err != nil {
		return err
	}
	for len(packet) > 0 {
		n, err := c.net.Write(packet)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		packet = packet[n:]
	}
	return nil
}

func (c *connection) process(requests []wire.Request) error {
	defer c.retireRequests(requests)
	var output []byte
	var inheritedSession uint64
	var inheritedTree uint32
	var inheritedFile wire.FileID
	var previousStatus uint32
	for i, r := range requests {
		original := r
		if r.Header.Flags&wire.FlagRelated == 0 {
			inheritedFile = wire.FileID{}
		}
		if r.Header.Flags&wire.FlagRelated != 0 {
			r.Header.SessionID = inheritedSession
			r.Header.TreeID = inheritedTree
			if _, ok := relatedFile(r); ok {
				body := append([]byte(nil), r.Body...)
				patchRelatedFile(r.Header.Command, body, inheritedFile)
				r.Body = body
				packet := append([]byte(nil), r.Packet...)
				copy(packet[64:], body)
				r.Packet = packet
			}
		}
		c.mu.Lock()
		pending := c.pending[r.Header.MessageID]
		pending.sessionID = r.Header.SessionID
		pending.treeID = r.Header.TreeID
		pending.fileID, _ = relatedFile(r)
		c.mu.Unlock()
		ctx := context.WithValue(pending.ctx, pendingFrameKey{}, pending.frame)
		cancel := pending.cancel
		h := r.Header
		h.Flags = 0
		h.NextCommand = 0
		h.Signature = [16]byte{}
		if !pending.async {
			h.Credits = c.grantCredits(r.Header.Credits)
		} else {
			h.Credits = 0
		}
		var body []byte
		var signer *signing.Session
		ctx = context.WithValue(ctx, pendingKey{}, func(key *signing.Session) error {
			for j := i; j < len(requests); j++ {
				if j > i && requests[j].Header.Flags&wire.FlagRelated == 0 {
					break
				}
				originalPending := requests[j]
				if err := key.Verify(originalPending.Packet); err != nil {
					return err
				}
				c.mu.Lock()
				p := c.pending[originalPending.Header.MessageID]
				already := p.async
				if !already {
					p.async = true
					p.sessionID = h.SessionID
				}
				c.mu.Unlock()
				if already {
					continue
				}
				hp := originalPending.Header
				hp.Flags = wire.FlagAsync
				hp.NextCommand = 0
				hp.Signature = [16]byte{}
				hp.SessionID = h.SessionID
				hp.AsyncID = hp.MessageID + 1
				hp.Status = statusPending
				if j == i {
					hp.Credits = h.Credits
				} else {
					hp.Credits = c.grantCredits(hp.Credits)
				}
				packet := wire.EncodeResponse(hp, wire.ErrorResponseBody())
				if err := key.Sign(packet); err != nil {
					return err
				}
				if err := c.write(packet); err != nil {
					return err
				}
			}
			return nil
		})
		if r.Header.Flags&wire.FlagRelated != 0 && previousStatus != 0 || ctx.Err() != nil || responseBudget(r) > c.server.config.Limits.MaxFrameBytes-len(output)-80*(len(requests)-i-1) {
			h.Status = statusResources
			if ctx.Err() != nil {
				h.Status = statusError(ctx.Err())
			}
			if r.Header.Flags&wire.FlagRelated != 0 && previousStatus != 0 {
				h.Status = previousStatus
			}
			c.mu.Lock()
			ss := c.sessions[h.SessionID]
			c.mu.Unlock()
			if ss != nil {
				ss.identityMu.RLock()
				signer = ss.signer
				ss.identityMu.RUnlock()
				if signer != nil && signer.Verify(original.Packet) != nil {
					return signing.ErrSignature
				}
			}
		} else {
			body, h.Status, signer = c.dispatch(ctx, r, original, &h)
		}
		cancel()

		if pending.async {
			h.Flags |= wire.FlagAsync
			h.AsyncID = r.Header.MessageID + 1
			h.Credits = 0
		}
		if body == nil {
			body = wire.ErrorResponseBody()
		}
		inheritedSession = h.SessionID
		inheritedTree = h.TreeID
		previousStatus = h.Status
		if h.Status == 0 {
			if id, ok := relatedFile(r); ok {
				inheritedFile = id
			}
		}
		if h.Command == wire.Create && h.Status == 0 && len(body) >= 80 {
			copy(inheritedFile[:], body[64:80])
		}
		if i > 0 {
			h.Flags |= wire.FlagRelated
		}
		if i+1 < len(requests) {
			padding := (-(64 + len(body))) & 7
			body = append(body, make([]byte, padding)...)
			h.NextCommand = uint32(64 + len(body))
		}
		packet := wire.EncodeResponse(h, body)
		if signer != nil {

			if err := signer.Sign(packet); err != nil {
				return err
			}
		}
		if h.Command == wire.Negotiate && h.Status == 0 {
			c.preauth = signing.Preauth(c.preauth, packet)
		}

		if logger := c.server.config.Logger; logger != nil {
			phase := "file"
			switch h.Command {
			case wire.Negotiate:
				phase = "negotiate"
			case wire.SessionSetup:
				phase = "authentication"
			case wire.TreeConnect, wire.TreeDisconnect:
				phase = "share"
			case wire.Lock:
				phase = "lock"
			case wire.ChangeNotify:
				phase = "notification"
			}
			logger.Debug("SMB request completed", "phase", phase, "connection", hex.EncodeToString(c.incarnation[:]), "session", h.SessionID, "tree", h.TreeID, "message", h.MessageID, "command", h.Command, "status", h.Status)
		}
		output = append(output, packet...)
	}
	return c.writeFinal(output, requests)
}

func patchRelatedFile(command uint16, body []byte, id wire.FileID) {
	offset := -1
	switch command {
	case wire.Close, wire.Flush:
		offset = 8
	case wire.Read, wire.Write:
		offset = 16
	case wire.Lock, wire.IOCTL, wire.QueryDirectory, wire.ChangeNotify:
		offset = 8
	case wire.QueryInfo:
		offset = 24
	case wire.SetInfo:
		offset = 16
	}
	if offset >= 0 && len(body) >= offset+16 {
		copy(body[offset:offset+16], id[:])
	}
}

func requiredCredits(r wire.Request) int {
	n := 0
	switch r.Header.Command {
	case wire.Write:
		if v, e := r.Write(); e == nil {
			n = len(v.Data)
		}
	case wire.Read:
		if v, e := r.Read(); e == nil {
			n = int(v.Length)
		}
	case wire.QueryDirectory:
		if v, e := r.QueryDirectory(); e == nil {
			n = int(v.OutputLength)
		}
	case wire.QueryInfo:
		if v, e := r.QueryInfo(); e == nil {
			n = int(v.OutputLength)
		}
	case wire.ChangeNotify:
		if v, e := r.Notify(); e == nil {
			n = int(v.OutputLength)
		}
	case wire.IOCTL:
		if v, e := r.IOCTL(); e == nil {
			n = max(len(v.Input), len(v.Output), int(v.MaxInputResponse), int(v.MaxOutputResponse))
		}
	}
	return max(1, (n+65535)/65536)
}

func (c *connection) grantCredits(requested uint16) uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := int(requested)
	if n < 1 {
		n = 1
	}
	capacity := c.server.config.Limits.MaxRequests - len(c.credits)
	if n > capacity {
		n = capacity
	}
	for i := 0; i < n; i++ {
		if c.nextCredit == math.MaxUint64 {
			return uint16(i)
		}
		c.credits[c.nextCredit] = struct{}{}
		c.nextCredit++
	}
	return uint16(n)
}

func relatedFile(r wire.Request) (wire.FileID, bool) {
	offset := -1
	switch r.Header.Command {
	case wire.Close, wire.Flush:
		offset = 8
	case wire.Read, wire.Write:
		offset = 16
	case wire.Lock, wire.IOCTL, wire.QueryDirectory, wire.ChangeNotify:
		offset = 8
	case wire.QueryInfo:
		offset = 24
	case wire.SetInfo:
		offset = 16
	}
	var id wire.FileID
	if offset < 0 || len(r.Body) < offset+16 {
		return id, false
	}
	copy(id[:], r.Body[offset:offset+16])
	return id, true
}

func (c *connection) notifyKey(id wire.FileID) string {
	return hex.EncodeToString(c.incarnation[:]) + hex.EncodeToString(id[:])
}

func (c *connection) cancelOpen(sessionID uint64, treeID uint32, id wire.FileID, except uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for message, p := range c.pending {
		if message != except && p.sessionID == sessionID && p.treeID == treeID && p.fileID == id {
			p.cancel()
		}
	}
}

func (c *connection) rejectRequests(requests []wire.Request) error {
	var output []byte
	var inheritedSession uint64
	var inheritedTree uint32
	for i, r := range requests {
		h := r.Header
		if h.Flags&wire.FlagRelated != 0 {
			h.SessionID = inheritedSession
			h.TreeID = inheritedTree
		}
		inheritedSession = h.SessionID
		inheritedTree = h.TreeID
		c.mu.Lock()
		s := c.sessions[h.SessionID]
		c.mu.Unlock()
		var key *signing.Session
		if s != nil {
			s.identityMu.RLock()
			key = s.signer
			s.identityMu.RUnlock()
			if key != nil && key.Verify(r.Packet) != nil {
				return signing.ErrSignature
			}
		}
		h.Status = statusResources
		h.Flags = 0
		h.Signature = [16]byte{}
		h.NextCommand = 0
		h.Credits = c.grantCredits(h.Credits)
		body := wire.ErrorResponseBody()
		if i+1 < len(requests) {
			body = append(body, make([]byte, (-(64+len(body)))&7)...)
			h.NextCommand = uint32(64 + len(body))
		}
		packet := wire.EncodeResponse(h, body)
		if key != nil {
			if err := key.Sign(packet); err != nil {
				return err
			}
		}
		output = append(output, packet...)
	}
	return c.write(output)
}

func (c *connection) closeExport(e *Export) error {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	c.mu.Lock()
	sessions := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	for _, s := range sessions {
		s.mu.Lock()
		for id, t := range s.trees {
			if t.export == e {
				if err := c.closeTree(t); err != nil {
					s.mu.Unlock()
					return err
				}
				delete(s.trees, id)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
		err := c.closeOrphansLocked(ctx, s, e)
		cancel()
		clean := len(s.trees) == 0 && len(s.authorities) == 0 && s.auth == nil
		s.mu.Unlock()
		if clean {
			c.mu.Lock()
			if c.disconnected && c.sessions[s.id] == s {
				delete(c.sessions, s.id)
			}
			c.mu.Unlock()
		}
		if err != nil {
			return err
		}
	}
	c.releaseDisconnected()
	return nil
}

func responseBudget(r wire.Request) int {
	n := 128
	switch r.Header.Command {
	case wire.Read:
		if q, e := r.Read(); e == nil {
			n = 80 + int(q.Length)
		}
	case wire.QueryDirectory:
		if q, e := r.QueryDirectory(); e == nil {
			n = 72 + int(q.OutputLength)
		}
	case wire.QueryInfo:
		if q, e := r.QueryInfo(); e == nil {
			n = 72 + int(q.OutputLength)
		}
	case wire.ChangeNotify:
		if q, e := r.Notify(); e == nil {
			n = 72 + int(q.OutputLength)
		}
	case wire.IOCTL:
		if q, e := r.IOCTL(); e == nil {
			n = 112 + int(q.MaxOutputResponse)
		}
	case wire.Create:
		n = 32 << 10
	case wire.SessionSetup:
		n = 72 + 65535
	case wire.Negotiate:
		n = 128 + 65535
	}
	return max(88, (n+7)&^7)
}
