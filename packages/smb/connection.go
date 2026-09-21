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
)

const (
	statusOK                    uint32 = 0
	statusPending               uint32 = 0x00000103
	statusMoreProcessing        uint32 = 0xc0000016
	statusInvalid               uint32 = 0xc000000d
	statusDenied                uint32 = 0xc0000022
	statusUnsupported           uint32 = 0xc00000bb
	statusIO                    uint32 = 0xc0000185
	statusResources             uint32 = 0xc000009a
	statusSessionDeleted        uint32 = 0xc0000203
	statusNetworkDeleted        uint32 = 0xc00000c9
	statusBadNetworkName        uint32 = 0xc00000cc
	statusCancelled             uint32 = 0xc0000120
	statusRequestNotAccepted    uint32 = 0xc00000d0
	statusNetworkSessionExpired uint32 = 0xc000035c
)

type connection struct {
	clientGUID   [16]byte
	cleanupMu    sync.Mutex
	closing      bool
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
	treeID    uint32
}

type pendingKey struct{}
type pendingFrameKey struct{}

type requestFrame struct {
	connection *connection
	id         uint64
}

type session struct {
	retirementMu                 sync.Mutex
	retiringFrames               map[requestFrame]struct{}
	resourcesClosed              bool
	retired                      bool
	finalizing                   bool
	logoffMu                     sync.Mutex
	cleanup                      cleanupGate
	cleaned                      bool
	mu                           sync.Mutex
	identityMu                   sync.RWMutex
	authMu                       sync.Mutex
	id                           uint64
	principal                    Principal
	auth                         Authentication
	authGeneration               uint64
	authArmed, authExpired       bool
	identityExpired              bool
	authWake, authStop, authDone chan struct{}
	authStopOnce                 sync.Once
	signer                       *signing.Session
	preauth                      [64]byte
	openingTrees                 int
	trees                        map[uint32]*tree
	authorities                  map[*Export]*authoritySession
	authDeadline                 time.Time
	identityDeadline             time.Time
}

type treeKind uint8

const (
	volumeTree treeKind = iota
	controlTree
)

type tree struct {
	kind      treeKind
	id        uint32
	sessionID uint64
	export    *Export
	authority *authoritySession
	closeMu   sync.Mutex
	cleanup   cleanupGate
	closed    bool
	done      chan struct{}
	stopOnce  sync.Once
}

func newConnection(server *Server, network net.Conn) *connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &connection{
		server: server, net: network, ctx: ctx, cancel: cancel,
		sessions: make(map[uint64]*session), pending: make(map[uint64]*pendingRequest),
		pendingWake: make(chan struct{}), credits: map[uint64]struct{}{0: {}}, nextCredit: 1,
	}
	_, _ = rand.Read(c.incarnation[:])
	return c
}

func (c *connection) run() error {
	defer c.cleanup()
	if err := c.net.SetReadDeadline(time.Now().Add(c.server.config.Limits.HandshakeTimeout)); err != nil {
		return err
	}
	firstPacket, bootstrap := true, false
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
		for _, request := range requests {
			if request.Header.Command == wire.Cancel {
				return wire.ErrMalformed
			}
			c.mu.Lock()
			charge := request.Header.CreditCharge
			if charge == 0 {
				charge = 1
			}
			if request.Header.MessageID > math.MaxUint64-uint64(charge) {
				c.mu.Unlock()
				return errors.New("SMB request capacity exhausted")
			}
			if int(charge) < requiredCredits(request) {
				c.mu.Unlock()
				return wire.ErrMalformed
			}
			for offset := uint64(0); offset < uint64(charge); offset++ {
				if _, ok := c.credits[request.Header.MessageID+offset]; !ok {
					c.mu.Unlock()
					return wire.ErrMalformed
				}
			}
			for offset := uint64(0); offset < uint64(charge); offset++ {
				delete(c.credits, request.Header.MessageID+offset)
			}
			if !reject {
				requestCtx, cancel := context.WithTimeout(c.ctx, limits.RequestTimeout)
				c.pending[request.Header.MessageID] = &pendingRequest{
					frame: requests[0].Header.MessageID, command: request.Header.Command,
					ctx: requestCtx, cancel: cancel, sessionID: request.Header.SessionID, charge: charge,
				}
			}
			c.mu.Unlock()
		}
		if reject {
			if err := c.rejectRequests(requests); err != nil {
				return err
			}
			continue
		}
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
		go func(requests []wire.Request) {
			defer c.wg.Done()
			if err := c.process(requests); err != nil {
				c.cancel()
				_ = c.net.Close()
			}
		}(requests)
	}
}

func (c *connection) retireRequests(requests []wire.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, request := range requests {
		if pending := c.pending[request.Header.MessageID]; pending != nil {
			pending.cancel()
			delete(c.pending, request.Header.MessageID)
		}
	}
	if len(requests) != 0 {
		c.retireResponseFrameLocked(requestFrame{connection: c, id: requests[0].Header.MessageID})
	}
	close(c.pendingWake)
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
	if err := c.net.SetWriteDeadline(time.Now().Add(c.server.config.Limits.RequestTimeout)); err != nil {
		return err
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(packet)))
	if n, err := c.net.Write(prefix[:]); err != nil {
		return err
	} else if n != len(prefix) {
		return io.ErrShortWrite
	}
	for len(packet) != 0 {
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
	var previousStatus uint32
	for index, request := range requests {
		original := request
		if request.Header.Flags&wire.FlagRelated != 0 {
			request.Header.SessionID = inheritedSession
			request.Header.TreeID = inheritedTree
		}
		c.mu.Lock()
		pending := c.pending[request.Header.MessageID]
		if pending == nil {
			c.mu.Unlock()
			return wire.ErrMalformed
		}
		pending.sessionID = request.Header.SessionID
		pending.treeID = request.Header.TreeID
		c.mu.Unlock()
		ctx := context.WithValue(pending.ctx, pendingFrameKey{}, requestFrame{connection: c, id: pending.frame})
		header := request.Header
		header.Flags = 0
		header.NextCommand = 0
		header.Signature = [16]byte{}
		if !pending.async {
			header.Credits = c.grantCredits(request.Header.Credits)
		}

		var body []byte
		var signer *signing.Session
		if request.Header.Flags&wire.FlagRelated != 0 && previousStatus != 0 || ctx.Err() != nil ||
			responseBudget(request) > c.server.config.Limits.MaxFrameBytes-len(output)-80*(len(requests)-index-1) {
			header.Status = statusResources
			if ctx.Err() != nil {
				header.Status = statusError(ctx.Err())
			}
			if request.Header.Flags&wire.FlagRelated != 0 && previousStatus != 0 {
				header.Status = previousStatus
			}
			c.mu.Lock()
			s := c.sessions[header.SessionID]
			c.retainSessionFrameLocked(s, ctx, request.Header.MessageID)
			c.mu.Unlock()
			if s != nil {
				s.identityMu.RLock()
				signer = s.signer
				s.identityMu.RUnlock()
				if signer != nil && signer.Verify(original.Packet) != nil {
					return signing.ErrSignature
				}
			}
		} else {
			body, header.Status, signer = c.dispatch(ctx, request, original, &header)
		}
		pending.cancel()
		if pending.async {
			header.Flags |= wire.FlagAsync
			header.AsyncID = request.Header.MessageID + 1
			header.Credits = 0
		}
		if body == nil {
			body = wire.ErrorResponseBody()
		}
		inheritedSession, inheritedTree, previousStatus = header.SessionID, header.TreeID, header.Status
		if index > 0 {
			header.Flags |= wire.FlagRelated
		}
		if index+1 < len(requests) {
			padding := (-(64 + len(body))) & 7
			body = append(body, make([]byte, padding)...)
			header.NextCommand = uint32(64 + len(body))
		}
		packet := wire.EncodeResponse(header, body)
		if signer != nil {
			if err := signer.Sign(packet); err != nil {
				return err
			}
		}
		if header.Command == wire.Negotiate && header.Status == statusOK {
			c.preauth = signing.Preauth(c.preauth, packet)
		}
		if logger := c.server.config.Logger; logger != nil {
			phase := "control"
			switch header.Command {
			case wire.Negotiate:
				phase = "negotiate"
			case wire.SessionSetup:
				phase = "authentication"
			case wire.TreeConnect, wire.TreeDisconnect:
				phase = "share"
			}
			logger.Debug("SMB request completed", "component", "smb", "phase", phase,
				"connection_id", hex.EncodeToString(c.incarnation[:]), "session", header.SessionID,
				"tree", header.TreeID, "message", header.MessageID, "command", header.Command,
				"status", header.Status)
		}
		output = append(output, packet...)
	}
	return c.writeFinal(output, requests)
}

func requiredCredits(wire.Request) int { return 1 }

func (c *connection) grantCredits(requested uint16) uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := int(requested)
	if count < 1 {
		count = 1
	}
	if capacity := c.server.config.Limits.MaxRequests - len(c.credits); count > capacity {
		count = capacity
	}
	for index := 0; index < count; index++ {
		if c.nextCredit == math.MaxUint64 {
			return uint16(index)
		}
		c.credits[c.nextCredit] = struct{}{}
		c.nextCredit++
	}
	return uint16(count)
}

func (c *connection) rejectRequests(requests []wire.Request) error {
	ctx := context.Background()
	if len(requests) != 0 {
		frame := requestFrame{connection: c, id: requests[0].Header.MessageID}
		ctx = context.WithValue(ctx, pendingFrameKey{}, frame)
		defer func() {
			c.mu.Lock()
			c.retireResponseFrameLocked(frame)
			c.mu.Unlock()
		}()
	}
	var output []byte
	var inheritedSession uint64
	var inheritedTree uint32
	for index, request := range requests {
		header := request.Header
		if header.Flags&wire.FlagRelated != 0 {
			header.SessionID, header.TreeID = inheritedSession, inheritedTree
		}
		inheritedSession, inheritedTree = header.SessionID, header.TreeID
		c.mu.Lock()
		s := c.sessions[header.SessionID]
		c.retainSessionFrameLocked(s, ctx, request.Header.MessageID)
		c.mu.Unlock()
		var signer *signing.Session
		if s != nil {
			s.identityMu.RLock()
			signer = s.signer
			s.identityMu.RUnlock()
			if signer != nil && signer.Verify(request.Packet) != nil {
				return signing.ErrSignature
			}
		}
		header.Status = statusResources
		header.Flags = 0
		header.Signature = [16]byte{}
		header.NextCommand = 0
		header.Credits = c.grantCredits(header.Credits)
		body := wire.ErrorResponseBody()
		if index+1 < len(requests) {
			body = append(body, make([]byte, (-(64+len(body)))&7)...)
			header.NextCommand = uint32(64 + len(body))
		}
		packet := wire.EncodeResponse(header, body)
		if signer != nil {
			if err := signer.Sign(packet); err != nil {
				return err
			}
		}
		output = append(output, packet...)
	}
	return c.write(output)
}

func responseBudget(request wire.Request) int {
	switch request.Header.Command {
	case wire.SessionSetup, wire.Negotiate:
		return 72 + 65535
	default:
		return 128
	}
}

// Session lookup and frame enrollment share c.mu, so retirement cannot remove
// the final signer between lookup and response ownership.
func (c *connection) retainSessionFrameLocked(s *session, ctx context.Context, message uint64) {
	if s == nil {
		return
	}
	frame, ok := ctx.Value(pendingFrameKey{}).(requestFrame)
	if !ok {
		if pending := c.pending[message]; pending != nil {
			frame = requestFrame{connection: c, id: pending.frame}
			ok = true
		}
	}
	if !ok || frame.connection != c {
		return
	}
	s.retirementMu.Lock()
	if s.retiringFrames == nil {
		s.retiringFrames = make(map[requestFrame]struct{})
	}
	s.retiringFrames[frame] = struct{}{}
	s.retirementMu.Unlock()
}

func (c *connection) retireResponseFrameLocked(frame requestFrame) {
	for _, s := range c.sessions {
		s.retirementMu.Lock()
		delete(s.retiringFrames, frame)
		s.retirementMu.Unlock()
	}
	c.pruneRetiredSessionsLocked()
}

func (c *connection) addStatus(status *Status) {
	c.mu.Lock()
	status.PendingRequests += len(c.pending)
	if c.disconnected {
		status.RetainedConnections++
	}
	sessions := make([]*session, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessions = append(sessions, session)
	}
	c.mu.Unlock()
	status.Sessions += len(sessions)
	for _, session := range sessions {
		session.mu.Lock()
		status.Trees += len(session.trees) + session.openingTrees
		authorities := make([]*authoritySession, 0, len(session.authorities))
		for _, authority := range session.authorities {
			authorities = append(authorities, authority)
		}
		session.mu.Unlock()
		for _, authority := range authorities {
			if authority.isStopping() && !authority.isClosed() {
				status.FencedAuthorities++
			}
		}
	}
}
