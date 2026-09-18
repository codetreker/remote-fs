package smb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type retirementTestState struct {
	retired, cleaned, resourcesClosed            bool
	opening, trees, authorities, frames, pending int
	local, global                                bool
	exportRefs, exportActive                     int
}

func retirementTestSnapshot(c *connection, s *session, export *Export) retirementTestState {
	var state retirementTestState
	s.mu.Lock()
	state.retired, state.cleaned = s.retired, s.cleaned
	state.opening, state.trees, state.authorities = s.openingTrees, len(s.trees), len(s.authorities)
	s.mu.Unlock()
	s.retirementMu.Lock()
	state.resourcesClosed, state.frames = s.resourcesClosed, len(s.retiringFrames)
	s.retirementMu.Unlock()
	c.mu.Lock()
	state.local, state.pending = c.sessions[s.id] == s, len(c.pending)
	c.mu.Unlock()
	state.global = c.server.sessions.get(s.id).session == s
	c.server.mu.Lock()
	state.exportRefs, state.exportActive = export.refs, export.active
	c.server.mu.Unlock()
	return state
}

func retirementTestFixture(t *testing.T, raw storage.FileSession, openErr error) (*connection, *session, *Export, *authorityTestBackend, *signing.Session) {
	t.Helper()
	backend := &authorityTestBackend{raw: raw, openErr: openErr}
	c, s, export := authorityTestConnection(t, backend, func(config *Config) { config.Limits.MaxSessions = 1 })
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	s.identityMu.Lock()
	s.signer = key
	s.identityMu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		ids := make([]uint64, 0, len(c.pending))
		for id := range c.pending {
			ids = append(ids, id)
		}
		c.mu.Unlock()
		for _, id := range ids {
			c.retireRequests([]wire.Request{{Header: wire.Header{MessageID: id}}})
		}
	})
	return c, s, export, backend, key
}

func retirementTestFrame(t *testing.T, c *connection, s *session, key *signing.Session, id uint64, command uint16) (context.Context, context.CancelFunc, wire.Request) {
	t.Helper()
	request := sessionRequest(command, wire.EmptyResponseBody())
	request.Header.MessageID, request.Header.SessionID = id, s.id
	request.Packet = requestPacket(request.Header, request.Body)
	if err := key.Sign(request.Packet); err != nil {
		t.Fatal(err)
	}
	var err error
	request.Header, err = wire.ParseHeader(request.Packet)
	if err != nil {
		t.Fatal(err)
	}
	request.Body = request.Packet[64:]
	ctx, cancel := context.WithCancel(WithPrincipal(t.Context(), s.principal))
	c.mu.Lock()
	c.pending[id] = &pendingRequest{frame: id, command: command, sessionID: s.id, ctx: ctx, cancel: cancel, charge: 1}
	c.mu.Unlock()
	return context.WithValue(ctx, pendingFrameKey{}, requestFrame{connection: c, id: id}), cancel, request
}

type retirementTestPausedContext struct {
	context.Context
	once            sync.Once
	entered, resume chan struct{}
}

// A reused authority evaluates Done after opening admission and before taking
// installation locks. The real cancellation state is preserved across the pause.
func (ctx *retirementTestPausedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered); <-ctx.resume })
	return ctx.Context.Done()
}

func retirementTestLateOpen(t *testing.T, c *connection, s *session, export *Export, raw *authorityTestSession, kind string) (func(), <-chan uint32) {
	t.Helper()
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	ctx, cancel := context.WithCancel(WithPrincipal(t.Context(), s.principal))
	var call context.Context = ctx
	if kind == "reused" {
		authorityTestConnect(t, c, s, export)
		call = &retirementTestPausedContext{Context: ctx, entered: entered, resume: resume}
	} else {
		raw.statusFn = func(context.Context) (storage.FileSessionStatus, error) {
			close(entered)
			<-resume
			return authorityTestStatus(), nil
		}
	}
	result, done := make(chan uint32, 1), make(chan struct{})
	go func() {
		defer close(done)
		header := wire.Header{SessionID: s.id}
		_, status := c.connectVolume(call, s, export.key, &header)
		result <- status
	}()
	t.Cleanup(func() { cancel(); release(); <-done })
	<-entered
	s.mu.Lock()
	opening := s.openingTrees
	s.mu.Unlock()
	if opening != 1 {
		t.Fatalf("paused opener has %d admission charges", opening)
	}
	return release, result
}

func retirementTestLogoff(t *testing.T, c *connection, s *session, key *signing.Session, kind string) wire.Request {
	t.Helper()
	ctx, cancel, frame := retirementTestFrame(t, c, s, key, 200, wire.Logoff)
	defer cancel()
	if kind == "reused" {
		if err := c.logoff(ctx, s); err != nil {
			t.Fatal(err)
		}
		return frame
	}
	result, done := make(chan error, 1), make(chan struct{})
	go func() { defer close(done); result <- c.logoff(ctx, s) }()
	t.Cleanup(func() { cancel(); <-done })
	synctest.Wait()
	s.mu.Lock()
	retired, opening := s.retired, s.openingTrees
	s.mu.Unlock()
	if !retired || opening != 1 {
		t.Fatal("LOGOFF did not reach the admitted creator")
	}
	select {
	case err := <-result:
		t.Fatalf("LOGOFF finished before creator readiness: %v", err)
	default:
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("bounded LOGOFF with pending creator: %v", err)
	}
	return frame
}

func retirementTestCapacity(t *testing.T, c *connection, held bool) {
	t.Helper()
	probe := &session{}
	err := c.server.sessions.add(c, probe)
	if err == nil {
		c.server.sessions.remove(probe)
	}
	if held && !errors.Is(err, syscall.EAGAIN) || !held && err != nil {
		t.Fatalf("session capacity held=%v: %v", held, err)
	}
}

func retirementTestSign(t *testing.T, key *signing.Session, sessionID uint64, destroyed bool) {
	t.Helper()
	packet := wire.EncodeResponse(wire.Header{Command: wire.Echo, SessionID: sessionID, Status: statusSessionDeleted}, wire.ErrorResponseBody())
	err := key.Sign(packet)
	if destroyed {
		if !errors.Is(err, signing.ErrDestroyed) {
			t.Fatalf("retired signer: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("retained signer: %v", err)
	}
	if err := key.Verify(packet); err != nil {
		t.Fatalf("retained signature: %v", err)
	}
}

func TestSessionRetirementCompletesAfterLateOpenerAndLastFrame(t *testing.T) {
	// The direct opener is unframed: its admission and the LOGOFF response are
	// independent owners. A transport frame never finishes ahead of its own call.
	for _, kind := range []string{"reused", "creator"} {
		for _, order := range []string{"frame first", "opener first"} {
			t.Run(kind+"/"+order, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					raw := newAuthorityTestSession()
					c, s, export, backend, key := retirementTestFixture(t, raw, nil)
					release, opened := retirementTestLateOpen(t, c, s, export, raw, kind)
					frame := retirementTestLogoff(t, c, s, key, kind)
					state := retirementTestSnapshot(c, s, export)
					authorities, closes := 0, int32(1)
					if kind == "creator" {
						authorities, closes = 1, 0
					}
					if !state.retired || state.cleaned || state.resourcesClosed || state.opening != 1 || state.trees != 0 || state.authorities != authorities || state.frames != 1 || !state.local || !state.global || state.exportActive != 1 || state.exportRefs != 1+authorities || raw.closes.Load() != closes {
						t.Fatalf("LOGOFF lost pending opener ownership: %+v, native closes %d", state, raw.closes.Load())
					}
					retirementTestCapacity(t, c, true)
					if order == "frame first" {
						c.retireRequests([]wire.Request{frame})
						state = retirementTestSnapshot(c, s, export)
						if state.frames != 0 || state.opening != 1 || state.resourcesClosed || !state.local || !state.global {
							t.Fatalf("last frame discarded pending opener: %+v", state)
						}
						retirementTestSign(t, key, s.id, false)
					}
					release()
					if status := <-opened; status != statusSessionDeleted {
						t.Fatalf("late opener installed after LOGOFF: %x", status)
					}
					if order == "opener first" {
						state = retirementTestSnapshot(c, s, export)
						if !state.cleaned || !state.resourcesClosed || state.opening != 0 || state.authorities != 0 || state.frames != 1 || !state.local || !state.global || state.exportRefs != 0 || state.exportActive != 0 {
							t.Fatalf("late opener did not settle resources while retaining its response owner: %+v", state)
						}
						retirementTestCapacity(t, c, true)
						retirementTestSign(t, key, s.id, false)
						c.retireRequests([]wire.Request{frame})
					}
					state = retirementTestSnapshot(c, s, export)
					if !state.retired || !state.cleaned || !state.resourcesClosed || state.opening != 0 || state.trees != 0 || state.authorities != 0 || state.frames != 0 || state.pending != 0 || state.local || state.global || state.exportRefs != 0 || state.exportActive != 0 {
						t.Fatalf("settled retirement kept capacity without another cleanup request: %+v", state)
					}
					if raw.closes.Load() != 1 || raw.statuses.Load() != 1 || raw.metadata.Load() != 0 || backend.opens.Load() != 1 || backend.closes.Load() != 0 {
						t.Fatal("retirement reopened, probed or repeated native cleanup")
					}
					retirementTestSign(t, key, s.id, true)
					retirementTestCapacity(t, c, false)
				})
			})
		}
	}
}

func TestSessionRetirementRetainsFailedCreatorCleanupUntilKnownRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		failure := errors.New("creator cleanup refused")
		var refuse atomic.Bool
		refuse.Store(true)
		raw.closeFn = func(context.Context, int32) error {
			if refuse.Load() {
				return failure
			}
			return nil
		}
		c, s, export, _, key := retirementTestFixture(t, raw, nil)
		t.Cleanup(func() { refuse.Store(false) })
		release, opened := retirementTestLateOpen(t, c, s, export, raw, "creator")
		frame := retirementTestLogoff(t, c, s, key, "creator")
		c.retireRequests([]wire.Request{frame})
		release()
		if status := <-opened; status != statusIO {
			t.Fatalf("failed creator cleanup: %x", status)
		}
		state := retirementTestSnapshot(c, s, export)
		if !state.retired || state.cleaned || state.resourcesClosed || state.opening != 0 || state.trees != 0 || state.authorities != 1 || state.frames != 0 || state.pending != 0 || !state.local || !state.global || state.exportRefs != 1 || state.exportActive != 0 || raw.closes.Load() != 1 {
			t.Fatalf("cleanup failure reclaimed unknown ownership: %+v, closes %d", state, raw.closes.Load())
		}
		c.finishSessionRetirement(s)
		if after := retirementTestSnapshot(c, s, export); after != state || raw.closes.Load() != 1 {
			t.Fatalf("completion notification performed cleanup: %+v", after)
		}
		c.server.mu.Lock()
		observedFailure := c.server.cleanupErr
		c.server.mu.Unlock()
		if !errors.Is(observedFailure, failure) {
			t.Fatalf("creator cleanup error was lost: %v", observedFailure)
		}
		retirementTestCapacity(t, c, true)
		retirementTestSign(t, key, s.id, false)
		refuse.Store(false)
		if err := export.Unpublish(t.Context()); err != nil {
			t.Fatal(err)
		}
		state = retirementTestSnapshot(c, s, export)
		if !state.cleaned || !state.resourcesClosed || state.authorities != 0 || state.local || state.global || state.exportRefs != 0 || raw.closes.Load() != 2 {
			t.Fatalf("confirmed cleanup did not complete retirement: %+v", state)
		}
		retirementTestSign(t, key, s.id, true)
		retirementTestCapacity(t, c, false)
	})
}

func TestSessionRetirementDoesNotRetireAnActiveFailedConnect(t *testing.T) {
	c, s, export, backend, key := retirementTestFixture(t, nil, syscall.EIO)
	header := wire.Header{SessionID: s.id}
	if _, status := c.connectVolume(WithPrincipal(t.Context(), s.principal), s, export.key, &header); status != statusIO {
		t.Fatalf("failed connect: %x", status)
	}
	c.finishSessionRetirement(s)
	state := retirementTestSnapshot(c, s, export)
	if state.retired || state.cleaned || state.resourcesClosed || state.opening != 0 || state.trees != 0 || state.authorities != 0 || !state.local || !state.global || state.exportRefs != 0 || state.exportActive != 0 || backend.opens.Load() != 1 {
		t.Fatalf("normal connect failure retired its active session: %+v", state)
	}
	ctx, cancel, request := retirementTestFrame(t, c, s, key, 300, wire.Echo)
	defer cancel()
	header = request.Header
	if _, status, signer := c.dispatch(ctx, request, request, &header); status != statusOK || signer != key {
		t.Fatalf("active session was no longer usable: %x", status)
	}
	c.retireRequests([]wire.Request{request})
	retirementTestSign(t, key, s.id, false)
	retirementTestCapacity(t, c, true)
}

func TestSessionRetirementPinsALateFrameUntilSigningFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw := newAuthorityTestSession()
		c, s, export, _, key := retirementTestFixture(t, raw, nil)
		release, opened := retirementTestLateOpen(t, c, s, export, raw, "reused")
		logoff := retirementTestLogoff(t, c, s, key, "reused")
		if state := retirementTestSnapshot(c, s, export); state.frames != 1 || state.opening != 1 {
			t.Fatalf("LOGOFF frame snapshot: %+v", state)
		}
		ctx, cancel, late := retirementTestFrame(t, c, s, key, 300, wire.Echo)
		defer cancel()
		header := late.Header
		_, status, escapedSigner := c.dispatch(ctx, late, late, &header)
		if status != statusSessionDeleted || escapedSigner != key {
			t.Fatalf("retired-session lookup: %x, signer %p", status, escapedSigner)
		}
		c.retireRequests([]wire.Request{logoff})
		release()
		if status := <-opened; status != statusSessionDeleted {
			t.Fatalf("late opener: %x", status)
		}
		state := retirementTestSnapshot(c, s, export)
		if !state.cleaned || !state.resourcesClosed || state.opening != 0 || state.trees != 0 || state.authorities != 0 || state.frames != 1 || state.pending != 1 || !state.local || !state.global || state.exportRefs != 0 || state.exportActive != 0 {
			t.Fatalf("final opener released a signer held by a later frame: %+v", state)
		}
		s.retirementMu.Lock()
		_, pinned := s.retiringFrames[requestFrame{connection: c, id: 300}]
		s.retirementMu.Unlock()
		if !pinned {
			t.Fatal("actual lookup did not retain its enclosing frame")
		}
		retirementTestCapacity(t, c, true)
		retirementTestSign(t, escapedSigner, s.id, false)
		c.retireRequests([]wire.Request{late})
		state = retirementTestSnapshot(c, s, export)
		if state.local || state.global || state.frames != 0 || state.pending != 0 || !state.resourcesClosed || raw.closes.Load() != 1 {
			t.Fatalf("last frame did not release its session: %+v", state)
		}
		retirementTestSign(t, escapedSigner, s.id, true)
		retirementTestCapacity(t, c, false)
	})
}
