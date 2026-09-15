package smb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type cleanupBackend struct {
	storage.WindowsStorage
	newCalls   atomic.Int32
	newSession func() storage.WindowsSession
}

func (b *cleanupBackend) NewWindowsSession(context.Context, storage.FileSessionOptions) (storage.WindowsSession, error) {
	b.newCalls.Add(1)
	return b.newSession(), nil
}

type cleanupAuthority struct {
	storage.WindowsSession
	statusErr  error
	closeFails atomic.Bool
	closes     atomic.Int32
}

func (a *cleanupAuthority) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{ActionEpoch: 1}, a.statusErr
}

func (a *cleanupAuthority) Close(context.Context) error {
	a.closes.Add(1)
	if a.closeFails.Load() {
		return syscall.EIO
	}
	return nil
}

func cleanupTreeConnectBody() []byte {
	name := wire.EncodeUTF16("\\\\127.0.0.1\\work")
	body := make([]byte, 8+len(name))
	smbLE.PutUint16(body, 9)
	smbLE.PutUint16(body[4:], 72)
	smbLE.PutUint16(body[6:], uint16(len(name)))
	copy(body[8:], name)
	return body
}

func cleanupDispatch(t *testing.T, ctx context.Context, c *connection, s *session, command uint16, treeID uint32, body []byte) (wire.Header, uint32) {
	t.Helper()
	r := fileRequest(command, body)
	r.Header.SessionID, r.Header.TreeID, r.Header.MessageID = s.id, treeID, 10
	r.Packet = requestPacket(r.Header, r.Body)
	if err := s.signer.Sign(r.Packet); err != nil {
		t.Fatal(err)
	}
	r.Header, _ = wire.ParseHeader(r.Packet)
	r.Body = r.Packet[64:]
	h := r.Header
	_, status, _ := c.dispatch(ctx, r, r, &h)
	return h, status
}

func TestFailedAuthorityAdmissionRetainsCleanupUntilRetry(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeDisconnect, tr.id, wire.EmptyResponseBody()); status != statusOK {
		t.Fatalf("initial disconnect = %x", status)
	}
	a := &cleanupAuthority{statusErr: syscall.EIO}
	a.closeFails.Store(true)
	backend := &cleanupBackend{WindowsStorage: tr.export.share.Backend, newSession: func() storage.WindowsSession { return a }}
	tr.export.share.Backend = backend
	for range 2 {
		if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeConnect, 0, cleanupTreeConnectBody()); status != statusIO {
			t.Fatalf("failed authority admission = %x", status)
		}
	}
	if backend.newCalls.Load() != 1 || a.closes.Load() != 1 {
		t.Fatalf("orphan replaced or abandoned: new=%d closes=%d", backend.newCalls.Load(), a.closes.Load())
	}
	if err := tr.export.Unpublish(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unpublish lost unresolved authority: %v", err)
	}
	if a.closes.Load() != 2 {
		t.Fatalf("unpublish did not retry orphan close: %d", a.closes.Load())
	}
	a.closeFails.Store(false)
	if err := tr.export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a.closes.Load() != 3 {
		t.Fatalf("successful cleanup attempts = %d, want 3", a.closes.Load())
	}
	s.mu.Lock()
	remaining := len(s.authorities)
	s.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("released orphan retained %d authority entries", remaining)
	}
}

func TestLogoffPreventsTreeConnectAfterAuthorizationReturns(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	backend := &cleanupBackend{WindowsStorage: tr.export.share.Backend, newSession: func() storage.WindowsSession { return &cleanupAuthority{} }}
	tr.export.share.Backend = backend
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	c.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, req authz.AccessRequest) error {
		if req.Operation == storage.OpWindowsSessionOpen {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan uint32, 1)
	go func() {
		_, status := cleanupDispatch(t, ctx, c, s, wire.TreeConnect, 0, cleanupTreeConnectBody())
		done <- status
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("tree connect did not reach authorization")
	}
	if _, status := cleanupDispatch(t, ctx, c, s, wire.Logoff, 0, wire.EmptyResponseBody()); status != statusOK {
		t.Fatalf("logoff = %x", status)
	}
	unblock()
	select {
	case status := <-done:
		if status != statusSessionDeleted {
			t.Fatalf("tree connect escaped logoff: %x", status)
		}
	case <-ctx.Done():
		t.Fatal("tree connect did not finish after authorization")
	}
	if backend.newCalls.Load() != 0 {
		t.Fatal("authority created after its SMB session retired")
	}
	if err := tr.export.Unpublish(ctx); err != nil {
		t.Fatalf("retired session leaked export ownership: %v", err)
	}
}

func TestLogoffClosesPendingReauthenticationContext(t *testing.T) {
	c, s, _, _, _, _ := testConnection(t)
	authenticator := &testAuthenticator{}
	c.server.config.Authenticator = authenticator
	packet := setupPacket(21, s.id, "initial")
	if err := s.signer.Sign(packet); err != nil {
		t.Fatal(err)
	}
	header, err := wire.ParseHeader(packet)
	if err != nil {
		t.Fatal(err)
	}
	r := wire.Request{Header: header, Packet: packet, Body: packet[64:]}
	h := header
	_, status, _ := c.dispatch(t.Context(), r, r, &h)
	if status != statusMoreProcessing || authenticator.closed.Load() != 0 {
		t.Fatalf("pending reauthentication = %x, closed=%d", status, authenticator.closed.Load())
	}
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.Logoff, 0, wire.EmptyResponseBody()); status != statusOK {
		t.Fatalf("logoff = %x", status)
	}
	if authenticator.closed.Load() != 1 {
		t.Fatalf("reauthentication context close count = %d, want 1", authenticator.closed.Load())
	}
	c.mu.Lock()
	retained := c.sessions[s.id] != nil
	c.mu.Unlock()
	if retained {
		t.Fatal("logged-off SMB session remained reachable")
	}
	c.cleanup()
	if authenticator.closed.Load() != 1 {
		t.Fatal("connection cleanup closed an already released authentication context")
	}
}

func TestAuthorityBookkeepingDoesNotAccumulateRetiredExports(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeDisconnect, tr.id, wire.EmptyResponseBody()); status != statusOK {
		t.Fatalf("initial disconnect = %x", status)
	}
	c.server.config.Limits.MaxTrees = 2
	var authorities []*cleanupAuthority
	backend := &cleanupBackend{WindowsStorage: tr.export.share.Backend, newSession: func() storage.WindowsSession {
		a := &cleanupAuthority{WindowsSession: &backendSession{commandSession: &commandSession{}}}
		authorities = append(authorities, a)
		return a
	}}
	export := tr.export
	export.share.Backend = backend
	for iteration := range 8 {
		if iteration != 0 {
			var err error
			export, err = c.server.Publish(Share{Name: "work", Volume: "trusted", Backend: backend, Changes: testNotifySource(testNotifyStream())})
			if err != nil {
				t.Fatal(err)
			}
		}
		header, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeConnect, 0, cleanupTreeConnectBody())
		if status != statusOK {
			t.Fatalf("tree connect after %d retired exports = %x", iteration, status)
		}
		if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeDisconnect, header.TreeID, wire.EmptyResponseBody()); status != statusOK {
			t.Fatalf("tree disconnect = %x", status)
		}
		if err := export.Unpublish(t.Context()); err != nil {
			t.Fatalf("unpublish iteration %d: %v", iteration, err)
		}
		s.mu.Lock()
		remaining := len(s.authorities)
		s.mu.Unlock()
		if remaining != 0 || authorities[iteration].closes.Load() != 1 {
			t.Fatalf("iteration %d retained %d authorities, close count %d", iteration, remaining, authorities[iteration].closes.Load())
		}
	}
	if backend.newCalls.Load() != 8 {
		t.Fatalf("new authority count = %d, want 8", backend.newCalls.Load())
	}
}

func TestDisconnectedAuthorityCleanupFailureRemainsRetryable(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeDisconnect, tr.id, wire.EmptyResponseBody()); status != statusOK {
		t.Fatalf("initial disconnect = %x", status)
	}
	a := &cleanupAuthority{statusErr: syscall.EIO}
	a.closeFails.Store(true)
	backend := &cleanupBackend{WindowsStorage: tr.export.share.Backend, newSession: func() storage.WindowsSession { return a }}
	tr.export.share.Backend = backend
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeConnect, 0, cleanupTreeConnectBody()); status != statusIO {
		t.Fatalf("failed admission = %x", status)
	}
	c.cleanup()
	c.mu.Lock()
	retainedSession := c.sessions[s.id] == s
	c.mu.Unlock()
	c.server.mu.Lock()
	_, retainedConnection := c.server.connections[c]
	refs := tr.export.refs
	c.server.mu.Unlock()
	if !retainedSession || !retainedConnection || refs != 1 || a.closes.Load() < 2 {
		t.Fatalf("disconnect lost unresolved ownership: session=%v connection=%v refs=%d closes=%d", retainedSession, retainedConnection, refs, a.closes.Load())
	}
	attempts := a.closes.Load()
	a.closeFails.Store(false)
	if err := tr.export.Unpublish(t.Context()); err != nil {
		t.Fatalf("unpublish could not recover disconnected orphan: %v", err)
	}
	c.mu.Lock()
	remaining := len(c.sessions)
	c.mu.Unlock()
	c.server.mu.Lock()
	_, retainedConnection = c.server.connections[c]
	refs = tr.export.refs
	c.server.mu.Unlock()
	if remaining != 0 || retainedConnection || refs != 0 || a.closes.Load() != attempts+1 {
		t.Fatalf("retry retained ownership: sessions=%d connection=%v refs=%d closes=%d", remaining, retainedConnection, refs, a.closes.Load())
	}
}

type cleanupDrainSession struct {
	storage.WindowsSession
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *cleanupDrainSession) Close(ctx context.Context) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return s.WindowsSession.Close(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestConcurrentLogoffRetainsSignerUntilBothFramesRetire(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	key := s.signer
	backend := &cleanupDrainSession{WindowsSession: tr.session, entered: make(chan struct{}), release: make(chan struct{})}
	tr.session = backend
	var once sync.Once
	unblock := func() { once.Do(func() { close(backend.release) }) }
	t.Cleanup(unblock)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	readContext, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	c.mu.Lock()
	c.pending[100] = &pendingRequest{frame: 100, command: wire.Logoff, ctx: ctx, cancel: func() {}, sessionID: s.id}
	c.pending[200] = &pendingRequest{frame: 200, command: wire.Read, ctx: readContext, cancel: cancelRead, sessionID: s.id}
	c.pending[201] = &pendingRequest{frame: 200, command: wire.Logoff, ctx: ctx, cancel: func() {}, sessionID: s.id}
	c.mu.Unlock()
	first := make(chan error, 1)
	go func() {
		first <- c.logoff(context.WithValue(ctx, pendingFrameKey{}, requestFrame{connection: c, id: 100}), s)
	}()
	select {
	case <-backend.entered:
	case <-ctx.Done():
		t.Fatal("logoff waited for another compound frame before closing its authority")
	}
	select {
	case <-readContext.Done():
	default:
		t.Fatal("logoff did not cancel the other frame's read")
	}
	second, started := make(chan error, 1), make(chan struct{})
	go func() {
		close(started)
		second <- c.logoff(context.WithValue(ctx, pendingFrameKey{}, requestFrame{connection: c, id: 200}), s)
	}()
	<-started
	unblock()
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first logoff waited on the unfinished second frame")
	}
	select {
	case err := <-second:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("second logoff outcome = %v, want retired session", err)
		}
	case <-ctx.Done():
		t.Fatal("second logoff deadlocked behind the first")
	}
	packet := wire.EncodeResponse(wire.Header{Command: wire.Logoff, SessionID: s.id, MessageID: 100}, wire.EmptyResponseBody())
	if err := key.Sign(packet); err != nil {
		t.Fatalf("key destroyed before the logoff response was signed: %v", err)
	}
	c.retireRequests([]wire.Request{{Header: wire.Header{MessageID: 100}}})
	if err := key.Verify(packet); err != nil {
		t.Fatalf("key destroyed while the earlier READ/LOGOFF frame remained pending: %v", err)
	}
	c.mu.Lock()
	retained := c.sessions[s.id] == s
	c.mu.Unlock()
	if !retained {
		t.Fatal("signing owner vanished before its last original frame retired")
	}
	c.retireRequests([]wire.Request{{Header: wire.Header{MessageID: 200}}, {Header: wire.Header{MessageID: 201}}})
	if err := key.Sign(packet); !errors.Is(err, signing.ErrDestroyed) {
		t.Fatalf("last frame retirement did not destroy the key: %v", err)
	}
	c.mu.Lock()
	remaining := len(c.sessions)
	c.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("retired session remained after all frames finished: %d", remaining)
	}
}

func TestShutdownRetriesDisconnectedAuthorityOwnership(t *testing.T) {
	c, s, tr, _, _, stream := testConnection(t)
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeDisconnect, tr.id, wire.EmptyResponseBody()); status != statusOK {
		t.Fatalf("initial disconnect = %x", status)
	}
	a := &cleanupAuthority{statusErr: syscall.EIO}
	a.closeFails.Store(true)
	backend := &cleanupBackend{WindowsStorage: tr.export.share.Backend, newSession: func() storage.WindowsSession { return a }}
	tr.export.share.Backend = backend
	if _, status := cleanupDispatch(t, t.Context(), c, s, wire.TreeConnect, 0, cleanupTreeConnectBody()); status != statusIO {
		t.Fatalf("failed admission = %x", status)
	}
	t.Cleanup(func() {
		a.closeFails.Store(false)
		_ = tr.export.Unpublish(context.Background())
	})
	c.cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := c.server.Shutdown(ctx); !errors.Is(err, syscall.EIO) {
		t.Fatalf("first shutdown hid failed ownership cleanup: %v", err)
	}
	first := c.server.Status()
	if !first.Stopped || first.RetainedConnections != 1 || first.CleanupFailures == 0 {
		t.Fatalf("first shutdown lost its retryable connection: %+v", first)
	}
	attempts := a.closes.Load()
	a.closeFails.Store(false)
	if err := c.server.Shutdown(ctx); !errors.Is(err, syscall.EIO) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown retry lost historical failure or failed to finish: %v", err)
	}
	last := c.server.Status()
	if last.RetainedConnections != 0 || last.Connections != 0 || a.closes.Load() != attempts+1 {
		t.Fatalf("shutdown did not retry ownership: status=%+v closes=%d, previous=%d", last, a.closes.Load(), attempts)
	}
	c.mu.Lock()
	remaining := len(c.sessions)
	c.mu.Unlock()
	c.server.mu.Lock()
	refs := tr.export.refs
	c.server.mu.Unlock()
	if remaining != 0 || refs != 0 {
		t.Fatalf("shutdown retry retained sessions=%d refs=%d", remaining, refs)
	}
	select {
	case <-stream.done:
	default:
		t.Fatal("shutdown retry retained the export notification stream")
	}
}
