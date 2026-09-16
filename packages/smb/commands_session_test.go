package smb

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/signing"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type sessionBackend struct {
	windowsBackend
	session  windowsSession
	stateErr error
}

func (b *sessionBackend) Check() error { return nil }
func (b *sessionBackend) State(context.Context) (windowsState, error) {
	return windowsState{RootID: 1, MaxEventBytes: 1024, VolumeIdentity: "authority:volume", VolumeSerial: 0x123456789}, b.stateErr
}
func (b *sessionBackend) NewSession(context.Context, storage.FileSessionOptions) (windowsSession, error) {
	return b.session, nil
}

type sessionFile struct {
	*commandFile
	mu      sync.Mutex
	batch   windowsLockBatch
	pending bool
}

func (f *sessionFile) LockBatch(ctx context.Context, b windowsLockBatch, id windowsActionID) (windowsActionResult, error) {
	f.mu.Lock()
	f.batch = b
	pending := f.pending
	f.mu.Unlock()
	if pending {
		if notify, ok := ctx.Value(rangePendingKey{}).(func() error); ok {
			if err := notify(); err != nil {
				return windowsActionResult{}, err
			}
		}
		<-ctx.Done()
		return windowsActionResult{}, syscall.EINTR
	}
	return windowsActionResult{Action: id, State: windowsActionCompleted}, nil
}

type backendSession struct {
	*commandSession
	cancelState windowsActionState
	cancelErr   error
	renewErr    error
}

func (s *backendSession) CancelAction(_ context.Context, id windowsActionID) (windowsActionResult, error) {
	return windowsActionResult{Action: id, State: s.cancelState}, s.cancelErr
}
func (s *backendSession) Renew(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{ActionEpoch: 1, Remaining: time.Minute}, s.renewErr
}

func testConnection(t *testing.T) (*connection, *session, *tree, *sessionFile, *backendSession, *notifyTestStream) {
	t.Helper()
	s, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	c := newConnection(s, left)
	c.negotiated = true
	s.connections[c] = struct{}{}
	t.Cleanup(func() {
		c.cleanup()
		s.mu.Lock()
		delete(s.connections, c)
		s.mu.Unlock()
		_ = s.Shutdown(context.Background())
	})
	f := &sessionFile{commandFile: &commandFile{attr: windowsAttr{windowsBasicAttr: windowsBasicAttr{Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory, Size: 3}}, NameInfo: windowsNameInfo{State: windowsNameRoot}}, data: []byte("abc"), result: windowsActionResult{State: windowsActionCompleted}}}
	ws := &backendSession{commandSession: &commandSession{}, cancelState: windowsActionCancelled}
	ws.open = func(windowsOpenRequest) (windowsOpenResult, error) {
		return windowsOpenResult{File: f, Attr: f.attr, CreateAction: windowsOpened}, nil
	}
	stream := testNotifyStream()
	e, err := s.publish(Share{Name: "work", Volume: "trusted", Changes: testNotifySource(stream)}, &sessionBackend{session: ws})
	if err != nil {
		t.Fatal(err)
	}
	key, err := signing.NewSession([64]byte{}, []byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	ss := &session{principal: Principal{SID: "S-1-5-21-1"}, signer: key, trees: make(map[uint32]*tree)}
	if err := s.sessions.add(c, ss); err != nil {
		t.Fatal(err)
	}
	c.sessions[ss.id] = ss
	c.nextTree = 1
	e.refs = 1
	e.opens = 1
	tr := &tree{id: 1, sessionID: 1, export: e, session: ws, files: newFileDispatcher(e.backend, ws, 1, s.config.Limits)}
	ss.trees[1] = tr
	tr.files.onOpen = func(delta int) { s.mu.Lock(); e.opens += delta; s.mu.Unlock() }
	tr.files.onUncertain = s.unconfirmedMutation
	tr.files.onFence = func(delta int) { s.mu.Lock(); s.fencedTrees += delta; s.mu.Unlock() }
	tr.files.handles[wire.FileID{1}] = &fileHandle{identity: notificationIdentity{ID: 1, Directory: true}, file: f, access: 0x1201ff}
	return c, ss, tr, f, ws, stream
}

func signedRequest(t *testing.T, s *session, r wire.Request) wire.Request {
	t.Helper()
	r.Header.SessionID = s.id
	r.Header.TreeID = 1
	r.Header.MessageID = 10
	r.Packet = requestPacket(r.Header, r.Body)
	if err := s.signer.Sign(r.Packet); err != nil {
		t.Fatal(err)
	}
	r.Header, _ = wire.ParseHeader(r.Packet)
	r.Body = r.Packet[64:]
	return r
}

func TestAuthorizedDispatchAndExportIsolation(t *testing.T) {
	c, s, tr, _, _, _ := testConnection(t)
	if err := tr.export.Unpublish(context.Background()); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	r := signedRequest(t, s, readCommand(wire.FileID{1}, 0, 1))
	h := r.Header
	body, status, _ := c.dispatch(context.Background(), r, r, &h)
	if status != 0 || string(body[16:]) != "a" {
		t.Fatalf("read %x %q", status, body)
	}
	c.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, a authz.AccessRequest) error {
		p, ok := PrincipalFromContext(ctx)
		if !ok || p.SID != "S-1-5-21-1" || a.Volume != "trusted" {
			t.Fatal("lost identity binding")
		}
		return authz.ErrDenied
	})
	_, status, _ = c.dispatch(context.Background(), r, r, &h)
	if status != statusDenied {
		t.Fatalf("revoked %x", status)
	}
	c.server.config.Authorize = testConfig().Authorize
	r = signedRequest(t, s, fileRequest(wire.TreeDisconnect, wire.EmptyResponseBody()))
	h = r.Header
	_, status, _ = c.dispatch(context.Background(), r, r, &h)
	if status != 0 {
		t.Fatalf("disconnect %x", status)
	}
	if err := tr.export.Unpublish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tr.export.Unpublish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.server.publish(Share{Name: "work", Volume: "v", Changes: testNotifySource(testNotifyStream())}, &sessionBackend{}); err != nil {
		t.Fatal(err)
	}
}

func TestTreeConnectBindsConfiguredVolume(t *testing.T) {
	c, s, _, _, _, _ := testConnection(t)
	name := wire.EncodeUTF16("\\\\127.0.0.1\\work")
	b := make([]byte, 8+len(name))
	smbLE.PutUint16(b, 9)
	smbLE.PutUint16(b[4:], 72)
	smbLE.PutUint16(b[6:], uint16(len(name)))
	copy(b[8:], name)
	r := signedRequest(t, s, fileRequest(wire.TreeConnect, b))
	h := r.Header
	body, status, _ := c.dispatch(context.Background(), r, r, &h)
	if status != 0 || len(body) != 16 {
		t.Fatalf("tree %x", status)
	}
	c.server.config.Limits.MaxTrees = 1
	_, status, _ = c.dispatch(context.Background(), r, r, &h)
	if status != statusResources {
		t.Fatalf("tree capacity %x", status)
	}
}

func TestLockCancellationReconcilesAuthority(t *testing.T) {
	c, _, tr, f, ws, _ := testConnection(t)
	b := make([]byte, 48)
	smbLE.PutUint16(b, 48)
	smbLE.PutUint16(b[2:], 1)
	b[8] = 1
	smbLE.PutUint64(b[32:], 10)
	smbLE.PutUint32(b[40:], wire.LockExclusive)
	r := fileRequest(wire.Lock, b)
	if _, status := c.lock(context.Background(), tr, r, nil); status != 0 {
		t.Fatalf("grant %x", status)
	}
	if f.batch.Ranges[0].Type != lockExclusive || f.batch.Ranges[0].Length != 10 {
		t.Fatal(f.batch)
	}
	f.pending = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, status := c.lock(ctx, tr, r, nil); status != statusCancelled {
		t.Fatalf("cancel %x", status)
	}
	ws.cancelState = windowsActionCompleted
	if _, status := c.lock(ctx, tr, r, nil); status != 0 {
		t.Fatalf("grant won cancellation %x", status)
	}
	ws.cancelErr = syscall.EIO
	ws.queryErr = syscall.EIO
	if _, status := c.lock(ctx, tr, r, nil); status != statusIO {
		t.Fatalf("unknown cancel %x", status)
	}
	if tr.files.get(wire.FileID{1}) != nil {
		t.Fatal("unknown lock did not fence its authority session")
	}
}

func TestNotifyCommandConfirmsRegistrationBeforePending(t *testing.T) {
	c, s, tr, _, _, stream := testConnection(t)
	b := make([]byte, 32)
	smbLE.PutUint16(b, 32)
	smbLE.PutUint32(b[4:], 1024)
	b[8] = 1
	smbLE.PutUint32(b[24:], 1)
	r := fileRequest(wire.ChangeNotify, b)
	registered := make(chan struct{})
	ctx := context.WithValue(WithPrincipal(context.Background(), s.principal), pendingKey{}, func(*signing.Session) error { close(registered); return nil })
	done := make(chan uint32, 1)
	go func() {
		body, status := c.notify(ctx, tr, r, s.signer)
		if status == 0 && len(body) < 8 {
			status = statusIO
		}
		done <- status
	}()
	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatal("not registered")
	}
	stream.changes <- namedChange(1, 1, "x", 0)
	select {
	case status := <-done:
		if status != 0 {
			t.Fatalf("notify %x", status)
		}
	case <-time.After(time.Second):
		t.Fatal("notification stalled")
	}
}

func TestNotifyUsesListDirectoryGrantWithoutRequiringAttributes(t *testing.T) {
	c, s, tr, f, _, stream := testConnection(t)
	h := tr.files.handles[wire.FileID{1}]
	restricted := &deniedStatFile{commandFile: f.commandFile}
	h.file = restricted
	h.access = 1
	body := make([]byte, 32)
	smbLE.PutUint16(body, 32)
	smbLE.PutUint32(body[4:], 1024)
	body[8] = 1
	smbLE.PutUint32(body[24:], 1)
	r := fileRequest(wire.ChangeNotify, body)
	registered := make(chan struct{})
	ctx := context.WithValue(context.Background(), pendingKey{}, func(*signing.Session) error { close(registered); return nil })
	done := make(chan uint32, 1)
	go func() { _, status := c.notify(ctx, tr, r, s.signer); done <- status }()
	<-registered
	stream.changes <- namedChange(1, 1, "file", 0)
	if status := <-done; status != 0 || restricted.statCalls != 0 {
		t.Fatalf("LIST_DIRECTORY notify %x statcalls%d", status, restricted.statCalls)
	}
	h.access = 0x80
	if _, status := c.notify(context.Background(), tr, r, s.signer); status != statusDenied {
		t.Fatalf("READ_ATTRIBUTES-only notify = %x", status)
	}
}

func TestNotifyRejectsADeadRetainedReferenceBeforeRegistration(t *testing.T) {
	c, s, tr, f, _, _ := testConnection(t)
	f.readErr = syscall.ESTALE
	body := make([]byte, 32)
	smbLE.PutUint16(body, 32)
	smbLE.PutUint32(body[4:], 1024)
	body[8] = 1
	smbLE.PutUint32(body[24:], 1)
	if _, status := c.notify(context.Background(), tr, fileRequest(wire.ChangeNotify, body), s.signer); status != statusIO {
		t.Fatalf("dead reference = %x", status)
	}
	tr.export.changes.mu.Lock()
	defer tr.export.changes.mu.Unlock()
	if len(tr.export.changes.watchers) != 0 {
		t.Fatal("dead reference established a watcher")
	}
}
