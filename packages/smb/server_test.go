package smb

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type endpointAuthenticator struct{ authentication Authentication }

func (a endpointAuthenticator) Begin(context.Context) (Authentication, error) {
	if a.authentication == nil {
		return &endpointAuthentication{}, nil
	}
	return a.authentication, nil
}

type endpointAuthentication struct {
	mu       sync.Mutex
	closed   bool
	closeErr error
	closes   int
}

func (a *endpointAuthentication) Step(context.Context, []byte) (AuthenticationResult, error) {
	return AuthenticationResult{}, errors.New("unused authentication")
}

func (a *endpointAuthentication) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closes++
	if a.closeErr != nil {
		return a.closeErr
	}
	a.closed = true
	return nil
}

type endpointStorage struct {
	checkErr     error
	session      *endpointFileSession
	sessionOpens atomic.Int32
	dataCalls    atomic.Int32
}

func (s *endpointStorage) Stat(context.Context, string) (storage.Attr, error) {
	s.dataCalls.Add(1)
	return storage.Attr{}, syscall.ENOSYS
}
func (s *endpointStorage) SetAttr(context.Context, string, storage.AttrChange) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) List(context.Context, string) ([]storage.Entry, error) {
	s.dataCalls.Add(1)
	return nil, syscall.ENOSYS
}
func (s *endpointStorage) Read(context.Context, string) ([]byte, error) {
	s.dataCalls.Add(1)
	return nil, syscall.ENOSYS
}
func (s *endpointStorage) Write(context.Context, string, []byte) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) Create(context.Context, string) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) Mkdir(context.Context, string) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) Remove(context.Context, string) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) RemoveDir(context.Context, string) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) Rename(context.Context, string, string) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) Space(context.Context) (storage.Space, error) {
	s.dataCalls.Add(1)
	return storage.Space{}, syscall.ENOSYS
}
func (s *endpointStorage) CheckBounded() error { return s.checkErr }
func (s *endpointStorage) ListBounded(context.Context, string, *storage.ListResult) error {
	s.dataCalls.Add(1)
	return syscall.ENOSYS
}
func (s *endpointStorage) ReadBounded(context.Context, string, int64) ([]byte, error) {
	s.dataCalls.Add(1)
	return nil, syscall.ENOSYS
}
func (s *endpointStorage) CheckFileStorage() error { return s.checkErr }
func (s *endpointStorage) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	s.sessionOpens.Add(1)
	if s.session == nil {
		s.session = newEndpointFileSession()
	}
	return s.session, nil
}

type endpointFileSession struct {
	mu           sync.Mutex
	status       storage.FileSessionStatus
	renewFn      func(context.Context, int) (storage.FileSessionStatus, error)
	closeErr     error
	renewals     int
	closes       int
	closeEntered chan struct{}
	closeRelease chan struct{}
}

func newEndpointFileSession() *endpointFileSession {
	return &endpointFileSession{status: storage.FileSessionStatus{
		Epoch: "endpoint", Remaining: time.Minute, Revision: 1, ActionEpoch: 1,
		HistoryRemaining: time.Minute,
	}}
}
func (*endpointFileSession) OpenFile(context.Context, string, storage.FileOpenOptions) (storage.File, error) {
	return nil, syscall.ENOSYS
}
func (*endpointFileSession) OpenNode(context.Context, uint64, storage.FileOpenOptions) (storage.File, error) {
	return nil, syscall.ENOSYS
}
func (*endpointFileSession) StatNode(context.Context, uint64) (storage.Attr, error) {
	return storage.Attr{}, syscall.ENOSYS
}
func (*endpointFileSession) SetNodeAttr(context.Context, uint64, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.ENOSYS
}
func (s *endpointFileSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.renewFn != nil {
		return s.renewFn(ctx, s.renewals)
	}
	s.status.Revision++
	return s.status, nil
}
func (s *endpointFileSession) Status(context.Context) (storage.FileSessionStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, nil
}
func (s *endpointFileSession) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closes++
	entered, release, err := s.closeEntered, s.closeRelease, s.closeErr
	s.mu.Unlock()
	if entered != nil {
		select {
		case <-entered:
		default:
			close(entered)
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}
func (s *endpointFileSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	err := s.Close(ctx)
	return storage.ReferenceCloseResult{Released: err == nil}, err
}

func endpointConfig() Config {
	return Config{
		Authenticator:     endpointAuthenticator{},
		AuthorizeIdentity: IdentityAuthorizerFunc(func(context.Context, Principal) error { return nil }),
		Authorize:         authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }),
		Limits:            DefaultLimits(),
	}
}

func TestEndpointConfigurationAndExportReservation(t *testing.T) {
	config := endpointConfig()
	if _, err := New(config); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"authenticator":       func(c *Config) { c.Authenticator = nil },
		"identity authorizer": func(c *Config) { c.AuthorizeIdentity = nil },
		"authorizer":          func(c *Config) { c.Authorize = nil },
		"frame":               func(c *Config) { c.Limits.MaxFrameBytes = c.Limits.MaxIOBytes },
		"sessions":            func(c *Config) { c.Limits.MaxSessions = 0 },
		"timeout":             func(c *Config) { c.Limits.CleanupTimeout = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			broken := config
			change(&broken)
			if _, err := New(broken); !errors.Is(err, ErrConfig) {
				t.Fatalf("New error = %v", err)
			}
		})
	}

	config.Limits.MaxExports = 1
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	backend := &endpointStorage{}
	export, err := server.Publish(Share{Name: "Volume", Volume: "volume-a", Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Publish(Share{Name: "Other", Volume: "volume-b", Backend: &endpointStorage{}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("second export = %v", err)
	}
	if _, err := server.Publish(Share{Name: "volume", Volume: "volume-c", Backend: &endpointStorage{}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("case-equivalent export = %v", err)
	}
	if err := export.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := server.Status(); status.Exports != 0 {
		t.Fatalf("status after unpublish = %+v", status)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPublishFailureDoesNotConsumeCapacity(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxExports = 1
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("capability chain")
	if _, err := server.Publish(Share{Name: "bad", Volume: "bad", Backend: &endpointStorage{checkErr: cause}}); !errors.Is(err, cause) {
		t.Fatalf("failed publish = %v", err)
	}
	if _, err := server.Publish(Share{Name: "good", Volume: "good", Backend: &endpointStorage{}}); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownRemovesQuiescentExportsWithoutTakingBackendOwnership(t *testing.T) {
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	backend := &endpointStorage{}
	if _, err := server.Publish(Share{Name: "data", Volume: "volume", Backend: backend}); err != nil {
		t.Fatal(err)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := server.Status(); !status.Stopped || status.Exports != 0 || status.StoppingExports != 0 {
		t.Fatalf("shutdown status = %+v", status)
	}
	if backend.sessionOpens.Load() != 0 {
		t.Fatal("quiescent export shutdown opened or closed a backend session")
	}
}

func TestServeAcceptsOnlyLoopbackAndStopsIncompleteConnections(t *testing.T) {
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for server.Status().Connections != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.Status().Connections != 1 {
		t.Fatal("accepted connection was not registered")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop")
	}
	_ = connection.Close()
	if status := server.Status(); !status.Stopped || status.Connections != 0 {
		t.Fatalf("stopped status = %+v", status)
	}
}

func TestConnectionLimitRejectsMaxPlusOneAndRecovers(t *testing.T) {
	config := endpointConfig()
	config.Limits.MaxConnections = 1
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	first, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	waitConnections := func(want int) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for server.Status().Connections != want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if got := server.Status().Connections; got != want {
			t.Fatalf("connections = %d, want %d", got, want)
		}
	}
	waitConnections(1)
	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		_ = first.Close()
		t.Fatal(err)
	}
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := second.Read(one[:]); err == nil {
		t.Fatal("connection max+1 remained open")
	}
	_ = second.Close()
	waitConnections(1)
	_ = first.Close()
	waitConnections(0)
	third, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	waitConnections(1)
	_ = third.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop")
	}
}

type nonTCPListener struct{ net.Listener }

func (nonTCPListener) Addr() net.Addr { return endpointAddr("local") }

type endpointAddr string

func (a endpointAddr) Network() string { return "test" }
func (a endpointAddr) String() string  { return string(a) }

func TestRejectedListenerRemainsCallerOwned(t *testing.T) {
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := server.Serve(t.Context(), nonTCPListener{Listener: listener}); !errors.Is(err, ErrConfig) {
		t.Fatalf("Serve error = %v", err)
	}
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("rejected listener was closed: %v", err)
	}
	_ = connection.Close()
}
