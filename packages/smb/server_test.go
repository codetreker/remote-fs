package smb

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
)

type endpointCheckStorage struct {
	storage.FileStorage
	check func() error
}

func (s *endpointCheckStorage) CheckFileStorage() error {
	if s.check != nil {
		return s.check()
	}
	return nil
}
func TestEndpointConfiguration(t *testing.T) {
	for _, change := range []func(*Config){func(c *Config) { c.Authenticator = nil }, func(c *Config) { c.Authorize = nil }, func(c *Config) { c.Limits.MaxExports = 0 }, func(c *Config) { c.Limits.MaxConnections = 0 }, func(c *Config) { c.Limits.MaxSessions = 0 }, func(c *Config) { c.Limits.MaxTrees = 0 }, func(c *Config) { c.Limits.MaxOpens = 0 }, func(c *Config) { c.Limits.MaxRequests = 65536 }, func(c *Config) { c.Limits.MaxCompound = 129 }, func(c *Config) { c.Limits.MaxContexts = 0 }, func(c *Config) { c.Limits.MaxIOBytes = 65535 }, func(c *Config) { c.Limits.MaxFrameBytes = 1 }, func(c *Config) { c.Limits.MaxTokenBytes = 65536 }, func(c *Config) { c.Limits.MaxDirectoryBytes = 0 }, func(c *Config) { c.Limits.MaxNotifyBytes = 0 }, func(c *Config) { c.Limits.MaxNotifyEvents = 0 }, func(c *Config) { c.Limits.HandshakeTimeout = 0 }, func(c *Config) { c.Limits.RequestTimeout = 0 }, func(c *Config) { c.Limits.CleanupTimeout = 0 }, func(c *Config) { c.Limits.FileSession.Lease = 0 }} {
		c := testConfig()
		change(&c)
		if s, err := New(c); err == nil || s != nil {
			t.Fatalf("invalid config accepted %+v", c.Limits)
		}
	}
	for _, name := range []string{"", " ", "a/b", "a\\b", "a?b", string([]byte{255})} {
		if _, err := shareKey(name); err == nil {
			t.Fatalf("invalid share %q", name)
		}
	}
	if key, err := shareKey("Work"); err != nil || key != "WORK" {
		t.Fatal(key, err)
	}
}
func TestEndpointExportReservationAndIdentity(t *testing.T) {
	config := testConfig()
	config.Limits.MaxExports = 1
	s, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("check failed")
	backend := &endpointCheckStorage{check: func() error { close(entered); <-release; return failure }}
	result := make(chan error, 1)
	go func() { _, err := s.Publish(Share{Name: "one", Volume: "v", Backend: backend}); result <- err }()
	<-entered
	if _, err := s.Publish(Share{Name: "two", Volume: "v", Backend: &endpointCheckStorage{}}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; err != failure {
		t.Fatal(err)
	}
	if s.Status().Exports != 0 {
		t.Fatal("failed publication retained slot")
	}
	for _, share := range []Share{{Name: "IPC$", Volume: "v", Backend: &endpointCheckStorage{}}, {Name: "x", Backend: &endpointCheckStorage{}}, {Name: "x", Volume: "v"}} {
		if _, err := s.Publish(share); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	}
	e, err := s.Publish(Share{Name: "one", Volume: "v", Backend: &endpointCheckStorage{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(e.share); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := e.Unpublish(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.mu.Lock()
	e.opens = 1
	s.mu.Unlock()
	if err := e.Unpublish(t.Context()); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	s.mu.Lock()
	e.opens = 0
	e.active = 1
	s.mu.Unlock()
	if err := e.Unpublish(t.Context()); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	s.mu.Lock()
	e.active = 0
	s.mu.Unlock()
	if err := e.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	next, err := s.Publish(e.share)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Unpublish(t.Context()); err != nil || s.exports["ONE"] != next {
		t.Fatal("old export removed replacement", err)
	}
	if err := s.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !s.Status().Stopped {
		t.Fatal(s.Status())
	}
	if _, err := s.Publish(Share{Name: "late", Volume: "v", Backend: &endpointCheckStorage{}}); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := s.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type endpointListener struct {
	addr    net.Addr
	failure error
	closed  atomic.Bool
}

func (l *endpointListener) Addr() net.Addr            { return l.addr }
func (l *endpointListener) Accept() (net.Conn, error) { return nil, l.failure }
func (l *endpointListener) Close() error              { l.closed.Store(true); return nil }
func TestEndpointListenerOwnership(t *testing.T) {
	s, _ := New(testConfig())
	if err := s.Serve(t.Context(), nil); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	bad := &endpointListener{addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 445}}
	if err := s.Serve(t.Context(), bad); !errors.Is(err, ErrConfig) || bad.closed.Load() {
		t.Fatal(err)
	}
	failure := errors.New("accept failure")
	l := &endpointListener{addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, failure: failure}
	if err := s.Serve(t.Context(), l); !errors.Is(err, failure) || !l.closed.Load() {
		t.Fatal(err)
	}
	next := &endpointListener{addr: l.addr, failure: syscall.EIO}
	if err := s.Serve(t.Context(), next); !errors.Is(err, ErrStopped) || next.closed.Load() {
		t.Fatal(err)
	}
	if !s.Status().Stopped {
		t.Fatal(s.Status())
	}
}

func TestEndpointUnpublishRetainsFailedIdleTreeCleanup(t *testing.T) {
	raw := newAuthorityTestSession()
	var fail atomic.Bool
	fail.Store(true)
	failure := errors.New("session close failed")
	raw.closeFn = func(context.Context, int32) error {
		if fail.Load() {
			return failure
		}
		return nil
	}
	c, s, e := authorityTestConnection(t, &authorityTestBackend{raw: raw}, nil)
	authorityTestConnect(t, c, s, e)
	authorityTestConnect(t, c, s, e)
	if err := e.Unpublish(t.Context()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	state := c.server.Status()
	if state.Exports != 1 || state.Trees != 1 || state.Sessions != 1 || state.FencedAuthorities != 1 {
		t.Fatalf("lost pending owner: %+v", state)
	}
	if _, err := c.server.Publish(e.share); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	fail.Store(false)
	if err := e.Unpublish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if raw.closes.Load() != 2 || c.server.Status().Exports != 0 || c.server.Status().Trees != 0 {
		t.Fatalf("cleanup did not settle: calls=%d state=%+v", raw.closes.Load(), c.server.Status())
	}
	if err := c.logoff(WithPrincipal(t.Context(), s.principal), s); err != nil {
		t.Fatal(err)
	}
	if c.server.Status().Sessions != 0 {
		t.Fatal(c.server.Status())
	}
}

func TestEndpointShutdownReturnsCurrentCleanupAttempt(t *testing.T) {
	raw := newAuthorityTestSession()
	var fail atomic.Bool
	fail.Store(true)
	failure := errors.New("cleanup not confirmed")
	raw.closeFn = func(context.Context, int32) error {
		if fail.Load() {
			return failure
		}
		return nil
	}
	c, s, e := authorityTestConnection(t, &authorityTestBackend{raw: raw}, nil)
	authorityTestConnect(t, c, s, e)
	c.mu.Lock()
	c.disconnected = true
	c.mu.Unlock()
	first := c.server.Shutdown(t.Context())
	if !errors.Is(first, failure) {
		t.Fatal(first)
	}
	state := c.server.Status()
	if state.Sessions != 1 || state.Connections != 1 || state.Trees != 1 || state.CleanupFailures == 0 || e.refs == 0 {
		t.Fatalf("failed cleanup lost owner: %+v", state)
	}
	fail.Store(false)
	if err := c.server.Shutdown(t.Context()); err != nil {
		t.Fatalf("confirmed retry: %v", err)
	}
	state = c.server.Status()
	if state.Sessions != 0 || state.Connections != 0 || state.Trees != 0 || e.refs != 0 || state.CleanupFailures == 0 {
		t.Fatalf("settlement: %+v refs%d", state, e.refs)
	}
	if !errors.Is(first, failure) {
		t.Fatal("retry overwrote prior attempt")
	}
}
