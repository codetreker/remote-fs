package smb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"
)

type Server struct {
	config Config

	mu              sync.Mutex
	exports         map[string]*Export
	connections     map[*connection]struct{}
	sessions        *sessionRegistry
	listener        net.Listener
	started         bool
	stopping        bool
	stopped         bool
	done            chan struct{}
	doneOnce        sync.Once
	wg              sync.WaitGroup
	cleanupMu       cleanupGate
	cleanupErr      error
	cleanupFailures int
	guid            [16]byte
}

type Export struct {
	server    *Server
	share     Share
	key       string
	refs      int
	trees     int
	active    int
	stopping  bool
	published bool
	cleanupMu cleanupGate
}

func interfaceNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// New validates all dependencies and bounds without acquiring credentials or
// starting background work.
func New(config Config) (*Server, error) {
	if interfaceNil(config.Authenticator) || interfaceNil(config.AuthorizeIdentity) || interfaceNil(config.Authorize) {
		return nil, ErrConfig
	}
	if err := config.Limits.check(); err != nil {
		return nil, fmt.Errorf("SMB limits: %w", err)
	}
	s := &Server{
		config: config, exports: make(map[string]*Export),
		connections: make(map[*connection]struct{}), sessions: newSessionRegistry(),
		done: make(chan struct{}),
	}
	if _, err := rand.Read(s.guid[:]); err != nil {
		return nil, fmt.Errorf("SMB server identity: %w", err)
	}
	return s, nil
}

func shareKey(name string) (string, error) {
	if name == "" || len(name) > 80 || !utf8.ValidString(name) ||
		strings.ContainsAny(name, "\\/\x00\"[]:|<>+=;,?*") || strings.TrimSpace(name) != name {
		return "", ErrConfig
	}
	return strings.ToUpper(name), nil
}

// Publish reserves export capacity and validates the retained-file chain. It
// acquires no file session and never takes ownership of the backend.
func (s *Server) Publish(share Share) (*Export, error) {
	key, err := shareKey(share.Name)
	if err != nil || key == "IPC$" || share.Volume == "" || interfaceNil(share.Backend) {
		return nil, ErrConfig
	}
	e := &Export{server: s, share: share, key: key, active: 1}
	s.mu.Lock()
	if s.stopping || s.stopped {
		s.mu.Unlock()
		return nil, ErrStopped
	}
	if len(s.exports) >= s.config.Limits.MaxExports || s.exports[key] != nil {
		s.mu.Unlock()
		return nil, ErrBusy
	}
	s.exports[key] = e
	s.mu.Unlock()

	err = share.Backend.CheckFileStorage()
	s.mu.Lock()
	e.active--
	if err == nil && (s.stopping || e.stopping) {
		err = ErrStopped
	}
	if err != nil && s.exports[key] == e {
		delete(s.exports, key)
	}
	if err == nil {
		e.published = true
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Unpublish is all-or-nothing for a live export. Once cleanup starts, failed
// cleanup remains owned by the Export and can be retried by another call.
func (e *Export) Unpublish(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := e.server
	s.mu.Lock()
	if s.exports[e.key] != e {
		s.mu.Unlock()
		return nil
	}
	if e.trees != 0 || e.active != 0 {
		s.mu.Unlock()
		return ErrBusy
	}
	e.stopping = true
	s.mu.Unlock()
	if err := e.close(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	if s.exports[e.key] == e && e.refs == 0 && e.active == 0 {
		delete(s.exports, e.key)
	}
	s.mu.Unlock()
	return nil
}

// Serve accepts one loopback TCP listener. Ownership transfers only after the
// listener has passed validation and admission.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return ErrConfig
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.IP == nil || !addr.IP.IsLoopback() {
		return fmt.Errorf("SMB listener must bind loopback TCP: %w", ErrConfig)
	}
	s.mu.Lock()
	if s.started || s.stopping || s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	s.started = true
	s.listener = listener
	s.mu.Unlock()

	stop := context.AfterFunc(ctx, s.stop)
	defer stop()
	var serveErr error
	for {
		network, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopping := s.stopping
			s.mu.Unlock()
			if !stopping {
				serveErr = err
			}
			break
		}
		peer, ok := network.RemoteAddr().(*net.TCPAddr)
		if !ok || peer.IP == nil || !peer.IP.IsLoopback() {
			_ = network.Close()
			continue
		}
		s.mu.Lock()
		if s.stopping || len(s.connections) >= s.config.Limits.MaxConnections {
			s.mu.Unlock()
			_ = network.Close()
			continue
		}
		connection := newConnection(s, network)
		s.connections[connection] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			err := connection.run()
			connection.releaseDisconnected()
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logFailure("connection", err)
			}
		}()
	}

	s.stop()
	s.wg.Wait()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.Limits.CleanupTimeout)
	err := s.retryCleanup(cleanup)
	cancel()
	s.cleanupFailure(err)
	s.mu.Lock()
	s.listener = nil
	if err == nil && len(s.connections) == 0 && len(s.exports) == 0 {
		s.stopped = true
	}
	s.mu.Unlock()
	s.doneOnce.Do(func() { close(s.done) })
	return errors.Join(serveErr, err)
}

func (s *Server) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return
	}
	s.stopping = true
	for _, export := range s.exports {
		export.stopping = true
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for connection := range s.connections {
		connection.cancel()
		_ = connection.net.Close()
	}
}

// Shutdown permanently stops admission and retries all unresolved cleanup.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stop()
	s.mu.Lock()
	started, stopped := s.started, s.stopped
	s.mu.Unlock()
	if started && !stopped {
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := s.retryCleanup(ctx)
	s.cleanupFailure(err)
	if err == nil {
		s.mu.Lock()
		if len(s.connections) == 0 && len(s.exports) == 0 {
			s.stopped = true
		}
		s.mu.Unlock()
	}
	if !started {
		s.doneOnce.Do(func() { close(s.done) })
	}
	return err
}

func (s *Server) retryCleanup(ctx context.Context) error {
	return s.cleanupMu.run(ctx, func() error {
		s.mu.Lock()
		connections := make([]*connection, 0, len(s.connections))
		for connection := range s.connections {
			connections = append(connections, connection)
		}
		exports := make([]*Export, 0, len(s.exports))
		for _, export := range s.exports {
			exports = append(exports, export)
		}
		s.mu.Unlock()
		var errs []error
		for _, connection := range connections {
			if err := connection.retryDisconnected(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		for _, export := range exports {
			if err := export.close(ctx); err != nil {
				errs = append(errs, err)
				continue
			}
			s.mu.Lock()
			if s.exports[export.key] == export && export.refs == 0 && export.active == 0 {
				delete(s.exports, export.key)
			}
			s.mu.Unlock()
		}
		return errors.Join(errs...)
	})
}

func (e *Export) close(ctx context.Context) error {
	return e.cleanupMu.run(ctx, func() error {
		s := e.server
		s.mu.Lock()
		connections := make([]*connection, 0, len(s.connections))
		for connection := range s.connections {
			connections = append(connections, connection)
		}
		s.mu.Unlock()
		var errs []error
		for _, connection := range connections {
			if err := connection.closeExport(ctx, e); err != nil {
				errs = append(errs, err)
			}
		}
		s.mu.Lock()
		busy := e.refs != 0 || e.active != 0
		s.mu.Unlock()
		if busy && len(errs) == 0 {
			errs = append(errs, ErrBusy)
		}
		return errors.Join(errs...)
	})
}

func (s *Server) cleanupFailure(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.cleanupFailures++
	if s.cleanupErr == nil {
		s.cleanupErr = err
	}
	s.mu.Unlock()
	s.logFailure("cleanup", err)
}

func (s *Server) logFailure(phase string, err error) {
	if err != nil && s.config.Logger != nil {
		s.config.Logger.Warn("SMB operation failed", "component", "smb", "phase", phase, "error_category", "io")
	}
}

func (s *Server) Status() Status {
	s.mu.Lock()
	out := Status{
		Serving: s.started && !s.stopping, Stopping: s.stopping && !s.stopped,
		Stopped: s.stopped, Exports: len(s.exports), Connections: len(s.connections),
		CleanupFailures: s.cleanupFailures,
	}
	for _, export := range s.exports {
		if export.stopping {
			out.StoppingExports++
		}
	}
	connections := make([]*connection, 0, len(s.connections))
	for connection := range s.connections {
		connections = append(connections, connection)
	}
	s.mu.Unlock()
	for _, connection := range connections {
		connection.addStatus(&out)
	}
	return out
}
