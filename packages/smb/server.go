package smb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/metastore"
	"io"
	"net"
	"strings"
	"sync"
	"unicode/utf8"
)

type Server struct {
	shutdownCleanupMu          sync.Mutex
	shutdownCleanup            *exportCleanup
	config                     Config
	mu                         sync.Mutex
	exports                    map[string]*Export
	connections                map[*connection]struct{}
	listener                   net.Listener
	started, stopping, stopped bool
	done                       chan struct{}
	wg                         sync.WaitGroup
	cleanupErr                 error
	cleanupFailures            int
	uncertain                  uint64
	fencedTrees                int
	guid                       [16]byte
}

type Export struct {
	server    *Server
	share     Share
	key       string
	refs      int
	opens     int
	active    int
	stopping  bool
	changes   *notificationManager
	cleanupMu sync.Mutex
	cleanup   *exportCleanup
}

func New(config Config) (*Server, error) {
	if config.Authenticator == nil || config.Authorize == nil {
		return nil, ErrConfig
	}
	if err := config.Limits.check(); err != nil {
		return nil, fmt.Errorf("SMB limits: %w", err)
	}
	s := &Server{config: config, exports: make(map[string]*Export), connections: make(map[*connection]struct{}), done: make(chan struct{})}
	rand.Read(s.guid[:])
	return s, nil
}

func shareKey(name string) (string, error) {
	if name == "" || len(name) > 80 || !utf8.ValidString(name) || strings.ContainsAny(name, "\\/\x00\"[]:|<>+=;,?*") || strings.TrimSpace(name) != name {
		return "", ErrConfig
	}
	return strings.ToUpper(name), nil
}

func (s *Server) Publish(share Share) (*Export, error) {
	key, err := shareKey(share.Name)
	if err != nil || share.Volume == "" || share.Backend == nil {
		return nil, ErrConfig
	}
	if err := share.Backend.CheckWindowsStorage(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.Limits.RequestTimeout)
	defer cancel()
	state, err := share.Backend.WindowsState(ctx)
	if err != nil {
		return nil, err
	}
	if !state.Enabled || state.MaxEventBytes <= 0 || state.MaxEventBytes > s.config.Limits.MaxDirectoryBytes || s.config.Limits.MaxNotifyEvents < 2 || state.MaxEventBytes > (s.config.Limits.MaxNotifyBytes-4*metastore.MaxNotificationAncestors-64)/2 {
		return nil, ErrConfig
	}
	changes, err := newNotificationManager(ctx, share.Changes, s.config.Limits)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			s.cleanupFailure(changes.Close())
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.stopped {
		return nil, ErrStopped
	}
	if _, ok := s.exports[key]; ok {
		return nil, ErrBusy
	}
	e := &Export{server: s, share: share, key: key, changes: changes}
	s.exports[key] = e
	keep = true
	return e, nil
}

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
	if e.opens != 0 || e.active != 0 {
		s.mu.Unlock()
		return ErrBusy
	}
	e.stopping = true
	s.mu.Unlock()
	if err := e.close(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	if s.exports[e.key] == e {
		delete(s.exports, e.key)
	}
	s.mu.Unlock()
	return nil
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return ErrConfig
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.IP == nil || !addr.IP.IsLoopback() {
		return fmt.Errorf("SMB listener must bind a loopback TCP address: %w", ErrConfig)
	}
	s.mu.Lock()
	if s.started || s.stopping {
		s.mu.Unlock()
		return ErrStopped
	}
	s.started = true
	s.listener = listener
	s.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { s.stop() })
	defer stop()
	var serveErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopping := s.stopping
			s.mu.Unlock()
			if !stopping {
				serveErr = err
			}
			break
		}
		remote, ok := conn.RemoteAddr().(*net.TCPAddr)
		if !ok || !remote.IP.IsLoopback() {
			_ = conn.Close()
			continue
		}
		s.mu.Lock()
		if s.stopping || len(s.connections) >= s.config.Limits.MaxConnections {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		c := newConnection(s, conn)
		s.connections[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			err := c.run()
			c.releaseDisconnected()
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && s.config.Logger != nil {
				s.config.Logger.Warn("SMB connection ended", "phase", "connection", "failed", true)
			}
		}()
	}
	s.stop()
	s.wg.Wait()
	s.closeExports()
	s.mu.Lock()
	s.stopped = true
	s.listener = nil
	cleanup := s.cleanupErr
	close(s.done)
	s.mu.Unlock()
	return errors.Join(serveErr, cleanup)
}

func (s *Server) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return
	}
	s.stopping = true
	for _, e := range s.exports {
		e.stopping = true
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for c := range s.connections {
		c.cancel()
		_ = c.net.Close()
	}
	if !s.started {
		go func() { s.closeExports(); s.mu.Lock(); s.stopped = true; close(s.done); s.mu.Unlock() }()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.stop()
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	retryErr := s.retryCleanup(ctx)
	s.mu.Lock()
	err := s.cleanupErr
	s.mu.Unlock()
	return errors.Join(err, retryErr)
}

func (s *Server) retryCleanup(ctx context.Context) error {
	s.mu.Lock()
	needed := len(s.connections) > 0
	exports := make([]*Export, 0, len(s.exports))
	for _, e := range s.exports {
		exports = append(exports, e)
	}
	s.mu.Unlock()
	for _, e := range exports {
		e.cleanupMu.Lock()
		a := e.cleanup
		e.cleanupMu.Unlock()
		if a == nil {
			needed = true
			continue
		}
		select {
		case <-a.done:
			if a.err != nil {
				needed = true
			}
		default:
			needed = true
		}
	}
	if !needed {
		return nil
	}
	s.shutdownCleanupMu.Lock()
	attempt := s.shutdownCleanup
	if attempt != nil {
		select {
		case <-attempt.done:
			attempt = nil
		default:
		}
	}
	if attempt == nil {
		attempt = &exportCleanup{done: make(chan struct{})}
		s.shutdownCleanup = attempt
		go func(a *exportCleanup) {
			s.mu.Lock()
			connections := make([]*connection, 0, len(s.connections))
			for c := range s.connections {
				connections = append(connections, c)
			}
			s.mu.Unlock()
			for _, c := range connections {
				c.cleanup()
			}
			for _, e := range exports {
				if err := e.close(context.Background()); err != nil && a.err == nil {
					a.err = err
				}
			}
			s.mu.Lock()
			if len(s.connections) != 0 && a.err == nil {
				a.err = ErrBusy
			}
			s.mu.Unlock()
			close(a.done)
		}(attempt)
	}
	s.shutdownCleanupMu.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Status() Status {
	s.mu.Lock()
	stopping := 0
	exports := make([]*Export, 0, len(s.exports))
	for _, e := range s.exports {
		exports = append(exports, e)
		if e.stopping {
			stopping++
		}
	}
	status := Status{Serving: s.started && !s.stopping, Stopping: s.stopping && !s.stopped, Stopped: s.stopped, Exports: len(s.exports), StoppingExports: stopping, Connections: len(s.connections), CleanupFailures: s.cleanupFailures, UnconfirmedMutations: s.uncertain, FencedTrees: s.fencedTrees}
	connections := make([]*connection, 0, len(s.connections))
	for c := range s.connections {
		connections = append(connections, c)
	}
	s.mu.Unlock()
	for _, c := range connections {
		c.mu.Lock()
		status.PendingRequests += len(c.pending)
		if c.disconnected {
			status.RetainedConnections++
			status.Connections--
		}
		c.mu.Unlock()
	}
	for _, e := range exports {
		e.changes.mu.Lock()
		if e.changes.failure != nil {
			status.DegradedExports++
		}
		e.changes.mu.Unlock()
	}
	return status
}

func (s *Server) cleanupFailure(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupFailures++
	// Retain one cause; resource counters bound the diagnostic without retaining
	// one potentially large error chain for every failed connection.
	if s.cleanupErr == nil {
		s.cleanupErr = err
	}
}

func (s *Server) closeExports() {
	s.mu.Lock()
	exports := make([]*Export, 0, len(s.exports))
	for _, e := range s.exports {
		exports = append(exports, e)
	}
	s.mu.Unlock()
	for _, e := range exports {
		s.cleanupFailure(e.close(context.Background()))
	}
}

type exportCleanup struct {
	done chan struct{}
	err  error
}

func (e *Export) close(ctx context.Context) error {
	e.cleanupMu.Lock()
	attempt := e.cleanup
	if attempt != nil {
		select {
		case <-attempt.done:
			if attempt.err != nil {
				attempt = nil
			}
		default:
		}
	}
	if attempt == nil {
		attempt = &exportCleanup{done: make(chan struct{})}
		e.cleanup = attempt
		go func(a *exportCleanup) {
			a.err = e.closeTrees()
			if a.err == nil {
				a.err = e.changes.Close()
			}
			close(a.done)
		}(attempt)
	}
	e.cleanupMu.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Export) closeTrees() error {
	s := e.server
	s.mu.Lock()
	connections := make([]*connection, 0, len(s.connections))
	for c := range s.connections {
		connections = append(connections, c)
	}
	s.mu.Unlock()
	for _, c := range connections {
		if err := c.closeExport(e); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.refs != 0 {
		return ErrBusy
	}
	return nil
}

func (s *Server) unconfirmedMutation() {
	s.mu.Lock()
	if s.uncertain < ^uint64(0) {
		s.uncertain++
	}
	s.mu.Unlock()
}
