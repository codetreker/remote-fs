package smb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"unicode/utf8"
)

type Server struct {
	config                     Config
	mu                         sync.Mutex
	exports                    map[string]*Export
	connections                map[*connection]struct{}
	sessions                   *sessionRegistry
	listener                   net.Listener
	started, stopping, stopped bool
	done                       chan struct{}
	wg                         sync.WaitGroup
	cleanupMu                  cleanupGate
	cleanupErr                 error
	cleanupFailures            int
	guid                       [16]byte
}
type Export struct {
	server              *Server
	share               Share
	key                 string
	refs, opens, active int
	stopping, published bool
	cleanupMu           cleanupGate
}

// New requires explicit limits, authentication and current authorization.
// Authentication providers and backends remain owned by the embedding host.
func New(config Config) (*Server, error) {
	if config.Authenticator == nil || config.Authorize == nil {
		return nil, ErrConfig
	}
	if err := config.Limits.check(); err != nil {
		return nil, fmt.Errorf("SMB limits: %w", err)
	}
	s := &Server{config: config, exports: make(map[string]*Export), connections: make(map[*connection]struct{}), sessions: newSessionRegistry(), done: make(chan struct{})}
	rand.Read(s.guid[:])
	return s, nil
}
func shareKey(name string) (string, error) {
	if name == "" || len(name) > 80 || !utf8.ValidString(name) || strings.ContainsAny(name, "\\/\x00\"[]:|<>+=;,?*") || strings.TrimSpace(name) != name {
		return "", ErrConfig
	}
	return strings.ToUpper(name), nil
}

// Publish reserves export capacity and checks the retained-file capability
// chain. It acquires no file reference and does not take ownership of Backend.
func (s *Server) Publish(share Share) (*Export, error) {
	key, err := shareKey(share.Name)
	if err != nil || key == "IPC$" || share.Volume == "" || share.Backend == nil {
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

// Unpublish refuses active requests or open handles with ErrBusy. Once
// retirement starts, new trees are refused and failed cleanup remains owned
// by this Export until a retry confirms completion.
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
	if s.exports[e.key] == e && e.refs == 0 && e.active == 0 && e.opens == 0 {
		delete(s.exports, e.key)
	}
	s.mu.Unlock()
	return nil
}

// Serve accepts a loopback TCP listener once. Accepted listener ownership
// transfers to the server; rejected listeners remain owned by the caller.
// Cleanup errors retain their owners for a later Shutdown retry.
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
		n, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			stopping := s.stopping
			s.mu.Unlock()
			if !stopping {
				serveErr = err
			}
			break
		}
		peer, ok := n.RemoteAddr().(*net.TCPAddr)
		if !ok || !peer.IP.IsLoopback() {
			_ = n.Close()
			continue
		}
		s.mu.Lock()
		if s.stopping || len(s.connections) >= s.config.Limits.MaxConnections {
			s.mu.Unlock()
			_ = n.Close()
			continue
		}
		c := newConnection(s, n)
		s.connections[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			err := c.run()
			c.releaseDisconnected()
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
	s.stopped = true
	s.listener = nil
	close(s.done)
	s.mu.Unlock()
	return errors.Join(serveErr, err)
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
}

// Shutdown stops admission and retries outstanding cleanup. Its result belongs
// to this attempt; older failures remain diagnostic facts, not retry results.
// Cancellation or failed cleanup does not release unresolved capacity.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stop()
	s.mu.Lock()
	started := s.started
	stopped := s.stopped
	if !started && !stopped {
		s.stopped = true
		close(s.done)
	}
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
	return err
}
func (s *Server) retryCleanup(ctx context.Context) error {
	return s.cleanupMu.run(ctx, func() error {
		s.mu.Lock()
		cs := make([]*connection, 0, len(s.connections))
		for c := range s.connections {
			cs = append(cs, c)
		}
		es := make([]*Export, 0, len(s.exports))
		for _, e := range s.exports {
			es = append(es, e)
		}
		s.mu.Unlock()
		var errs []error
		for _, c := range cs {
			if err := c.retryDisconnected(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		for _, e := range es {
			if err := e.close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	})
}
func (e *Export) close(ctx context.Context) error {
	return e.cleanupMu.run(ctx, func() error {
		s := e.server
		s.mu.Lock()
		cs := make([]*connection, 0, len(s.connections))
		for c := range s.connections {
			cs = append(cs, c)
		}
		s.mu.Unlock()
		var errs []error
		for _, c := range cs {
			if err := c.closeExport(ctx, e); err != nil {
				errs = append(errs, err)
			}
		}
		s.mu.Lock()
		busy := e.refs != 0 || e.active != 0 || e.opens != 0
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

// Status reports bounded resource counts, including cleanup-only owners.
func (s *Server) Status() Status {
	s.mu.Lock()
	out := Status{Serving: s.started && !s.stopping, Stopping: s.stopping && !s.stopped, Stopped: s.stopped, Exports: len(s.exports), Connections: len(s.connections), CleanupFailures: s.cleanupFailures}
	for _, e := range s.exports {
		if e.stopping {
			out.StoppingExports++
		}
	}
	cs := make([]*connection, 0, len(s.connections))
	for c := range s.connections {
		cs = append(cs, c)
	}
	s.mu.Unlock()
	s.sessions.mu.Lock()
	out.Sessions = len(s.sessions.owners)
	s.sessions.mu.Unlock()
	for _, c := range cs {
		c.mu.Lock()
		if c.disconnected {
			out.RetainedConnections++
		}
		out.PendingRequests += len(c.pending)
		ss := make([]*session, 0, len(c.sessions))
		for _, session := range c.sessions {
			ss = append(ss, session)
		}
		c.mu.Unlock()
		for _, session := range ss {
			session.mu.Lock()
			out.Trees += len(session.trees) + session.openingTrees
			ts := make([]*tree, 0, len(session.trees))
			for _, t := range session.trees {
				ts = append(ts, t)
			}
			as := make([]*authoritySession, 0, len(session.authorities))
			for _, a := range session.authorities {
				as = append(as, a)
			}
			session.mu.Unlock()
			for _, a := range as {
				if a.isStopping() && !a.isClosed() {
					out.FencedAuthorities++
				}
			}
			for _, t := range ts {
				if t.files == nil {
					continue
				}
				r := t.files
				r.mu.Lock()
				out.InstalledHandles += len(r.handles)
				for p := range r.reservations {
					p.mu.Lock()
					if !p.installed {
						out.OpenReservations++
					}
					if p.finishing {
						out.CleanupPendingHandles++
					}
					p.mu.Unlock()
				}
				for _, h := range r.handles {
					h.mu.Lock()
					if h.retiring && !h.closed {
						out.CleanupPendingHandles++
					}
					h.mu.Unlock()
				}
				r.mu.Unlock()
			}
		}
	}
	return out
}
