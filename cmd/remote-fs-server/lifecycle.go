package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type drainingServer struct {
	*http.Server
	drain       *drainingHandler
	connections *connectionTracker
}

type connectionTracker struct {
	mu       sync.Mutex
	new      map[net.Conn]struct{}
	stopping bool
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{new: make(map[net.Conn]struct{})}
}

func (t *connectionTracker) update(connection net.Conn, state http.ConnState) {
	t.mu.Lock()
	if state == http.StateNew {
		if t.stopping {
			t.mu.Unlock()
			_ = connection.Close()
			return
		}
		t.new[connection] = struct{}{}
		t.mu.Unlock()
		return
	}
	delete(t.new, connection)
	t.mu.Unlock()
}

func (t *connectionTracker) closeNew() error {
	t.mu.Lock()
	t.stopping = true
	connections := make([]net.Conn, 0, len(t.new))
	for connection := range t.new {
		connections = append(connections, connection)
	}
	t.mu.Unlock()

	errs := make([]error, 0, len(connections))
	for _, connection := range connections {
		if err := expectedClose(connection.Close()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *connectionTracker) newCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.new)
}

// acceptedConnectionListener bounds accepted connections with constant-size bookkeeping.
// net/http has one Accept loop, so a condition variable can stop it before the underlying
// listener allocates another process file descriptor.
type acceptedConnectionListener struct {
	net.Listener
	max int

	mu     sync.Mutex
	ready  *sync.Cond
	active int
	closed bool
}

func limitAcceptedConnections(listener net.Listener, max int) net.Listener {
	limited := &acceptedConnectionListener{Listener: listener, max: max}
	limited.ready = sync.NewCond(&limited.mu)
	return limited
}

func (l *acceptedConnectionListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	for l.active >= l.max && !l.closed {
		l.ready.Wait()
	}
	if l.closed {
		l.mu.Unlock()
		return nil, net.ErrClosed
	}
	l.active++
	l.mu.Unlock()

	connection, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}
	return &acceptedConnection{Conn: connection, release: l.release}, nil
}

func (l *acceptedConnectionListener) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		l.ready.Broadcast()
	}
	l.mu.Unlock()
	return l.Listener.Close()
}

func (l *acceptedConnectionListener) release() {
	l.mu.Lock()
	l.active--
	l.ready.Signal()
	l.mu.Unlock()
}

type acceptedConnection struct {
	net.Conn
	release     func()
	releaseOnce sync.Once
}

func (c *acceptedConnection) Close() error {
	err := c.Conn.Close()
	c.releaseOnce.Do(c.release)
	return err
}

// startedListener signals after http.Server has registered the listener for shutdown and
// reached its accept loop. A termination signal in the readiness-to-Serve gap can then
// close the listener and wait for Serve to leave without racing an unregistered listener.
type startedListener struct {
	net.Listener
	started chan struct{}
	once    sync.Once
}

func (l *startedListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.started) })
	return l.Listener.Accept()
}

// drainingHandler closes request admission before storage shutdown and records every
// handler that crossed that gate. A shutdown deadline may expire while an application
// handler is still running; the storage lifetime must outlive that handler anyway.
type drainingHandler struct {
	inner *httprest.Handler

	mu       sync.Mutex
	stopping bool
	active   int
	drained  chan struct{}
	stopOnce sync.Once
}

func newDrainingHandler(inner *httprest.Handler) *drainingHandler {
	return &drainingHandler{inner: inner, drained: make(chan struct{})}
}

func (h *drainingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		w.Header().Set(httprest.HeaderProtocol, httprest.Version)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "remote-fs-server is stopping", http.StatusServiceUnavailable)
		return
	}
	h.active++
	h.mu.Unlock()

	defer h.leave()
	h.inner.ServeHTTP(w, r)
}

func (h *drainingHandler) leave() {
	h.mu.Lock()
	h.active--
	if h.stopping && h.active == 0 {
		close(h.drained)
	}
	h.mu.Unlock()
}

// Stop is idempotent because http.Server may invoke it through RegisterOnShutdown while
// serve also invokes it synchronously before beginning shutdown.
func (h *drainingHandler) Stop() {
	h.stopOnce.Do(func() {
		h.mu.Lock()
		h.stopping = true
		if h.active == 0 {
			close(h.drained)
		}
		h.mu.Unlock()
		h.inner.Stop()
	})
}

func (h *drainingHandler) Wait() { <-h.drained }

func shutdownAndDrainServer(server *drainingServer, stopped <-chan error, ctx context.Context) error {
	server.drain.Stop()
	shutdownErr := server.Shutdown(ctx)
	var closeErr error
	if shutdownErr != nil {
		closeErr = server.Close()
	}
	serveErr := <-stopped
	server.drain.Wait()
	return errors.Join(expectedClose(serveErr), shutdownErr, closeErr)
}

func closeAndDrainServer(server *drainingServer, serveErr error) error {
	server.drain.Stop()
	closeErr := server.Close()
	server.drain.Wait()
	return errors.Join(serveErr, closeErr)
}

func closeStartingServer(server *drainingServer, listener net.Listener, stopped <-chan error) error {
	server.drain.Stop()
	listenerErr := listener.Close()
	serveErr := <-stopped
	closeErr := server.Close()
	server.drain.Wait()
	return errors.Join(expectedClose(listenerErr), expectedClose(serveErr), closeErr)
}

func terminateServer(
	server *drainingServer,
	listener net.Listener,
	stopped <-chan error,
	started <-chan struct{},
	grace time.Duration,
) error {
	if started != nil {
		select {
		case <-started:
			started = nil
		default:
		}
	}
	if started != nil {
		return closeStartingServer(server, listener, stopped)
	}
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	return shutdownAndDrainServer(server, stopped, ctx)
}

func expectedClose(err error) error {
	if onlyErrorLeaves(err, net.ErrClosed, http.ErrServerClosed) {
		return nil
	}
	return err
}
