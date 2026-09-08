package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func testLockConfig(t *testing.T) lockConfig {
	t.Helper()
	return lockConfig{options: locking.DefaultOptions(), initialize: true}
}

func newTestNamespace(t *testing.T) (*objectstore.Storage, *sqlite.LockingStore) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "metastore.db"), Namespace: "workspace",
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	namespace := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("close namespace: %v", err)
		}
	})
	return namespace, meta
}

func TestStorageClosesAfterServingAndItsFailureIsReturned(t *testing.T) {
	actionFailure := errors.New("HTTP server failed")
	closeFailure := errors.New("storage close failed")
	var events []string
	ns := opened{close: func() error {
		events = append(events, "close")
		return closeFailure
	}}
	err := withOpened(ns, func() error {
		if len(events) != 0 {
			t.Fatalf("storage closed before the server action: %v", events)
		}
		events = append(events, "serve")
		return actionFailure
	})
	if !errors.Is(err, actionFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("withOpened returned %v, want both serving and close failures", err)
	}
	if len(events) != 2 || events[0] != "serve" || events[1] != "close" {
		t.Fatalf("lifecycle order is %v", events)
	}
}

func TestInvalidListenAddressDoesNotInitializeALocalStore(t *testing.T) {
	root := emptyPrivateDirectory(t)
	err := run([]string{
		"-listen", "127.0.0.1",
		"-local-store", root,
		"-initialize-lock-state",
		"-workspace", "workspace",
		"-quota", "8M",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("invalid listen address returned %v", err)
	}
	assertDirectoryEmpty(t, root)
}

func TestAddressInUseDoesNotInitializeALocalStore(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })

	root := emptyPrivateDirectory(t)
	err = run([]string{
		"-listen", held.Addr().String(),
		"-local-store", root,
		"-initialize-lock-state",
		"-workspace", "workspace",
		"-quota", "8M",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("address in use returned %v", err)
	}
	assertDirectoryEmpty(t, root)
}

func TestListenerCleanupFailureIsReturnedWithStartupFailure(t *testing.T) {
	startupFailure := errors.New("storage open failed")
	cleanupFailure := errors.New("listener close failed")
	for name, closeErr := range map[string]error{
		"failure":             cleanupFailure,
		"closed plus failure": errors.Join(net.ErrClosed, cleanupFailure),
	} {
		t.Run(name, func(t *testing.T) {
			err := withListener(&closeErrorListener{err: closeErr}, func() error {
				return startupFailure
			})
			if !errors.Is(err, startupFailure) || !errors.Is(err, cleanupFailure) {
				t.Fatalf("withListener returned %v, want startup and listener cleanup failures", err)
			}
		})
	}
}

func TestAcceptedConnectionLimitReleasesExactlyOnce(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := &countingListener{Listener: base}
	listener := limitAcceptedConnections(observed, 1)
	t.Cleanup(func() { _ = listener.Close() })

	accepts := make(chan acceptResult, 3)
	go acceptOne(listener, accepts)
	clientOne := dialTCP(t, listener.Addr().String())
	t.Cleanup(func() { _ = clientOne.Close() })
	serverOne := receiveAccepted(t, accepts)

	go acceptOne(listener, accepts)
	clientTwo := dialTCP(t, listener.Addr().String())
	t.Cleanup(func() { _ = clientTwo.Close() })
	assertNoAccept(t, accepts, 25*time.Millisecond)
	if count := observed.Count(); count != 1 {
		t.Fatalf("saturated listener called the underlying Accept %d times, want 1", count)
	}
	doubleClose := make(chan struct{})
	go func() {
		_ = serverOne.Close()
		_ = serverOne.Close()
		close(doubleClose)
	}()
	select {
	case <-doubleClose:
	case <-time.After(time.Second):
		t.Fatal("closing one accepted connection twice released or blocked more than once")
	}
	serverTwo := receiveAccepted(t, accepts)

	go acceptOne(listener, accepts)
	clientThree := dialTCP(t, listener.Addr().String())
	t.Cleanup(func() { _ = clientThree.Close() })
	assertNoAccept(t, accepts, 25*time.Millisecond)
	if count := observed.Count(); count != 2 {
		t.Fatalf("double close admitted another underlying Accept; count is %d, want 2", count)
	}
	if err := serverTwo.Close(); err != nil {
		t.Fatal(err)
	}
	serverThree := receiveAccepted(t, accepts)
	if err := serverThree.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedConnectionLimitUsesConstantStateForALargeFiniteBound(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := limitAcceptedConnections(base, math.MaxInt-1)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionTrackerClosesConnectionsAdmittedAfterShutdownStarts(t *testing.T) {
	tracker := newConnectionTracker()
	if err := tracker.closeNew(); err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	tracker.update(server, http.StateNew)
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection admitted after shutdown remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("connection admitted after shutdown was not closed: %v", err)
	}
}

func TestClosingASaturatedConnectionLimitUnblocksAccept(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := limitAcceptedConnections(base, 1)
	accepts := make(chan acceptResult, 2)
	go acceptOne(listener, accepts)
	client := dialTCP(t, listener.Addr().String())
	t.Cleanup(func() { _ = client.Close() })
	server := receiveAccepted(t, accepts)
	t.Cleanup(func() { _ = server.Close() })

	go acceptOne(listener, accepts)
	assertNoAccept(t, accepts, 25*time.Millisecond)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-accepts:
		if result.conn != nil {
			_ = result.conn.Close()
			t.Fatal("closed saturated listener accepted another connection")
		}
		if !errors.Is(result.err, net.ErrClosed) {
			t.Fatalf("blocked Accept returned %v, want net.ErrClosed", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener Close did not unblock Accept waiting on connection capacity")
	}
}

func TestConfiguredServerBoundsHeadersAndIdleConnectionsWithoutBoundingStreams(t *testing.T) {
	server := newServerWithOptions(unreplicatedHandler(t), standaloneHTTPOptions{
		readHeaderTimeout: 3 * time.Second,
		idleTimeout:       45 * time.Second,
	})
	if server.ReadHeaderTimeout != 3*time.Second || server.IdleTimeout != 45*time.Second {
		t.Fatalf("server timeouts are header=%v idle=%v", server.ReadHeaderTimeout, server.IdleTimeout)
	}
	if server.ReadTimeout != 0 || server.WriteTimeout != 0 {
		t.Fatalf("server has request-wide timeouts read=%v write=%v", server.ReadTimeout, server.WriteTimeout)
	}
}

func TestSlowRequestHeaderIsClosed(t *testing.T) {
	server, listener, stopped := startHTTPServer(t, standaloneHTTPOptions{
		readHeaderTimeout: 40 * time.Millisecond,
		idleTimeout:       time.Second,
	})
	connection := dialTCP(t, listener.Addr().String())
	if _, err := io.WriteString(connection, "GET /v2/stat?path= HTTP/1.1\r\nHost:"); err != nil {
		t.Fatal(err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(connection); err != nil {
		t.Fatalf("slow-header connection was not closed within its timeout: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	stopHTTPServer(t, server, stopped)
}

func TestIdleKeepAliveConnectionIsClosed(t *testing.T) {
	server, listener, stopped := startHTTPServer(t, standaloneHTTPOptions{
		readHeaderTimeout: time.Second,
		idleTimeout:       40 * time.Millisecond,
	})
	connection := dialTCP(t, listener.Addr().String())
	reader := bufio.NewReader(connection)
	request := httprest.Request{Op: httprest.OpStat, Path: ""}
	base, err := url.Parse("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	requestURL, err := request.URL(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(connection,
		"GET "+requestURL.RequestURI()+" HTTP/1.1\r\nHost: "+base.Host+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	httpRequest, err := http.NewRequest(http.MethodGet, requestURL.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	if response.Close {
		response.Body.Close()
		t.Fatal("server closed a valid HTTP/1.1 response before its idle interval")
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("idle keep-alive connection remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("idle keep-alive connection outlived its timeout: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	stopHTTPServer(t, server, stopped)
}

func TestShutdownClosesAConnectionLimitSaturatedByAnIncompleteHeader(t *testing.T) {
	server := newServerWithOptions(unreplicatedHandler(t), standaloneHTTPOptions{
		readHeaderTimeout: 30 * time.Second,
		idleTimeout:       time.Second,
	})
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := limitAcceptedConnections(raw, 1)
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	served := make(chan error, 1)
	go func() {
		served <- serveWithGrace(server, listener, opened{what: "slow header fixture"}, io.Discard, 200*time.Millisecond)
	}()
	connection := dialTCP(t, listener.Addr().String())
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := io.WriteString(connection, "G"); err != nil {
		t.Fatal(err)
	}
	waitForNewConnections(t, server.connections, 1, time.Second)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("shutdown with a saturated listener failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for the longer request-header timeout")
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitForNewConnections(t *testing.T, tracker *connectionTracker, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if count := tracker.newCount(); count >= want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("server tracked %d new connections, want at least %d", tracker.newCount(), want)
		}
	}
}

func unreplicatedHandler(t *testing.T) *httprest.Handler {
	t.Helper()
	backing, _ := newTestNamespace(t)
	handler, err := httprest.NewHandler(backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func startHTTPServer(
	t *testing.T,
	options standaloneHTTPOptions,
) (*drainingServer, net.Listener, <-chan error) {
	t.Helper()
	server := newServerWithOptions(unreplicatedHandler(t), options)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	return server, listener, stopped
}

func stopHTTPServer(t *testing.T, server *drainingServer, stopped <-chan error) {
	t.Helper()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("server stopped with %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}

type acceptResult struct {
	conn net.Conn
	err  error
}

func acceptOne(listener net.Listener, results chan<- acceptResult) {
	connection, err := listener.Accept()
	results <- acceptResult{conn: connection, err: err}
}

func dialTCP(t *testing.T, address string) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func receiveAccepted(t *testing.T, results <-chan acceptResult) net.Conn {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatal(result.err)
		}
		return result.conn
	case <-time.After(time.Second):
		t.Fatal("listener did not accept a connection")
		return nil
	}
}

func assertNoAccept(t *testing.T, results <-chan acceptResult, duration time.Duration) {
	t.Helper()
	select {
	case result := <-results:
		if result.conn != nil {
			_ = result.conn.Close()
		}
		t.Fatalf("connection limit admitted another Accept: %v", result.err)
	case <-time.After(duration):
	}
}

type countingListener struct {
	net.Listener
	mu    sync.Mutex
	count int
}

func (l *countingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.count++
		l.mu.Unlock()
	}
	return connection, err
}

func (l *countingListener) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

func emptyPrivateDirectory(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func assertDirectoryEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed startup created %d local-store entries", len(entries))
	}
}

type closeErrorListener struct {
	err error
}

func (l *closeErrorListener) Accept() (net.Conn, error) { return nil, errors.New("unexpected Accept") }
func (l *closeErrorListener) Close() error              { return l.err }
func (l *closeErrorListener) Addr() net.Addr            { return testAddress("listener") }

type testAddress string

func (a testAddress) Network() string { return "test" }
func (a testAddress) String() string  { return string(a) }

func TestOpenLocalExposesReplicationAndOperationalStatus(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	source := localSource{
		root:      root,
		workspace: "workspace",
		objects:   localdisk.Options{MaxWaitingOperations: 11},
	}
	maintenance := objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8}
	const maxReaderConnections = 3
	const maxSnapshotReaderConnections = 4
	const maxIntegrityRecords = 101
	const maxIntegrityBytes = 7 << 20
	ns, err := openLocal(
		source, 1<<20, sqlite.DefaultObjectLimits(), maxReaderConnections,
		maxSnapshotReaderConnections, maxIntegrityRecords, maxIntegrityBytes, maintenance, testLockConfig(t),
	)
	if err != nil {
		t.Fatalf("openLocal: %v", err)
	}
	t.Cleanup(func() { _ = ns.close() })
	if ns.log == nil || ns.status == nil {
		t.Fatalf("local namespace has log=%t and status=%t, want both", ns.log != nil, ns.status != nil)
	}
	status, err := ns.status(t.Context())
	if err != nil {
		t.Fatalf("query local status: %v", err)
	}
	if !strings.Contains(status, "SQLite reader-connection limit is 3") {
		t.Fatalf("local status does not report the configured reader limit: %s", status)
	}
	if !strings.Contains(status, "snapshot reader-connection limit is 4") {
		t.Fatalf("local status does not report the configured snapshot reader limit: %s", status)
	}
	if !strings.Contains(status, "integrity record work limit is 101") {
		t.Fatalf("local status does not report the configured integrity limit: %s", status)
	}
	if !strings.Contains(status, "integrity name-byte work limit is 7340032") {
		t.Fatalf("local status does not report the configured integrity byte limit: %s", status)
	}
	if !strings.Contains(status, "0 operations waiting under a limit of 11") {
		t.Fatalf("local status does not report the configured waiting-operation limit: %s", status)
	}
	if !strings.Contains(status, "SQLite checkpoint has accepted generation") ||
		!strings.Contains(status, "checkpointed generation") ||
		!strings.Contains(status, "checkpoint pending is false") {
		t.Fatalf("local status does not report the healthy checkpoint state: %s", status)
	}
	if err := ns.close(); err != nil {
		t.Fatalf("close local namespace: %v", err)
	}
}

func TestOpenBlobsExposesPendingAndMaintenanceStatus(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(service.Close)
	t.Setenv(connectionEnv, blobConnectionString(service.URL+"/devstoreaccount1"))
	limits := sqlite.ObjectLimits{MaxPendingObjects: 17, MaxPendingBytes: 1 << 20}
	const maxReaderConnections = 5
	const maxSnapshotReaderConnections = 7
	const maxIntegrityRecords = 103
	const maxIntegrityBytes = 13 << 20
	maintenance := objectstore.Options{SweepInterval: 47 * time.Second, SweepBatch: 31}
	ns, err := openBlobs(blobSource{
		container: "container",
		database:  filepath.Join(t.TempDir(), "metastore.sqlite"),
		workspace: "workspace",
	}, 0, limits, maxReaderConnections, maxSnapshotReaderConnections, maxIntegrityRecords, maxIntegrityBytes, maintenance, testLockConfig(t))
	if err != nil {
		t.Fatalf("openBlobs: %v", err)
	}
	t.Cleanup(func() { _ = ns.close() })
	if ns.status == nil || ns.statusName != "blob namespace" {
		t.Fatalf("blob namespace has status=%t name=%q", ns.status != nil, ns.statusName)
	}
	var output bytes.Buffer
	handleHangup(t.Context(), ns, &output)
	for _, phrase := range []string{
		"blob namespace status",
		`workspace "workspace"`,
		"0 reserved objects",
		"0 unresolved objects",
		"pending reservation thresholds are 17 objects and 1048576 bytes",
		"SQLite reader-connection limit is 5",
		"snapshot reader-connection limit is 7",
		"integrity record work limit is 103",
		"integrity name-byte work limit is 13631488",
		"garbage sweeps run every 47s with at most 31 objects per attempt",
		"garbage sweep",
	} {
		if !strings.Contains(output.String(), phrase) {
			t.Fatalf("blob status does not contain %q: %s", phrase, output.String())
		}
	}
}

func TestBlobStatusFailsWhenTheObjectStoreIsUnreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv(connectionEnv, blobConnectionString("http://"+address+"/devstoreaccount1"))

	ns, err := openBlobs(blobSource{
		container: "container",
		database:  filepath.Join(t.TempDir(), "metastore.sqlite"),
		workspace: "workspace",
	}, 0, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes,
		objectstore.DefaultOptions(), testLockConfig(t))
	if err != nil {
		t.Fatalf("open blob namespace: %v", err)
	}
	t.Cleanup(func() { _ = ns.close() })

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	assertBlobStatusFailure(t, readStatus(ctx, ns), ns, syscall.EIO)
}

func TestBlobStatusFailsWhenCredentialsAreRejected(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAuthenticationFailure(w)
	}))
	t.Cleanup(service.Close)
	t.Setenv(connectionEnv, blobConnectionString(service.URL+"/devstoreaccount1"))

	ns, err := openBlobs(blobSource{
		container: "container",
		database:  filepath.Join(t.TempDir(), "metastore.sqlite"),
		workspace: "workspace",
	}, 0, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes,
		objectstore.DefaultOptions(), testLockConfig(t))
	if err != nil {
		t.Fatalf("open blob namespace: %v", err)
	}
	t.Cleanup(func() { _ = ns.close() })

	assertBlobStatusFailure(t, readStatus(t.Context(), ns), ns, syscall.EACCES)
}

func TestBlobStatusReportsUnresolvedWrites(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			writeAuthenticationFailure(w)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(service.Close)
	t.Setenv(connectionEnv, blobConnectionString(service.URL+"/devstoreaccount1"))

	ns, err := openBlobs(blobSource{
		container: "container",
		database:  filepath.Join(t.TempDir(), "metastore.sqlite"),
		workspace: "workspace",
	}, 0, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes,
		objectstore.DefaultOptions(), testLockConfig(t))
	if err != nil {
		t.Fatalf("open blob namespace: %v", err)
	}
	t.Cleanup(func() { _ = ns.close() })

	if err := ns.namespace.Write(t.Context(), "artifact", []byte("unknown")); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("write rejected with %v, want EACCES", err)
	}
	var output bytes.Buffer
	handleHangup(t.Context(), ns, &output)
	if !strings.Contains(output.String(), "1 unresolved objects (7 bytes)") {
		t.Fatalf("blob status does not report the quarantined write: %s", output.String())
	}
}

func TestOpenBlobsRefusesLegacyPendingObjectsBeforeStartingMaintenance(t *testing.T) {
	for name, state := range map[string]int{
		"reserved": 0,
		"garbage":  2,
	} {
		t.Run(name, func(t *testing.T) {
			database := filepath.Join(t.TempDir(), "metastore.sqlite")
			writeVersionTwoDatabaseWithPendingObject(t, database, state)
			assertLegacyBlobOpenRefused(t, database, "legacy pending object")
		})
	}
}

func TestOpenBlobsRefusesLegacyDisconnectedCycleBeforeStartingMaintenance(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metastore.sqlite")
	writeVersionTwoDatabaseWithDisconnectedCycle(t, database)
	assertLegacyBlobOpenRefused(t, database, "legacy disconnected cycle")
}

func TestOpenBlobsRefusesLegacyUsedAccountingMismatchBeforeStartingMaintenance(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metastore.sqlite")
	writeVersionTwoDatabase(t, database)
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE namespaces SET used = 1 WHERE id = 1`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertLegacyBlobOpenRefused(t, database, "legacy used accounting mismatch")
}

func TestOpenBlobsRefusesLegacyIntegrityWorkAboveTheConfiguredLimit(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metastore.sqlite")
	writeVersionTwoDatabaseWithDisconnectedCycle(t, database)
	assertLegacyBlobOpenRefusedWithContext(
		t, t.Context(), database, "legacy integrity work above its limit", sqlite.MinIntegrityRecords, syscall.EFBIG,
	)
}

func TestCanceledLegacyBlobOpenDoesNotMigrateOrTouchObjects(t *testing.T) {
	database := filepath.Join(t.TempDir(), "metastore.sqlite")
	writeVersionTwoDatabase(t, database)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assertLegacyBlobOpenRefusedWithContext(
		t, ctx, database, "canceled legacy open", sqlite.DefaultMaxIntegrityRecords, context.Canceled,
	)
}

func assertLegacyBlobOpenRefused(t *testing.T, database, subject string) {
	t.Helper()
	assertLegacyBlobOpenRefusedWithContext(
		t, t.Context(), database, subject, sqlite.DefaultMaxIntegrityRecords, syscall.EIO,
	)
}

func assertLegacyBlobOpenRefusedWithContext(
	t *testing.T,
	ctx context.Context,
	database string,
	subject string,
	maxIntegrityRecords int64,
	want error,
) {
	t.Helper()
	var methodsMu sync.Mutex
	var methods []string
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methodsMu.Lock()
		methods = append(methods, r.Method)
		methodsMu.Unlock()
		http.Error(w, "unexpected object request", http.StatusInternalServerError)
	}))
	t.Cleanup(service.Close)
	t.Setenv(connectionEnv, blobConnectionString(service.URL+"/devstoreaccount1"))

	ns, err := openBlobsContext(ctx, blobSource{
		container: "container",
		database:  database,
		workspace: "workspace",
	}, 0, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, maxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes,
		objectstore.DefaultOptions(), testLockConfig(t))
	if err == nil {
		_ = ns.close()
		t.Fatalf("%s was migrated and served", subject)
	}
	if !errors.Is(err, want) {
		t.Fatalf("%s returned %v, want %v", subject, err, want)
	}
	if version := readSchemaVersion(t, database); version != 2 {
		t.Fatalf("refused legacy database recorded schema version %d, want 2", version)
	}
	methodsMu.Lock()
	defer methodsMu.Unlock()
	for _, method := range methods {
		if method == http.MethodDelete || method == http.MethodPut {
			t.Fatalf("refused legacy database started object maintenance with %s", method)
		}
	}
}

func writeVersionTwoDatabaseWithPendingObject(t *testing.T, path string, state int) {
	t.Helper()
	writeVersionTwoDatabase(t, path)
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		INSERT INTO objects (key, namespace, state, size, digest, created_sec, created_nsec)
		VALUES ('ambiguous', 1, ?, 0, NULL, 0, 0)`, state); err != nil {
		t.Fatalf("add pending object to schema version 2 database: %v", err)
	}
}

func writeVersionTwoDatabaseWithDisconnectedCycle(t *testing.T, path string) {
	t.Helper()
	writeVersionTwoDatabase(t, path)
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES
			(2, 1, ?, 0, 0, 0, 0, 0, NULL),
			(3, 1, ?, 0, 0, 0, 0, 0, NULL);
		INSERT INTO entries (namespace, parent, name, node) VALUES
			(1, 2, X'61', 3),
			(1, 3, X'62', 2);`,
		int64(fs.ModeDir|0o755), int64(fs.ModeDir|0o755),
	); err != nil {
		t.Fatalf("add disconnected directory cycle to schema version 2 database: %v", err)
	}
}

func writeVersionTwoDatabase(t *testing.T, path string) {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate server test source")
	}
	schema, err := os.ReadFile(filepath.Join(
		filepath.Dir(source), "..", "..", "packages", "metastore", "sqlite", "testdata", "version2.sql",
	))
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(string(schema)); err != nil {
		t.Fatalf("create schema version 2 database: %v", err)
	}
	const rootID = 1
	if _, err := database.Exec(`
		INSERT INTO namespaces (id, name, root, used) VALUES (1, 'workspace', ?, 0);
		INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES (?, 1, ?, 0, 0, 0, 0, 0, NULL);
		INSERT INTO logs (namespace, incarnation, committed_position, trimmed_through, trimmed_by_age)
		VALUES (1, 'version-two-incarnation', 0, 0, 0);`,
		rootID, rootID, int64(fs.ModeDir|0o755),
	); err != nil {
		t.Fatalf("populate schema version 2 database: %v", err)
	}
}

func readSchemaVersion(t *testing.T, path string) int {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var version int
	if err := database.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func writeAuthenticationFailure(w http.ResponseWriter) {
	w.Header().Set("x-ms-error-code", "AuthenticationFailed")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>AuthenticationFailed</Code><Message>Authentication failed.</Message></Error>`)
}

func blobConnectionString(endpoint string) string {
	return "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=AQ==;BlobEndpoint=" + endpoint + ";"
}

func assertBlobStatusFailure(t *testing.T, report statusReport, ns opened, want error) {
	t.Helper()
	if report.err == nil || !errors.Is(report.err, want) {
		t.Fatalf("blob status returned %v, want %v", report.err, want)
	}
	var output bytes.Buffer
	writeStatus(report, ns, &output)
	if !strings.Contains(output.String(), "status") || !strings.Contains(output.String(), "failed") {
		t.Fatalf("blob status failure was not reported: %s", output.String())
	}
	for _, plausible := range []string{"reserved objects", "unresolved objects", "garbage objects", "garbage sweep"} {
		if strings.Contains(output.String(), plausible) {
			t.Fatalf("failed blob status reported successful-looking %q: %s", plausible, output.String())
		}
	}
}

func TestBlobCapacityIgnoresOnlyPureENOSYS(t *testing.T) {
	if !onlyErrorLeaves(syscall.ENOSYS, syscall.ENOSYS) {
		t.Fatal("pure ENOSYS was not recognized")
	}
	if onlyErrorLeaves(errors.Join(syscall.ENOSYS, syscall.EIO), syscall.ENOSYS) {
		t.Fatal("ENOSYS joined with an I/O failure was treated as unsupported capacity")
	}
}

func TestExpectedCloseDoesNotHideAJoinedFailure(t *testing.T) {
	if err := expectedClose(net.ErrClosed); err != nil {
		t.Fatalf("a plain closed-listener result remained an error: %v", err)
	}
	failure := errors.New("listener cleanup failed")
	if err := expectedClose(errors.Join(net.ErrClosed, failure)); !errors.Is(err, failure) {
		t.Fatalf("expectedClose returned %v, want the joined cleanup failure", err)
	}
}

func TestTerminationReturnsAFatalServeErrorThatWasAlreadyBuffered(t *testing.T) {
	backing, _ := newTestNamespace(t)
	handler, err := httprest.NewHandler(backing, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(handler)
	failure := errors.New("listener accept failed")
	listener := &acceptErrorListener{err: failure}
	started := make(chan struct{})
	stopped := make(chan error, 1)
	serveReturned := make(chan struct{})
	go func() {
		stopped <- server.Serve(&startedListener{Listener: listener, started: started})
		close(serveReturned)
	}()
	<-started
	<-serveReturned

	if err := terminateServer(server, listener, stopped, nil, time.Second); !errors.Is(err, failure) {
		t.Fatalf("termination returned %v, want the buffered Serve failure", err)
	}
}

type acceptErrorListener struct {
	err error
}

func (l *acceptErrorListener) Accept() (net.Conn, error) { return nil, l.err }
func (l *acceptErrorListener) Close() error              { return nil }
func (l *acceptErrorListener) Addr() net.Addr            { return testAddress("listener") }

func TestReadinessIsPublishedAfterSignalControlIsInstalled(t *testing.T) {
	handler, ns := replicableNamespace(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	output := &signalOnFirstWrite{signal: syscall.SIGTERM}
	if err := serveWithGrace(newServer(handler), listener, ns, output, 100*time.Millisecond); err != nil {
		t.Fatalf("the signal published with readiness did not stop cleanly: %v", err)
	}
	if output.err != nil {
		t.Fatalf("send readiness signal: %v", output.err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "remote-fs-server: serving ") {
		t.Fatalf("first output is not readiness: %q", output.String())
	}
	wantAddress := "http://" + listener.Addr().String()
	if !strings.HasSuffix(lines[0], wantAddress) {
		t.Fatalf("readiness line %q does not end in %q", lines[0], wantAddress)
	}
}

func TestBlockingStoppingDiagnosticCannotHoldAdmissionOpen(t *testing.T) {
	handler, ns := replicableNamespace(t)
	server := newServer(handler)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	output := &blockingWrite{blockAt: 2, blocked: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(output.unblock)
	served := make(chan error, 1)
	go func() { served <- serveWithGrace(server, listener, ns, output, 100*time.Millisecond) }()
	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Stat(t.Context(), ""); err != nil {
		t.Fatalf("prove server readiness: %v", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-output.blocked:
	case <-time.After(time.Second):
		t.Fatal("stopping diagnostic did not reach the blocking writer")
	}
	server.drain.mu.Lock()
	stopping := server.drain.stopping
	server.drain.mu.Unlock()
	server.connections.mu.Lock()
	connectionsStopping := server.connections.stopping
	server.connections.mu.Unlock()
	if !stopping || !connectionsStopping {
		t.Fatalf("blocked diagnostic left admission open: handler=%t connections=%t", stopping, connectionsStopping)
	}
	select {
	case err := <-served:
		t.Fatalf("server returned while its diagnostic writer was blocked: %v", err)
	default:
	}
	output.unblock()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("server stopped with %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish after the diagnostic writer resumed")
	}
}

type blockingWrite struct {
	mu      sync.Mutex
	writes  int
	blockAt int
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWrite) Write(content []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	block := w.writes == w.blockAt
	w.mu.Unlock()
	if block {
		close(w.blocked)
		<-w.release
	}
	return len(content), nil
}

func (w *blockingWrite) unblock() {
	w.once.Do(func() { close(w.release) })
}

func TestStartingShutdownCancelsAConnectionAcceptedBeforeStartAcknowledgement(t *testing.T) {
	backing, _ := newTestNamespace(t)
	canceling := &cancelingStatStorage{
		boundedStorageAdapter: boundedStorageAdapter{Storage: backing},
		entered:               make(chan struct{}),
		canceled:              make(chan struct{}),
	}
	handler, err := httprest.NewHandler(canceling, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(handler)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		stopped <- server.Serve(&startedListener{Listener: listener, started: started})
	}()
	<-started

	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := remote.Stat(context.Background(), "")
		requestDone <- err
	}()
	select {
	case <-canceling.entered:
	case <-time.After(time.Second):
		t.Fatal("the accepted request did not enter its handler")
	}

	shutdown := make(chan error, 1)
	go func() { shutdown <- closeStartingServer(server, listener, stopped) }()
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatalf("close starting server: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("starting shutdown did not cancel the accepted connection")
	}
	select {
	case <-canceling.canceled:
	default:
		t.Fatal("accepted request context was not canceled")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("client request did not finish after connection cancellation")
	}
}

func TestClosedStartSignalUsesGracefulShutdownBeforeTheSelectConsumesIt(t *testing.T) {
	backing, _ := newTestNamespace(t)
	controlled := &releaseOrCancelStatStorage{
		boundedStorageAdapter: boundedStorageAdapter{Storage: backing},
		entered:               make(chan struct{}),
		release:               make(chan struct{}),
		outcome:               make(chan bool, 1),
	}
	t.Cleanup(controlled.unblock)
	handler, err := httprest.NewHandler(controlled, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(handler)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveStarted := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		stopped <- server.Serve(&startedListener{Listener: listener, started: serveStarted})
	}()
	<-serveStarted
	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := remote.Stat(context.Background(), "")
		requestDone <- err
	}()
	select {
	case <-controlled.entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter the handler")
	}

	closedButUnconsumed := make(chan struct{})
	close(closedButUnconsumed)
	terminated := make(chan error, 1)
	go func() {
		terminated <- terminateServer(server, listener, stopped, closedButUnconsumed, time.Second)
	}()
	select {
	case canceled := <-controlled.outcome:
		t.Fatalf("termination completed the handler before its release; canceled=%t", canceled)
	case <-time.After(25 * time.Millisecond):
	}
	controlled.unblock()
	select {
	case err := <-terminated:
		if err != nil {
			t.Fatalf("graceful termination failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful termination did not finish after the request was released")
	}
	if canceled := <-controlled.outcome; canceled {
		t.Fatal("a closed start signal was treated as pre-start and canceled the active request")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("client request did not finish")
	}
}

type cancelingStatStorage struct {
	boundedStorageAdapter
	entered  chan struct{}
	canceled chan struct{}
}

func (s *cancelingStatStorage) Stat(ctx context.Context, _ string) (storage.Attr, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.canceled)
	return storage.Attr{}, ctx.Err()
}

type releaseOrCancelStatStorage struct {
	boundedStorageAdapter
	entered chan struct{}
	release chan struct{}
	outcome chan bool
}

func (s *releaseOrCancelStatStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	close(s.entered)
	select {
	case <-s.release:
		s.outcome <- false
		return s.Storage.Stat(ctx, path)
	case <-ctx.Done():
		s.outcome <- true
		return storage.Attr{}, ctx.Err()
	}
}

func (s *releaseOrCancelStatStorage) unblock() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

type signalOnFirstWrite struct {
	bytes.Buffer
	signal syscall.Signal
	once   sync.Once
	err    error
}

func (w *signalOnFirstWrite) Write(content []byte) (int, error) {
	written, err := w.Buffer.Write(content)
	w.once.Do(func() { w.err = syscall.Kill(os.Getpid(), w.signal) })
	return written, err
}

func TestSIGHUPReportsLocalStatusAndTheLockOutlivesServing(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	source := localSource{
		root:      root,
		workspace: "workspace",
	}
	maintenance := objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8}
	ns, err := openLocal(source, 1<<20, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes, maintenance, testLockConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ns.close() })
	handler, err := httprest.NewHandlerWithOptions(ns.namespace, ns.log, httprest.DefaultHandlerOptions())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		reader.Close()
		writer.Close()
	})
	lines := make(chan string, 4)
	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	served := make(chan error, 1)
	go func() {
		served <- withOpened(ns, func() error {
			return serve(newServer(handler), listener, ns, writer)
		})
	}()

	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := remote.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	contender, err := openLocal(source, 1<<20, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes, maintenance, lockConfig{options: locking.DefaultOptions()})
	if err == nil {
		contender.close()
		t.Fatal("a second server acquired the local-store lock while the first was serving")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("a second local-store owner was refused with %v, want EBUSY", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	line := waitForLine(t, lines, "local-store status", 2*time.Second)
	if !strings.Contains(line, "local-store status for workspace in local store ") ||
		!strings.Contains(line, `workspace "workspace" in store `) || !strings.Contains(line, "workspace bytes used") {
		t.Fatalf("SIGHUP reported %q", line)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("stop local server: %v", err)
		}
	case <-time.After(shutdownGrace / 2):
		t.Fatal("local server did not stop promptly")
	}
	reopened, err := openLocal(source, 1<<20, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes, maintenance, lockConfig{options: locking.DefaultOptions()})
	if err != nil {
		t.Fatalf("the local-store lock remained after HTTP shutdown drained: %v", err)
	}
	if err := reopened.close(); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedStatusDoesNotDelayTermination(t *testing.T) {
	handler, ns := replicableNamespace(t)
	statusStarted := make(chan struct{})
	statusCanceled := make(chan struct{})
	statusBudget := make(chan time.Duration, 1)
	var startedOnce sync.Once
	var canceledOnce sync.Once
	ns.what = "blocked status fixture"
	ns.status = func(ctx context.Context) (string, error) {
		startedOnce.Do(func() { close(statusStarted) })
		deadline, ok := ctx.Deadline()
		if !ok {
			statusBudget <- 0
		} else {
			statusBudget <- time.Until(deadline)
		}
		<-ctx.Done()
		canceledOnce.Do(func() { close(statusCanceled) })
		return "", ctx.Err()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() {
		served <- serveWithGrace(newServer(handler), listener, ns, io.Discard, 100*time.Millisecond)
	}()
	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := remote.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-statusStarted:
	case <-time.After(time.Second):
		t.Fatal("SIGHUP did not start the status query")
	}
	budget := <-statusBudget
	if budget <= 0 || budget > statusDeadline {
		t.Fatalf("status query received a deadline budget of %v, want (0, %v]", budget, statusDeadline)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("termination with a blocked status query failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("a blocked status query delayed termination")
	}
	select {
	case <-statusCanceled:
	default:
		t.Fatal("termination did not cancel the status query")
	}
}

func TestUncooperativeStatusWaitsOnlyAfterHTTPShutdown(t *testing.T) {
	for _, test := range []struct {
		name    string
		trigger func(*testing.T, *failureListener) error
		fatal   bool
	}{
		{
			name: "termination signal",
			trigger: func(t *testing.T, _ *failureListener) error {
				t.Helper()
				return syscall.Kill(os.Getpid(), syscall.SIGTERM)
			},
		},
		{
			name: "fatal Serve error",
			trigger: func(t *testing.T, listener *failureListener) error {
				t.Helper()
				failure := errors.New("listener accept failed")
				listener.Fail(failure)
				return failure
			},
			fatal: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backing, _ := newTestNamespace(t)
			handler, err := httprest.NewHandlerWithOptions(backing, nil, httprest.DefaultHandlerOptions())
			if err != nil {
				t.Fatal(err)
			}
			statusStarted := make(chan struct{})
			statusRelease := make(chan struct{})
			statusOutcome := make(chan error, 1)
			ns := opened{
				namespace:  backing,
				what:       "uncooperative status fixture",
				statusName: "test",
				status: func(ctx context.Context) (string, error) {
					close(statusStarted)
					<-statusRelease
					statusOutcome <- ctx.Err()
					return "", ctx.Err()
				},
				close: func() error { return nil },
			}
			server := newServer(handler)
			baseListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			listener := &failureListener{Listener: baseListener}
			t.Cleanup(func() {
				_ = server.Close()
				_ = listener.Close()
				select {
				case <-statusRelease:
				default:
					close(statusRelease)
				}
			})
			served := make(chan error, 1)
			go func() {
				served <- serveWithGrace(server, listener, ns, io.Discard, 100*time.Millisecond)
			}()
			remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := remote.Stat(t.Context(), ""); err != nil {
				t.Fatalf("prove server readiness: %v", err)
			}
			if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
				t.Fatal(err)
			}
			select {
			case <-statusStarted:
			case <-time.After(time.Second):
				t.Fatal("SIGHUP did not enter the status worker")
			}
			triggerErr := test.trigger(t, listener)
			if triggerErr != nil && !test.fatal {
				t.Fatal(triggerErr)
			}
			waitForListenerRefusal(t, listener.Addr().String(), 500*time.Millisecond)
			select {
			case err := <-served:
				t.Fatalf("server returned before its status worker left: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			close(statusRelease)
			if err := <-statusOutcome; !errors.Is(err, context.Canceled) {
				t.Fatalf("status worker observed %v, want context cancellation", err)
			}
			select {
			case err := <-served:
				if test.fatal {
					if !errors.Is(err, triggerErr) {
						t.Fatalf("fatal Serve returned %v, want %v", err, triggerErr)
					}
				} else if err != nil {
					t.Fatalf("termination returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not finish after its status worker left")
			}
		})
	}
}

func TestFatalServeErrorCancelsStatusBeforeDrainingHandlers(t *testing.T) {
	backing, _ := newTestNamespace(t)
	dependency := &sync.Mutex{}
	storage := &statusGateStorage{
		boundedStorageAdapter: boundedStorageAdapter{Storage: backing},
		dependency:            dependency,
	}
	handler, err := httprest.NewHandlerWithOptions(storage, nil, httprest.DefaultHandlerOptions())
	if err != nil {
		t.Fatal(err)
	}
	statusStarted := make(chan struct{})
	statusCanceled := make(chan struct{})
	ns := opened{
		namespace:  storage,
		what:       "blocked status fixture",
		statusName: "test",
		status: func(ctx context.Context) (string, error) {
			dependency.Lock()
			close(statusStarted)
			<-ctx.Done()
			dependency.Unlock()
			close(statusCanceled)
			return "", ctx.Err()
		},
		close: func() error { return nil },
	}
	server := newServer(handler)
	baseListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &failureListener{Listener: baseListener}
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	served := make(chan error, 1)
	go func() { served <- serveWithGrace(server, listener, ns, io.Discard, 100*time.Millisecond) }()
	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Stat(t.Context(), ""); err != nil {
		t.Fatalf("prove server readiness: %v", err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-statusStarted:
	case <-time.After(time.Second):
		t.Fatal("SIGHUP did not enter the status dependency")
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := remote.Stat(context.Background(), "")
		requestDone <- err
	}()
	waitForActiveHandlers(t, server.drain, 1, time.Second)
	failure := errors.New("listener accept failed")
	listener.Fail(failure)
	select {
	case err := <-served:
		if !errors.Is(err, failure) {
			t.Fatalf("server returned %v, want %v", err, failure)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("fatal Serve exit drained a handler before canceling status")
	}
	select {
	case <-statusCanceled:
	default:
		t.Fatal("fatal Serve exit did not cancel status")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request behind the status dependency did not finish")
	}
}

type statusGateStorage struct {
	boundedStorageAdapter
	dependency *sync.Mutex
}

func (s *statusGateStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	s.dependency.Lock()
	defer s.dependency.Unlock()
	return s.Storage.Stat(ctx, path)
}

type failureListener struct {
	net.Listener
	mu      sync.Mutex
	failure error
}

func (l *failureListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		return connection, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failure != nil {
		return nil, l.failure
	}
	return nil, err
}

func (l *failureListener) Fail(err error) {
	l.mu.Lock()
	l.failure = err
	l.mu.Unlock()
	_ = l.Listener.Close()
}

func waitForActiveHandlers(t *testing.T, handler *drainingHandler, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		handler.mu.Lock()
		active := handler.active
		handler.mu.Unlock()
		if active >= want {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("handler count stayed at %d, want at least %d", active, want)
		}
	}
}

func waitForListenerRefusal(t *testing.T, address string, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			connection, err := net.DialTimeout("tcp", address, 25*time.Millisecond)
			if err != nil {
				return
			}
			_ = connection.Close()
		case <-deadline.C:
			t.Fatalf("listener %s still admitted connections while status was blocked", address)
		}
	}
}

func waitForLine(t *testing.T, lines <-chan string, contains string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line := <-lines:
			if strings.Contains(line, contains) {
				return line
			}
		case <-deadline:
			t.Fatalf("no output line containing %q arrived within %v", contains, timeout)
		}
	}
}

func TestShutdownTimeoutKeepsTheLockUntilABlockedRequestLeaves(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	source := localSource{
		root:      root,
		workspace: "workspace",
	}
	maintenance := objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8}
	ns, err := openLocal(source, 1<<20, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes, maintenance, testLockConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ns.close() })
	blocked := &blockingStatStorage{
		boundedStorageAdapter: boundedStorageAdapter{Storage: ns.namespace},
		entered:               make(chan struct{}),
		release:               make(chan struct{}),
	}
	t.Cleanup(blocked.unblock)
	handler, err := httprest.NewHandlerWithOptions(blocked, ns.log, httprest.DefaultHandlerOptions())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const grace = 25 * time.Millisecond
	served := make(chan error, 1)
	go func() {
		served <- withOpened(ns, func() error {
			return serveWithGrace(newServer(handler), listener, ns, io.Discard, grace)
		})
	}()
	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan error, 1)
	go func() {
		_, err := remote.Stat(context.Background(), "")
		requestDone <- err
	}()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("the request did not enter the blocked handler")
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		t.Fatalf("server returned while its handler was blocked: %v", err)
	case <-time.After(2 * grace):
	}

	contender, err := openLocal(source, 1<<20, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes, maintenance, lockConfig{options: locking.DefaultOptions()})
	if err == nil {
		contender.close()
		t.Fatal("shutdown released the local-store lock while a handler was still running")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("the contender failed with %v, want EBUSY", err)
	}
	blocked.unblock()
	select {
	case err := <-served:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timed-out shutdown returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish after its blocked handler left")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("blocked request did not finish")
	}
	reopened, err := openLocal(source, 1<<20, sqlite.DefaultObjectLimits(), sqlite.DefaultMaxReaderConnections,
		sqlite.DefaultMaxSnapshotReaderConnections, sqlite.DefaultMaxIntegrityRecords,
		sqlite.DefaultMaxIntegrityBytes, maintenance, lockConfig{options: locking.DefaultOptions()})
	if err != nil {
		t.Fatalf("the lock remained after the handler drained: %v", err)
	}
	if err := reopened.close(); err != nil {
		t.Fatal(err)
	}
}

type blockingStatStorage struct {
	boundedStorageAdapter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type boundedStorageAdapter struct {
	storage.Storage
}

func (s boundedStorageAdapter) LockService() locking.Service {
	return s.Storage.(interface{ LockService() locking.Service }).LockService()
}

func (s boundedStorageAdapter) CheckBounded() error {
	return s.Storage.(storage.BoundedStorage).CheckBounded()
}

func (s boundedStorageAdapter) ListBounded(
	ctx context.Context,
	path string,
	result *storage.ListResult,
) error {
	return s.Storage.(storage.BoundedStorage).ListBounded(ctx, path, result)
}

func (s boundedStorageAdapter) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	return s.Storage.(storage.BoundedStorage).ReadBounded(ctx, path, maxBytes)
}

func (s *blockingStatStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Storage.Stat(ctx, path)
}

func (s *blockingStatStorage) unblock() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

// Stopping this server with a mount attached must be prompt and must succeed.
//
// A change stream never becomes idle, and http.Server.Shutdown waits for connections that
// are, so a server that did not tell its streams to let go would wait out the whole of
// shutdownGrace and then return the deadline it missed — which this command reports as a
// failure and exits 1 on. An operator stopping a server that is working perfectly would be
// told it did not stop properly, every time, for as long as anything was mounted.
//
// The whole path is exercised: the server this command builds, the signal handling it
// installs, the grace it allows, and the value it returns.
func TestStoppingWithAReplicaAttachedIsPromptAndClean(t *testing.T) {
	handler, ns := replicableNamespace(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- serve(newServer(handler), listener, ns, io.Discard) }()

	remote, err := httprest.Dial("http://"+listener.Addr().String(), &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Subscribing proves two things at once: that the server is serving, so the signal
	// handling it installs before it starts is already in place, and that a stream really
	// is attached when the signal arrives.
	sub, err := remote.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal this process: %v", err)
	}

	// Comfortably inside the grace: a server that waited for the stream would take all of
	// it and then some.
	prompt := shutdownGrace / 2
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("stopping with a replica attached failed: %v", err)
		}
	case <-time.After(prompt):
		t.Fatalf("stopping took longer than %v with a replica attached, and this command allows %v before it gives up and reports failure", prompt, shutdownGrace)
	}

	// And the replica is told the server went away, rather than left to work it out from a
	// connection that stopped.
	if _, err := sub.Next(); !errors.Is(err, httprest.ErrServerStopping) {
		t.Fatalf("the replica was given %v, want it to be told the server was stopping", err)
	}
}

func replicableNamespace(t *testing.T) (*httprest.Handler, opened) {
	t.Helper()
	namespace, meta := newTestNamespace(t)
	handler, err := httprest.NewHandler(namespace, meta)
	if err != nil {
		t.Fatal(err)
	}
	return handler, opened{namespace: namespace, log: meta, close: namespace.Close}
}
