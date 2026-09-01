package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

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

// replicableNamespace builds what this command serves when the namespace it was given keeps
// a change log.
//
// The tree and the log are not the same store here, which they would be in the command
// itself. Nothing in this test reads one against the other — what is under test is how the
// server stops — and pairing a local directory with a real log keeps it away from the blob
// service the command's own replicable path needs.
func replicableNamespace(t *testing.T) (*httprest.Handler, opened) {
	t.Helper()
	backing, err := localdir.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	log, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "metastore.db"), "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })

	handler, err := httprest.NewHandler(backing, log)
	if err != nil {
		t.Fatal(err)
	}
	return handler, opened{namespace: backing, log: log, close: func() error { return nil }}
}
