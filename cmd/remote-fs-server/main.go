// Command remote-fs-server serves one local directory as a namespace over HTTP.
//
// It parses a command line, wires three packages together, reports failures, and stops
// when asked. Every behaviour it appears to have belongs to something underneath it: the
// namespace is packages/storage/localdir, the protocol is packages/transport/httprest.
//
// R-INT-2 forbids a package from printing, installing signal handlers or exiting the
// process. This is not a package. Those three things are exactly what a program is for,
// and keeping them here is how the packages stay free of them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// shutdownGrace is how long requests already in flight have to finish once a signal has
// arrived. A write that is cut off here would leave the caller unable to tell whether it
// happened, which is the one answer this system must never give.
const shutdownGrace = 5 * time.Second

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		// A usage error has already been reported in full by the flag package; saying it
		// again in different words would only invite the reader to look for a second
		// mistake.
		if !errors.Is(err, errUsage) {
			fmt.Fprintf(os.Stderr, "remote-fs-server: %v\n", err)
		}
		os.Exit(1)
	}
}

// errUsage marks a command line that was rejected and already explained.
var errUsage = errors.New("the command line was rejected")

func run(args []string, errOut io.Writer) error {
	flags := flag.NewFlagSet("remote-fs-server", flag.ContinueOnError)
	flags.SetOutput(errOut)
	listen := flags.String("listen", "", "address to accept connections on, as host:port")
	dir := flags.String("dir", "", "existing directory whose contents are served as the namespace")
	flags.Usage = func() {
		fmt.Fprint(errOut, "usage: remote-fs-server -listen ADDR -dir DIR\n\n"+
			"Serves DIR as one namespace over HTTP. Mount it with remote-fs.\n\n"+
			"There is no authentication and no authorization: anything that can connect can\n"+
			"read and write everything under DIR. Serve only on a trusted network.\n\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errUsage
	}
	if extra := flags.Args(); len(extra) > 0 {
		return fmt.Errorf("%q is not an argument this command takes; everything is a flag", extra[0])
	}
	if *listen == "" {
		return errors.New("-listen is required: the address to accept connections on")
	}
	if *dir == "" {
		return errors.New("-dir is required: the directory to serve")
	}

	backing, err := localdir.New(*dir)
	if err != nil {
		return err
	}
	handler, err := httprest.NewHandler(backing)
	if err != nil {
		return err
	}

	// The listener is opened here rather than by http.Server.ListenAndServe so that an
	// address already in use is reported before anything claims to be serving, and so
	// that a port of 0 can be resolved and printed.
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	fmt.Fprintf(errOut, "remote-fs-server: serving %s at http://%s\n", *dir, listener.Addr())

	return serve(&http.Server{Handler: handler}, listener, errOut)
}

// serve runs until a request loop fails or a signal arrives.
func serve(httpServer *http.Server, listener net.Listener, errOut io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stopped := make(chan error, 1)
	go func() { stopped <- httpServer.Serve(listener) }()

	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		// Disarmed before shutting down: a second signal from an operator who has decided
		// not to wait should kill the process the way it normally would.
		stop()
		fmt.Fprintln(errOut, "remote-fs-server: stopping")
		graceful, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		return httpServer.Shutdown(graceful)
	}
}
