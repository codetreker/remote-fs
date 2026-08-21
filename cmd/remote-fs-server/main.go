// Command remote-fs-server serves one local directory as a namespace over HTTP, optionally
// holding it under a byte allowance.
//
// It parses a command line, wires four packages together, reports failures, and stops
// when asked. Every behaviour it appears to have belongs to something underneath it: the
// namespace is packages/storage/localdir, the allowance is packages/storage/limited, the
// protocol is packages/transport/httprest.
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

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// shutdownGrace is how long requests already in flight have to finish once a signal has
// arrived. A write that is cut off here would leave the caller unable to tell whether it
// happened, which is the one answer this system must never give.
const shutdownGrace = 5 * time.Second

// noticeableWalk is how long measuring the namespace has to take before the startup line
// says how long it took. That walk is the one thing standing between the command line and
// the open listener, so a pause an operator notices is one they are owed the cause of;
// below this there is nothing to explain.
const noticeableWalk = 500 * time.Millisecond

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
	var quota sizeFlag
	flags.Var(&quota, "quota", "allowance the namespace is held under, as a `SIZE`: a whole number of bytes,\n"+
		"optionally with one of the suffixes\n"+
		suffixes+".\n"+
		"Without it no write is refused, and the figures reported are the host\n"+
		"filesystem's own.")
	flags.Usage = func() {
		fmt.Fprint(errOut, "usage: remote-fs-server -listen ADDR -dir DIR [-quota SIZE]\n\n"+
			"Serves DIR as one namespace over HTTP. Mount it with remote-fs.\n\n"+
			"Given -quota, SIGHUP measures DIR again and replaces the count of what it holds,\n"+
			"which is the way back from a count that drifted because DIR was modified behind\n"+
			"this server's back.\n\n"+
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

	// Held under an allowance, or served exactly as it is. Measuring what the namespace
	// already holds is the only step here that can take real time, so the line below says
	// how long it took whenever that is worth knowing.
	var namespace storage.Storage = backing
	var held *limited.Storage
	allowance := ""
	if quota.bytes != 0 {
		started := time.Now()
		if held, err = limited.New(context.Background(), backing, quota.bytes); err != nil {
			return err
		}
		walk := time.Since(started)

		// Read before anything is served, and a failure here is a failure to start. A
		// namespace whose room cannot be read cannot be held to an allowance at all: a mount
		// that is given no figure checks no write against one, so serving anyway would offer
		// a workspace that claims a limit and enforces none of it.
		space, err := held.Space(context.Background())
		if err != nil {
			return err
		}
		measured := ""
		if walk >= noticeableWalk {
			measured = fmt.Sprintf(" (measured in %v)", rounded(walk))
		}
		namespace = held
		allowance = fmt.Sprintf(" under an allowance of %d bytes, %d of them taken%s,", space.Total, space.Used, measured)
	}

	handler, err := httprest.NewHandler(namespace)
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
	// The address stays last on the line: it is what a person copies out of it, and what
	// anything reading this output parses it for.
	fmt.Fprintf(errOut, "remote-fs-server: serving %s%s at http://%s\n", *dir, allowance, listener.Addr())

	return serve(&http.Server{Handler: handler}, listener, held, errOut)
}

// serve runs until a request loop fails or a signal arrives. held is the allowance the
// namespace is under, and nil when the server was given none.
func serve(httpServer *http.Server, listener net.Listener, held *limited.Storage, errOut io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// SIGHUP is delivered on a channel of its own rather than through the context above,
	// because it asks for something the server survives rather than for the end of it.
	hangups := make(chan os.Signal, 1)
	signal.Notify(hangups, syscall.SIGHUP)
	defer signal.Stop(hangups)

	stopped := make(chan error, 1)
	go func() { stopped <- httpServer.Serve(listener) }()

	for {
		select {
		case err := <-stopped:
			return err
		case <-hangups:
			// The walk runs here rather than on a goroutine of its own: it stalls every
			// writer for its duration, and two of them at once would stall them for twice
			// as long to arrive at one answer. Connections keep being accepted meanwhile,
			// and each request waits exactly where it would have waited.
			recount(context.Background(), held, errOut)
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
}

// recount measures the served namespace again and replaces the count of what it holds.
//
// It is the operator's way back from a count that drifted, which happens when the served
// directory is modified behind this server's back — an unsupported use, and the only one
// the count cannot follow. The walk stalls every writer for as long as it runs, which is
// why it happens when somebody asks for it and never on a schedule.
//
// The count before and the count after are both reported. A repair that says nothing
// leaves the operator with no way to tell whether it was needed, or whether it did
// anything.
func recount(ctx context.Context, held *limited.Storage, errOut io.Writer) {
	if held == nil {
		fmt.Fprintln(errOut, "remote-fs-server: SIGHUP asks for a recount, and this server holds its namespace under no allowance; -quota gives it one")
		return
	}

	// The count as it stands is read first, because a repair that cannot be shown against
	// what it replaced is one nobody can act on. Failing to read it therefore stops the
	// walk from happening at all rather than producing a figure with nothing to compare.
	before, err := held.Space(ctx)
	if err != nil {
		fmt.Fprintf(errOut, "remote-fs-server: what the namespace holds cannot be read, so it was not recounted and the count stands: %v\n", err)
		return
	}
	started := time.Now()
	if err := held.Recount(ctx); err != nil {
		fmt.Fprintf(errOut, "remote-fs-server: recounting the namespace failed, and the count from before it stands: %v\n", err)
		return
	}
	walk := time.Since(started)
	after, err := held.Space(ctx)
	if err != nil {
		fmt.Fprintf(errOut, "remote-fs-server: the namespace was recounted, and what it now holds cannot be read: %v\n", err)
		return
	}
	fmt.Fprintf(errOut, "remote-fs-server: recounted the namespace in %v: %d bytes taken, where the count said %d\n",
		rounded(walk), after.Used, before.Used)
}

// rounded trims a measured interval to what is worth reading. Milliseconds are the useful
// unit for a tree walk, and one that finished inside a millisecond is reported in the unit
// it took rather than as no time at all.
func rounded(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return d.Round(time.Microsecond)
	}
	return d.Round(time.Millisecond)
}
