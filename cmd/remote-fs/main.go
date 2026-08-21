// Command remote-fs mounts a server's namespace at a local directory.
//
// It parses a command line, wires two packages together, reports failures, and detaches
// the mountpoint when asked. Everything a user of the mountpoint experiences belongs to
// packages/fuse and packages/transport/httprest.
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
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// unmountGrace is how long the mountpoint is kept being asked to detach after a signal.
//
// Detaching fails while anything still holds a file or a working directory inside the
// mount, and that usually clears within moments of the shell being told to stop. Leaving
// the mountpoint attached is the failure worth this wait: until somebody detaches it by
// hand, every access to that path fails (R-ERR-5).
const unmountGrace = 10 * time.Second

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		// A usage error has already been reported in full by the flag package; saying it
		// again in different words would only invite the reader to look for a second
		// mistake.
		if !errors.Is(err, errUsage) {
			fmt.Fprintf(os.Stderr, "remote-fs: %v\n", err)
		}
		os.Exit(1)
	}
}

// errUsage marks a command line that was rejected and already explained.
var errUsage = errors.New("the command line was rejected")

func run(args []string, errOut io.Writer) error {
	flags := flag.NewFlagSet("remote-fs", flag.ContinueOnError)
	flags.SetOutput(errOut)
	serverURL := flags.String("server", "", "base URL of the server holding the namespace, as http://host:port")
	mountpoint := flags.String("mountpoint", "", "existing directory to present the namespace at")
	// The transport refuses to invent a timeout because it cannot know how long the
	// caller is willing to wait. Deciding that is this program's job, and the decision has
	// to be a number: with no timeout anywhere, an operation against a server that has
	// gone away waits forever instead of failing, and a filesystem that hangs is worse to
	// be behind than one that reports an error.
	timeout := flags.Duration("timeout", 30*time.Second, "how long one operation may take before it fails as an I/O error")
	debug := flags.Bool("debug", false, "trace every kernel request and reply to standard error")
	flags.Usage = func() {
		fmt.Fprint(errOut, "usage: remote-fs -server URL -mountpoint DIR\n\n"+
			"Presents the namespace served at URL as an ordinary directory tree at DIR.\n"+
			"Runs until interrupted, then detaches DIR.\n\n"+
			"Nothing is cached: every operation is a request to the server, so what is read\n"+
			"is what the server holds at that moment.\n\n")
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
	if *serverURL == "" {
		return errors.New("-server is required: the URL of the server holding the namespace")
	}
	if *mountpoint == "" {
		return errors.New("-mountpoint is required: the directory to mount at")
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, not %v", *timeout)
	}

	namespace, err := httprest.Dial(*serverURL, &http.Client{Timeout: *timeout})
	if err != nil {
		return err
	}

	// Armed before the mountpoint exists, so that a signal arriving during the mount
	// cannot kill the process at the one moment when doing so would leave a mountpoint
	// attached to a filesystem nobody is serving (R-ERR-5).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Reaching the namespace once before mounting turns "that server is not there" into
	// a message at the moment the operator is looking at the terminal. Skipping it would
	// still be safe — every operation would fail with an I/O error rather than inventing
	// an answer — but it would present as a mounted filesystem that is somehow broken,
	// rather than as a server that was never reached.
	if err := reach(ctx, namespace, *timeout); err != nil {
		return fmt.Errorf("the namespace at %s cannot be reached: %w", *serverURL, err)
	}

	m, err := fuse.New(*mountpoint, namespace, fuse.Options{
		Logger: log.New(errOut, "remote-fs: ", log.LstdFlags),
		Debug:  *debug,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(errOut, "remote-fs: %s mounted at %s\n", *serverURL, *mountpoint)

	return wait(ctx, stop, m, *mountpoint, errOut)
}

// reach asks the namespace for its root once, so that a server nobody is listening on is
// reported now rather than as an unexplained failure on the first `ls`.
func reach(ctx context.Context, namespace storage.Storage, timeout time.Duration) error {
	probe, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := namespace.Stat(probe, "")
	return err
}

// wait runs until a signal arrives or the mount stops serving on its own, and detaches
// the mountpoint in the first case.
func wait(ctx context.Context, stop context.CancelFunc, m *fuse.Mount, mountpoint string, errOut io.Writer) error {
	stopped := make(chan struct{})
	go func() { m.Wait(); close(stopped) }()

	select {
	case <-stopped:
		// Somebody detached the mountpoint from outside, or the kernel tore the
		// connection down. Either way there is nothing left to detach.
		fmt.Fprintf(errOut, "remote-fs: %s is no longer mounted\n", mountpoint)
		return nil
	case <-ctx.Done():
		// Disarmed before unmounting: a second signal from an operator who has decided
		// not to wait should kill the process the way it normally would.
		stop()
		return unmountPatiently(m, mountpoint, errOut)
	}
}

// unmountPatiently detaches the mountpoint, retrying until unmountGrace has passed.
//
// Every failure is retried rather than only the one that means "still in use", because
// detaching runs fusermount and reports what it printed, which does not carry an errno
// this side could match on. Retrying an error that will never clear costs the operator
// the grace period; not retrying one that would have cleared costs them a mountpoint.
func unmountPatiently(m *fuse.Mount, mountpoint string, errOut io.Writer) error {
	fmt.Fprintf(errOut, "remote-fs: detaching %s\n", mountpoint)
	deadline := time.Now().Add(unmountGrace)
	for attempt := 0; ; attempt++ {
		err := m.Unmount()
		if err == nil {
			m.Wait()
			fmt.Fprintf(errOut, "remote-fs: %s detached\n", mountpoint)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s is still mounted after %v: %w\n"+
				"\tsomething is still using it; detach it with `fusermount3 -u %s` once nothing is",
				mountpoint, unmountGrace, err, mountpoint)
		}
		time.Sleep(min(time.Duration(attempt+1)*50*time.Millisecond, 500*time.Millisecond))
	}
}
