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
	"path/filepath"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
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
	timeout := flags.Duration("timeout", 30*time.Second, "maximum time for one remote exchange or file cleanup attempt;\n"+
		"a blocking advisory lock can wait across exchanges while its session remains valid")
	maxFileSize := clientPositiveSizeFlag{bytes: fuse.DefaultMaxFileSize}
	flags.Var(&maxFileSize, "max-file-size", "largest file this mount accepts for writes and truncation, as SIZE")
	fileSession := storage.DefaultFileSessionOptions()
	fileLease := flags.Duration("file-session-lease", fileSession.Lease, "renewable lifetime requested for this mount's open files and advisory locks")
	fileHistory := flags.Duration("file-session-history", fileSession.History, "action history interval requested for file and advisory-lock reconciliation")
	replicaDir := flags.String("replica-dir", "", "directory to keep the local copy of the namespace's metadata under.\n"+
		"A directory of its own is made inside it, readable only by this user, and\n"+
		"removed when the mountpoint is detached. The default is the system\n"+
		"temporary directory, which on many systems is held in memory — give a path\n"+
		"on disk for a workspace whose tree is large.")
	confirmationDefaults := replicated.DefaultOptions()
	confirmationGrace := flags.Duration("confirmation-grace", confirmationDefaults.ConfirmationGrace,
		"maximum time a successful mutation waits for its replication barrier before failing with EIO")
	maxActiveConfirmations := flags.Int("max-active-mutation-confirmations", confirmationDefaults.MaxActiveConfirmations,
		"maximum mutations concurrently retaining fixed-size confirmation state")
	maxWaitingConfirmations := flags.Int("max-waiting-mutation-confirmations", confirmationDefaults.MaxWaitingConfirmations,
		"maximum callers waiting for mutation confirmation capacity; additional mutations fail with EAGAIN")
	dialDefaults := httprest.DefaultDialOptions()
	maxHTTPFrameBytes := clientPositiveSizeFlag{bytes: dialDefaults.MaxFrameBytes}
	flags.Var(&maxHTTPFrameBytes, "http-max-frame-bytes",
		"maximum encoded replication frame retained by the client, as SIZE")
	debug := flags.Bool("debug", false, "trace every kernel request and reply to standard error")
	flags.Usage = func() {
		fmt.Fprint(errOut, "usage: remote-fs -server URL -mountpoint DIR\n\n"+
			"Presents the namespace served at URL as an ordinary directory tree at DIR.\n"+
			"Runs until interrupted, then detaches DIR.\n\n"+
			"A copy of the namespace's tree is kept locally and fed by a stream of the\n"+
			"changes the server records. Named lookups and directory lists use that copy;\n"+
			"inode attributes and open-file operations are confirmed by the server.\n"+
			"The copy is answered from only while that stream is being read: if\n"+
			"it breaks, every operation fails until it is back, and nothing stale is served.\n"+
			"Mounting waits for the copy to be built. A namespace that keeps no change log is\n"+
			"mounted without one, and every operation on it is a request to the server.\n\n"+
			"Open file descriptors retain their file object through rename and unlink.\n"+
			"Writes and truncation are confirmed by the server before returning success.\n"+
			"File sessions renew while mounted; explicit flock and POSIX record locks are\n"+
			"advisory and do not restrict applications that do not participate.\n\n")
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
	fileSession.Lease, fileSession.History = *fileLease, *fileHistory
	if err := fileSession.Check(); err != nil {
		return fmt.Errorf("invalid file session limits: %w", err)
	}
	confirmationOptions := replicated.Options{
		ConfirmationGrace:       *confirmationGrace,
		MaxActiveConfirmations:  *maxActiveConfirmations,
		MaxWaitingConfirmations: *maxWaitingConfirmations,
	}
	if err := confirmationOptions.Check(); err != nil {
		return fmt.Errorf("invalid mutation confirmation limits: %w", err)
	}

	namespace, err := dialNamespace(*serverURL, *timeout, maxHTTPFrameBytes.bytes)
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

	served, release, err := replicateWithOptions(ctx, namespace, *replicaDir, errOut, confirmationOptions)
	if err != nil {
		return err
	}
	m, err := fuse.New(*mountpoint, served, fuse.Options{
		Logger:       log.New(errOut, "remote-fs: ", log.LstdFlags),
		Debug:        *debug,
		FlushTimeout: *timeout,
		MaxFileSize:  maxFileSize.bytes,
		FileSession:  &fileSession,
	})
	if err != nil {
		if m != nil {
			cleanupErr := unmountPatiently(m, *mountpoint, errOut)
			select {
			case <-m.Done():
				return errors.Join(err, cleanupErr, release())
			default:
				return errors.Join(err, cleanupErr)
			}
		}
		return errors.Join(err, release())
	}
	fmt.Fprintf(errOut, "remote-fs: %s mounted at %s\n", *serverURL, *mountpoint)

	err = wait(ctx, stop, m, *mountpoint, errOut)
	select {
	case <-m.Done():
		return errors.Join(err, release())
	default:
		return err
	}
}

// callerClient is the HTTP client every request to the server is made with.
//
// The timeout is the one this program decides on, because the transport cannot: with no
// timeout anywhere, an operation against a server that has gone away waits forever instead
// of failing, and a filesystem that hangs is worse to be behind than one that reports an
// error.
//
// It bounds a whole exchange, the reading of the response included, which is why the
// transport drops it for the two operations that are streams — a change stream is meant to
// stay open with nothing on it. The header timeout below is what still bounds those: it
// covers getting an answer at all, which is where a server that is not answering shows up,
// and leaves the body unbounded, which is where a healthy stream lives.
func callerClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = timeout
	return &http.Client{Timeout: timeout, Transport: transport}
}

func dialNamespace(baseURL string, timeout time.Duration, maxFrameBytes int64) (*httprest.Storage, error) {
	options := httprest.DefaultDialOptions()
	options.MaxFrameBytes = maxFrameBytes
	return httprest.DialWithOptions(baseURL, callerClient(timeout), options)
}

// replicate builds the local copy of the namespace's metadata, and returns the storage the
// mountpoint is served from together with what releases it.
//
// It blocks until the copy has been built. There is no mode in which the mountpoint comes up
// first and the copy catches up behind it: until the copy is there, an operation answered
// from it would be answered from an empty tree, and a mount that reported a namespace as
// empty is the failure this whole system is arranged to avoid.
//
// A namespace that does not publish a change log answers ENOSYS and is mounted with
// direct remote metadata access. Transport failures remain errors rather than selecting
// that mode.
func replicate(ctx context.Context, namespace *httprest.Storage, where string, errOut io.Writer) (storage.Storage, func() error, error) {
	return replicateWithOptions(ctx, namespace, where, errOut, replicated.DefaultOptions())
}

func replicateWithOptions(
	ctx context.Context,
	namespace *httprest.Storage,
	where string,
	errOut io.Writer,
	options replicated.Options,
) (storage.Storage, func() error, error) {
	if err := options.Check(); err != nil {
		return nil, nil, fmt.Errorf("invalid mutation confirmation limits: %w", err)
	}
	dir, database, err := privateDatabase(where)
	if err != nil {
		return nil, nil, err
	}
	discard := func() error {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("the copy of the namespace's metadata is still at %s: %w", dir, err)
		}
		return nil
	}

	replica, err := sqlite.OpenReplica(ctx, database)
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("making room for a copy of the namespace's metadata: %w", err), discard())
	}

	started := time.Now()
	served, err := replicated.NewWithOptions(ctx, replica, namespace, options)
	switch {
	case errors.Is(err, syscall.ENOSYS):
		if closeErr := replica.Close(); closeErr != nil {
			return nil, nil, errors.Join(err, closeErr)
		}
		if discardErr := discard(); discardErr != nil {
			return nil, nil, discardErr
		}
		fmt.Fprintln(errOut, "remote-fs: this namespace keeps no record of what changes in it, so every operation is a request to the server")
		return namespace, func() error { return nil }, nil
	case err != nil:
		if closeErr := replica.Close(); closeErr != nil {
			return nil, nil, errors.Join(err, closeErr)
		}
		return nil, nil, errors.Join(fmt.Errorf("copying the namespace's metadata: %w", err), discard())
	}
	fmt.Fprintf(errOut, "remote-fs: copied the namespace's metadata in %v\n", time.Since(started).Round(time.Millisecond))

	return served, func() error {
		if err := served.Close(); err != nil {
			return fmt.Errorf("releasing the copy of the namespace's metadata: %w", err)
		}
		return discard()
	}, nil
}

// privateDatabase makes the directory the copy is held in and the file it is held in, both
// out of everybody else's reach.
//
// R-SEC-3 asks for both halves of that, and the contents are what make it worth asking: the
// names, the sizes and the times of somebody's whole workspace. The directory is created with
// only its owner able to enter it, and with a name nothing could have taken first — os.MkdirTemp
// fails rather than opening one that is already there, so a path somebody planted is a failure
// to mount rather than somewhere this writes into.
func privateDatabase(where string) (dir, database string, err error) {
	dir, err = os.MkdirTemp(where, "remote-fs-replica-")
	if err != nil {
		return "", "", fmt.Errorf("making a directory for the copy of the namespace's metadata: %w", err)
	}
	database = filepath.Join(dir, "tree.db")
	if err := createPrivately(database); err != nil {
		os.RemoveAll(dir)
		return "", "", fmt.Errorf("making a file for the copy of the namespace's metadata: %w", err)
	}
	return dir, database, nil
}

// createPrivately makes an empty file that only its owner can read or write, and refuses a
// path anything is already at rather than opening it.
//
// The file is made here rather than left to SQLite so that its mode is decided rather than
// inherited from whatever umask this process was started with; SQLite gives the write-ahead
// log and the shared-memory file beside it the mode of the database file, so deciding it once
// decides it for all three. O_EXCL is what makes "already there" a refusal, and it refuses a
// symbolic link without following it — the case that matters most, because following one
// would write the shape of somebody's workspace wherever it pointed.
func createPrivately(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	return file.Close()
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
	stopped := make(chan error, 1)
	go func() { stopped <- m.Wait() }()

	select {
	case err := <-stopped:
		// Somebody detached the mountpoint from outside, or the kernel tore the
		// connection down. Either way there is nothing left to detach.
		fmt.Fprintf(errOut, "remote-fs: %s is no longer mounted\n", mountpoint)
		return err
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
	finish := func() error {
		if err := m.Wait(); err != nil {
			return fmt.Errorf("releasing files after detaching %s: %w", mountpoint, err)
		}
		fmt.Fprintf(errOut, "remote-fs: %s detached\n", mountpoint)
		return nil
	}
	deadline := time.Now().Add(unmountGrace)
	for attempt := 0; ; attempt++ {
		err := m.Unmount()
		if err == nil {
			return finish()
		}
		select {
		case <-m.Done():
			return finish()
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s is still mounted after %v: %w\n"+
				"\tsomething is still using it; detach it with `fusermount3 -u %s` once nothing is",
				mountpoint, unmountGrace, err, mountpoint)
		}
		time.Sleep(min(time.Duration(attempt+1)*50*time.Millisecond, 500*time.Millisecond))
	}
}
