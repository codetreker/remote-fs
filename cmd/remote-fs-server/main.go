// Command remote-fs-server serves one namespace over HTTP, optionally holding it under a
// byte allowance.
//
// It parses a command line, wires packages together, reports failures, and stops when
// asked. Every behaviour it appears to have belongs to something underneath it: the
// namespace is packages/storage/localdir or packages/storage/objectstore, the allowance is
// packages/storage/limited or the metastore's own accounting, the protocol is
// packages/transport/httprest.
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

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
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
	container := flags.String("blob-container", "", "name of the Azure Blob container holding the namespace's contents.\n"+
		"Credentials come from AZURE_STORAGE_CONNECTION_STRING, never from a flag:\n"+
		"a flag is visible to everyone who can list processes.")
	prefix := flags.String("blob-prefix", "", "key prefix within the container, so one container may hold several\n"+
		"namespaces")
	database := flags.String("metastore", "", "path to the SQLite database holding the namespace's tree")
	workspace := flags.String("workspace", "", "name of the namespace within the metastore")
	var quota sizeFlag
	flags.Var(&quota, "quota", "allowance the namespace is held under, as a `SIZE`: a whole number of bytes,\n"+
		"optionally with one of the suffixes\n"+
		suffixes+".\n"+
		"Without it no write is refused, and the figures reported are the host\n"+
		"filesystem's own for a directory, and nothing at all for a blob container,\n"+
		"which has no capacity to report.")
	flags.Usage = func() {
		fmt.Fprint(errOut, "usage: remote-fs-server -listen ADDR -dir DIR [-quota SIZE]\n"+
			"       remote-fs-server -listen ADDR -blob-container NAME -metastore PATH -workspace NAME [-blob-prefix PREFIX] [-quota SIZE]\n\n"+
			"Serves one namespace over HTTP. Mount it with remote-fs.\n\n"+
			"The namespace is either a local directory, or a namespace whose file contents\n"+
			"live in an Azure Blob container and whose tree lives in a SQLite database. The\n"+
			"second form reads its credentials from AZURE_STORAGE_CONNECTION_STRING.\n\n"+
			"Given -quota with -dir, SIGHUP measures DIR again and replaces the count of what\n"+
			"it holds, which is the way back from a count that drifted because DIR was\n"+
			"modified behind this server's back. A namespace in a blob container keeps its\n"+
			"count exactly and has nothing to repair.\n\n"+
			"There is no authentication and no authorization: anything that can connect can\n"+
			"read and write everything in the namespace. Serve only on a trusted network.\n\n")
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

	ns, err := open(*dir, blobSource{
		container: *container,
		prefix:    *prefix,
		database:  *database,
		workspace: *workspace,
	}, quota.bytes)
	if err != nil {
		return err
	}
	defer ns.close()

	// The log is what a mount replicates the namespace's metadata from. A namespace with no
	// metastore behind it has none, and is served with a nil one: its replication endpoints
	// then answer ENOSYS, and a mount of it goes on making a request for every operation.
	handler, err := httprest.NewHandler(ns.namespace, ns.log)
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
	fmt.Fprintf(errOut, "remote-fs-server: serving %s%s at http://%s\n", ns.what, ns.allowance, listener.Addr())

	return serve(&http.Server{Handler: handler}, listener, ns, errOut)
}

// blobSource names the parts of a namespace whose contents are in a blob container. The
// credentials are deliberately absent: they arrive through the environment, because a flag
// is visible to every process on the machine that can read /proc, and R-SEC-4 puts
// credentials out of reach of anything that reads like a log.
type blobSource struct {
	container string
	prefix    string
	database  string
	workspace string
}

// given reports whether this form was chosen at all.
func (b blobSource) given() bool { return b.container != "" }

// connectionEnv is where the credentials for a blob container are read from. The name is
// the one the Azure tooling already uses, so an operator who has configured any other Azure
// client has configured this one.
const connectionEnv = "AZURE_STORAGE_CONNECTION_STRING"

// opened is a namespace ready to be served.
type opened struct {
	namespace storage.Storage

	// log is the record of what changes in the namespace, and nil for a namespace that keeps
	// none. Only a namespace held in a metastore has one: the log's positions are allocated
	// inside the same transaction that changes the tree, so nothing that does not own that
	// transaction can produce one.
	log metastore.Log

	// held is the allowance wrapped around the namespace, and nil when the namespace keeps
	// its own count or is under no allowance at all. Only a count that can drift has
	// anything for SIGHUP to repair, and only this layer's count can.
	held *limited.Storage

	// exact marks a namespace that keeps its own count of what it holds. Such a count
	// cannot drift, so SIGHUP has nothing to repair on it — which is a different answer from
	// "there is no allowance here at all", and an operator is owed the difference.
	exact bool

	// what is being served, for the line that says so.
	what      string
	allowance string
	close     func() error
}

// open builds the namespace the command line asked for.
//
// The two forms are exclusive rather than layered. A directory and a blob container are two
// answers to "where does this namespace live", and a command line naming both has not said
// which one it means; guessing would serve one of them while the operator watched the other.
func open(dir string, blob blobSource, quota int64) (opened, error) {
	switch {
	case dir != "" && blob.given():
		return opened{}, errors.New("-dir and -blob-container name two different namespaces; give one of them")
	case dir == "" && !blob.given():
		return opened{}, errors.New("a namespace is required: -dir for a local directory, or -blob-container with -metastore and -workspace")
	case dir != "":
		return openDirectory(dir, quota)
	default:
		return openBlobs(blob, quota)
	}
}

// openDirectory serves a local directory, held under an allowance or exactly as it is.
//
// Measuring what the namespace already holds is the only step here that can take real time,
// so the description says how long it took whenever that is worth knowing.
func openDirectory(dir string, quota int64) (opened, error) {
	backing, err := localdir.New(dir)
	if err != nil {
		return opened{}, err
	}
	result := opened{namespace: backing, what: dir, close: func() error { return nil }}
	if quota == 0 {
		return result, nil
	}

	started := time.Now()
	held, err := limited.New(context.Background(), backing, quota)
	if err != nil {
		return opened{}, err
	}
	walk := time.Since(started)

	// Read before anything is served, and a failure here is a failure to start. A namespace
	// whose room cannot be read cannot be held to an allowance at all: a mount that is given
	// no figure checks no write against one, so serving anyway would offer a workspace that
	// claims a limit and enforces none of it.
	space, err := held.Space(context.Background())
	if err != nil {
		return opened{}, err
	}
	measured := ""
	if walk >= noticeableWalk {
		measured = fmt.Sprintf(" (measured in %v)", rounded(walk))
	}
	result.namespace, result.held = held, held
	result.allowance = fmt.Sprintf(" under an allowance of %d bytes, %d of them taken%s,", space.Total, space.Used, measured)
	return result, nil
}

// openBlobs serves a namespace whose contents are in a blob container and whose tree is in
// a metastore.
//
// Nothing is wrapped in an allowance here. The metastore keeps the count itself, inside the
// same change that moves the bytes, so there is no walk to seed it and no drift to repair —
// and wrapping it would put an extra round trip in front of every write to ask a question
// the metastore has already answered.
func openBlobs(blob blobSource, quota int64) (opened, error) {
	switch {
	case blob.database == "":
		return opened{}, errors.New("-metastore is required with -blob-container: the database holding the namespace's tree")
	case blob.workspace == "":
		return opened{}, errors.New("-workspace is required with -blob-container: the name of the namespace within the metastore")
	}
	connection := os.Getenv(connectionEnv)
	if connection == "" {
		return opened{}, fmt.Errorf("%s is not set, and it is where the credentials for %s come from", connectionEnv, blob.container)
	}

	objects, err := azblob.NewFromConnectionString(connection, blob.container, blob.prefix)
	if err != nil {
		return opened{}, err
	}
	meta, err := sqlite.Open(context.Background(), blob.database, blob.workspace, quota, sqlite.DefaultWindow())
	if err != nil {
		return opened{}, err
	}
	namespace := objectstore.New(objects, meta)

	what := fmt.Sprintf("%s in container %s", blob.workspace, blob.container)
	if blob.prefix != "" {
		what = fmt.Sprintf("%s under %s", what, blob.prefix)
	}
	result := opened{namespace: namespace, log: meta, what: what, close: namespace.Close}
	if quota == 0 {
		return result, nil
	}
	// Under an allowance the count is kept as the bytes move, so SIGHUP has nothing to
	// repair here. Without one there is no count at all and nothing to say that about.
	result.exact = true

	space, err := namespace.Space(context.Background())
	if err != nil {
		return opened{}, err
	}
	result.allowance = fmt.Sprintf(" under an allowance of %d bytes, %d of them taken,", space.Total, space.Used)
	return result, nil
}

// serve runs until a request loop fails or a signal arrives.
func serve(httpServer *http.Server, listener net.Listener, ns opened, errOut io.Writer) error {
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
			recount(context.Background(), ns, errOut)
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
func recount(ctx context.Context, ns opened, errOut io.Writer) {
	held := ns.held
	switch {
	case held != nil:
	case ns.exact:
		fmt.Fprintln(errOut, "remote-fs-server: SIGHUP asks for a recount, and this namespace counts what it holds as it holds it, so there is nothing to repair")
		return
	default:
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
