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
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// shutdownGrace is how long requests already in flight have to finish once a signal has
// arrived. A write that is cut off here would leave the caller unable to tell whether it
// happened, which is the one answer this system must never give.
const shutdownGrace = 5 * time.Second

// statusDeadline bounds an operator query independently of request shutdown. The status
// path reads SQLite, operation admission and filesystem capacity; none may occupy the
// signal loop indefinitely when termination is waiting behind it.
const statusDeadline = 2 * time.Second

// noticeableWalk is how long measuring the namespace has to take before the startup line
// says how long it took. That walk is the one thing standing between the acquired listener
// and serving it, so a pause an operator notices is one they are owed the cause of;
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
	config, help, err := parseConfig(args, errOut)
	if err != nil || help {
		return err
	}

	// Acquire the public address before opening storage. A local store creates durable
	// state while it opens, and a server that cannot own its address must leave a new store
	// untouched.
	rawListener, err := net.Listen("tcp", config.listen)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", config.listen, err)
	}
	listener := limitAcceptedConnections(rawListener, config.standalone.maxAcceptedConnections)
	return withListener(listener, func() error {
		ns, err := open(config)
		if err != nil {
			return err
		}
		return withOpened(ns, func() error {
			// The log is what a mount replicates the namespace's metadata from. A namespace with no
			// metastore behind it has none, and is served with a nil one: its replication endpoints
			// then answer ENOSYS, and a mount of it goes on making a request for every operation.
			handler, err := httprest.NewHandlerWithOptions(ns.namespace, ns.log, config.http)
			if err != nil {
				return err
			}

			return serve(newServerWithOptions(handler, config.standalone), listener, ns, errOut)
		})
	})
}

// withListener retains the acquired address through storage opening and serving. Serve
// normally closes it; failures before Serve reaches it are closed here, and an unexpected
// cleanup failure remains part of the command's result.
func withListener(listener net.Listener, action func() error) (returned error) {
	defer func() {
		closeErr := listener.Close()
		if onlyErrorLeaves(closeErr, net.ErrClosed) {
			closeErr = nil
		}
		if closeErr != nil {
			closeErr = fmt.Errorf("closing the listener: %w", closeErr)
		}
		returned = errors.Join(returned, closeErr)
	}()
	return action()
}

// withOpened keeps every owned storage resource, including a local store's lifetime lock,
// until the HTTP server has drained. Closing failures are part of the command's result.
func withOpened(ns opened, action func() error) (returned error) {
	defer func() { returned = errors.Join(returned, ns.close()) }()
	return action()
}

// newServer builds the HTTP server this command serves with.
//
// The registration is the whole reason it is a function rather than a literal. A change
// stream never becomes idle, and http.Server.Shutdown waits for connections that are, so
// without telling the streams to let go, stopping a server with one mount attached waits
// out the entire grace period and then reports that it expired — an ordinary stop turned
// into a stall and a failure — while every attached mount sees its stream break rather than
// being told the server was going away.
func newServer(handler *httprest.Handler) *drainingServer {
	return newServerWithOptions(handler, defaultStandaloneHTTPOptions())
}

func newServerWithOptions(handler *httprest.Handler, options standaloneHTTPOptions) *drainingServer {
	draining := newDrainingHandler(handler)
	connections := newConnectionTracker()
	server := &http.Server{
		Handler:           draining,
		ReadHeaderTimeout: options.readHeaderTimeout,
		IdleTimeout:       options.idleTimeout,
		ConnState:         connections.update,
	}
	server.RegisterOnShutdown(draining.Stop)
	return &drainingServer{Server: server, drain: draining, connections: connections}
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
	what        string
	allowance   string
	statusName  string
	status      func(context.Context) (string, error)
	close       func() error
	measurement limited.MeasurementLimits
}

// open builds the namespace the validated command line selected.
func open(config commandConfig) (opened, error) {
	switch {
	case config.directory != "":
		return openDirectory(config.directory, config.quota, config.measurement)
	case config.local.given():
		return openLocal(
			config.local, config.quota, config.objectLimits,
			config.maxReaderConnections, config.maxSnapshotReaderConnections,
			config.maxIntegrityRecords, config.maintenance,
		)
	default:
		return openBlobs(
			config.blob, config.quota, config.objectLimits,
			config.maxReaderConnections, config.maxSnapshotReaderConnections,
			config.maxIntegrityRecords, config.maintenance,
		)
	}
}

// openDirectory serves a local directory, held under an allowance or exactly as it is.
//
// Measuring what the namespace already holds is the only step here that can take real time,
// so the description says how long it took whenever that is worth knowing.
func openDirectory(dir string, quota int64, measurement limited.MeasurementLimits) (opened, error) {
	backing, err := localdir.New(dir)
	if err != nil {
		return opened{}, err
	}
	result := opened{namespace: backing, what: dir, close: func() error { return nil }}
	if quota == 0 {
		return result, nil
	}

	started := time.Now()
	effectiveMeasurement, err := measurement.Effective()
	if err != nil {
		return opened{}, err
	}
	held, err := limited.NewWithLimits(context.Background(), backing, quota, effectiveMeasurement)
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
	result.measurement = effectiveMeasurement
	result.allowance = fmt.Sprintf(" under an allowance of %d bytes, %d of them taken%s,", space.Total, space.Used, measured)
	return result, nil
}

func openLocal(
	source localSource,
	quota int64,
	objectLimits sqlite.ObjectLimits,
	maxReaderConnections int,
	maxSnapshotReaderConnections int,
	maxIntegrityRecords int64,
	maintenance objectstore.Options,
) (opened, error) {
	store, err := localstore.Open(context.Background(), localstore.Config{
		Root:                         source.root,
		Workspace:                    source.workspace,
		Quota:                        quota,
		Window:                       sqlite.DefaultWindow(),
		LocalDisk:                    source.objects,
		ObjectLimits:                 objectLimits,
		MaxReaderConnections:         maxReaderConnections,
		MaxSnapshotReaderConnections: maxSnapshotReaderConnections,
		MaxIntegrityRecords:          maxIntegrityRecords,
		Maintenance:                  maintenance,
	})
	if err != nil {
		return opened{}, err
	}
	status, err := store.Status(context.Background())
	if err != nil {
		return opened{}, errors.Join(err, closeAfterOpenFailure("local store", store.Close()))
	}
	return opened{
		namespace:  store,
		log:        store.Log(),
		exact:      true,
		what:       fmt.Sprintf("%s in local store %s", source.workspace, source.root),
		allowance:  fmt.Sprintf(" under an allowance of %d bytes, %d of them taken,", status.Space.Total, status.Space.Used),
		statusName: "local-store",
		status: func(ctx context.Context) (string, error) {
			current, err := store.Status(ctx)
			if err != nil {
				return "", err
			}
			return formatLocalStatus(current, maintenance), nil
		},
		close: store.Close,
	}, nil
}

func closeAfterOpenFailure(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("closing the %s after startup failed: %w", what, err)
}

// openBlobs serves a namespace whose contents are in a blob container and whose tree is in
// a metastore.
//
// Nothing is wrapped in an allowance here. The metastore keeps the count itself, inside the
// same change that moves the bytes, so there is no walk to seed it and no drift to repair —
// and wrapping it would put an extra round trip in front of every write to ask a question
// the metastore has already answered.
func openBlobs(
	blob blobSource,
	quota int64,
	objectLimits sqlite.ObjectLimits,
	maxReaderConnections int,
	maxSnapshotReaderConnections int,
	maxIntegrityRecords int64,
	maintenance objectstore.Options,
) (opened, error) {
	return openBlobsContext(
		context.Background(), blob, quota, objectLimits, maxReaderConnections,
		maxSnapshotReaderConnections, maxIntegrityRecords, maintenance,
	)
}

func openBlobsContext(
	ctx context.Context,
	blob blobSource,
	quota int64,
	objectLimits sqlite.ObjectLimits,
	maxReaderConnections int,
	maxSnapshotReaderConnections int,
	maxIntegrityRecords int64,
	maintenance objectstore.Options,
) (opened, error) {
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

	options, err := (sqlite.Options{
		Window:                       sqlite.DefaultWindow(),
		ObjectLimits:                 objectLimits,
		MaxReaderConnections:         maxReaderConnections,
		MaxSnapshotReaderConnections: maxSnapshotReaderConnections,
		MaxIntegrityRecords:          maxIntegrityRecords,
	}).Effective()
	if err != nil {
		return opened{}, err
	}
	objects, err := azblob.NewFromConnectionString(connection, blob.container, blob.prefix)
	if err != nil {
		return opened{}, err
	}
	meta, err := sqlite.OpenWithOptions(
		ctx, blob.database, blob.workspace, quota, options,
	)
	if err != nil {
		return opened{}, errors.Join(err, closeAfterOpenFailure("blob object store", objects.Close()))
	}
	namespace, err := objectstore.NewWithOptions(objects, meta, maintenance)
	if err != nil {
		return opened{}, errors.Join(
			err,
			closeAfterOpenFailure("metastore", meta.Close()),
			closeAfterOpenFailure("blob object store", objects.Close()),
		)
	}

	what := fmt.Sprintf("%s in container %s", blob.workspace, blob.container)
	if blob.prefix != "" {
		what = fmt.Sprintf("%s under %s", what, blob.prefix)
	}
	result := opened{
		namespace:  namespace,
		log:        meta,
		what:       what,
		statusName: "blob namespace",
		status: func(ctx context.Context) (string, error) {
			status, statusErr := meta.ObjectStatus(ctx)
			_, availabilityErr := objects.Available(ctx)
			if onlyErrorLeaves(availabilityErr, syscall.ENOSYS) {
				availabilityErr = nil
			} else if availabilityErr != nil {
				availabilityErr = fmt.Errorf("probing the blob object store: %w", availabilityErr)
			}
			if err := errors.Join(statusErr, availabilityErr); err != nil {
				return "", err
			}
			return formatObjectStoreStatus(
				blob.workspace, status, options.ObjectLimits,
				options.MaxReaderConnections, options.MaxSnapshotReaderConnections,
				options.MaxIntegrityRecords,
				namespace.MaintenanceStatus(),
				maintenance,
			), nil
		},
		close: namespace.Close,
	}
	if quota == 0 {
		return result, nil
	}
	// Under an allowance the count is kept as the bytes move, so SIGHUP has nothing to
	// repair here. Without one there is no count at all and nothing to say that about.
	result.exact = true

	space, err := namespace.Space(ctx)
	if err != nil {
		return opened{}, errors.Join(err, closeAfterOpenFailure("blob namespace", namespace.Close()))
	}
	result.allowance = fmt.Sprintf(" under an allowance of %d bytes, %d of them taken,", space.Total, space.Used)
	return result, nil
}

// onlyErrorLeaves recognizes expected errors without hiding an unexpected leaf joined to
// one of them. errors.Is alone cannot make that distinction.
func onlyErrorLeaves(err error, targets ...error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyErrorLeaves(child, targets...) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyErrorLeaves(wrapped.Unwrap(), targets...)
	}
	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// serve runs until a request loop fails or a signal arrives.
func serve(httpServer *drainingServer, listener net.Listener, ns opened, errOut io.Writer) error {
	return serveWithGrace(httpServer, listener, ns, errOut, shutdownGrace)
}

func serveWithGrace(httpServer *drainingServer, listener net.Listener, ns opened, errOut io.Writer, grace time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// SIGHUP is delivered on a channel of its own rather than through the context above,
	// because it asks for something the server survives rather than for the end of it.
	hangups := make(chan os.Signal, 1)
	signal.Notify(hangups, syscall.SIGHUP)
	defer signal.Stop(hangups)

	// The signal handlers are part of being ready: after this line every lifecycle signal
	// has a command-owned outcome. The address stays last because it is copied by people
	// and parsed by launchers.
	fmt.Fprintf(errOut, "remote-fs-server: serving %s%s at http://%s\n", ns.what, ns.allowance, listener.Addr())

	stopped := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		stopped <- httpServer.Serve(&startedListener{Listener: listener, started: started})
	}()
	statusResults := make(chan statusReport, 1)
	recountResults := make(chan string, 1)
	var hangupCancel context.CancelFunc
	var hangupDone chan struct{}
	cancelHangup := func() {
		if hangupCancel == nil {
			return
		}
		hangupCancel()
	}
	finishHangup := func() {
		if hangupCancel == nil {
			return
		}
		cancelHangup()
		<-hangupDone
		hangupCancel = nil
		hangupDone = nil
	}
	defer finishHangup()

	for {
		select {
		case <-started:
			started = nil
		case err := <-stopped:
			stop()
			httpServer.drain.Stop()
			cancelHangup()
			connectionErr := httpServer.connections.closeNew()
			serverErr := closeAndDrainServer(httpServer, err)
			finishHangup()
			return errors.Join(serverErr, connectionErr)
		case <-hangups:
			if hangupCancel != nil {
				continue
			}
			if ns.status == nil {
				hangupCancel, hangupDone = startRecount(ns, recountResults)
			} else {
				hangupCancel, hangupDone = startStatus(ns, statusDeadline, statusResults)
			}
		case report := <-statusResults:
			finishHangup()
			writeStatus(report, ns, errOut)
		case output := <-recountResults:
			finishHangup()
			fmt.Fprint(errOut, output)
		case <-ctx.Done():
			// Disarmed before shutting down: a second signal from an operator who has decided
			// not to wait should kill the process the way it normally would.
			stop()
			httpServer.drain.Stop()
			cancelHangup()
			connectionErr := httpServer.connections.closeNew()
			fmt.Fprintln(errOut, "remote-fs-server: stopping")
			serverErr := terminateServer(httpServer, listener, stopped, started, grace)
			finishHangup()
			return errors.Join(serverErr, connectionErr)
		}
	}
}

// handleHangup executes the requested operator action synchronously. The server loop gives
// recount and status their owned goroutines so neither occupies signal handling.
func handleHangup(ctx context.Context, ns opened, errOut io.Writer) {
	if ns.status == nil {
		recount(ctx, ns, errOut)
		return
	}
	writeStatus(readStatus(ctx, ns), ns, errOut)
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
	fmt.Fprintf(errOut, "remote-fs-server: recounted the namespace in %v: %d bytes taken, where the count said %d; "+
		"directory listings were limited to %d bytes and the traversal frontier to %d bytes\n",
		rounded(walk), after.Used, before.Used,
		ns.measurement.MaxDirectoryBytes, ns.measurement.MaxFrontierBytes)
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
