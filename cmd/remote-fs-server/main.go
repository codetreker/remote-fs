// Command remote-fs-server serves a metastore-backed volume over HTTP.
// File contents reside in Azure Blob Storage or a private local object store.
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

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// shutdownGrace bounds graceful HTTP draining. After it expires, handlers are cancelled
// and drained before storage ownership is released.
const shutdownGrace = 5 * time.Second

// statusDeadline bounds an operator query independently of request shutdown. The status
// path reads SQLite, operation admission and filesystem capacity; none may occupy the
// signal loop indefinitely when termination is waiting behind it.
const statusDeadline = 2 * time.Second

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
		v, err := open(config)
		if err != nil {
			return err
		}
		return withOpened(v, func() error {
			handler, err := httprest.NewHandlerWithOptions(v.volume, v.log, config.http)
			if err != nil {
				return err
			}

			return serve(newServerWithOptions(handler, config.standalone), listener, v, errOut)
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

// withOpened releases storage only after HTTP calls and the handler's retained file
// sessions have drained. A failed registry close preserves the backend's ownership.
func withOpened(v opened, action func() error) (returned error) {
	defer func() {
		var unfinished *fileRegistryCloseError
		if !errors.As(returned, &unfinished) {
			returned = errors.Join(returned, v.close())
		}
	}()
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

// blobSource names the parts of a volume whose contents are in a blob container. The
// credentials are deliberately absent: they arrive through the environment, because a flag
// is visible to every process on the machine that can read /proc, and R-SEC-4 puts
// credentials out of reach of anything that reads like a log.
type blobSource struct {
	container string
	prefix    string
	database  string
	volume    string
}

// given reports whether this form was chosen at all.
func (b blobSource) given() bool { return b.container != "" }

// connectionEnv is where the credentials for a blob container are read from. The name is
// the one the Azure tooling already uses, so an operator who has configured any other Azure
// client has configured this one.
const connectionEnv = "AZURE_STORAGE_CONNECTION_STRING"

// opened is a volume ready to be served.
type opened struct {
	volume     storage.Storage
	log        metastore.Log
	what       string
	allowance  string
	statusName string
	status     func(context.Context) (string, error)
	close      func() error
	lockStatus func(context.Context) (locking.Status, error)
}

type authorityConfig struct {
	locks           locking.Options
	initializeLocks bool
	files           fileBackendOptions
}

// open builds the volume the validated command line selected.
func open(config commandConfig) (opened, error) {
	authority := authorityConfig{locks: config.locks, initializeLocks: config.initializeLockState, files: config.files}
	switch {
	case config.local.given():
		return openLocal(
			config.local, config.quota, config.objectLimits,
			config.maxReaderConnections, config.maxSnapshotReaderConnections,
			config.maxIntegrityRecords, config.maxIntegrityBytes, config.maintenance, authority,
		)
	default:
		return openBlobs(
			config.blob, config.quota, config.objectLimits,
			config.maxReaderConnections, config.maxSnapshotReaderConnections,
			config.maxIntegrityRecords, config.maxIntegrityBytes, config.maintenance, authority,
		)
	}
}

func openLocal(
	source localSource,
	quota int64,
	objectLimits sqlite.ObjectLimits,
	maxReaderConnections int,
	maxSnapshotReaderConnections int,
	maxIntegrityRecords int64,
	maxIntegrityBytes int64,
	maintenance objectstore.Options,
	authority authorityConfig,
) (opened, error) {
	maxWaitingOperations := source.objects.MaxWaitingOperations
	if maxWaitingOperations == 0 {
		maxWaitingOperations = localdisk.DefaultMaxWaitingOperations
	}
	store, err := localstore.Open(context.Background(), localstore.Config{
		Root:                         source.root,
		Volume:                       source.volume,
		Quota:                        quota,
		Window:                       sqlite.DefaultWindow(),
		LocalDisk:                    source.objects,
		ObjectLimits:                 objectLimits,
		MaxReaderConnections:         maxReaderConnections,
		MaxSnapshotReaderConnections: maxSnapshotReaderConnections,
		MaxIntegrityRecords:          maxIntegrityRecords,
		MaxIntegrityBytes:            maxIntegrityBytes,
		MaxRetainedFiles:             authority.files.retained,
		Advisory:                     authority.files.advisory,
		Maintenance:                  maintenance,
		Locks:                        &authority.locks,
		InitializeLocks:              authority.initializeLocks,
	})
	if err != nil {
		return opened{}, err
	}
	status, err := store.Status(context.Background())
	if err != nil {
		return opened{}, errors.Join(err, closeAfterOpenFailure("local store", store.Close()))
	}
	return withLockStatus(opened{
		volume:     store,
		log:        store.Log(),
		what:       fmt.Sprintf("%s in local store %s", source.volume, source.root),
		allowance:  fmt.Sprintf(" under an allowance of %d bytes, %d of them taken,", status.Space.Total, status.Space.Used),
		statusName: "local-store",
		status: func(ctx context.Context) (string, error) {
			current, err := store.Status(ctx)
			if err != nil {
				return "", err
			}
			return formatLocalStatus(current, maintenance, maxWaitingOperations), nil
		},
		close: store.Close,
	}, store.LockService())
}

func closeAfterOpenFailure(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("closing the %s after startup failed: %w", what, err)
}

// openBlobs serves a volume whose contents are in a blob container and whose tree is in
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
	maxIntegrityBytes int64,
	maintenance objectstore.Options,
	authority authorityConfig,
) (opened, error) {
	return openBlobsContext(
		context.Background(), blob, quota, objectLimits, maxReaderConnections,
		maxSnapshotReaderConnections, maxIntegrityRecords, maxIntegrityBytes, maintenance, authority,
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
	maxIntegrityBytes int64,
	maintenance objectstore.Options,
	authority authorityConfig,
) (opened, error) {
	switch {
	case blob.database == "":
		return opened{}, errors.New("-metastore is required with -blob-container: the database holding the volume's tree")
	case blob.volume == "":
		return opened{}, errors.New("-volume is required with -blob-container: the name of the volume within the metastore")
	}
	connection := os.Getenv(connectionEnv)
	if connection == "" {
		return opened{}, fmt.Errorf("%s is not set, and it is where the credentials for %s come from", connectionEnv, blob.container)
	}

	options, err := (sqlite.Options{
		MaxRetainedFiles:             authority.files.retained,
		Advisory:                     authority.files.advisory,
		Window:                       sqlite.DefaultWindow(),
		ObjectLimits:                 objectLimits,
		MaxReaderConnections:         maxReaderConnections,
		MaxSnapshotReaderConnections: maxSnapshotReaderConnections,
		MaxIntegrityRecords:          maxIntegrityRecords,
		MaxIntegrityBytes:            maxIntegrityBytes,
	}).Effective()
	if err != nil {
		return opened{}, err
	}
	objects, err := azblob.NewFromConnectionString(connection, blob.container, blob.prefix)
	if err != nil {
		return opened{}, err
	}
	meta, err := sqlite.OpenLocking(ctx, sqlite.LockingConfig{
		Database: blob.database, Volume: blob.volume, Allowance: quota,
		SQLite: options, Locks: authority.locks, Initialize: authority.initializeLocks,
	})
	if err != nil {
		return opened{}, errors.Join(err, closeAfterOpenFailure("blob object store", objects.Close()))
	}
	volume, err := objectstore.NewWithOptions(objects, meta, maintenance)
	if err != nil {
		return opened{}, errors.Join(
			err,
			closeAfterOpenFailure("metastore", meta.Close()),
			closeAfterOpenFailure("blob object store", objects.Close()),
		)
	}

	what := fmt.Sprintf("%s in container %s", blob.volume, blob.container)
	if blob.prefix != "" {
		what = fmt.Sprintf("%s under %s", what, blob.prefix)
	}
	result := opened{
		volume:     volume,
		log:        meta,
		what:       what,
		statusName: "blob volume",
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
				blob.volume, status, options.ObjectLimits,
				options.MaxReaderConnections, options.MaxSnapshotReaderConnections,
				options.MaxIntegrityRecords, options.MaxIntegrityBytes,
				volume.MaintenanceStatus(),
				maintenance,
			), nil
		},
		close: volume.Close,
	}
	if quota == 0 {
		return withLockStatus(result, volume.LockService())
	}

	space, err := volume.Space(ctx)
	if err != nil {
		return opened{}, errors.Join(err, closeAfterOpenFailure("blob volume", volume.Close()))
	}
	result.allowance = fmt.Sprintf(" under an allowance of %d bytes, %d of them taken,", space.Total, space.Used)
	return withLockStatus(result, volume.LockService())
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
func serve(httpServer *drainingServer, listener net.Listener, v opened, errOut io.Writer) error {
	return serveWithGrace(httpServer, listener, v, errOut, shutdownGrace)
}

func serveWithGrace(httpServer *drainingServer, listener net.Listener, v opened, errOut io.Writer, grace time.Duration) (returned error) {
	defer func() { returned = errors.Join(returned, closeFileRegistry(httpServer, grace)) }()
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
	lockState := ""
	if v.lockStatus != nil {
		statusContext, cancel := context.WithTimeout(ctx, statusDeadline)
		current, err := v.lockStatus(statusContext)
		cancel()
		if err != nil {
			return fmt.Errorf("file-lock status before serving: %w", err)
		}
		if current.Unavailable {
			return errors.New("file-lock authority is unavailable")
		}
		lockState = "; " + formatLockStatus(current)
	}
	fmt.Fprintf(errOut, "remote-fs-server: serving %s%s%s at http://%s\n", v.what, v.allowance, lockState, listener.Addr())

	stopped := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		stopped <- httpServer.Serve(&startedListener{Listener: listener, started: started})
	}()
	statusResults := make(chan statusReport, 1)
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
			hangupCancel, hangupDone = startStatus(v, statusDeadline, statusResults)
		case report := <-statusResults:
			finishHangup()
			writeStatus(report, v, errOut)
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

func handleHangup(ctx context.Context, v opened, errOut io.Writer) {
	writeStatus(readStatus(ctx, v), v, errOut)
}
