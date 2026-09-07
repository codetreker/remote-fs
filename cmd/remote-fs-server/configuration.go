package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

const (
	defaultMaxAcceptedConnections = 256
	defaultReadHeaderTimeout      = 2 * time.Second
	defaultIdleTimeout            = time.Minute
)

type standaloneHTTPOptions struct {
	maxAcceptedConnections int
	readHeaderTimeout      time.Duration
	idleTimeout            time.Duration
}

func defaultStandaloneHTTPOptions() standaloneHTTPOptions {
	return standaloneHTTPOptions{
		maxAcceptedConnections: defaultMaxAcceptedConnections,
		readHeaderTimeout:      defaultReadHeaderTimeout,
		idleTimeout:            defaultIdleTimeout,
	}
}

type commandConfig struct {
	listen                       string
	directory                    string
	blob                         blobSource
	local                        localSource
	quota                        int64
	objectLimits                 sqlite.ObjectLimits
	maxReaderConnections         int
	maxSnapshotReaderConnections int
	maxIntegrityRecords          int64
	maxIntegrityBytes            int64
	maintenance                  objectstore.Options
	http                         httprest.HandlerOptions
	standalone                   standaloneHTTPOptions
	measurement                  limited.MeasurementLimits
}

type localSource struct {
	root      string
	workspace string
	objects   localdisk.Options
}

func (s localSource) given() bool { return s.root != "" }

var localOnlyFlags = []string{
	"local-max-object-bytes",
	"local-max-in-flight-operations",
	"local-max-waiting-operations",
	"local-max-in-flight-bytes",
	"local-maintenance-reserve-bytes",
	"local-max-recovery-entries",
}

func parseConfig(args []string, errOut io.Writer) (commandConfig, bool, error) {
	flags := flag.NewFlagSet("remote-fs-server", flag.ContinueOnError)
	flags.SetOutput(errOut)
	listen := flags.String("listen", "", "address to accept connections on, as host:port")
	dir := flags.String("dir", "", "existing directory whose contents are served as the namespace")
	container := flags.String("blob-container", "", "name of the Azure Blob container holding the namespace's contents.\n"+
		"Credentials come from AZURE_STORAGE_CONNECTION_STRING, never from a flag:\n"+
		"a flag is visible to everyone who can list processes.")
	prefix := flags.String("blob-prefix", "", "key prefix within the container, so one container may hold several\n"+
		"namespaces")
	database := flags.String("metastore", "", "path to the SQLite database holding the namespace's tree; valid only\n"+
		"with -blob-container")
	localRoot := flags.String("local-store", "", "existing owner-only directory below a deployment-controlled parent; holds\n"+
		"local objects, SQLite metadata and the owner lock")
	workspace := flags.String("workspace", "", "name of the namespace in a blob container or local store")

	var quota sizeFlag
	flags.Var(&quota, "quota", "allowance the namespace is held under, as a `SIZE`: a whole number of bytes,\n"+
		"optionally with one of the suffixes\n"+
		suffixes+".\n"+
		"It is required with -local-store. Without it a directory or blob namespace\n"+
		"has no configured allowance.")

	maxObject := positiveSizeFlag{bytes: localdisk.DefaultMaxObjectBytes}
	flags.Var(&maxObject, "local-max-object-bytes", "largest payload one local object may retain, as SIZE")
	maxInFlightOperations := flags.Int("local-max-in-flight-operations", localdisk.DefaultMaxInFlightOperations,
		"maximum local object operations admitted at once")
	maxWaitingOperations := flags.Int("local-max-waiting-operations", localdisk.DefaultMaxWaitingOperations,
		"maximum local object operations waiting for key, shard or resource admission")
	maxInFlightBytes := positiveSizeFlag{bytes: localdisk.DefaultMaxInFlightBytes}
	flags.Var(&maxInFlightBytes, "local-max-in-flight-bytes", "maximum encoded local object bytes admitted at once, as SIZE")
	maintenanceReserve := positiveSizeFlag{bytes: localdisk.DefaultMaintenanceReserveBytes}
	flags.Var(&maintenanceReserve, "local-maintenance-reserve-bytes", "disk space kept for local-store recovery and metadata, as SIZE")
	maxRecoveryEntries := flags.Int("local-max-recovery-entries", localdisk.DefaultMaxRecoveryEntries,
		"maximum crash-residue records examined while opening a local store")
	objectLimits := sqlite.DefaultObjectLimits()
	maxPendingObjects := flags.Int64("max-pending-objects", objectLimits.MaxPendingObjects,
		"reservation-admission threshold for combined reserved, unresolved and garbage\n"+
			"object records;\n"+
			"a new reservation that would cross it fails with EAGAIN")
	maxPendingBytes := positiveSizeFlag{bytes: objectLimits.MaxPendingBytes}
	flags.Var(&maxPendingBytes, "max-pending-bytes",
		"reservation-admission threshold for recorded payload bytes across reserved and\n"+
			"unresolved and garbage objects, as SIZE; a larger payload fails EFBIG, while\n"+
			"backlog pressure on a payload that fits fails EAGAIN")
	maxReaderConnections := flags.Int("max-reader-connections", sqlite.DefaultMaxReaderConnections,
		"maximum SQLite reader connections serving a metastore-backed namespace")
	maxSnapshotReaderConnections := flags.Int(
		"max-snapshot-reader-connections", sqlite.DefaultMaxSnapshotReaderConnections,
		"maximum SQLite reader connections held by concurrent snapshots in a metastore-backed namespace",
	)
	maxIntegrityRecords := flags.Int64("max-integrity-records", sqlite.DefaultMaxIntegrityRecords,
		"integrity record work limit for a metastore-backed namespace; raise it for\n"+
			"larger retained namespaces")
	maxIntegrityBytes := positiveSizeFlag{bytes: sqlite.DefaultMaxIntegrityBytes}
	flags.Var(&maxIntegrityBytes, "max-integrity-bytes",
		"integrity name-byte work limit for a metastore-backed namespace, as SIZE;\n"+
			"raise it for namespaces with larger directory or retained-log names")
	maintenance := objectstore.DefaultOptions()
	sweepInterval := flags.Duration("sweep-interval", maintenance.SweepInterval,
		"interval between garbage sweeps in a blob or local-store namespace")
	sweepBatch := flags.Int("sweep-batch", maintenance.SweepBatch,
		"maximum garbage objects removed by one sweep in a blob or local-store namespace")

	httpOptions := httprest.DefaultHandlerOptions()
	maxHTTPBody := positiveSizeFlag{bytes: httpOptions.MaxBodyBytes}
	flags.Var(&maxHTTPBody, "http-max-body-bytes", "largest non-streaming HTTP request or response body, as SIZE; larger\n"+
		"reads fail with EFBIG, and oversized listings fail with EIO")
	var maxHTTPWrite inheritedSizeFlag
	flags.Var(&maxHTTPWrite, "http-max-write-bytes", "largest file-content HTTP request, as SIZE; larger writes fail with EFBIG.\n"+
		"Without it this inherits -http-max-body-bytes. With -local-store its effective\n"+
		"value may not exceed -local-max-object-bytes")
	maxConcurrentHTTPBodies := flags.Int("http-max-concurrent-bodies", httpOptions.MaxConcurrentBodies,
		"maximum HTTP request bodies retained at once")
	maxWaitingHTTPBodies := flags.Int("http-max-waiting-bodies", httpOptions.MaxWaitingBodies,
		"maximum requests waiting for body admission; additional requests fail with EAGAIN;\n"+
			"zero selects the package default")
	maxInFlightHTTPBodyBytes := positiveSizeFlag{bytes: httpOptions.MaxInFlightBodyBytes}
	flags.Var(&maxInFlightHTTPBodyBytes, "http-max-in-flight-body-bytes",
		"maximum HTTP request-body bytes retained across concurrent requests, as SIZE")
	maxConcurrentHTTPResponses := flags.Int("http-max-concurrent-responses", httpOptions.MaxConcurrentResponses,
		"maximum non-streaming HTTP responses retained at once")
	maxInFlightHTTPResponseBytes := positiveSizeFlag{bytes: httpOptions.MaxInFlightResponseBytes}
	flags.Var(&maxInFlightHTTPResponseBytes, "http-max-in-flight-response-bytes",
		"maximum bytes retained across concurrent non-streaming HTTP responses, as SIZE;\n"+
			"must be at least four times -http-max-body-bytes")
	maxWaitingHTTPResponses := flags.Int("http-max-waiting-responses", httpOptions.MaxWaitingResponses,
		"maximum requests waiting for response admission; additional requests fail with EAGAIN;\n"+
			"zero selects the package default")
	maxHTTPSubscriptions := flags.Int("http-max-subscriptions", httpOptions.Replication.MaxSubscriptions,
		"maximum change streams attached to a metastore-backed namespace")
	maxHTTPFrameBytes := positiveSizeFlag{bytes: httpOptions.MaxFrameBytes}
	flags.Var(&maxHTTPFrameBytes, "http-max-frame-bytes",
		"largest encoded change, snapshot-row or initial stream frame, as SIZE")
	maxConcurrentHTTPSnapshotFrames := flags.Int(
		"http-max-concurrent-snapshot-frames", httpOptions.MaxConcurrentSnapshotFrames,
		"maximum snapshot frames produced concurrently",
	)
	maxInFlightHTTPSnapshotFrameBytes := positiveSizeFlag{bytes: httpOptions.MaxInFlightSnapshotFrameBytes}
	flags.Var(&maxInFlightHTTPSnapshotFrameBytes, "http-max-in-flight-snapshot-frame-bytes",
		"maximum bytes retained across concurrent snapshot frames, as SIZE;\n"+
			"must be at least three times -http-max-frame-bytes")
	maxWaitingHTTPSnapshotFrames := flags.Int("http-max-waiting-snapshot-frames", httpOptions.MaxWaitingSnapshotFrames,
		"maximum snapshot frames waiting for admission; additional frames fail with\n"+
			"EAGAIN;\n"+
			"zero selects the package default")
	standaloneDefaults := defaultStandaloneHTTPOptions()
	maxAcceptedConnections := flags.Int("http-max-connections", standaloneDefaults.maxAcceptedConnections,
		"maximum TCP connections accepted and retained by this process")
	readHeaderTimeout := flags.Duration("http-read-header-timeout", standaloneDefaults.readHeaderTimeout,
		"maximum time allowed to receive one HTTP request's headers")
	idleTimeout := flags.Duration("http-idle-timeout", standaloneDefaults.idleTimeout,
		"maximum time a keep-alive connection waits for its next request")
	measurementDefaults := limited.DefaultMeasurementLimits()
	maxDirectoryBytes := positiveSizeFlag{bytes: measurementDefaults.MaxDirectoryBytes}
	flags.Var(&maxDirectoryBytes, "quota-max-directory-bytes",
		"largest directory listing retained while measuring a quota-limited -dir, as SIZE")
	maxFrontierBytes := positiveSizeFlag{bytes: measurementDefaults.MaxFrontierBytes}
	flags.Var(&maxFrontierBytes, "quota-max-frontier-bytes",
		"largest traversal frontier retained while measuring a quota-limited -dir, as SIZE")

	flags.Usage = func() {
		fmt.Fprint(errOut, "usage: remote-fs-server -listen ADDR -dir DIR [-quota SIZE] [HTTP OPTIONS]\n"+
			"       remote-fs-server -listen ADDR -blob-container NAME -metastore PATH -workspace NAME [-blob-prefix PREFIX] [-quota SIZE] [METASTORE OPTIONS] [HTTP OPTIONS]\n"+
			"       remote-fs-server -listen ADDR -local-store DIR -workspace NAME -quota SIZE [METASTORE OPTIONS] [LOCAL OPTIONS] [HTTP OPTIONS]\n\n"+
			"Serves one namespace over HTTP. Mount it with remote-fs.\n\n"+
			"The namespace comes from exactly one of -dir, -blob-container and -local-store.\n"+
			"A local store owns its directory exclusively for the server lifetime and keeps\n"+
			"its objects, SQLite metadata and recovery state below that directory.\n\n"+
			"The pending-object thresholds, SQLite reader pools and integrity work limits\n"+
			"apply to both metastore-backed forms. SIGHUP recounts a quota-limited -dir\n"+
			"namespace; for either metastore-backed form it reports current backlog and\n"+
			"maintenance state without changing either.\n\n"+
			"The standalone server bounds accepted connections and header/idle waits. Active\n"+
			"change streams keep their handler-owned deadlines rather than a process-wide\n"+
			"write timeout.\n\n"+
			"There is no authentication and no authorization: anything that can connect can\n"+
			"read and write everything in the namespace. Serve only on a trusted network.\n\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return commandConfig{}, true, nil
		}
		return commandConfig{}, false, errUsage
	}

	given := make(map[string]bool)
	flags.Visit(func(f *flag.Flag) { given[f.Name] = true })
	config := commandConfig{
		listen:    *listen,
		directory: *dir,
		blob: blobSource{
			container: *container,
			prefix:    *prefix,
			database:  *database,
			workspace: *workspace,
		},
		local: localSource{
			root:      *localRoot,
			workspace: *workspace,
			objects: localdisk.Options{
				MaxObjectBytes:          maxObject.bytes,
				MaxInFlightOperations:   *maxInFlightOperations,
				MaxWaitingOperations:    *maxWaitingOperations,
				MaxInFlightBytes:        maxInFlightBytes.bytes,
				MaintenanceReserveBytes: maintenanceReserve.bytes,
				MaxRecoveryEntries:      *maxRecoveryEntries,
			},
		},
		quota: quota.bytes,
		objectLimits: sqlite.ObjectLimits{
			MaxPendingObjects: *maxPendingObjects,
			MaxPendingBytes:   maxPendingBytes.bytes,
		},
		maxReaderConnections:         *maxReaderConnections,
		maxSnapshotReaderConnections: *maxSnapshotReaderConnections,
		maxIntegrityRecords:          *maxIntegrityRecords,
		maxIntegrityBytes:            maxIntegrityBytes.bytes,
		maintenance: objectstore.Options{
			SweepInterval: *sweepInterval,
			SweepBatch:    *sweepBatch,
		},
		http: httpOptions,
		standalone: standaloneHTTPOptions{
			maxAcceptedConnections: *maxAcceptedConnections,
			readHeaderTimeout:      *readHeaderTimeout,
			idleTimeout:            *idleTimeout,
		},
		measurement: limited.MeasurementLimits{
			MaxDirectoryBytes: maxDirectoryBytes.bytes,
			MaxFrontierBytes:  maxFrontierBytes.bytes,
		},
	}
	config.http.MaxBodyBytes = maxHTTPBody.bytes
	config.http.MaxWriteBytes = maxHTTPWrite.bytes
	config.http.MaxConcurrentBodies = *maxConcurrentHTTPBodies
	config.http.MaxWaitingBodies = *maxWaitingHTTPBodies
	config.http.MaxInFlightBodyBytes = maxInFlightHTTPBodyBytes.bytes
	config.http.MaxConcurrentResponses = *maxConcurrentHTTPResponses
	config.http.MaxInFlightResponseBytes = maxInFlightHTTPResponseBytes.bytes
	config.http.MaxWaitingResponses = *maxWaitingHTTPResponses
	config.http.Replication.MaxSubscriptions = *maxHTTPSubscriptions
	config.http.MaxFrameBytes = maxHTTPFrameBytes.bytes
	config.http.MaxConcurrentSnapshotFrames = *maxConcurrentHTTPSnapshotFrames
	config.http.MaxInFlightSnapshotFrameBytes = maxInFlightHTTPSnapshotFrameBytes.bytes
	config.http.MaxWaitingSnapshotFrames = *maxWaitingHTTPSnapshotFrames
	return validateConfig(config, given, flags.Args())
}

func validateConfig(config commandConfig, given map[string]bool, extra []string) (commandConfig, bool, error) {
	if len(extra) > 0 {
		return commandConfig{}, false, fmt.Errorf("%q is not an argument this command takes; everything is a flag", extra[0])
	}
	if config.listen == "" {
		return commandConfig{}, false, errors.New("-listen is required: the address to accept connections on")
	}
	switch {
	case config.standalone.maxAcceptedConnections <= 0:
		return commandConfig{}, false, errors.New("-http-max-connections must be positive")
	case config.standalone.maxAcceptedConnections == math.MaxInt:
		return commandConfig{}, false, errors.New("-http-max-connections must be bounded below the largest integer")
	case config.standalone.readHeaderTimeout <= 0:
		return commandConfig{}, false, errors.New("-http-read-header-timeout must be positive")
	case config.standalone.idleTimeout <= 0:
		return commandConfig{}, false, errors.New("-http-idle-timeout must be positive")
	}
	if config.http.MaxConcurrentBodies <= 0 {
		return commandConfig{}, false, errors.New("-http-max-concurrent-bodies must be positive")
	}
	if config.http.MaxWaitingBodies < 0 {
		return commandConfig{}, false, errors.New("-http-max-waiting-bodies must be non-negative")
	}
	if config.http.MaxConcurrentResponses <= 0 {
		return commandConfig{}, false, errors.New("-http-max-concurrent-responses must be positive")
	}
	if config.http.MaxWaitingResponses < 0 {
		return commandConfig{}, false, errors.New("-http-max-waiting-responses must be non-negative")
	}
	if config.http.Replication.MaxSubscriptions <= 0 {
		return commandConfig{}, false, errors.New("-http-max-subscriptions must be positive")
	}
	if config.http.MaxConcurrentSnapshotFrames <= 0 {
		return commandConfig{}, false, errors.New("-http-max-concurrent-snapshot-frames must be positive")
	}
	if config.http.MaxWaitingSnapshotFrames < 0 {
		return commandConfig{}, false, errors.New("-http-max-waiting-snapshot-frames must be non-negative")
	}
	if err := config.http.Check(); err != nil {
		return commandConfig{}, false, err
	}

	sources := 0
	if config.directory != "" {
		sources++
	}
	if config.blob.given() {
		sources++
	}
	if config.local.given() {
		sources++
	}
	if sources > 1 {
		return commandConfig{}, false, errors.New("-dir, -blob-container and -local-store name different namespaces; give exactly one of them")
	}

	if !config.blob.given() {
		for _, name := range []string{"blob-prefix", "metastore"} {
			if given[name] {
				return commandConfig{}, false, fmt.Errorf("-%s requires -blob-container", name)
			}
		}
	}
	if !config.local.given() {
		for _, name := range localOnlyFlags {
			if given[name] {
				return commandConfig{}, false, fmt.Errorf("-%s requires -local-store", name)
			}
		}
	}
	if !config.blob.given() && !config.local.given() {
		for _, name := range []string{
			"max-pending-objects", "max-pending-bytes", "max-reader-connections",
			"max-snapshot-reader-connections", "max-integrity-records", "max-integrity-bytes",
			"sweep-interval", "sweep-batch",
			"http-max-subscriptions", "http-max-frame-bytes", "http-max-concurrent-snapshot-frames",
			"http-max-in-flight-snapshot-frame-bytes", "http-max-waiting-snapshot-frames",
		} {
			if given[name] {
				return commandConfig{}, false, fmt.Errorf("-%s requires -blob-container or -local-store", name)
			}
		}
	}
	for _, name := range []string{"quota-max-directory-bytes", "quota-max-frontier-bytes"} {
		if !given[name] {
			continue
		}
		if config.directory == "" {
			return commandConfig{}, false, fmt.Errorf("-%s requires -dir", name)
		}
		if config.quota == 0 {
			return commandConfig{}, false, fmt.Errorf("-%s requires -quota", name)
		}
	}
	if given["workspace"] && !config.blob.given() && !config.local.given() {
		return commandConfig{}, false, errors.New("-workspace requires -blob-container or -local-store")
	}
	if sources == 0 {
		return commandConfig{}, false, errors.New("a namespace is required: give exactly one of -dir, -blob-container and -local-store")
	}
	if config.directory != "" && config.quota != 0 {
		if err := config.measurement.Validate(); err != nil {
			return commandConfig{}, false, err
		}
	}
	if config.blob.given() || config.local.given() {
		if config.standalone.maxAcceptedConnections < 2 {
			return commandConfig{}, false, errors.New(
				"-http-max-connections must be at least 2 for a metastore-backed namespace to open its change stream and snapshot")
		}
		if config.objectLimits.MaxPendingObjects <= 0 {
			return commandConfig{}, false, errors.New("-max-pending-objects must be positive")
		}
		if err := config.objectLimits.Validate(); err != nil {
			return commandConfig{}, false, err
		}
		switch {
		case config.maxReaderConnections <= 0:
			return commandConfig{}, false, errors.New("-max-reader-connections must be positive")
		case config.maxReaderConnections == math.MaxInt:
			return commandConfig{}, false, errors.New("-max-reader-connections must be bounded below the largest integer")
		case config.maxSnapshotReaderConnections <= 0:
			return commandConfig{}, false, errors.New("-max-snapshot-reader-connections must be positive")
		case config.maxSnapshotReaderConnections == math.MaxInt:
			return commandConfig{}, false, errors.New("-max-snapshot-reader-connections must be bounded below the largest integer")
		case config.maxIntegrityRecords < sqlite.MinIntegrityRecords:
			return commandConfig{}, false, fmt.Errorf(
				"-max-integrity-records must be at least %d", sqlite.MinIntegrityRecords)
		case config.maxIntegrityRecords == math.MaxInt64:
			return commandConfig{}, false, errors.New("-max-integrity-records must be bounded below the largest integer")
		case config.maxIntegrityBytes <= 0:
			return commandConfig{}, false, errors.New("-max-integrity-bytes must be positive")
		case config.maxIntegrityBytes == math.MaxInt64:
			return commandConfig{}, false, errors.New("-max-integrity-bytes must be bounded below the largest integer")
		case config.maintenance.SweepInterval <= 0:
			return commandConfig{}, false, errors.New("-sweep-interval must be positive")
		case config.maintenance.SweepBatch <= 0:
			return commandConfig{}, false, errors.New("-sweep-batch must be positive")
		case config.maintenance.SweepBatch > objectstore.MaxSweepBatch:
			return commandConfig{}, false, fmt.Errorf("-sweep-batch must be at most %d", objectstore.MaxSweepBatch)
		}
	}

	if config.blob.given() {
		maxWriteBytes := config.http.MaxWriteBytes
		if maxWriteBytes == 0 {
			maxWriteBytes = config.http.MaxBodyBytes
		}
		maxBlobWriteBytes := min(int64(azblob.MaxObjectBytes), config.objectLimits.MaxPendingBytes)
		switch {
		case config.blob.database == "":
			return commandConfig{}, false, errors.New("-metastore is required with -blob-container: the database holding the namespace's tree")
		case config.blob.workspace == "":
			return commandConfig{}, false, errors.New("-workspace is required with -blob-container: the name of the namespace within the metastore")
		case maxWriteBytes > maxBlobWriteBytes:
			return commandConfig{}, false, fmt.Errorf(
				"-http-max-write-bytes is %d, above the blob namespace write bound of %d; the server would retain and then reject that write with EFBIG",
				maxWriteBytes, maxBlobWriteBytes)
		}
	}
	if config.local.given() {
		maxObjectBytes := config.local.objects.MaxObjectBytes
		if maxObjectBytes == 0 {
			maxObjectBytes = localdisk.DefaultMaxObjectBytes
		}
		maxWriteBytes := config.http.MaxWriteBytes
		if maxWriteBytes == 0 {
			maxWriteBytes = config.http.MaxBodyBytes
		}
		switch {
		case config.local.workspace == "":
			return commandConfig{}, false, errors.New("-workspace is required with -local-store: the namespace held in the store")
		case config.quota == 0:
			return commandConfig{}, false, errors.New("-quota is required with -local-store: every workspace needs a positive capacity")
		case config.local.objects.MaxInFlightOperations <= 0:
			return commandConfig{}, false, errors.New("-local-max-in-flight-operations must be positive")
		case config.local.objects.MaxWaitingOperations <= 0:
			return commandConfig{}, false, errors.New("-local-max-waiting-operations must be positive")
		case config.local.objects.MaxWaitingOperations == math.MaxInt:
			return commandConfig{}, false, errors.New("-local-max-waiting-operations must be bounded below the largest integer")
		case config.local.objects.MaxRecoveryEntries <= 0:
			return commandConfig{}, false, errors.New("-local-max-recovery-entries must be positive")
		case maxWriteBytes > maxObjectBytes:
			return commandConfig{}, false, fmt.Errorf(
				"-http-max-write-bytes is %d, above -local-max-object-bytes at %d; the local store would retain and then reject that write with EFBIG",
				maxWriteBytes, maxObjectBytes)
		}
	}
	return config, false, nil
}
