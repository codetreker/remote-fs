package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"strconv"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/azblob"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type fileBackendOptions struct {
	retained int
	advisory advisory.Config
}

func defaultFileBackendOptions() fileBackendOptions {
	return fileBackendOptions{retained: sqlite.DefaultMaxRetainedFiles, advisory: advisory.DefaultConfig()}
}

func bindFileOptions(flags *flag.FlagSet, backend *fileBackendOptions, files *httprest.FileLimits) {
	flags.IntVar(&backend.retained, "max-retained-files", backend.retained,
		"maximum live native file references in the namespace, including unlinked files")
	flags.Var((*fileByteLimit)(&backend.advisory.MaxFileBytes), "max-file-size",
		"largest retained file object, as SIZE; writes and truncation above it fail with EFBIG.\n"+
			"The default is capped by configured object and pending-byte limits")
	flags.Var((*fileByteLimit)(&backend.advisory.MaxMaterializedBytes), "max-file-staging-bytes",
		"maximum aggregate bytes held while applying range writes and truncation, as SIZE;\n"+
			"must cover twice -max-file-size")
	flags.DurationVar(&backend.advisory.FileOperationTimeout, "file-operation-timeout", backend.advisory.FileOperationTimeout,
		"maximum native time for one retained-file operation, including content revision retries")
	flags.IntVar(&files.MaxSessions, "http-max-file-sessions", files.MaxSessions,
		"maximum file sessions owned by this server and admitted by its namespace")
	flags.IntVar(&files.MaxActions, "http-max-file-actions", files.MaxActions,
		"maximum retained file action results in one HTTP session")
	flags.IntVar(&files.MaxCleanupActions, "http-max-file-cleanup-actions", files.MaxCleanupActions,
		"maximum cleanup action results in one HTTP file session, independent of data actions")
	flags.DurationVar(&files.PendingAck, "http-file-open-ack-timeout", files.PendingAck,
		"maximum lifetime of an opened reference awaiting client acknowledgment")
	flags.DurationVar(&files.Session.Lease, "file-session-lease", files.Session.Lease,
		"maximum renewable file-session lifetime accepted from a client")
	flags.DurationVar(&files.Session.History, "file-session-history", files.Session.History,
		"maximum file-session action history interval accepted from a client")
}

type fileByteLimit int64

func (f *fileByteLimit) String() string { return strconv.FormatInt(int64(*f), 10) }

func (f *fileByteLimit) Set(value string) error {
	parsed := positiveSizeFlag{}
	if err := parsed.Set(value); err != nil {
		return err
	}
	*f = fileByteLimit(parsed.bytes)
	return nil
}

func validateFileOptions(config commandConfig) error {
	files, backend := config.http.Files, config.files
	switch {
	case backend.retained <= 0 || backend.retained == math.MaxInt:
		return errors.New("-max-retained-files must be positive and below the largest integer")
	case backend.advisory.MaxMaterializedBytes == math.MaxInt64:
		return errors.New("-max-file-staging-bytes must be below the largest integer")
	case backend.advisory.MaxFileBytes > backend.advisory.MaxMaterializedBytes/2:
		return errors.New("-max-file-staging-bytes must cover twice -max-file-size")
	case backend.advisory.FileOperationTimeout <= 0:
		return errors.New("-file-operation-timeout must be positive")
	case files.MaxSessions <= 0:
		return errors.New("-http-max-file-sessions must be positive")
	case files.MaxActions <= 0:
		return errors.New("-http-max-file-actions must be positive")
	case files.MaxCleanupActions <= 0:
		return errors.New("-http-max-file-cleanup-actions must be positive")
	case files.Session.Lease <= 0:
		return errors.New("-file-session-lease must be positive")
	case files.Session.History <= 0:
		return errors.New("-file-session-history must be positive")
	case files.PendingAck <= 0 || files.PendingAck > files.Session.Lease:
		return errors.New("-http-file-open-ack-timeout must be positive and no longer than -file-session-lease")
	}
	if err := backend.advisory.Check(); err != nil {
		return err
	}
	if err := files.Check(); err != nil {
		return err
	}
	if config.local.given() && backend.advisory.MaxFileBytes > config.local.objects.MaxObjectBytes {
		return errors.New("-max-file-size may not exceed -local-max-object-bytes")
	}
	if config.blob.given() {
		maxObject := min(int64(azblob.MaxObjectBytes), config.objectLimits.MaxPendingBytes)
		if backend.advisory.MaxFileBytes > maxObject {
			return fmt.Errorf("-max-file-size is %d, above the blob object bound of %d", backend.advisory.MaxFileBytes, maxObject)
		}
	}
	return nil
}
