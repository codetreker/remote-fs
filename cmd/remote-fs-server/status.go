package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type statusReport struct {
	line string
	err  error
}

func readStatus(ctx context.Context, ns opened) statusReport {
	line, err := ns.status(ctx)
	if err == nil && ns.lockStatus != nil {
		var current locking.Status
		current, err = ns.lockStatus(ctx)
		if err == nil {
			line += "; " + formatLockStatus(current)
		}
	}
	return statusReport{line: line, err: err}
}

func withLockStatus(ns opened, service locking.Service) (opened, error) {
	status, ok := service.(locking.StatusService)
	if !ok {
		return opened{}, errors.Join(errors.New("file-lock service has no recovery status"), ns.close())
	}
	ns.lockStatus = status.Status
	return ns, nil
}

func formatLockStatus(status locking.Status) string {
	state := "ready"
	switch {
	case status.Unavailable:
		state = "unavailable"
	case status.Recovering:
		state = fmt.Sprintf("recovering for %v", time.Duration(status.RecoveryRemainingMillis)*time.Millisecond)
	}
	return fmt.Sprintf("strong file locks %s; %d sessions, %d owners, %d resources, %d actions, %d grants, %d queued",
		state, status.Sessions, status.Owners, status.Resources, status.Actions, status.Grants, status.Queued)
}

func startStatus(ns opened, timeout time.Duration, results chan<- statusReport) (context.CancelFunc, chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	done := make(chan struct{})
	go func() {
		defer close(done)
		results <- readStatus(ctx, ns)
	}()
	return cancel, done
}

func writeStatus(report statusReport, ns opened, output io.Writer) {
	if report.err != nil {
		fmt.Fprintf(output, "remote-fs-server: %s status for %s failed: %v\n", ns.statusName, ns.what, report.err)
		return
	}
	fmt.Fprintf(output, "remote-fs-server: %s status for %s: %s\n", ns.statusName, ns.what, report.line)
}

func formatLocalStatus(status localstore.Status, options objectstore.Options, maxWaitingOperations int) string {
	pending := formatPendingStatus(status.Objects, status.ObjectLimits)
	maintenance := formatMaintenanceStatus(status.Maintenance)
	return fmt.Sprintf(
		"workspace %q in store %s: %d of %d workspace bytes used, %d writable; %d physical bytes available; "+
			"%s; SQLite reader-connection limit is %d; snapshot reader-connection limit is %d; "+
			"integrity record work limit is %d; integrity name-byte work limit is %d; garbage sweeps run every %v with at most %d objects per attempt; "+
			"%d operations and %d bytes in flight; %d operations waiting under a limit of %d; %d recovery records; "+
			"SQLite checkpoint has accepted generation %d and checkpointed generation %d; checkpoint pending is %t; %s",
		status.Workspace,
		status.LocalDisk.StoreID,
		status.Space.Used,
		status.Space.Total,
		status.Space.Avail,
		status.LocalDisk.PhysicalAvailable,
		pending,
		status.MaxReaderConnections,
		status.MaxSnapshotReaderConnections,
		status.MaxIntegrityRecords,
		status.MaxIntegrityBytes,
		options.SweepInterval,
		options.SweepBatch,
		status.LocalDisk.InFlightOperations,
		status.LocalDisk.InFlightBytes,
		status.LocalDisk.WaitingOperations,
		maxWaitingOperations,
		status.LocalDisk.RecoveryRecords,
		status.Checkpoint.AcceptedGeneration,
		status.Checkpoint.CheckpointedGeneration,
		status.Checkpoint.Pending,
		maintenance,
	)
}

func formatObjectStoreStatus(
	workspace string,
	objects sqlite.ObjectStatus,
	limits sqlite.ObjectLimits,
	maxReaderConnections int,
	maxSnapshotReaderConnections int,
	maxIntegrityRecords int64,
	maxIntegrityBytes int64,
	maintenance objectstore.MaintenanceStatus,
	options objectstore.Options,
) string {
	return fmt.Sprintf(
		"workspace %q: %s; SQLite reader-connection limit is %d; snapshot reader-connection limit is %d; "+
			"integrity record work limit is %d; integrity name-byte work limit is %d; garbage sweeps run every %v with at most %d objects per attempt; %s",
		workspace, formatPendingStatus(objects, limits), maxReaderConnections, maxSnapshotReaderConnections,
		maxIntegrityRecords, maxIntegrityBytes,
		options.SweepInterval,
		options.SweepBatch,
		formatMaintenanceStatus(maintenance),
	)
}

func formatMaintenanceStatus(status objectstore.MaintenanceStatus) string {
	maintenance := "no garbage sweep has completed"
	if !status.LastSweepTime.IsZero() {
		maintenance = fmt.Sprintf("last garbage sweep at %s removed %d objects",
			status.LastSweepTime.UTC().Format(time.RFC3339Nano),
			status.LastSweepRemoved)
		if status.LastSweepError != nil {
			maintenance += fmt.Sprintf(" and failed: %v", status.LastSweepError)
		}
	}
	return maintenance
}

func formatPendingStatus(status sqlite.ObjectStatus, limits sqlite.ObjectLimits) string {
	pending := "backlog is within thresholds"
	if status.OverLimit {
		pending = "backlog is over threshold"
	}
	return fmt.Sprintf(
		"%d reserved objects (%d bytes), %d unresolved objects (%d bytes), %d garbage objects (%d bytes); "+
			"pending reservation thresholds are %d objects and %d bytes (%s); "+
			"a payload above the byte threshold fails EFBIG, otherwise insufficient backlog room fails EAGAIN",
		status.ReservedCount,
		status.ReservedBytes,
		status.UnresolvedCount,
		status.UnresolvedBytes,
		status.GarbageCount,
		status.GarbageBytes,
		limits.MaxPendingObjects,
		limits.MaxPendingBytes,
		pending,
	)
}
