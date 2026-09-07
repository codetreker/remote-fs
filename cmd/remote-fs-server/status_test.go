package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

func TestLocalStatusReportsEveryBoundedAndDurablePart(t *testing.T) {
	failed := errors.New("disk still busy")
	status := localstore.Status{
		Workspace: "workspace",
		Space:     storage.Space{Total: 1000, Used: 250, Avail: 700},
		Objects: sqlite.ObjectStatus{
			ReservedCount:   2,
			ReservedBytes:   30,
			UnresolvedCount: 4,
			UnresolvedBytes: 60,
			GarbageCount:    3,
			GarbageBytes:    40,
			OverLimit:       true,
		},
		ObjectLimits:                 sqlite.ObjectLimits{MaxPendingObjects: 11, MaxPendingBytes: 120},
		MaxReaderConnections:         9,
		MaxSnapshotReaderConnections: 10,
		MaxIntegrityRecords:          101,
		MaxIntegrityBytes:            8 << 20,
		LocalDisk: localdisk.Status{
			StoreID:            localdisk.ID{1},
			InFlightOperations: 4,
			InFlightBytes:      50,
			WaitingOperations:  5,
			PhysicalAvailable:  800,
			RecoveryRecords:    6,
		},
		Maintenance: objectstore.MaintenanceStatus{
			LastSweepTime:    time.Date(2026, 9, 4, 12, 30, 0, 0, time.FixedZone("offset", 2*60*60)),
			LastSweepRemoved: 7,
			LastSweepError:   failed,
		},
		Checkpoint: localstore.CheckpointStatus{
			AcceptedGeneration:     23,
			CheckpointedGeneration: 17,
			Pending:                true,
		},
	}
	line := formatLocalStatus(status, objectstore.Options{SweepInterval: 45 * time.Second, SweepBatch: 17}, 19)
	for _, phrase := range []string{
		`workspace "workspace" in store`,
		"250 of 1000 workspace bytes used, 700 writable",
		"800 physical bytes available",
		"2 reserved objects (30 bytes)",
		"4 unresolved objects (60 bytes)",
		"3 garbage objects (40 bytes)",
		"pending reservation thresholds are 11 objects and 120 bytes",
		"backlog is over threshold",
		"payload above the byte threshold fails EFBIG",
		"insufficient backlog room fails EAGAIN",
		"SQLite reader-connection limit is 9",
		"snapshot reader-connection limit is 10",
		"integrity record work limit is 101",
		"integrity name-byte work limit is 8388608",
		"garbage sweeps run every 45s with at most 17 objects per attempt",
		"4 operations and 50 bytes in flight",
		"5 operations waiting under a limit of 19",
		"6 recovery records",
		"SQLite checkpoint has accepted generation 23 and checkpointed generation 17",
		"checkpoint pending is true",
		"2026-09-04T10:30:00Z",
		"removed 7 objects",
		failed.Error(),
	} {
		if !strings.Contains(line, phrase) {
			t.Fatalf("status does not contain %q: %s", phrase, line)
		}
	}
}

func TestLocalStatusFailureDoesNotPrintPartialFigures(t *testing.T) {
	failure := errors.New("cannot read physical capacity")
	ns := opened{what: "workspace in local store /data", statusName: "local-store", status: func(context.Context) (string, error) {
		return "plausible but incomplete figures", failure
	}}
	var output bytes.Buffer
	handleHangup(t.Context(), ns, &output)
	if !strings.Contains(output.String(), failure.Error()) {
		t.Fatalf("status failure was not reported: %s", output.String())
	}
	if strings.Contains(output.String(), "plausible") {
		t.Fatalf("partial status was presented with a failed query: %s", output.String())
	}
}
