package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestLocalStatusReportsEveryBoundedAndDurablePart(t *testing.T) {
	failed := errors.New("disk still busy")
	status := localstore.Status{
		Volume: "workspace",
		Space:  storage.Space{Total: 1000, Used: 250, Avail: 700},
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
		`volume "workspace" in store`,
		"250 of 1000 volume bytes used, 700 writable",
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

func TestLockStatusReportsLifecycleWithoutCapabilityMaterial(t *testing.T) {
	for _, test := range []struct {
		name   string
		status locking.Status
		want   string
	}{
		{"ready", locking.Status{}, "file locks ready"},
		{"recovery", locking.Status{Recovering: true, RecoveryRemainingMillis: 1250}, "file locks recovering for 1.25s"},
		{"unavailable", locking.Status{Unavailable: true}, "file locks unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.status.Authority = "private-authority-material"
			test.status.Sessions, test.status.Owners, test.status.Resources = 1, 2, 3
			test.status.Actions, test.status.Grants, test.status.Queued = 4, 5, 6
			line := formatLockStatus(test.status)
			if !strings.Contains(line, test.want) || !strings.Contains(line, "1 sessions, 2 owners, 3 resources, 4 actions, 5 grants, 6 queued") {
				t.Fatalf("incorrect lock status: %s", line)
			}
			if strings.Contains(line, test.status.Authority) {
				t.Fatalf("status exposed authority material: %s", line)
			}
		})
	}
}

func TestLockStatusFailureSuppressesPartialOperationalFigures(t *testing.T) {
	failure := errors.New("lock state unavailable")
	v := opened{
		what: "workspace", statusName: "local-store",
		status:     func(context.Context) (string, error) { return "partial capacity", nil },
		lockStatus: func(context.Context) (locking.Status, error) { return locking.Status{}, failure },
	}
	var output bytes.Buffer
	handleHangup(t.Context(), v, &output)
	if !strings.Contains(output.String(), failure.Error()) || strings.Contains(output.String(), "partial capacity") {
		t.Fatalf("failed status was not isolated: %s", output.String())
	}
}

func TestMissingLockStatusClosesVolumeAndPreservesCloseFailure(t *testing.T) {
	failure := errors.New("ownership close uncertain")
	closed := false
	_, err := withLockStatus(opened{close: func() error { closed = true; return failure }}, nil)
	if !closed || !errors.Is(err, failure) || !strings.Contains(err.Error(), "file-lock service has no recovery status") {
		t.Fatalf("missing status returned closed=%t error=%v", closed, err)
	}
}

func TestUnavailableLockAuthorityCannotAnnounceReadiness(t *testing.T) {
	for _, failure := range []error{nil, errors.New("recovery status read failed")} {
		volume, metadata := newTestVolume(t)
		handler, err := httprest.NewHandler(volume, metadata)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		v := opened{what: "workspace", lockStatus: func(context.Context) (locking.Status, error) {
			return locking.Status{Unavailable: true}, failure
		}}
		var output bytes.Buffer
		err = serveWithGrace(newServer(handler), listener, v, &output, time.Millisecond)
		closeErr := listener.Close()
		if err == nil || closeErr != nil {
			t.Fatalf("unavailable authority startup returned %v; listener close %v", err, closeErr)
		}
		if strings.Contains(output.String(), "serving") {
			t.Fatalf("unavailable authority announced readiness: %s", output.String())
		}
	}
}

func TestLocalStatusFailureDoesNotPrintPartialFigures(t *testing.T) {
	failure := errors.New("cannot read physical capacity")
	v := opened{what: "workspace in local store /data", statusName: "local-store", status: func(context.Context) (string, error) {
		return "plausible but incomplete figures", failure
	}}
	var output bytes.Buffer
	handleHangup(t.Context(), v, &output)
	if !strings.Contains(output.String(), failure.Error()) {
		t.Fatalf("status failure was not reported: %s", output.String())
	}
	if strings.Contains(output.String(), "plausible") {
		t.Fatalf("partial status was presented with a failed query: %s", output.String())
	}
}
