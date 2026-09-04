package main

import (
	"bytes"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestOneNamespaceSourceIsRequired(t *testing.T) {
	for _, args := range [][]string{
		{"-listen", "127.0.0.1:0"},
		{"-listen", "127.0.0.1:0", "-dir", "/one", "-blob-container", "two"},
		{"-listen", "127.0.0.1:0", "-dir", "/one", "-local-store", "/two"},
		{"-listen", "127.0.0.1:0", "-blob-container", "one", "-local-store", "/two"},
	} {
		_, _, err := parseConfig(args, &bytes.Buffer{})
		if err == nil {
			t.Fatalf("%v was accepted", args)
		}
		if !strings.Contains(err.Error(), "namespace") {
			t.Fatalf("%v was refused without identifying the namespace choice: %v", args, err)
		}
	}
}

func TestAFlagForAnotherSourceIsRefused(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		flag string
	}{
		{"blob prefix on a directory", []string{"-dir", "/data", "-blob-prefix", "p"}, "-blob-prefix"},
		{"metastore on a local store", []string{"-local-store", "/data", "-metastore", "meta.db"}, "-metastore"},
		{"local bound on blobs", []string{"-blob-container", "container", "-local-max-object-bytes", "1M"}, "-local-max-object-bytes"},
		{"local recovery on a directory", []string{"-dir", "/data", "-local-max-recovery-entries", "8"}, "-local-max-recovery-entries"},
		{"pending threshold on a directory", []string{"-dir", "/data", "-max-pending-objects", "8"}, "-max-pending-objects"},
		{"reader limit on a directory", []string{"-dir", "/data", "-max-reader-connections", "8"}, "-max-reader-connections"},
		{"snapshot reader limit on a directory", []string{"-dir", "/data", "-max-snapshot-reader-connections", "8"}, "-max-snapshot-reader-connections"},
		{"integrity limit on a directory", []string{"-dir", "/data", "-max-integrity-records", "100"}, "-max-integrity-records"},
		{"sweep interval on a directory", []string{"-dir", "/data", "-sweep-interval", "2m"}, "-sweep-interval"},
		{"sweep batch on a directory", []string{"-dir", "/data", "-sweep-batch", "8"}, "-sweep-batch"},
		{"subscription limit on a directory", []string{"-dir", "/data", "-http-max-subscriptions", "8"}, "-http-max-subscriptions"},
		{"frame limit on a directory", []string{"-dir", "/data", "-http-max-frame-bytes", "2M"}, "-http-max-frame-bytes"},
		{"directory measurement on local store", []string{"-local-store", "/data", "-quota-max-directory-bytes", "8M"}, "-quota-max-directory-bytes"},
		{"directory measurement without quota", []string{"-dir", "/data", "-quota-max-frontier-bytes", "8M"}, "-quota"},
		{"workspace on a directory", []string{"-dir", "/data", "-workspace", "workspace"}, "-workspace"},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"-listen", "127.0.0.1:0"}, c.args...)
			_, _, err := parseConfig(args, &bytes.Buffer{})
			if err == nil {
				t.Fatalf("%v was accepted", args)
			}
			if !strings.Contains(err.Error(), c.flag) {
				t.Fatalf("the refusal does not name %s: %v", c.flag, err)
			}
		})
	}
}

func TestALocalStoreRequiresAWorkspaceAndQuota(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"workspace", nil, "-workspace"},
		{"quota", []string{"-workspace", "workspace"}, "-quota"},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := []string{"-listen", "127.0.0.1:0", "-local-store", "/data"}
			args = append(args, c.args...)
			_, _, err := parseConfig(args, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("%v returned %v, want a refusal naming %s", args, err, c.want)
			}
		})
	}
}

func TestLocalDefaultsFollowThePackagesThatEnforceThem(t *testing.T) {
	config, help, err := parseConfig([]string{
		"-listen", "127.0.0.1:0",
		"-local-store", "/data",
		"-workspace", "workspace",
		"-quota", "8M",
	}, &bytes.Buffer{})
	if err != nil || help {
		t.Fatalf("parse local-store command: help=%t error=%v", help, err)
	}
	if got := config.local.objects; got != (localdisk.Options{
		MaxObjectBytes:          localdisk.DefaultMaxObjectBytes,
		MaxInFlightOperations:   localdisk.DefaultMaxInFlightOperations,
		MaxInFlightBytes:        localdisk.DefaultMaxInFlightBytes,
		MaintenanceReserveBytes: localdisk.DefaultMaintenanceReserveBytes,
		MaxRecoveryEntries:      localdisk.DefaultMaxRecoveryEntries,
	}) {
		t.Fatalf("local object defaults are %+v", got)
	}
	if config.maintenance != objectstore.DefaultOptions() {
		t.Fatalf("maintenance defaults are %+v, want %+v", config.maintenance, objectstore.DefaultOptions())
	}
	if config.objectLimits != sqlite.DefaultObjectLimits() {
		t.Fatalf("pending-object defaults are %+v, want %+v", config.objectLimits, sqlite.DefaultObjectLimits())
	}
	if config.maxReaderConnections != sqlite.DefaultMaxReaderConnections {
		t.Fatalf("SQLite reader default is %d, want %d", config.maxReaderConnections, sqlite.DefaultMaxReaderConnections)
	}
	if config.maxSnapshotReaderConnections != sqlite.DefaultMaxSnapshotReaderConnections {
		t.Fatalf("SQLite snapshot reader default is %d, want %d", config.maxSnapshotReaderConnections, sqlite.DefaultMaxSnapshotReaderConnections)
	}
	if config.maxIntegrityRecords != sqlite.DefaultMaxIntegrityRecords {
		t.Fatalf("integrity default is %d, want %d", config.maxIntegrityRecords, sqlite.DefaultMaxIntegrityRecords)
	}
	if config.standalone != defaultStandaloneHTTPOptions() {
		t.Fatalf("standalone HTTP defaults are %+v, want %+v", config.standalone, defaultStandaloneHTTPOptions())
	}
	if config.measurement != limited.DefaultMeasurementLimits() {
		t.Fatalf("measurement defaults are %+v, want %+v", config.measurement, limited.DefaultMeasurementLimits())
	}
	wantHTTP := httprest.DefaultHandlerOptions()
	wantHTTP.MaxWriteBytes = 0
	if config.http != wantHTTP {
		t.Fatalf("HTTP defaults are %+v, want %+v with the write bound inherited", config.http, wantHTTP)
	}
}

func TestDirectoryMeasurementBoundsReachTheQuotaLayer(t *testing.T) {
	config, help, err := parseConfig([]string{
		"-listen", "127.0.0.1:0",
		"-dir", "/data",
		"-quota", "8M",
		"-quota-max-directory-bytes", "3M",
		"-quota-max-frontier-bytes", "5M",
	}, &bytes.Buffer{})
	if err != nil || help {
		t.Fatalf("parse quota-limited directory: help=%t error=%v", help, err)
	}
	if config.measurement != (limited.MeasurementLimits{
		MaxDirectoryBytes: 3 << 20,
		MaxFrontierBytes:  5 << 20,
	}) {
		t.Fatalf("measurement limits are %+v", config.measurement)
	}
}

func TestLocalBoundsAndHTTPBodyLimitReachTheirOwners(t *testing.T) {
	config, help, err := parseConfig([]string{
		"-listen", "127.0.0.1:0",
		"-local-store", "/data",
		"-workspace", "workspace",
		"-quota", "64M",
		"-local-max-object-bytes", "4M",
		"-local-max-in-flight-operations", "3",
		"-local-max-in-flight-bytes", "16M",
		"-local-maintenance-reserve-bytes", "2M",
		"-local-max-recovery-entries", "17",
		"-max-pending-objects", "19",
		"-max-pending-bytes", "6M",
		"-max-reader-connections", "5",
		"-max-snapshot-reader-connections", "6",
		"-max-integrity-records", "101",
		"-sweep-interval", "45s",
		"-sweep-batch", "23",
		"-http-max-body-bytes", "5M",
		"-http-max-write-bytes", "4M",
		"-http-max-concurrent-bodies", "7",
		"-http-max-waiting-bodies", "11",
		"-http-max-in-flight-body-bytes", "40M",
		"-http-max-concurrent-responses", "13",
		"-http-max-in-flight-response-bytes", "24M",
		"-http-max-waiting-responses", "17",
		"-http-max-subscriptions", "19",
		"-http-max-frame-bytes", "2M",
		"-http-max-concurrent-snapshot-frames", "3",
		"-http-max-in-flight-snapshot-frame-bytes", "18M",
		"-http-max-waiting-snapshot-frames", "5",
		"-http-max-connections", "23",
		"-http-read-header-timeout", "3s",
		"-http-idle-timeout", "45s",
	}, &bytes.Buffer{})
	if err != nil || help {
		t.Fatalf("parse local-store command: help=%t error=%v", help, err)
	}
	if config.quota != 64<<20 {
		t.Fatalf("quota is %d", config.quota)
	}
	if got := config.local.objects; got.MaxObjectBytes != 4<<20 || got.MaxInFlightOperations != 3 ||
		got.MaxInFlightBytes != 16<<20 || got.MaintenanceReserveBytes != 2<<20 || got.MaxRecoveryEntries != 17 {
		t.Fatalf("local bounds are %+v", got)
	}
	if got := config.objectLimits; got.MaxPendingObjects != 19 || got.MaxPendingBytes != 6<<20 {
		t.Fatalf("pending-object limits are %+v", got)
	}
	if config.maxReaderConnections != 5 {
		t.Fatalf("SQLite reader limit is %d", config.maxReaderConnections)
	}
	if config.maxSnapshotReaderConnections != 6 {
		t.Fatalf("SQLite snapshot reader limit is %d", config.maxSnapshotReaderConnections)
	}
	if config.maxIntegrityRecords != 101 {
		t.Fatalf("integrity record work limit is %d", config.maxIntegrityRecords)
	}
	if config.maintenance.SweepInterval != 45*time.Second || config.maintenance.SweepBatch != 23 {
		t.Fatalf("maintenance bounds are %+v", config.maintenance)
	}
	if config.http.MaxBodyBytes != 5<<20 || config.http.MaxWriteBytes != 4<<20 ||
		config.http.MaxConcurrentBodies != 7 || config.http.MaxWaitingBodies != 11 ||
		config.http.MaxInFlightBodyBytes != 40<<20 || config.http.MaxConcurrentResponses != 13 ||
		config.http.MaxInFlightResponseBytes != 24<<20 || config.http.MaxWaitingResponses != 17 ||
		config.http.Replication.MaxSubscriptions != 19 || config.http.MaxFrameBytes != 2<<20 ||
		config.http.MaxConcurrentSnapshotFrames != 3 || config.http.MaxInFlightSnapshotFrameBytes != 18<<20 ||
		config.http.MaxWaitingSnapshotFrames != 5 {
		t.Fatalf("HTTP body limits are %+v", config.http)
	}
	if config.standalone != (standaloneHTTPOptions{
		maxAcceptedConnections: 23,
		readHeaderTimeout:      3 * time.Second,
		idleTimeout:            45 * time.Second,
	}) {
		t.Fatalf("standalone HTTP options are %+v", config.standalone)
	}
}

func TestHTTPWriteLimitCannotExceedTheLocalObjectLimit(t *testing.T) {
	base := []string{
		"-listen", "127.0.0.1:0",
		"-local-store", "/data",
		"-workspace", "workspace",
		"-quota", "1M",
		"-local-max-object-bytes", "4M",
		"-http-max-body-bytes", "5M",
	}
	refused := append(append([]string{}, base...), "-http-max-write-bytes", "5M")
	_, _, err := parseConfig(refused, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "-local-max-object-bytes") || !strings.Contains(err.Error(), "EFBIG") {
		t.Fatalf("an HTTP write above the local object limit returned %v", err)
	}
	accepted := append(append([]string{}, base...), "-http-max-write-bytes", "4M")
	if _, help, err := parseConfig(accepted, &bytes.Buffer{}); err != nil || help {
		t.Fatalf("a write matching the object limit above the quota was refused: help=%t error=%v", help, err)
	}
	_, _, inheritedErr := parseConfig(base, &bytes.Buffer{})
	if inheritedErr == nil || !strings.Contains(inheritedErr.Error(), "-http-max-write-bytes") {
		t.Fatalf("an inherited 5M write limit above the 4M object limit returned %v", inheritedErr)
	}
}

func TestMetastoreBoundsApplyToBlobNamespaces(t *testing.T) {
	config, help, err := parseConfig([]string{
		"-listen", "127.0.0.1:0",
		"-blob-container", "container",
		"-metastore", "/data/meta.sqlite",
		"-workspace", "workspace",
		"-max-pending-objects", "29",
		"-max-pending-bytes", "12M",
		"-max-reader-connections", "7",
		"-max-snapshot-reader-connections", "11",
		"-max-integrity-records", "103",
		"-sweep-interval", "47s",
		"-sweep-batch", "31",
		"-http-max-write-bytes", "12M",
	}, &bytes.Buffer{})
	if err != nil || help {
		t.Fatalf("parse blob command: help=%t error=%v", help, err)
	}
	if got := config.objectLimits; got.MaxPendingObjects != 29 || got.MaxPendingBytes != 12<<20 {
		t.Fatalf("blob pending thresholds are %+v", got)
	}
	if config.maxReaderConnections != 7 {
		t.Fatalf("blob SQLite reader limit is %d", config.maxReaderConnections)
	}
	if config.maxSnapshotReaderConnections != 11 {
		t.Fatalf("blob SQLite snapshot reader limit is %d", config.maxSnapshotReaderConnections)
	}
	if config.maxIntegrityRecords != 103 {
		t.Fatalf("blob integrity record work limit is %d", config.maxIntegrityRecords)
	}
	if config.maintenance != (objectstore.Options{SweepInterval: 47 * time.Second, SweepBatch: 31}) {
		t.Fatalf("blob maintenance limits are %+v", config.maintenance)
	}
}

func TestHTTPWriteLimitCannotExceedBlobOrPendingObjectBounds(t *testing.T) {
	base := []string{
		"-listen", "127.0.0.1:0",
		"-blob-container", "container",
		"-metastore", "/data/meta.sqlite",
		"-workspace", "workspace",
	}
	for _, c := range []struct {
		name string
		args []string
	}{
		{
			name: "pending byte threshold",
			args: []string{"-max-pending-bytes", "12M", "-http-max-body-bytes", "13M", "-http-max-write-bytes", "13M"},
		},
		{
			name: "Azure single object maximum",
			args: []string{
				"-http-max-body-bytes", "5001M", "-http-max-write-bytes", "5001M",
				"-http-max-in-flight-body-bytes", "5001M",
				"-http-max-in-flight-response-bytes", "20004M",
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := parseConfig(append(append([]string{}, base...), c.args...), &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), "blob namespace write bound") || !strings.Contains(err.Error(), "EFBIG") {
				t.Fatalf("oversized blob HTTP write returned %v", err)
			}
		})
	}
	accepted := append(append([]string{}, base...),
		"-http-max-body-bytes", "5000M", "-http-max-write-bytes", "5000M",
		"-http-max-in-flight-body-bytes", "5000M",
		"-http-max-in-flight-response-bytes", "20000M",
	)
	if _, help, err := parseConfig(accepted, &bytes.Buffer{}); err != nil || help {
		t.Fatalf("the exact Azure single-object bound was refused: help=%t error=%v", help, err)
	}
}

func TestNonPositiveCountsAndDurationsAreRefused(t *testing.T) {
	base := []string{
		"-listen", "127.0.0.1:0",
		"-local-store", "/data",
		"-workspace", "workspace",
		"-quota", "8M",
	}
	for _, c := range []struct {
		flag  string
		value string
	}{
		{"-local-max-in-flight-operations", "0"},
		{"-local-max-recovery-entries", "-1"},
		{"-max-pending-objects", "0"},
		{"-max-reader-connections", "0"},
		{"-max-snapshot-reader-connections", "0"},
		{"-max-integrity-records", "1"},
		{"-sweep-interval", "0s"},
		{"-sweep-batch", "0"},
		{"-sweep-batch", strconv.Itoa(objectstore.MaxSweepBatch + 1)},
	} {
		args := append(append([]string{}, base...), c.flag, c.value)
		_, _, err := parseConfig(args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), c.flag) {
			t.Fatalf("%s %s returned %v", c.flag, c.value, err)
		}
	}
}

func TestHTTPResourceBoundsAreValidatedBeforeOpeningStorage(t *testing.T) {
	base := []string{"-listen", "127.0.0.1:0", "-dir", "/does/not/need/to/exist"}
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"body too small", []string{"-http-max-body-bytes", "1"}, "HandlerOptions"},
		{"request aggregate too small", []string{"-http-max-body-bytes", "2M", "-http-max-in-flight-body-bytes", "1M"}, "HandlerOptions"},
		{"negative body waiters", []string{"-http-max-waiting-bodies", "-1"}, "-http-max-waiting-bodies"},
		{"no response operations", []string{"-http-max-concurrent-responses", "0"}, "-http-max-concurrent-responses"},
		{"response aggregate too small", []string{"-http-max-body-bytes", "2M", "-http-max-in-flight-response-bytes", "7M"}, "HandlerOptions"},
		{"negative response waiters", []string{"-http-max-waiting-responses", "-1"}, "-http-max-waiting-responses"},
		{"no subscriptions", []string{"-http-max-subscriptions", "0"}, "-http-max-subscriptions"},
		{"frame too small", []string{"-http-max-frame-bytes", "1"}, "HandlerOptions"},
		{"no frame operations", []string{"-http-max-concurrent-snapshot-frames", "0"}, "-http-max-concurrent-snapshot-frames"},
		{"frame aggregate too small", []string{"-http-max-frame-bytes", "2M", "-http-max-in-flight-snapshot-frame-bytes", "5M"}, "HandlerOptions"},
		{"negative frame waiters", []string{"-http-max-waiting-snapshot-frames", "-1"}, "-http-max-waiting-snapshot-frames"},
		{"no accepted connections", []string{"-http-max-connections", "0"}, "-http-max-connections"},
		{"negative accepted connections", []string{"-http-max-connections", "-1"}, "-http-max-connections"},
		{"no header timeout", []string{"-http-read-header-timeout", "0s"}, "-http-read-header-timeout"},
		{"negative header timeout", []string{"-http-read-header-timeout", "-1s"}, "-http-read-header-timeout"},
		{"no idle timeout", []string{"-http-idle-timeout", "0s"}, "-http-idle-timeout"},
		{"negative idle timeout", []string{"-http-idle-timeout", "-1s"}, "-http-idle-timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := append(append([]string{}, base...), test.args...)
			_, _, err := parseConfig(command, &bytes.Buffer{})
			if err == nil {
				t.Fatalf("%v was accepted", command)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("%v returned an unrelated error: %v", command, err)
			}
		})
	}
}

func TestInvalidHTTPBoundsDoNotInitializeALocalStore(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"request aggregate", []string{"-http-max-body-bytes", "2M", "-http-max-in-flight-body-bytes", "1M"}},
		{"body waiters", []string{"-http-max-waiting-bodies", "-1"}},
		{"response operations", []string{"-http-max-concurrent-responses", "0"}},
		{"response aggregate", []string{"-http-max-in-flight-response-bytes", "1M"}},
		{"response waiters", []string{"-http-max-waiting-responses", "-1"}},
		{"subscriptions", []string{"-http-max-subscriptions", "0"}},
		{"frame size", []string{"-http-max-frame-bytes", "1"}},
		{"frame operations", []string{"-http-max-concurrent-snapshot-frames", "0"}},
		{"frame aggregate", []string{"-http-max-in-flight-snapshot-frame-bytes", "1M"}},
		{"frame waiters", []string{"-http-max-waiting-snapshot-frames", "-1"}},
		{"connections", []string{"-http-max-connections", "0"}},
		{"unbounded connections", []string{"-http-max-connections", strconv.Itoa(math.MaxInt)}},
		{"header timeout", []string{"-http-read-header-timeout", "0s"}},
		{"idle timeout", []string{"-http-idle-timeout", "0s"}},
		{"one connection cannot cold-mount", []string{"-http-max-connections", "1"}},
		{"snapshot readers", []string{"-max-snapshot-reader-connections", "0"}},
		{"unbounded snapshot readers", []string{"-max-snapshot-reader-connections", strconv.Itoa(math.MaxInt)}},
		{"integrity work below root minimum", []string{"-max-integrity-records", "1"}},
		{"unbounded integrity work", []string{"-max-integrity-records", strconv.FormatInt(math.MaxInt64, 10)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			args := []string{
				"-listen", "127.0.0.1:0",
				"-local-store", root,
				"-workspace", "workspace",
				"-quota", "8M",
			}
			err := run(append(args, test.args...), io.Discard)
			if err == nil {
				t.Fatal("incoherent HTTP resource bounds were accepted")
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("invalid HTTP configuration created %d local-store entries", len(entries))
			}
		})
	}
}

func TestInvalidHTTPBoundsAreRejectedBeforeAnInvalidListenAddress(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"-listen", "127.0.0.1",
		"-local-store", root,
		"-workspace", "workspace",
		"-quota", "8M",
		"-http-max-waiting-responses", "-1",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-http-max-waiting-responses") {
		t.Fatalf("invalid HTTP bounds did not precede listener acquisition: %v", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid HTTP configuration created %d local-store entries", len(entries))
	}
}

func TestInvalidMeasurementBoundsAreRejectedBeforeListenerAcquisition(t *testing.T) {
	for _, flagName := range []string{"-quota-max-directory-bytes", "-quota-max-frontier-bytes"} {
		t.Run(flagName, func(t *testing.T) {
			_, _, err := parseConfig([]string{
				"-listen", "127.0.0.1",
				"-dir", t.TempDir(),
				"-quota", "8M",
				flagName, strconv.FormatInt(math.MaxInt64, 10),
			}, &bytes.Buffer{})
			if err == nil || strings.Contains(err.Error(), "listen") {
				t.Fatalf("invalid measurement bound did not precede listener acquisition: %v", err)
			}
		})
	}
}

func TestInvalidPendingObjectLimitsDoNotInitializeALocalStore(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"-listen", "127.0.0.1:0",
		"-local-store", root,
		"-workspace", "workspace",
		"-quota", "8M",
		"-max-pending-bytes", "9223372036854775807",
	}, io.Discard)
	if err == nil {
		t.Fatal("an unbounded pending-byte limit was accepted")
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid pending-object limits created %d local-store entries", len(entries))
	}
}

func TestInvalidReaderLimitsDoNotInitializeALocalStore(t *testing.T) {
	for _, test := range []struct {
		name  string
		flag  string
		value string
	}{
		{"zero readers", "-max-reader-connections", "0"},
		{"unbounded readers", "-max-reader-connections", strconv.Itoa(math.MaxInt)},
		{"zero snapshot readers", "-max-snapshot-reader-connections", "0"},
		{"unbounded snapshot readers", "-max-snapshot-reader-connections", strconv.Itoa(math.MaxInt)},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			err := run([]string{
				"-listen", "127.0.0.1:0",
				"-local-store", root,
				"-workspace", "workspace",
				"-quota", "8M",
				test.flag, test.value,
			}, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.flag) {
				t.Fatalf("an invalid SQLite reader limit returned %v", err)
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("invalid SQLite reader limit created %d local-store entries", len(entries))
			}
		})
	}
}

func TestHelpNamesTheLocalStoreAndStatusSignal(t *testing.T) {
	var output bytes.Buffer
	_, help, err := parseConfig([]string{"-help"}, &output)
	if err != nil || !help {
		t.Fatalf("help=%t error=%v", help, err)
	}
	for _, phrase := range []string{
		"-local-store",
		"-http-max-body-bytes",
		"-http-max-write-bytes",
		"-http-max-waiting-bodies",
		"-http-max-concurrent-responses",
		"-http-max-in-flight-response-bytes",
		"-http-max-waiting-responses",
		"-http-max-subscriptions",
		"-http-max-frame-bytes",
		"-http-max-concurrent-snapshot-frames",
		"-http-max-in-flight-snapshot-frame-bytes",
		"-http-max-waiting-snapshot-frames",
		"-http-max-connections",
		"-http-read-header-timeout",
		"-http-idle-timeout",
		"-quota-max-directory-bytes",
		"-quota-max-frontier-bytes",
		"-max-pending-objects",
		"-max-pending-bytes",
		"-max-reader-connections",
		"-max-snapshot-reader-connections",
		"-max-integrity-records",
		"-sweep-interval",
		"-sweep-batch",
		"maximum SQLite reader connections",
		"maximum SQLite reader connections held by concurrent snapshots",
		"integrity record work limit",
		"larger retained namespaces",
		"reservation-admission threshold",
		"unresolved",
		"a larger payload fails EFBIG",
		"fails with EAGAIN",
		"request or response body",
		"waiting for body admission",
		"non-streaming HTTP responses",
		"four times -http-max-body-bytes",
		"waiting for response admission",
		"change streams attached",
		"encoded change, snapshot-row",
		"snapshot frames produced",
		"three times -http-max-frame-bytes",
		"snapshot frames waiting",
		"TCP connections accepted",
		"receive one HTTP request's headers",
		"keep-alive connection waits",
		"directory listing retained while measuring",
		"traversal frontier retained while measuring",
		"handler-owned deadlines",
		"process-wide",
		"additional requests fail with EAGAIN",
		"inherits -http-max-body-bytes",
		"may not exceed -local-max-object-bytes",
		"reads fail with EFBIG",
		"listings fail with EIO",
		"SIGHUP",
		"maintenance state",
	} {
		if !strings.Contains(output.String(), phrase) {
			t.Fatalf("help does not contain %q:\n%s", phrase, output.String())
		}
	}
}
