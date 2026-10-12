package smb

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

var directorySnapshotScope = storage.UseScope{Token: "directory-scope"}

type directorySnapshotTestSource struct {
	storage.FileSession
	entries                    []storage.Entry
	observation                storage.DirectoryObservation
	before                     func(storage.DirectoryTarget, *storage.ListResult) error
	checkErr, readErr, unitErr error
	loads                      int
	unit                       uint64
}

func (source *directorySnapshotTestSource) CheckDirectoryRead() error { return source.checkErr }
func (source *directorySnapshotTestSource) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	panic("unbounded directory observation")
}
func (source *directorySnapshotTestSource) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	if source.before != nil {
		if err := source.before(target, result); err != nil {
			return source.observation, err
		}
	}
	for _, entry := range source.entries {
		metadataBytes, err := storage.MetadataSize(entry.Attr.Metadata)
		if err != nil {
			return source.observation, err
		}
		scalar := entry.Attr
		scalar.Metadata = nil
		reservation, err := result.Reserve(int64(len(entry.Name)), int64(metadataBytes), scalar)
		if err != nil {
			return source.observation, err
		}
		source.loads++
		if err := reservation.Commit(entry.Name, entry.Attr.Metadata); err != nil {
			return source.observation, err
		}
	}
	return source.observation, source.readErr
}
func (source *directorySnapshotTestSource) CheckAllocationReporting() error { return nil }
func (source *directorySnapshotTestSource) AllocationUnitBytes(context.Context) (uint64, error) {
	return source.unit, source.unitErr
}

func directorySnapshotFixture() (*tree, *fileHandle, *directorySnapshotTestSource) {
	server := &Server{config: Config{Limits: DefaultLimits()}, nameComparer: testNameCompare}
	export := &Export{server: server}
	source := &directorySnapshotTestSource{unit: 4096, observation: storage.DirectoryObservation{ParentID: 1, Revision: []byte("one")}}
	t := &tree{export: export, authority: &authoritySession{raw: source}}
	h := &fileHandle{nodeID: 1, nodeKind: storage.NodeDirectory, use: storage.UseClaim{Uses: storage.ReadEntries}}
	return t, h, source
}

func directoryTestEntry(name string, id uint64, kind storage.NodeKind) storage.Entry {
	instant := time.Unix(100, 0).UTC()
	return storage.Entry{Name: name, Attr: storage.Attr{ID: id, Kind: kind, BirthTime: &instant, ChangeTime: &instant, AccessTime: instant, ModTime: instant, Size: 7, AllocationKnown: true, AllocationSize: 4096}}
}

func TestDirectorySnapshotCompleteCompactCapture(t *testing.T) {
	tree, handle, source := directorySnapshotFixture()
	source.entries = []storage.Entry{directoryTestEntry("z", 2, storage.NodeRegular), directoryTestEntry("A", 3, storage.NodeDirectory), directoryTestEntry("link", 4, storage.NodeSymlink)}
	source.entries[1].Attr.Size = -1
	source.entries[0].Attr.Metadata = map[string]storage.OpaquePayload{"other": {Version: []byte{1}, Data: []byte("private")}}
	if err := reserveDirectoryCursor(tree, handle); err != nil {
		t.Fatal(err)
	}
	defer handle.releaseDirectoryEnumeration()
	source.before = func(target storage.DirectoryTarget, result *storage.ListResult) error {
		if target.NodeID != handle.nodeID || target.Scope == nil || *target.Scope != directorySnapshotScope {
			t.Fatalf("capture target changed: %+v", target)
		}
		want := directoryCaptureFixedBytes + 4*int(result.MaxBytes())
		if got := tree.export.server.Status().RetainedDirectoryBytes; got != want {
			t.Fatalf("pre-payload staging: got %d want %d", got, want)
		}
		return nil
	}
	snapshot, err := captureDirectorySnapshot(context.Background(), tree, handle, directorySnapshotScope, nameUnits("*"))
	if err != nil {
		t.Fatal(err)
	}
	handle.directory.snapshot = snapshot
	if snapshot.richStatus() != statusOK || len(snapshot.entries) != 3 {
		t.Fatalf("capture fields: %+v", snapshot)
	}
	var names []string
	for _, entry := range snapshot.entries {
		if !entry.matched {
			t.Fatal("all-match entry filtered")
		}
		names = append(names, string(rune(entry.record.Name[0])))
	}
	if !reflect.DeepEqual(names, []string{"A", "l", "z"}) {
		t.Fatalf("raw ordering changed: %v", names)
	}
	if snapshot.entries[0].record.EndOfFile != 0 || snapshot.entries[1].record.EaSize != symlinkReparseTag || snapshot.entries[1].record.Attributes&dosReparsePoint == 0 {
		t.Fatalf("kind projection: %+v", snapshot.entries)
	}
	want := directoryCursorBytes + snapshot.bytes
	if got := tree.export.server.Status().RetainedDirectoryBytes; got != want || tree.export.directoryBytes != want {
		t.Fatalf("compact charge: server %d export %d want %d", got, tree.export.directoryBytes, want)
	}
	source.entries[0].Attr.Metadata["other"].Data[0] = 'X'
	source.observation.Revision[0] = 'X'
	if string(snapshot.observation.Revision) != "one" || snapshot.entries[2].record.EndOfFile != 7 {
		t.Fatal("snapshot retained mutable source state")
	}
	if unsafe.Sizeof(directorySnapshotEntry{})+unsafe.Sizeof(wire.DirectoryEntry{}) > 256 {
		t.Fatal("compact entry and exact-cap page copy exceed retained scalar allowance")
	}
}

func TestDirectorySnapshotFailureRefundsEveryReservation(t *testing.T) {
	for _, mode := range []string{"read", "check", "missing-reader", "zero-id", "self-id", "invalid-kind", "negative-file", "last-invalid", "collision", "parent", "revision", "local-limit", "work", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			tree, handle, source := directorySnapshotFixture()
			source.entries = []storage.Entry{directoryTestEntry("wanted", 2, storage.NodeRegular), directoryTestEntry("z", 3, storage.NodeRegular)}
			ctx := context.Background()
			switch mode {
			case "read":
				source.readErr = syscall.EIO
			case "check":
				source.checkErr = syscall.EIO
			case "missing-reader":
				tree.authority.raw = &struct{ storage.FileSession }{}
			case "zero-id":
				source.entries[1].Attr.ID = 0
			case "self-id":
				source.entries[1].Attr.ID = handle.nodeID
			case "invalid-kind":
				source.entries[1].Attr.Kind = storage.NodeKind(255)
			case "negative-file":
				source.entries[1].Attr.Size = -1
			case "last-invalid":
				source.entries[1].Name = "bad."
			case "collision":
				source.entries[1].Name = "WANTED"
			case "parent":
				source.observation.ParentID = 7
			case "revision":
				source.observation.Revision = nil
			case "local-limit":
				charge, _ := directoryEntryCaptureBytes(int64(len(source.entries[0].Name)), 6)
				tree.export.server.config.Limits.MaxDirectoryBytes = directoryCollectorFixedBytes + int(charge) - 1
			case "work":
				tree.export.server.config.Limits.MaxDirectoryMatchWork = 1
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				source.before = func(storage.DirectoryTarget, *storage.ListResult) error { cancel(); return nil }
			}
			if err := reserveDirectoryCursor(tree, handle); err != nil {
				t.Fatal(err)
			}
			handle.directory.pattern = nameUnits("wanted")
			if snapshot, err := captureDirectorySnapshot(ctx, tree, handle, directorySnapshotScope, handle.directory.pattern); err == nil || snapshot != nil {
				t.Fatalf("failed capture exposed data: %v %v", snapshot, err)
			}
			if got := tree.export.server.Status().RetainedDirectoryBytes; got != directoryCursorBytes {
				t.Fatalf("failed capture charge: %d", got)
			}
			if mode == "local-limit" && source.loads != 0 {
				t.Fatal("local rejection loaded payload")
			}
			handle.releaseDirectoryEnumeration()
			handle.releaseDirectoryEnumeration()
			if got := tree.export.server.Status().RetainedDirectoryBytes; got != 0 || tree.export.directoryBytes != 0 {
				t.Fatalf("close charge: %d / %d", got, tree.export.directoryBytes)
			}
		})
	}
}

func TestDirectorySnapshotRichFactsDoNotRejectNames(t *testing.T) {
	for _, mode := range []string{"unknown-allocation", "misaligned", "unit-error", "unit-missing", "zero-unit", "invalid-time", "unsupported-metadata"} {
		t.Run(mode, func(t *testing.T) {
			tree, handle, source := directorySnapshotFixture()
			source.entries = []storage.Entry{directoryTestEntry("visible", 2, storage.NodeRegular), directoryTestEntry("unmatched", 3, storage.NodeRegular)}
			want := statusIO
			switch mode {
			case "unknown-allocation":
				source.entries[1].Attr.AllocationKnown = false
				source.entries[1].Attr.AllocationSize = 0
			case "misaligned":
				source.entries[1].Attr.AllocationSize = 7
			case "unit-error":
				source.unitErr = errors.New("private backend diagnosis")
			case "unit-missing":
				source.unitErr = syscall.EOPNOTSUPP
				want = statusUnsupported
			case "zero-unit":
				source.unit = 0
			case "invalid-time":
				source.entries[1].Attr.AccessTime = time.Time{}
			case "unsupported-metadata":
				source.entries[1].Attr.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}, Data: []byte("SMW\x02")}}
				want = statusUnsupported
			}
			if err := reserveDirectoryCursor(tree, handle); err != nil {
				t.Fatal(err)
			}
			defer handle.releaseDirectoryEnumeration()
			snapshot, err := captureDirectorySnapshot(context.Background(), tree, handle, directorySnapshotScope, nameUnits("visible"))
			if err != nil {
				t.Fatal(err)
			}
			handle.directory.snapshot = snapshot
			if snapshot.richStatus() != want {
				t.Fatalf("rich classification: got %#x want %#x", snapshot.richStatus(), want)
			}
			if snapshot.entries[0].matched || !snapshot.entries[1].matched {
				t.Fatalf("names changed by unavailable rich facts: %+v", snapshot.entries)
			}
		})
	}
}

func TestDirectorySnapshotAggregateBudgetAndRestart(t *testing.T) {
	tree, handle, source := directorySnapshotFixture()
	source.entries = []storage.Entry{directoryTestEntry("old", 2, storage.NodeRegular)}
	if err := reserveDirectoryCursor(tree, handle); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captureDirectorySnapshot(context.Background(), tree, handle, directorySnapshotScope, nameUnits("*"))
	if err != nil {
		t.Fatal(err)
	}
	handle.directory.snapshot = snapshot
	handle.directory.pattern = nameUnits("*")
	releaseDirectorySnapshot(tree, handle)
	if got := tree.export.server.Status().RetainedDirectoryBytes; got != directoryCursorBytes || !reflect.DeepEqual(handle.directory.pattern, nameUnits("*")) {
		t.Fatalf("restart pattern charge/state: %d %+v", got, handle.directory)
	}
	limit := directoryCaptureFixedBytes + 5*8192
	tree.export.server.config.Limits.MaxRetainedDirectoryBytes = limit
	source.before = func(_ storage.DirectoryTarget, result *storage.ListResult) error {
		if result.MaxBytes() != 8192 {
			t.Fatalf("affordable small capture limit: %d", result.MaxBytes())
		}
		return nil
	}
	snapshot, err = captureDirectorySnapshot(context.Background(), tree, handle, directorySnapshotScope, handle.directory.pattern)
	if err != nil {
		t.Fatal(err)
	}
	handle.directory.snapshot = snapshot
	secondExport := &Export{server: tree.export.server}
	if err := reserveDirectoryBytes(secondExport, limit-directoryCursorBytes-snapshot.bytes); err != nil {
		t.Fatal(err)
	}
	if got := tree.export.server.Status().RetainedDirectoryBytes; got != limit {
		t.Fatalf("cross-export total: %d", got)
	}
	if err := reserveDirectoryBytes(secondExport, 1); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("aggregate denial: %v", err)
	}
	if _, err := captureDirectorySnapshot(context.Background(), tree, handle, directorySnapshotScope, handle.directory.pattern); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("staging denial: %v", err)
	}
	foreign := secondExport.directoryBytes
	handle.releaseDirectoryEnumeration()
	if got := tree.export.server.Status().RetainedDirectoryBytes; got != foreign || tree.export.directoryBytes != 0 {
		t.Fatalf("saturated cleanup: %d, foreign %d", got, foreign)
	}
	releaseDirectoryBytes(secondExport, foreign)
	if got := tree.export.server.Status().RetainedDirectoryBytes; got != 0 {
		t.Fatalf("final aggregate: %d", got)
	}
}

func TestDirectorySnapshotConfigBounds(t *testing.T) {
	defaults := DefaultLimits()
	if defaults.MaxRetainedDirectoryBytes != 64<<20 || defaults.MaxDirectoryMatchWork != 16<<20 {
		t.Fatalf("directory defaults: %+v", defaults)
	}
	for _, work := range []bool{false, true} {
		limits := defaults
		if work {
			limits.MaxDirectoryMatchWork = 0
		} else {
			limits.MaxRetainedDirectoryBytes = 0
		}
		if !errors.Is(limits.check(), ErrConfig) {
			t.Fatal("omitted directory bound accepted")
		}
	}
}

func TestDirectorySnapshotCancelledCloseRetainsUntilCleanup(t *testing.T) {
	tree, handle, source := directorySnapshotFixture()
	source.entries = []storage.Entry{directoryTestEntry("retained", 2, storage.NodeRegular)}
	if err := reserveDirectoryCursor(tree, handle); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captureDirectorySnapshot(context.Background(), tree, handle, directorySnapshotScope, nameUnits("*"))
	if err != nil {
		t.Fatal(err)
	}
	handle.directory.snapshot = snapshot
	handle.published = true
	handle.file = &ownerActionFile{status: func() (storage.CloseOwnerStatus, error) {
		if handle.directory != nil || tree.export.server.Status().RetainedDirectoryBytes != 0 {
			t.Fatal("native close began before read-only cache release")
		}
		return storage.CloseOwnerStatus{}, syscall.EIO
	}}
	handle.ioRunning = &fileIOTicket{started: true}
	handle.ioIdle = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tree.closeFileHandle(ctx, handle); !errors.Is(err, context.Canceled) {
		t.Fatalf("close cancellation: %v", err)
	}
	if handle.directory == nil || tree.export.server.Status().RetainedDirectoryBytes != directoryCursorBytes+snapshot.bytes {
		t.Fatal("cancelled close released active snapshot")
	}
	close(handle.ioIdle)
	handle.ioIdle = nil
	handle.ioRunning = nil
	if err := tree.closeFileHandle(context.Background(), handle); err == nil {
		t.Fatal("unknown native close reported success")
	}
	if handle.directory != nil || tree.export.server.Status().RetainedDirectoryBytes != 0 || handleState(handle.state.Load()) != handleCleanupOnly {
		t.Fatal("uncertain native close retained read-only snapshot")
	}
	if err := tree.closeFileHandle(context.Background(), handle); err == nil {
		t.Fatal("close failure disappeared")
	}
	if tree.export.server.Status().RetainedDirectoryBytes != 0 {
		t.Fatal("close retry changed released charge")
	}
}

type directoryPendingCloseNode struct {
	*mutationOwnerNode
	storage.ReferenceCloseActions
}

func (*directoryPendingCloseNode) CloseOwnerStatus(context.Context) (storage.CloseOwnerStatus, error) {
	return storage.CloseOwnerStatus{}, syscall.EIO
}

func TestDirectorySnapshotInternalCloseReleasesBeforePendingMutationQuery(t *testing.T) {
	server, tree, handle, node, session, command := metadataOwnerFixture(t, storage.NodeDirectory)
	handle.node = &directoryPendingCloseNode{mutationOwnerNode: node}
	handle.published = true
	node.mutate = func(storage.FileMutation) (storage.Attr, error) { return storage.Attr{}, syscall.EIO }
	session.query = func(storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, syscall.EIO
	}
	owner, err := server.reserveMutationOwner(tree, handle, 1, command, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.executeMutation(t.Context(), func(context.Context) error { return nil }, nil, nil); !errors.Is(err, syscall.EIO) || !owner.pending() {
		t.Fatalf("unknown directory Basic mutation: %v", err)
	}
	handle.pendingWrite = owner
	if err := reserveDirectoryCursor(tree, handle); err != nil {
		t.Fatal(err)
	}
	name := nameUnits("captured")
	charge := 256 + 2*len(name)
	if err := reserveDirectoryBytes(tree.export, charge); err != nil {
		t.Fatal(err)
	}
	handle.directory.snapshot = &directorySnapshot{bytes: charge, entries: []directorySnapshotEntry{{record: wire.DirectoryEntry{Name: name, FileID: 2}, matched: true}}}
	entered, resume := make(chan struct{}), make(chan struct{})
	session.query = func(storage.FileActionID) (storage.FileActionReceipt, error) {
		close(entered)
		<-resume
		return storage.FileActionReceipt{}, syscall.EIO
	}
	done := make(chan error, 1)
	go func() { done <- tree.closeFileHandle(t.Context(), handle) }()
	defer func() {
		close(resume)
		if err := <-done; !errors.Is(err, syscall.EIO) {
			t.Errorf("pending mutation settlement: %v", err)
		}
		if !owner.pending() || server.Status().WriteOwners != 1 {
			t.Error("snapshot release discharged the unknown mutation owner")
		}
	}()
	<-entered
	if got := server.Status().RetainedDirectoryBytes; got != 0 || tree.export.directoryBytes != 0 {
		t.Fatalf("internal close retained snapshot while mutation query blocked: %d / %d", got, tree.export.directoryBytes)
	}
	if handle.directory != nil {
		t.Fatal("fenced cursor remained reachable during mutation recovery")
	}
}
