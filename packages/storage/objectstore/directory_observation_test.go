package objectstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func objectMetadataList(t *testing.T) *storage.ListResult {
	t.Helper()
	result, err := storage.NewListResult(storage.MaxDirectoryBytes, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(nameBytes, metadataBytes)
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func objectObservationNode(t *testing.T, namespace storage.NamespaceAccess, kind storage.NameOperation, name storage.ChildName, metadata map[string][]byte) storage.Attr {
	t.Helper()
	created, err := namespace.MutateName(t.Context(), storage.NameCommand{Action: objectAction(t), Kind: kind, Name: name, Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{Metadata: metadata}})
	if err != nil || created.Attr == nil {
		t.Fatalf("observation fixture node = %+v, %v", created, err)
	}
	return *created.Attr
}

func requireObjectMetadataFailure(t *testing.T, observed storage.DirectoryMetadataObservation, result *storage.ListResult, err, cause error) {
	t.Helper()
	if !errors.Is(err, cause) || !reflect.DeepEqual(observed, storage.DirectoryMetadataObservation{}) {
		t.Fatalf("failed observation = %+v, %v; want zero observation and %v", observed, err, cause)
	}
	if entries, listErr := result.Entries(); entries != nil || !errors.Is(listErr, cause) {
		t.Fatalf("failed observation retained a usable result = %+v, %v; want %v", entries, listErr, cause)
	}
}

func TestObjectDirectoryMetadataObservationKeepsApplicationPermissionsSeparate(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := fileSessionFor(t, volume, options)
	namespace := objectCapability[storage.NamespaceAccess](t, session)
	reader := objectCapability[storage.DirectoryReader](t, session)
	observer := objectCapability[storage.DirectoryMetadataObserver](t, session)
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directoryLeaf, childLeaf := []byte{'d', 0xff}, []byte{'c', 0xfe}
	directory := objectObservationNode(t, namespace, storage.NameMkdir, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: directoryLeaf}, nil)
	child := objectObservationNode(t, namespace, storage.NameCreate, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: directory.ID}, RawLeaf: childLeaf}, map[string][]byte{"test.raw": {0xff, 0, 7}})
	opened, err := objectCapability[storage.NodeReferences](t, session).OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{
		Action: objectAction(t), Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}, Use: storage.UseClaim{Deny: storage.ReadEntries},
	})
	if err != nil || opened.Reference == nil {
		t.Fatalf("metadata-disclosure reference = %+v, %v", opened, err)
	}
	scope := objectScope(t, opened.Reference)
	target := storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}
	result := objectMetadataList(t)
	withoutName, err := observer.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{}, result)
	if err != nil || withoutName.Name != nil || withoutName.Observation.ParentID != directory.ID || len(withoutName.Observation.Revision) == 0 {
		t.Fatalf("metadata observation without name = %+v, %v", withoutName, err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || !bytes.Equal([]byte(entries[0].Name), childLeaf) || !reflect.DeepEqual(entries[0].Attr, child) {
		t.Fatalf("metadata observation entries = %+v, %v", entries, err)
	}
	result = objectMetadataList(t)
	withName, err := observer.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil || withName.Name == nil || withName.Name.State != storage.NameLinked || withName.Name.NodeID != directory.ID || withName.Name.ParentID != root.ID || !bytes.Equal(withName.Name.RawLeaf, directoryLeaf) || !reflect.DeepEqual(withName.Observation, withoutName.Observation) {
		t.Fatalf("metadata observation with name = %+v, %v", withName, err)
	}
	wantRevision := bytes.Clone(withName.Observation.Revision)
	withName.Name.RawLeaf[0] = 'x'
	withName.Observation.Revision[0] ^= 1
	entries[0].Attr.Metadata["test.raw"].Data[0] ^= 1
	entries[0].Attr.Metadata["test.raw"].Version[0] ^= 1
	result = objectMetadataList(t)
	again, err := observer.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil || again.Name == nil || !bytes.Equal(again.Name.RawLeaf, directoryLeaf) || !bytes.Equal(again.Observation.Revision, wantRevision) {
		t.Fatalf("returned observation bytes changed authority state = %+v, %v", again, err)
	}
	if current, err := result.Entries(); err != nil || len(current) != 1 || !reflect.DeepEqual(current[0].Attr, child) || !bytes.Equal([]byte(current[0].Name), childLeaf) {
		t.Fatalf("returned metadata bytes changed authority state = %+v, %v", current, err)
	}
	if _, err := reader.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata observation granted scoped enumeration: %v", err)
	}
	if _, err := reader.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("metadata observation bypassed application enumeration denial: %v", err)
	}
	if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata observation granted reference Stat: %v", err)
	}
	if _, err := objectCapability[storage.ReferenceStateAccess](t, opened.Reference).State(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata observation granted reference State: %v", err)
	}
}

func TestObjectDirectoryMetadataObservationChargesTheActualPrefixOnce(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	namespace := objectCapability[storage.NamespaceAccess](t, session)
	observer := objectCapability[storage.DirectoryMetadataObserver](t, session)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	rawLeaf := []byte{'d', 0xff}
	directory := objectObservationNode(t, namespace, storage.NameMkdir, storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: rawLeaf}, nil)
	child := objectObservationNode(t, namespace, storage.NameCreate, objectChild(directory.ID, "kid"), nil)
	minimum, err := storage.NameObservationRetentionBytes(int64(len(rawLeaf)))
	if err != nil {
		t.Fatal(err)
	}
	const fixed, childCharge = int64(7), int64(3 + 6)
	prefixCharge := minimum + 37
	for _, test := range []struct {
		name        string
		includeName bool
		maximum     int64
		failure     error
	}{
		{name: "without name", maximum: fixed + childCharge},
		{name: "exact prefix and child", includeName: true, maximum: fixed + prefixCharge + childCharge},
		{name: "one byte short", includeName: true, maximum: fixed + prefixCharge + childCharge - 1, failure: syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			var order []string
			ctx := storage.WithNameObservationBudget(t.Context(), func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
				order = append(order, "prefix")
				if scalar.NodeID != directory.ID || scalar.ParentID != root.ID || scalar.State != storage.NameLinked || scalar.RawLeaf != nil || leafBytes != int64(len(rawLeaf)) {
					return 0, fmt.Errorf("unexpected unloaded name header %+v, %d: %w", scalar, leafBytes, syscall.EINVAL)
				}
				return prefixCharge, nil
			})
			result, err := storage.NewListResult(test.maximum, fixed, func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
				order = append(order, "child")
				if index != 0 || nameBytes != 3 || metadataBytes != 6 || attr.ID != child.ID || len(attr.Metadata) != 0 {
					return 0, fmt.Errorf("unexpected unloaded child header %d, %d, %d, %+v: %w", index, nameBytes, metadataBytes, attr, syscall.EINVAL)
				}
				return nameBytes + metadataBytes, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			observed, err := observer.ObserveDirectoryMetadata(ctx, storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{IncludeName: test.includeName}, result)
			wantOrder := []string{"child"}
			if test.includeName {
				wantOrder = []string{"prefix", "child"}
			}
			if !reflect.DeepEqual(order, wantOrder) {
				t.Fatalf("observation charges = %v, want %v", order, wantOrder)
			}
			if test.failure != nil {
				requireObjectMetadataFailure(t, observed, result, err, test.failure)
				return
			}
			if err != nil || observed.Observation.ParentID != directory.ID || len(observed.Observation.Revision) == 0 || (observed.Name != nil) != test.includeName {
				t.Fatalf("exact-budget observation = %+v, %v", observed, err)
			}
			if observed.Name != nil && !bytes.Equal(observed.Name.RawLeaf, rawLeaf) {
				t.Fatalf("exact-budget raw binding = %+v", observed.Name)
			}
			if entries, err := result.Entries(); err != nil || len(entries) != 1 || entries[0].Name != "kid" || !reflect.DeepEqual(entries[0].Attr, child) {
				t.Fatalf("exact-budget directory entries = %+v, %v", entries, err)
			}
		})
	}
}

func TestObjectDirectoryMetadataObservationPreservesGuardsScopesAndCancellation(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	foreign := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	namespace := objectCapability[storage.NamespaceAccess](t, session)
	reader := objectCapability[storage.DirectoryReader](t, session)
	observer := objectCapability[storage.DirectoryMetadataObserver](t, session)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	parent := objectObservationNode(t, namespace, storage.NameMkdir, objectChild(root.ID, "parent"), nil)
	directory := objectObservationNode(t, namespace, storage.NameMkdir, objectChild(parent.ID, "directory"), nil)
	child := objectObservationNode(t, namespace, storage.NameCreate, objectChild(directory.ID, "child"), nil)
	rootView, err := reader.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: root.ID})
	if err != nil {
		t.Fatal(err)
	}
	parentView, err := reader.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: parent.ID})
	if err != nil {
		t.Fatal(err)
	}
	guards := &storage.NamespaceGuards{
		RootID: root.ID, Directories: []storage.DirectoryObservation{rootView.Observation, parentView.Observation},
		Edges: []storage.ObservedEdge{{ParentID: root.ID, RawLeaf: []byte("parent"), ChildID: parent.ID}, {ParentID: parent.ID, RawLeaf: []byte("directory"), ChildID: directory.ID}},
	}
	before, err := json.Marshal(guards)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := objectCapability[storage.NodeReferences](t, session).OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{Action: objectAction(t), Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: directory.ID}})
	if err != nil || opened.Reference == nil {
		t.Fatalf("directory scope fixture = %+v, %v", opened, err)
	}
	scope := objectScope(t, opened.Reference)
	target := storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}
	options := storage.DirectoryMetadataOptions{Guards: guards, IncludeName: true}
	result := objectMetadataList(t)
	observed, err := observer.ObserveDirectoryMetadata(t.Context(), target, options, result)
	if err != nil || observed.Name == nil || observed.Name.ParentID != parent.ID || string(observed.Name.RawLeaf) != "directory" {
		t.Fatalf("guarded directory observation = %+v, %v", observed, err)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 1 || entries[0].Attr.ID != child.ID {
		t.Fatalf("guarded directory entries = %+v, %v", entries, err)
	}
	result = objectMetadataList(t)
	observed, err = objectCapability[storage.DirectoryMetadataObserver](t, foreign).ObserveDirectoryMetadata(t.Context(), target, options, result)
	requireObjectMetadataFailure(t, observed, result, err, storage.ErrInvalidScope)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	result = objectMetadataList(t)
	observed, err = observer.ObserveDirectoryMetadata(canceled, target, options, result)
	requireObjectMetadataFailure(t, observed, result, err, context.Canceled)
	budgetFailure := errors.New("own-name budget refused")
	budgeted := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) { return 0, budgetFailure })
	result = objectMetadataList(t)
	observed, err = observer.ObserveDirectoryMetadata(budgeted, target, options, result)
	requireObjectMetadataFailure(t, observed, result, err, budgetFailure)
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	result = objectMetadataList(t)
	observed, err = observer.ObserveDirectoryMetadata(t.Context(), target, options, result)
	requireObjectMetadataFailure(t, observed, result, err, storage.ErrInvalidScope)
	if err := volume.Rename(t.Context(), "parent", "moved"); err != nil {
		t.Fatal(err)
	}
	result = objectMetadataList(t)
	observed, err = observer.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}, options, result)
	requireObjectMetadataFailure(t, observed, result, err, storage.ErrConditionConflict)
	after, err := json.Marshal(guards)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("observer changed supplied prefix guards: %v", err)
	}
	result = objectMetadataList(t)
	observed, err = observer.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil || observed.Observation.ParentID != directory.ID || observed.Name == nil || observed.Name.ParentID != parent.ID {
		t.Fatalf("fresh identity observation after prefix move = %+v, %v", observed, err)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 1 || entries[0].Attr.ID != child.ID {
		t.Fatalf("fresh observation after prefix move = %+v, %v", entries, err)
	}
}

type objectDirectoryObservationProbe struct {
	*sqlite.LockingStore
	checkErr, observeErr error
	malformed            string
	calls                int
	capturedEntries      int
	captured             storage.DirectoryMetadataObservation
	seenTarget           storage.DirectoryTarget
	seenOptions          storage.DirectoryMetadataOptions
	seenResult           *storage.ListResult
}

type objectNamespaceSubstitutionProbe struct {
	*sqlite.LockingStore
}

type objectDirectoryReaderOnly struct {
	metastore.Store
	metastore.FileStore
	metastore.BoundedLister
	metastore.DirectoryReader
}

type objectDirectoryReaderProbe struct{}

func (objectDirectoryReaderProbe) CheckDirectoryRead() error { return nil }
func (objectDirectoryReaderProbe) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{1}}}, nil
}
func (objectDirectoryReaderProbe) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, _ *storage.ListResult) (storage.DirectoryObservation, error) {
	return storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte{1}}, nil
}

func TestObjectDirectoryReadCapabilityIsIndependentOfNamespaceMutation(t *testing.T) {
	native, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "directory-only.db"), Volume: "directory-only", SQLite: sqlite.DefaultOptions(),
		Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := objectDirectoryReaderOnly{Store: native, FileStore: native, BoundedLister: native, DirectoryReader: objectDirectoryReaderProbe{}}
	volume := objectstore.New(memory.New(), backend)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close directory-only volume: %v", err)
		}
		if err := native.Close(); err != nil {
			t.Errorf("close directory-only metastore: %v", err)
		}
	})
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	reader := objectCapability[storage.DirectoryReader](t, session)
	if err := reader.CheckDirectoryRead(); err != nil {
		t.Fatal(err)
	}
	if err := session.(storage.NamespaceAccess).CheckNamespaceAccess(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("directory-only backend exposed namespace mutation: %v", err)
	}
	if observed, err := reader.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: root.ID}); err != nil || observed.Observation.ParentID != root.ID || len(observed.Entries) != 0 {
		t.Fatalf("directory-only read = %+v, %v", observed, err)
	}
}

func (p *objectNamespaceSubstitutionProbe) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	observed, err := p.LockingStore.ReadDirNode(ctx, target)
	if err == nil {
		observed.Observation.ParentID++
	}
	return observed, err
}

func (p *objectNamespaceSubstitutionProbe) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	observation, err := p.LockingStore.ReadDirNodeBounded(ctx, target, result)
	if err == nil {
		observation.ParentID++
	}
	return observation, err
}

func TestObjectNamespaceRejectsSubstitutedDirectoryIdentity(t *testing.T) {
	native, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "namespace.db"), Volume: "namespace", SQLite: sqlite.DefaultOptions(),
		Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), &objectNamespaceSubstitutionProbe{LockingStore: native})
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close namespace substitution volume: %v", err)
		}
	})
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	reader := objectCapability[storage.DirectoryReader](t, session)
	target := storage.DirectoryTarget{NodeID: root.ID}
	if observed, err := reader.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.ObservedDirectory{}) {
		t.Fatalf("substituted directory = %+v, %v", observed, err)
	}
	result := objectMetadataList(t)
	if observed, err := reader.ReadDirNodeBounded(t.Context(), target, result); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.DirectoryObservation{}) {
		t.Fatalf("substituted bounded directory = %+v, %v", observed, err)
	}
	if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("substituted bounded directory exposed %+v, %v", entries, err)
	}
}

func (p *objectDirectoryObservationProbe) CheckDirectoryMetadataObservation() error {
	return p.checkErr
}

func (p *objectDirectoryObservationProbe) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	p.calls++
	p.seenTarget, p.seenOptions, p.seenResult = target, options, result
	observed, err := p.LockingStore.ObserveDirectoryMetadata(ctx, target, options, result)
	if err != nil {
		return observed, err
	}
	entries, err := result.Entries()
	if err != nil {
		return observed, err
	}
	p.capturedEntries = len(entries)
	p.captured = observed
	switch p.malformed {
	case "missing name":
		observed.Name = nil
	case "wrong directory":
		observed.Observation.ParentID++
	}
	return observed, p.observeErr
}

func TestObjectDirectoryMetadataObservationRejectsUnsupportedAndMalformedBackends(t *testing.T) {
	checkFailure, nativeFailure := errors.New("directory metadata check failed"), errors.New("directory metadata capture failed")
	for _, test := range []struct {
		name                string
		missing, nilResult  bool
		checkErr, nativeErr error
		malformed           string
		want                error
	}{
		{name: "missing capability", missing: true, want: syscall.EOPNOTSUPP},
		{name: "check failure", checkErr: checkFailure, want: checkFailure},
		{name: "nil result", nilResult: true, want: syscall.EINVAL},
		{name: "native error with entries", nativeErr: nativeFailure, want: nativeFailure},
		{name: "missing requested name", malformed: "missing name", want: syscall.EIO},
		{name: "mismatched directory", malformed: "wrong directory", want: syscall.EIO},
		{name: "owned observation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			native, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
				Database: filepath.Join(t.TempDir(), "directory.db"), Volume: "directory", SQLite: sqlite.DefaultOptions(),
				Locks: locking.DefaultOptions(), Initialize: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			probe := &objectDirectoryObservationProbe{LockingStore: native, checkErr: test.checkErr, observeErr: test.nativeErr, malformed: test.malformed}
			var backend metastore.Store = probe
			if test.missing {
				backend = struct {
					metastore.Store
					metastore.FileStore
					metastore.BoundedLister
				}{Store: native, FileStore: native, BoundedLister: native}
			}
			volume := objectstore.New(memory.New(), backend)
			t.Cleanup(func() {
				if err := volume.Close(); err != nil {
					t.Errorf("close directory observation probe: %v", err)
				}
			})
			if err := volume.Mkdir(t.Context(), "d\xff"); err != nil {
				t.Fatal(err)
			}
			if err := volume.Create(t.Context(), "d\xff/c\xfe"); err != nil {
				t.Fatal(err)
			}
			root, err := volume.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			directory, err := volume.Stat(t.Context(), "d\xff")
			if err != nil {
				t.Fatal(err)
			}
			session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			observer := objectCapability[storage.DirectoryMetadataObserver](t, session)
			checkWant := test.checkErr
			if test.missing {
				checkWant = syscall.EOPNOTSUPP
			}
			if err := observer.CheckDirectoryMetadataObservation(); !errors.Is(err, checkWant) {
				t.Fatalf("directory metadata check = %v, want %v", err, checkWant)
			}
			target := storage.DirectoryTarget{NodeID: directory.ID}
			guards := &storage.NamespaceGuards{RootID: root.ID, Edges: []storage.ObservedEdge{{ParentID: root.ID, RawLeaf: []byte("d\xff"), ChildID: directory.ID}}}
			options := storage.DirectoryMetadataOptions{Guards: guards, IncludeName: true}
			result := objectMetadataList(t)
			if test.nilResult {
				result = nil
			}
			observed, err := observer.ObserveDirectoryMetadata(t.Context(), target, options, result)
			wantCalls := 1
			if test.missing || test.checkErr != nil || test.nilResult {
				wantCalls = 0
			}
			if probe.calls != wantCalls {
				t.Fatalf("native observation calls = %d, want %d", probe.calls, wantCalls)
			}
			if wantCalls == 1 && (probe.captured.Check(target, options) != nil || probe.capturedEntries != 1) {
				t.Fatalf("native failure fixture did not capture its complete observation: %+v, %d entries", probe.captured, probe.capturedEntries)
			}
			if test.nilResult {
				if !errors.Is(err, test.want) || !reflect.DeepEqual(observed, storage.DirectoryMetadataObservation{}) {
					t.Fatalf("nil result observation = %+v, %v", observed, err)
				}
				return
			}
			if test.want != nil {
				requireObjectMetadataFailure(t, observed, result, err, test.want)
				return
			}
			if err != nil || !reflect.DeepEqual(observed, probe.captured) || probe.seenTarget != target || probe.seenOptions.Guards != guards || !probe.seenOptions.IncludeName || probe.seenResult != result {
				t.Fatalf("forwarded directory observation = %+v, %v", observed, err)
			}
			if entries, err := result.Entries(); err != nil || len(entries) != 1 || entries[0].Name != "c\xfe" {
				t.Fatalf("forwarded directory entries = %+v, %v", entries, err)
			}
			originalRevision := bytes.Clone(probe.captured.Observation.Revision)
			observed.Observation.Revision[0] ^= 1
			observed.Name.RawLeaf[0] ^= 1
			if !bytes.Equal(probe.captured.Observation.Revision, originalRevision) || string(probe.captured.Name.RawLeaf) != "d\xff" {
				t.Fatal("returned observation aliases native name or revision bytes")
			}
		})
	}
}
