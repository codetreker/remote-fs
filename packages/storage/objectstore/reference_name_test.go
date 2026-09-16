package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type nameOnlyObjects struct {
	*memory.Objects
	reads atomic.Int64
}

func (o *nameOnlyObjects) Get(context.Context, string) ([]byte, error) {
	o.reads.Add(1)
	return nil, syscall.EIO
}

func (o *nameOnlyObjects) GetBounded(context.Context, string, int64) ([]byte, error) {
	o.reads.Add(1)
	return nil, syscall.EIO
}

func TestObjectReferenceNameObservationTracksIdentityWithoutDataIO(t *testing.T) {
	objects := &nameOnlyObjects{Objects: memory.New()}
	volume, meta := fileVolume(t, objects, 4096, func(options *sqlite.Options) { options.MaxRetainedFiles = 1 })
	for _, dir := range []string{"p", "q"} {
		if err := volume.Mkdir(t.Context(), dir); err != nil {
			t.Fatal(err)
		}
	}
	if err := volume.Write(t.Context(), "p/a\xff", []byte("retained")); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := volume.Stat(t.Context(), "q")
	if err != nil {
		t.Fatal(err)
	}
	original, err := volume.Stat(t.Context(), "p/a\xff")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := fileSessionFor(t, volume, options)
	file := openFileFor(t, session, "p/a\xff", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	observer := objectCapability[storage.ReferenceNameObserver](t, file)
	if err := observer.CheckReferenceNameObservation(); err != nil {
		t.Fatal(err)
	}
	if id, err := storage.ReferenceNodeID(file); err != nil || id != original.ID {
		t.Fatalf("reference identity = %d, %v", id, err)
	}
	if err := volume.Rename(t.Context(), "p/a\xff", "q/b\xfe"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Create(t.Context(), "p/a\xff"); err != nil {
		t.Fatal(err)
	}
	budgetFailure := errors.New("name result capacity exhausted")
	budgetCalls := 0
	budgetCtx := storage.WithNameObservationBudget(t.Context(), func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
		budgetCalls++
		if scalar.NodeID != original.ID || scalar.RawLeaf != nil || leafBytes != 2 {
			t.Fatalf("unloaded name budget = %+v, %d", scalar, leafBytes)
		}
		return 0, budgetFailure
	})
	if got, err := observer.ObserveName(budgetCtx, nil); !errors.Is(err, budgetFailure) || !reflect.DeepEqual(got, storage.NameObservation{}) || budgetCalls != 1 {
		t.Fatalf("name budget refusal = %+v, %v; calls %d", got, err, budgetCalls)
	}
	observed, err := observer.ObserveName(t.Context(), nil)
	if err != nil || observed.NodeID != original.ID || observed.State != storage.NameLinked || observed.ParentID != parent.ID || !bytes.Equal(observed.RawLeaf, []byte("b\xfe")) {
		t.Fatalf("current binding = %+v, %v", observed, err)
	}
	observed.RawLeaf[0] = 'x'
	if fresh, err := observer.ObserveName(t.Context(), nil); err != nil || !bytes.Equal(fresh.RawLeaf, []byte("b\xfe")) {
		t.Fatalf("owned binding bytes = %+v, %v", fresh, err)
	}
	list, err := storage.NewListResult(4096, 0, func(_ int, names, metadata int64, _ storage.Attr) (int64, error) { return names + metadata + 64, nil })
	if err != nil {
		t.Fatal(err)
	}
	directory := objectCapability[storage.DirectoryMetadataObserver](t, session)
	prefix, err := directory.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: root.ID}, storage.DirectoryMetadataOptions{}, list)
	if err != nil {
		t.Fatal(err)
	}
	guards := &storage.NamespaceGuards{RootID: root.ID, Directories: []storage.DirectoryObservation{prefix.Observation},
		Edges: []storage.ObservedEdge{{ParentID: root.ID, RawLeaf: []byte("q"), ChildID: parent.ID}}}
	if err := volume.Rename(t.Context(), "q", "moved"); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), guards); !errors.Is(err, storage.ErrConditionConflict) || !reflect.DeepEqual(got, storage.NameObservation{}) {
		t.Fatalf("stale prefix = %+v, %v", got, err)
	}
	if attr, err := file.Stat(t.Context()); err != nil || attr.ID != original.ID || attr.Size != 8 {
		t.Fatalf("independent identity state = %+v, %v", attr, err)
	}
	if err := volume.Remove(t.Context(), "moved/b\xfe"); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), nil); err != nil || got.NodeID != original.ID || got.State != storage.NameDetached || got.ParentID != 0 || got.RawLeaf != nil {
		t.Fatalf("detached binding = %+v, %v", got, err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("retained usage = %d, %v", used, err)
	}
	if objects.reads.Load() != 0 {
		t.Fatalf("name observations materialized %d content objects", objects.reads.Load())
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), nil); !errors.Is(err, syscall.EBADF) || !reflect.DeepEqual(got, storage.NameObservation{}) {
		t.Fatalf("closed observation = %+v, %v", got, err)
	}
	if id, err := storage.ReferenceNodeID(file); err != nil || id != original.ID {
		t.Fatalf("closed immutable identity = %d, %v", id, err)
	}
}

func TestObjectReferenceNameObservationRequiresNoMetadataPermission(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Create(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	attr, err := volume.Stat(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	references := objectCapability[storage.NodeReferences](t, session)
	opened, err := references.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{Kind: storage.NodeRegular,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	observer := objectCapability[storage.ReferenceNameObserver](t, opened.Reference)
	if err := observer.CheckReferenceNameObservation(); err != nil {
		t.Fatal(err)
	}
	if id, err := storage.ReferenceNodeID(opened.Reference); err != nil || id != attr.ID {
		t.Fatalf("metadata reference identity = %d, %v", id, err)
	}
	if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata-only permission = %v", err)
	}
	if _, err := objectCapability[storage.ReferenceStateAccess](t, opened.Reference).State(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("state permission = %v", err)
	}
	if err := volume.Rename(t.Context(), "source", "current"); err != nil {
		t.Fatal(err)
	}
	observed, err := observer.ObserveName(t.Context(), nil)
	if err != nil || observed.NodeID != attr.ID || observed.ParentID != root.ID || string(observed.RawLeaf) != "current" {
		t.Fatalf("independently permitted name = %+v, %v", observed, err)
	}
	scope := objectScope(t, opened.Reference)
	command := storage.NameCommand{Kind: storage.NameRename, Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: observed.ParentID}, RawLeaf: observed.RawLeaf},
		Target:      storage.ChildCondition{State: storage.SameNode, NodeID: observed.NodeID},
		Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: root.ID}, ObservedLeaf: []byte("renamed"), OutputLeaf: []byte("renamed"), Expected: storage.ChildCondition{State: storage.Absent}},
		Uses:        []storage.TargetUse{{NodeID: attr.ID, Scope: scope}}}
	if _, err := objectCapability[storage.NamespaceAccess](t, session).MutateName(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), nil); err != nil || string(got.RawLeaf) != "renamed" || got.NodeID != attr.ID {
		t.Fatalf("rename via observed edge = %+v, %v", got, err)
	}
	if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("name observation granted metadata permission: %v", err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), nil); !errors.Is(err, syscall.EBADF) || !reflect.DeepEqual(got, storage.NameObservation{}) {
		t.Fatalf("closed metadata reference = %+v, %v", got, err)
	}
}

type nameProbeStore struct {
	*sqlite.LockingStore
	wrap func(metastore.File) metastore.File
}

type borrowedNameProbeStore struct{ *nameProbeStore }

// The parent fixture owns the authority; subcases drain their own volumes and references.
func (*borrowedNameProbeStore) Close() error { return nil }

func (s *nameProbeStore) OpenFile(ctx context.Context, name string, options storage.FileOpenOptions) (metastore.File, error) {
	file, err := s.LockingStore.OpenFile(ctx, name, options)
	if err != nil {
		return file, err
	}
	return s.wrap(file), nil
}

func newNameProbeVolume(t *testing.T, wrap func(metastore.File) metastore.File) *objectstore.Storage {
	t.Helper()
	native, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "names.db"), Volume: "names", SQLite: sqlite.DefaultOptions(),
		Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), &nameProbeStore{LockingStore: native, wrap: wrap})
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close name-probe volume: %v", err)
		}
	})
	return volume
}

type nameProbeFile struct {
	metastore.File
	id                          uint64
	idErr, checkErr, observeErr error
	result                      storage.NameObservation
	seenGuards                  *storage.NamespaceGuards
	nodes                       atomic.Int64
}

type blockedNameFile struct {
	metastore.File
	entered, release, retired chan struct{}
	once, retiredOnce         sync.Once
}

func (f *blockedNameFile) ReferenceNodeID() (uint64, error) { return storage.ReferenceNodeID(f.File) }
func (f *blockedNameFile) CheckReferenceNameObservation() error {
	return f.File.(storage.ReferenceNameObserver).CheckReferenceNameObservation()
}
func (f *blockedNameFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	f.once.Do(func() { close(f.entered) })
	select {
	case <-f.release:
		return f.File.(storage.ReferenceNameObserver).ObserveName(ctx, guards)
	case <-ctx.Done():
		return storage.NameObservation{}, ctx.Err()
	}
}
func (f *blockedNameFile) Retire(ctx context.Context) error {
	err := f.File.Retire(ctx)
	f.retiredOnce.Do(func() { close(f.retired) })
	return err
}

func TestObjectReferenceNameObservationSharesRetirementAndDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var blocked *blockedNameFile
		volume := newNameProbeVolume(t, func(file metastore.File) metastore.File {
			blocked = &blockedNameFile{File: file, entered: make(chan struct{}), release: make(chan struct{}), retired: make(chan struct{})}
			return blocked
		})
		if err := volume.Create(t.Context(), "f"); err != nil {
			t.Fatal(err)
		}
		session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
		file := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
		observer := objectCapability[storage.ReferenceNameObserver](t, file)
		release := sync.OnceFunc(func() { close(blocked.release) })
		type outcome struct {
			name storage.NameObservation
			err  error
		}
		observed := make(chan outcome, 1)
		observingDone := make(chan struct{})
		go func() {
			defer close(observingDone)
			name, err := observer.ObserveName(t.Context(), nil)
			observed <- outcome{name, err}
		}()
		t.Cleanup(func() { release(); <-observingDone })
		<-blocked.entered
		closed := make(chan error, 1)
		closingDone := make(chan struct{})
		go func() { defer close(closingDone); closed <- file.Close(context.Background()) }()
		t.Cleanup(func() { release(); <-closingDone })
		<-blocked.retired
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("close finished before name observation drained: %v", err)
		default:
		}
		release()
		got := <-observed
		if !errors.Is(got.err, syscall.ESTALE) || !reflect.DeepEqual(got.name, storage.NameObservation{}) {
			t.Fatalf("retired observation = %+v, %v", got.name, got.err)
		}
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
	})
}

func TestObjectReferenceNameObservationCancellationReleasesAdmission(t *testing.T) {
	var blocked *blockedNameFile
	volume := newNameProbeVolume(t, func(file metastore.File) metastore.File {
		blocked = &blockedNameFile{File: file, entered: make(chan struct{}), release: make(chan struct{}), retired: make(chan struct{})}
		return blocked
	})
	if err := volume.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 1
	session := fileSessionFor(t, volume, options)
	file := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	observer := objectCapability[storage.ReferenceNameObserver](t, file)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type outcome struct {
		name storage.NameObservation
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		name, err := observer.ObserveName(ctx, nil)
		done <- outcome{name, err}
	}()
	<-blocked.entered
	cancel()
	if got := <-done; !errors.Is(got.err, context.Canceled) || !reflect.DeepEqual(got.name, storage.NameObservation{}) {
		t.Fatalf("cancelled name observation = %+v, %v", got.name, got.err)
	}
	close(blocked.release)
	if name, err := observer.ObserveName(t.Context(), nil); err != nil || string(name.RawLeaf) != "f" {
		t.Fatalf("released admission = %+v, %v", name, err)
	}
}

func (f *nameProbeFile) ReferenceNodeID() (uint64, error)     { return f.id, f.idErr }
func (f *nameProbeFile) CheckReferenceNameObservation() error { return f.checkErr }
func (f *nameProbeFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	f.seenGuards = guards
	if metastore.ReferenceSession(ctx) == nil {
		return storage.NameObservation{}, syscall.EIO
	}
	return f.result, f.observeErr
}
func (f *nameProbeFile) Node(context.Context) (metastore.FileState, error) {
	f.nodes.Add(1)
	return metastore.FileState{}, syscall.EACCES
}

type nameWithoutIdentity struct{ metastore.File }

func (*nameWithoutIdentity) CheckReferenceNameObservation() error { return nil }
func (*nameWithoutIdentity) ObserveName(context.Context, *storage.NamespaceGuards) (storage.NameObservation, error) {
	return storage.NameObservation{}, syscall.EIO
}

func TestObjectReferenceNameObservationRejectsUnsupportedAndMalformedBackends(t *testing.T) {
	fixture, native := fileVolume(t, memory.New(), 0, nil)
	if err := fixture.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("name authority failure")
	for _, test := range []struct {
		name      string
		configure func(*nameProbeFile)
		missing   string
		want      error
		checkWant error
	}{
		{name: "missing observer", missing: "observer", want: syscall.EOPNOTSUPP, checkWant: syscall.EOPNOTSUPP},
		{name: "missing identity", missing: "identity", want: syscall.EOPNOTSUPP, checkWant: syscall.EOPNOTSUPP},
		{name: "capability failure", configure: func(f *nameProbeFile) { f.checkErr = failure }, want: failure, checkWant: failure},
		{name: "identity failure", configure: func(f *nameProbeFile) { f.idErr = failure }, want: failure, checkWant: failure},
		{name: "zero identity", configure: func(f *nameProbeFile) { f.id = 0 }, want: syscall.EIO, checkWant: syscall.EIO},
		{name: "partial error", configure: func(f *nameProbeFile) { f.observeErr = failure }, want: failure},
		{name: "substituted identity", configure: func(f *nameProbeFile) { f.result.NodeID++ }, want: syscall.EIO},
		{name: "contradictory binding", configure: func(f *nameProbeFile) { f.result.State = storage.NameDetached }, want: syscall.EIO},
		{name: "owned result"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var probe *nameProbeFile
			backend := &nameProbeStore{LockingStore: native, wrap: func(file metastore.File) metastore.File {
				id, err := storage.ReferenceNodeID(file)
				if err != nil {
					t.Fatal(err)
				}
				probe = &nameProbeFile{File: file, id: id, result: storage.NameObservation{NodeID: id, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte("f")}}
				if test.configure != nil {
					test.configure(probe)
				}
				switch test.missing {
				case "observer":
					return struct{ metastore.File }{file}
				case "identity":
					return &nameWithoutIdentity{File: file}
				default:
					return probe
				}
			}}
			volume := objectstore.New(memory.New(), &borrowedNameProbeStore{backend})
			t.Cleanup(func() {
				if err := volume.Close(); err != nil {
					t.Errorf("close name-probe volume: %v", err)
				}
			})
			session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			file := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			observer := objectCapability[storage.ReferenceNameObserver](t, file)
			if err := observer.CheckReferenceNameObservation(); !errors.Is(err, test.checkWant) {
				t.Fatalf("name capability = %v; want %v", err, test.checkWant)
			}
			guards := &storage.NamespaceGuards{}
			result, err := observer.ObserveName(t.Context(), guards)
			if !errors.Is(err, test.want) {
				t.Fatalf("name observation = %+v, %v; want %v", result, err, test.want)
			}
			if test.want != nil {
				if !reflect.DeepEqual(result, storage.NameObservation{}) {
					t.Fatalf("failed observation retained facts: %+v", result)
				}
			} else {
				if result.NodeID != probe.id || string(result.RawLeaf) != "f" || probe.seenGuards != guards {
					t.Fatalf("forwarded observation = %+v", result)
				}
				result.RawLeaf[0] = 'x'
				if string(probe.result.RawLeaf) != "f" {
					t.Fatal("returned name aliases backing bytes")
				}
			}
			if probe.nodes.Load() != 0 {
				t.Fatal("name observation probed metadata attributes")
			}
		})
	}
}
