package objectstore_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

// Each caller owns its Store and worker. Only ordinary schema preparation is shared;
// native lease authorities and their database identities are never copied or reopened here.
func plainMetastoreFactory(database string) func(*testing.T, int64) *sqlite.Store {
	var sequence atomic.Uint64
	return func(t *testing.T, allowance int64) *sqlite.Store {
		t.Helper()
		meta, err := sqlite.Open(t.Context(), database, fmt.Sprintf("volume-%d", sequence.Add(1)), allowance, sqlite.DefaultWindow())
		if err != nil {
			t.Fatalf("opening the metastore: %v", err)
		}
		return meta
	}
}

type borrowedNativeStore struct{ *sqlite.LockingStore }

// The parent fixture retains the native owner; child Storage.Close still drains its
// sessions and stops its maintenance worker before releasing this borrowed backend.
func (*borrowedNativeStore) Close() error { return nil }

type readOnlyNativeSnapshot struct {
	identity   storage.BackendIdentityResult
	durable    sqlite.DurableState
	maxLease   time.Duration
	barrier    metastore.LogBarrier
	nodes      map[string]metastore.Node
	children   map[string][]metastore.Child
	space      storage.Space
	spaceError string
	usage      int64
	objects    sqlite.ObjectStatus
}

type readOnlyNativeFixture struct {
	ctx      context.Context
	native   *sqlite.LockingStore
	baseline readOnlyNativeSnapshot
	poisoned bool
}

func newReadOnlyNativeFixture(t *testing.T, prepare func(*objectstore.Storage)) *readOnlyNativeFixture {
	t.Helper()
	native, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "readonly.db"), Volume: "readonly", SQLite: sqlite.DefaultOptions(),
		Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), native)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("closing the native fixture owner: %v", err)
		}
	})
	if prepare != nil {
		prepare(volume)
	}
	fixture := &readOnlyNativeFixture{ctx: t.Context(), native: native}
	fixture.baseline, err = fixture.snapshot()
	if err != nil {
		t.Fatalf("capturing the native fixture: %v", err)
	}
	return fixture
}

func (f *readOnlyNativeFixture) requireHealthy(t *testing.T) {
	t.Helper()
	if f.poisoned {
		t.Fatal("native fixture cleanup failed; later children cannot borrow it")
	}
}

func (f *readOnlyNativeFixture) volume(t *testing.T, backend metastore.Store) *objectstore.Storage {
	t.Helper()
	f.requireHealthy(t)
	volume := objectstore.New(memory.New(), backend)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			f.poisoned = true
			t.Errorf("closing the borrowed native volume: %v", err)
		}
		if err := f.check(); err != nil {
			f.poisoned = true
			t.Errorf("native fixture changed after child cleanup: %v", err)
		}
		if t.Failed() {
			f.poisoned = true
		}
	})
	return volume
}

func (f *readOnlyNativeFixture) snapshot() (readOnlyNativeSnapshot, error) {
	state := readOnlyNativeSnapshot{nodes: make(map[string]metastore.Node), children: make(map[string][]metastore.Child)}
	if f.native.Terminal() {
		return state, errors.New("the parent native owner is terminal")
	}
	var err error
	if state.identity, err = f.native.BackendIdentity(f.ctx); err != nil {
		return state, err
	}
	if state.durable, err = f.native.DurableState(f.ctx); err != nil {
		return state, err
	}
	if state.maxLease, err = f.native.MaxLease(f.ctx); err != nil || state.maxLease != 0 {
		return state, fmt.Errorf("readonly native fixture max lease = %s, error = %v", state.maxLease, err)
	}
	if state.barrier, err = f.native.Barrier(f.ctx, 1024); err != nil {
		return state, err
	}
	if state.space, err = f.native.Space(f.ctx); !errors.Is(err, syscall.ENOSYS) {
		return state, fmt.Errorf("unlimited fixture space returned %+v, %v", state.space, err)
	}
	state.spaceError = err.Error()
	if state.usage, err = f.native.Usage(f.ctx); err != nil {
		return state, err
	}
	if state.objects, err = f.native.ObjectStatus(f.ctx); err != nil {
		return state, err
	}
	var visit func(string) error
	visit = func(path string) error {
		node, err := f.native.Stat(f.ctx, path)
		if err != nil {
			return err
		}
		state.nodes[path] = node
		if !node.IsDir() {
			return nil
		}
		children, err := f.native.List(f.ctx, path)
		if err != nil {
			return err
		}
		state.children[path] = children
		for _, child := range children {
			name := string(child.Name)
			if path != "" {
				name = path + "/" + name
			}
			if err := visit(name); err != nil {
				return err
			}
		}
		return nil
	}
	return state, visit("")
}

func (f *readOnlyNativeFixture) check() error {
	state, err := f.snapshot()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(state, f.baseline) {
		return fmt.Errorf("native state = %+v, want %+v", state, f.baseline)
	}
	for path, node := range state.nodes {
		if node.IsDir() {
			action, err := storage.NewFileActionID(1)
			if err != nil {
				return err
			}
			opened, err := f.native.OpenNodeRef(f.ctx, uint64(node.ID), storage.NodeRefOptions{
				Action: action, Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(node.ID)},
				MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Deny: storage.ReadEntries},
			})
			if err != nil {
				if opened.Reference != nil {
					err = errors.Join(err, opened.Reference.Close(f.ctx))
				}
				return fmt.Errorf("healthy directory successor %q: %w", path, err)
			}
			if opened.Reference == nil {
				return fmt.Errorf("healthy directory successor %q returned no reference", path)
			}
			if err := opened.Reference.Close(f.ctx); err != nil {
				return fmt.Errorf("closing healthy directory successor %q: %w", path, err)
			}
		} else {
			file, err := f.native.OpenFile(f.ctx, path, storage.FileOpenOptions{
				OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: uint64(node.ID), Use: storage.UseClaim{Deny: storage.ReadData},
			})
			if err != nil {
				if file != nil {
					err = errors.Join(err, file.Close(f.ctx))
				}
				return fmt.Errorf("healthy file successor %q: %w", path, err)
			}
			if err := file.Close(f.ctx); err != nil {
				return fmt.Errorf("closing healthy file successor %q: %w", path, err)
			}
		}
	}
	after, err := f.snapshot()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(after, f.baseline) {
		return fmt.Errorf("healthy successors changed native state: %+v, want %+v", after, f.baseline)
	}
	return nil
}

func TestReadOnlyNativeFixtureKeepsChildLifetimesIndependent(t *testing.T) {
	fixture := newReadOnlyNativeFixture(t, func(volume *objectstore.Storage) {
		if err := volume.Create(t.Context(), "f"); err != nil {
			t.Fatal(err)
		}
	})
	var firstEpoch string
	var firstSession storage.FileSession
	t.Run("faulted child", func(t *testing.T) {
		var probe *nameProbeFile
		backend := &nameProbeStore{LockingStore: fixture.native, wrap: func(file metastore.File) metastore.File {
			id, err := storage.ReferenceNodeID(file)
			if err != nil {
				t.Fatal(err)
			}
			probe = &nameProbeFile{File: file, id: id, result: storage.NameObservation{NodeID: id + 1, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte("f")}}
			return probe
		}}
		volume := fixture.volume(t, &borrowedNameProbeStore{backend})
		session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
		status, err := session.Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		firstEpoch = status.Epoch
		firstSession = session
		file := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData}})
		if observed, err := objectCapability[storage.ReferenceNameObserver](t, file).ObserveName(t.Context(), nil); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observed, storage.NameObservation{}) {
			t.Fatalf("malformed child observation = %+v, %v", observed, err)
		}
		probe.result.RawLeaf[0] = 'x'
	})
	fixture.requireHealthy(t)
	if _, err := firstSession.Status(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("closed child session status = %v, want EIO", err)
	}
	t.Run("healthy successor", func(t *testing.T) {
		volume := fixture.volume(t, &borrowedNativeStore{fixture.native})
		if err := volume.CheckFileStorage(); err != nil {
			t.Fatal(err)
		}
		if err := volume.CheckBackendIdentity(); err != nil {
			t.Fatal(err)
		}
		session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
		status, err := session.Status(t.Context())
		if err != nil || status.Epoch == firstEpoch {
			t.Fatalf("successor session epoch = %q, first = %q, error = %v", status.Epoch, firstEpoch, err)
		}
		if err := objectCapability[storage.DirectoryMetadataObserver](t, session).CheckDirectoryMetadataObservation(); err != nil {
			t.Fatal(err)
		}
		if err := objectCapability[storage.NodeReferences](t, session).CheckNodeReferences(); err != nil {
			t.Fatal(err)
		}
		file := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Deny: storage.ReadData}})
		observed, err := objectCapability[storage.ReferenceNameObserver](t, file).ObserveName(t.Context(), nil)
		if err != nil || observed.State != storage.NameLinked || observed.NodeID != uint64(fixture.baseline.nodes["f"].ID) || string(observed.RawLeaf) != "f" {
			t.Fatalf("healthy successor observation = %+v, %v", observed, err)
		}
	})
}

func TestPlainMetastoreFactoryKeepsChildrenIndependent(t *testing.T) {
	factory := plainMetastoreFactory(filepath.Join(t.TempDir(), "meta.db"))
	var first *sqlite.Store
	var firstRoot metastore.Node
	var firstDurable sqlite.DurableState
	t.Run("populated child", func(t *testing.T) {
		first = factory(t, 16384)
		volume := composedVolume(t, first, memory.New())
		if err := volume.Write(t.Context(), "same", []byte("first content")); err != nil {
			t.Fatal(err)
		}
		if _, err := first.Reserve(t.Context(), "reserved", 9); err != nil {
			t.Fatal(err)
		}
		var err error
		firstRoot, err = first.Stat(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		firstDurable, err = first.DurableState(t.Context())
		if err != nil {
			t.Fatal(err)
		}
	})
	if t.Failed() {
		return
	}
	if !first.Terminal() {
		t.Fatal("the first child retained its Store after cleanup")
	}
	t.Run("fresh child", func(t *testing.T) {
		second := factory(t, 8192)
		volume := composedVolume(t, second, memory.New())
		if entries, err := second.List(t.Context(), ""); err != nil || len(entries) != 0 {
			t.Fatalf("fresh child listing = %+v, error = %v", entries, err)
		}
		if content, err := volume.Read(t.Context(), "same"); !errors.Is(err, syscall.ENOENT) || content != nil {
			t.Fatalf("fresh child inherited content = %q, error = %v", content, err)
		}
		root, err := second.Stat(t.Context(), "")
		if err != nil || root.ID == firstRoot.ID {
			t.Fatalf("fresh child root = %+v, first = %+v, error = %v", root, firstRoot, err)
		}
		if status, err := second.ObjectStatus(t.Context()); err != nil || status != (sqlite.ObjectStatus{}) {
			t.Fatalf("fresh child inherited object state = %+v, error = %v", status, err)
		}
		if space, err := second.Space(t.Context()); err != nil || space != (storage.Space{Total: 8192, Avail: 8192}) {
			t.Fatalf("fresh child space = %+v, error = %v", space, err)
		}
		if usage, err := second.Usage(t.Context()); err != nil || usage != 0 {
			t.Fatalf("fresh child usage = %d, error = %v", usage, err)
		}
		if durable, err := second.DurableState(t.Context()); err != nil || durable.DatabaseID != firstDurable.DatabaseID || durable.Generation <= firstDurable.Generation {
			t.Fatalf("shared database = %+v, first = %+v, error = %v", durable, firstDurable, err)
		}
		if err := volume.Write(t.Context(), "same", []byte("second content")); err != nil {
			t.Fatal(err)
		}
	})
}
