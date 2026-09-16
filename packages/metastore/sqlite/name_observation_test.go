package sqlite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func nameReference(t *testing.T, s *Store, attr storage.Attr, permissions storage.MetadataPermissions) metastore.NodeReference {
	t.Helper()
	opened, err := s.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{
		Kind: attr.Kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID},
		MetadataAccess: permissions, Use: storage.UseClaim{Uses: storage.DeleteName},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return opened.Reference
}

func requireNameObservationError(t *testing.T, observed storage.NameObservation, err, want error) {
	t.Helper()
	if !errors.Is(err, want) || !reflect.DeepEqual(observed, storage.NameObservation{}) {
		t.Fatalf("name observation=%+v error=%v, want zero result and %v", observed, err, want)
	}
}

func TestReferenceNameObservationFollowsTheNodeWithoutAttributePermission(t *testing.T) {
	s := pendingUnlinkStore(t)
	p := namespaceCreate(t, s.Store, s.root, "p", storage.NameMkdir)
	q := namespaceCreate(t, s.Store, s.root, "q", storage.NameMkdir)
	attr := namespaceCreate(t, s.Store, int64(p.ID), "a", storage.NameCreate)
	ref := nameReference(t, s.Store, attr, 0)
	observer := ref.(storage.ReferenceNameObserver)
	if err := observer.CheckReferenceNameObservation(); err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Node(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("name support granted attribute access: %v", err)
	}
	if _, err := ref.(metastore.ReferenceStateAccess).State(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("name support granted State access: %v", err)
	}
	if err := s.Rename(t.Context(), "p/a", "q/b"); err != nil {
		t.Fatal(err)
	}
	replacement := namespaceCreate(t, s.Store, int64(p.ID), "a", storage.NameCreate)
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	files := s.fileDomain.files
	observed, err := observer.ObserveName(t.Context(), nil)
	if err != nil || observed.NodeID != attr.ID || observed.State != storage.NameLinked || observed.ParentID != q.ID || !bytes.Equal(observed.RawLeaf, []byte("b")) {
		t.Fatalf("current binding=%+v error=%v", observed, err)
	}
	observed.RawLeaf[0] = 'x'
	observed, err = observer.ObserveName(t.Context(), nil)
	if err != nil || string(observed.RawLeaf) != "b" {
		t.Fatalf("returned name alias changed native binding=%+v error=%v", observed, err)
	}
	if after, err := s.CommittedPosition(t.Context()); err != nil || after != position || s.fileDomain.files != files {
		t.Fatalf("observation changed history/reference ownership: position=%d files=%d error=%v", after, s.fileDomain.files, err)
	}
	stale := namespaceRename(int64(p.ID), "a", attr.ID, "wrong", 0, "wrong")
	if _, err := s.MutateName(t.Context(), stale); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale source selected its replacement: %v", err)
	}
	scope, err := ref.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	command := namespaceRename(int64(q.ID), string(observed.RawLeaf), attr.ID, "c", 0, "c")
	command.Uses = []storage.TargetUse{{NodeID: attr.ID, Scope: scope}}
	if _, err := s.MutateName(t.Context(), command); err != nil {
		t.Fatalf("DELETE-only current binding rename=%v", err)
	}
	if got, err := s.Stat(t.Context(), "q/c"); err != nil || uint64(got.ID) != attr.ID {
		t.Fatalf("renamed target=%+v error=%v", got, err)
	}
	if got, err := s.Stat(t.Context(), "p/a"); err != nil || uint64(got.ID) != replacement.ID {
		t.Fatalf("old-path replacement=%+v error=%v", got, err)
	}
}

func TestReferenceNameObservationDistinguishesRootAndDetachedNodes(t *testing.T) {
	s := pendingUnlinkStore(t)
	root, err := s.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	rootRef := nameReference(t, s.Store, root.Attr(), storage.ReadMetadata)
	got, err := rootRef.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
	if err != nil || got.State != storage.NameRoot || got.NodeID != uint64(s.root) || got.ParentID != 0 || got.RawLeaf != nil {
		t.Fatalf("root binding=%+v error=%v", got, err)
	}
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			command := storage.NameCommand{Kind: storage.NameCreate, Name: namespaceName(s.root, "node"), Target: storage.ChildCondition{State: storage.Absent}}
			if kind == storage.NodeDirectory {
				command.Kind = storage.NameMkdir
			}
			if kind == storage.NodeSymlink {
				command.Kind, command.Initial.LinkTarget = storage.NameSymlink, []byte("target")
			}
			created, err := s.MutateName(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			ref := nameReference(t, s.Store, *created.Attr, storage.ReadMetadata)
			remove := storage.NameRemove
			if kind == storage.NodeDirectory {
				remove = storage.NameRemoveDir
			}
			if _, err := s.MutateName(t.Context(), storage.NameCommand{Kind: remove, Name: command.Name, Target: storage.ChildCondition{State: storage.SameNode, NodeID: created.Attr.ID}}); err != nil {
				t.Fatal(err)
			}
			observed, err := ref.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
			if err != nil || observed.State != storage.NameDetached || observed.NodeID != created.Attr.ID || observed.ParentID != 0 || observed.RawLeaf != nil {
				t.Fatalf("detached binding=%+v error=%v", observed, err)
			}
			state, err := ref.Node(t.Context())
			if err != nil || !state.Detached || state.Kind != kind {
				t.Fatalf("detached identity metadata=%+v error=%v", state, err)
			}
			if err := ref.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			id, err := storage.ReferenceNodeID(ref)
			if err != nil || id != created.Attr.ID {
				t.Fatalf("closed immutable identity=%d error=%v", id, err)
			}
			observed, err = ref.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
			requireNameObservationError(t, observed, err, syscall.ESTALE)
		})
	}
}

func TestReferenceIdentityDoesNotWaitForNativeAdmission(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		id, err := storage.ReferenceNodeID(f)
		if err == nil && id != uint64(f.id) {
			err = syscall.EIO
		}
		result <- err
	}()
	select {
	case err := <-result:
		s.coordinator.commit.release()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		s.coordinator.commit.release()
		<-result
		t.Fatal("immutable identity getter waited for the native gate")
	}
}

func TestReferenceNameObservationChargesActualBytesAndPreservesRawNames(t *testing.T) {
	s := pendingUnlinkStore(t)
	leaf := []byte{0xff, 'x'}
	attr := namespaceCreate(t, s.Store, s.root, string(leaf), storage.NameCreate)
	ref := nameReference(t, s.Store, attr, storage.ReadMetadata)
	observer := ref.(storage.ReferenceNameObserver)
	before, err := ref.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ctx := storage.WithNameObservationBudget(t.Context(), func(scalar storage.NameObservation, length int64) (int64, error) {
		calls++
		if scalar.NodeID != attr.ID || scalar.State != storage.NameLinked || scalar.ParentID != uint64(s.root) || scalar.RawLeaf != nil || length != int64(len(leaf)) {
			t.Errorf("unloaded name budget=%+v length=%d", scalar, length)
		}
		charge, err := storage.NameObservationRetentionBytes(length)
		if charge > 1024 {
			return 0, syscall.EFBIG
		}
		return charge, err
	})
	observed, err := observer.ObserveName(ctx, nil)
	if err != nil || !bytes.Equal(observed.RawLeaf, leaf) || calls != 1 {
		t.Fatalf("raw name=%+v error=%v budget calls=%d", observed, err, calls)
	}
	fault := errors.New("caller name result refused")
	ctx = storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) { return 0, fault })
	observed, err = observer.ObserveName(ctx, nil)
	requireNameObservationError(t, observed, err, fault)
	after, err := ref.Node(t.Context())
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("name projection affected identity state=%+v error=%v", after, err)
	}
}

func TestReferenceNameObservationRejectsOversizedLeafBeforeLoading(t *testing.T) {
	s := pendingUnlinkStore(t)
	f := pendingUnlinkFile(t, s.Store, "file", false)
	before, err := f.State(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	const payloadBytes = 1 << 20
	if _, err := s.write.ExecContext(t.Context(), `UPDATE entries SET name=zeroblob(?) WHERE volume=? AND node=?`, payloadBytes, s.volume, f.id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.write.ExecContext(context.Background(), `UPDATE entries SET name=? WHERE volume=? AND node=?`, []byte("file"), s.volume, f.id); err != nil {
			t.Error(err)
		}
	})
	calls := 0
	ctx := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) {
		calls++
		return 0, syscall.EIO
	})
	runtime.GC()
	var allocatedBefore, allocatedAfter runtime.MemStats
	runtime.ReadMemStats(&allocatedBefore)
	observed, err := f.ObserveName(ctx, nil)
	runtime.ReadMemStats(&allocatedAfter)
	requireNameObservationError(t, observed, err, syscall.ENAMETOOLONG)
	if calls != 0 || allocatedAfter.TotalAlloc-allocatedBefore.TotalAlloc >= payloadBytes/4 {
		t.Fatalf("oversized leaf reached allocation/budget: calls=%d allocated=%d", calls, allocatedAfter.TotalAlloc-allocatedBefore.TotalAlloc)
	}
	after, err := f.State(t.Context())
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("name limit poisoned plain State=%+v error=%v", after, err)
	}
}

func TestReferenceNameObservationChecksGuardsAndReferenceLifetime(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	attr := namespaceCreate(t, s.Store, int64(directory.ID), "file", storage.NameCreate)
	first := nodeReferenceSessionContext(t, s.Store)
	second := nodeReferenceSessionContext(t, s.Store)
	opened, err := s.OpenNodeRef(first, attr.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	observer := opened.Reference.(storage.ReferenceNameObserver)
	observed, err := observer.ObserveName(second, nil)
	requireNameObservationError(t, observed, err, storage.ErrInvalidScope)
	view, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil {
		t.Fatal(err)
	}
	guards := &storage.NamespaceGuards{
		RootID: uint64(s.root), Directories: []storage.DirectoryObservation{view.Observation},
		Edges: []storage.ObservedEdge{{ParentID: uint64(s.root), RawLeaf: []byte("dir"), ChildID: directory.ID}, {ParentID: directory.ID, RawLeaf: []byte("file"), ChildID: attr.ID}},
	}
	if _, err := observer.ObserveName(first, guards); err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "dir", "moved"); err != nil {
		t.Fatal(err)
	}
	observed, err = observer.ObserveName(first, guards)
	requireNameObservationError(t, observed, err, storage.ErrConditionConflict)
	if got, err := observer.ObserveName(first, nil); err != nil || got.ParentID != directory.ID || string(got.RawLeaf) != "file" {
		t.Fatalf("ancestor move substituted reference=%+v error=%v", got, err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(first)
	done := make(chan error, 1)
	go func() { _, err := observer.ObserveName(ctx, nil); done <- err }()
	cancel()
	select {
	case err := <-done:
		s.coordinator.commit.release()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation=%v", err)
		}
	case <-time.After(time.Second):
		s.coordinator.commit.release()
		<-done
		t.Fatal("name observation ignored canceled native admission")
	}
	var admitted atomic.Bool
	admitted.Store(true)
	fault := errors.New("reference lifetime ended")
	ctx = metastore.WithFilePublicationGuard(first, func() error {
		if !admitted.Load() {
			return fault
		}
		return nil
	})
	ctx = storage.WithNameObservationBudget(ctx, func(_ storage.NameObservation, length int64) (int64, error) {
		admitted.Store(false)
		return storage.NameObservationRetentionBytes(length)
	})
	observed, err = observer.ObserveName(ctx, nil)
	requireNameObservationError(t, observed, err, fault)
	if err := opened.Reference.Close(first); err != nil {
		t.Fatal(err)
	}
	observed, err = observer.ObserveName(first, nil)
	requireNameObservationError(t, observed, err, syscall.ESTALE)
}

func TestReferenceNameObservationRejectsInconsistentBindings(t *testing.T) {
	for _, test := range []struct {
		name         string
		breakBinding string
	}{
		{"missing", `DELETE FROM entries WHERE node=?`},
		{"duplicate", `INSERT INTO entries(volume,parent,name,node) SELECT volume,parent,CAST('second' AS BLOB),node FROM entries WHERE node=?`},
		{"text name", `UPDATE entries SET name=CAST('file' AS TEXT) WHERE node=?`},
		{"detached but named", `UPDATE nodes SET detached=1 WHERE id=?`},
		{"self parent", `UPDATE entries SET parent=node WHERE node=?`},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := pendingUnlinkStore(t)
			f := pendingUnlinkFile(t, s.Store, "file", false)
			if _, err := s.write.ExecContext(t.Context(), test.breakBinding, f.id); err != nil {
				t.Fatal(err)
			}
			restore := func() {
				if _, err := s.write.ExecContext(context.Background(), `DELETE FROM entries WHERE node=?`, f.id); err != nil {
					t.Error(err)
				}
				if _, err := s.write.ExecContext(context.Background(), `UPDATE nodes SET detached=0 WHERE id=?`, f.id); err != nil {
					t.Error(err)
				}
				if _, err := s.write.ExecContext(context.Background(), `INSERT INTO entries(volume,parent,name,node) VALUES(?,?,?,?)`, s.volume, s.root, []byte("file"), f.id); err != nil {
					t.Error(err)
				}
			}
			t.Cleanup(restore)
			calls := 0
			ctx := storage.WithNameObservationBudget(t.Context(), func(_ storage.NameObservation, length int64) (int64, error) {
				calls++
				return storage.NameObservationRetentionBytes(length)
			})
			observed, err := f.ObserveName(ctx, nil)
			requireNameObservationError(t, observed, err, syscall.EIO)
			if calls != 0 {
				t.Fatal("inconsistent binding reached output admission")
			}
		})
	}
}
