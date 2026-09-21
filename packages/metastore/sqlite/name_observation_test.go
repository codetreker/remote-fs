package sqlite

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func openNameObservationStore(t *testing.T, edit func(*Options)) (*LockingStore, LockingConfig) {
	t.Helper()
	config := lockingTestConfig(t)
	if edit != nil {
		edit(&config.SQLite)
	}
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	return store, config
}

func observedDirectory(t *testing.T, store *Store, id uint64) storage.ObservedDirectory {
	t.Helper()
	observed, err := store.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: id})
	if err != nil {
		t.Fatal(err)
	}
	return observed
}

func revisionNumber(t *testing.T, token []byte) uint64 {
	t.Helper()
	if len(token) != 8 {
		t.Fatalf("directory revision length=%d", len(token))
	}
	return binary.BigEndian.Uint64(token)
}

func TestDirectoryRevisionIsPersistentAndAdvancesWithCommittedNameChanges(t *testing.T) {
	store, config := openNameObservationStore(t, nil)
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	initial := observedDirectory(t, store.Store, uint64(root.ID))
	if revisionNumber(t, initial.Observation.Revision) != 1 {
		t.Fatalf("initial revision=%x", initial.Observation.Revision)
	}
	const writers = 8
	var wait sync.WaitGroup
	errorsSeen := make(chan error, writers)
	for i := range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsSeen <- store.Create(t.Context(), fmt.Sprintf("file-%d", i))
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	after := observedDirectory(t, store.Store, uint64(root.ID))
	if got := revisionNumber(t, after.Observation.Revision); got <= revisionNumber(t, initial.Observation.Revision) {
		t.Fatalf("revision did not advance after %d committed creates: %d", writers, got)
	}
	if len(after.Entries) != writers {
		t.Fatalf("complete listing entries=%d", len(after.Entries))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	persisted := observedDirectory(t, reopened.Store, uint64(root.ID))
	if !bytes.Equal(persisted.Observation.Revision, after.Observation.Revision) || len(persisted.Entries) != writers {
		t.Fatalf("reopen changed observation: before=%+v after=%+v", after.Observation, persisted.Observation)
	}
}

func TestDirectoryRevisionCoversEveryNameMutationAndNotFailures(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	revision := func(id uint64) uint64 {
		return revisionNumber(t, observedDirectory(t, store.Store, id).Observation.Revision)
	}
	rootID := uint64(root.ID)
	previous := revision(rootID)
	advance := func(name string, mutate func() error) {
		t.Helper()
		if err := mutate(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		next := revision(rootID)
		if next <= previous {
			t.Fatalf("%s did not advance root revision: %d -> %d", name, previous, next)
		}
		previous = next
	}
	advance("create", func() error { return store.Create(t.Context(), "file") })
	advance("mkdir", func() error { return store.Mkdir(t.Context(), "left") })
	advance("second mkdir", func() error { return store.Mkdir(t.Context(), "right") })
	advance("symlink", func() error {
		_, err := store.MutateName(t.Context(), storage.NameCommand{
			Kind: storage.NameSymlink, Action: fileAction(t),
			Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: rootID}, RawLeaf: []byte("link")},
			Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte("target")},
		})
		return err
	})
	advance("unlink", func() error { return store.Remove(t.Context(), "link") })

	left, err := store.Stat(t.Context(), "left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := store.Stat(t.Context(), "right")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "left/item"); err != nil {
		t.Fatal(err)
	}
	leftBefore, rightBefore := revision(uint64(left.ID)), revision(uint64(right.ID))
	if err := store.Rename(t.Context(), "left/item", "right/item"); err != nil {
		t.Fatal(err)
	}
	if leftAfter, rightAfter := revision(uint64(left.ID)), revision(uint64(right.ID)); leftAfter <= leftBefore || rightAfter <= rightBefore {
		t.Fatalf("cross-directory rename revisions left %d->%d right %d->%d", leftBefore, leftAfter, rightBefore, rightAfter)
	}

	advance("replacement source", func() error { return store.Create(t.Context(), "source") })
	advance("replacement target", func() error { return store.Create(t.Context(), "target") })
	advance("replacement rename", func() error { return store.Rename(t.Context(), "source", "target") })
	beforeFailure := revision(rootID)
	if err := store.Create(t.Context(), "target"); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("failed create=%v", err)
	}
	if got := revision(rootID); got != beforeFailure {
		t.Fatalf("failed create advanced revision: %d -> %d", beforeFailure, got)
	}
	if err := store.Rename(t.Context(), "target", "target"); err != nil {
		t.Fatal(err)
	}
	if got := revision(rootID); got != beforeFailure {
		t.Fatalf("no-op rename advanced revision: %d -> %d", beforeFailure, got)
	}
	if err := store.RemoveDir(t.Context(), "left"); err != nil {
		t.Fatal(err)
	}
	if got := revision(rootID); got <= beforeFailure {
		t.Fatalf("rmdir did not advance revision: %d -> %d", beforeFailure, got)
	}
}

func TestDirectoryRevisionOverflowRollsBackTheNameMutation(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	maximum := []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET directory_revision=? WHERE volume=? AND id=?`, maximum, store.volume, root.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "blocked"); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("revision exhaustion=%v", err)
	}
	if _, err := store.Stat(t.Context(), "blocked"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("overflow left a child: %v", err)
	}
	if got := observedDirectory(t, store.Store, uint64(root.ID)).Observation.Revision; !bytes.Equal(got, maximum) {
		t.Fatalf("overflow changed revision: %x", got)
	}
}

func TestAuthorityObservationsRejectMalformedDirectoryRevisions(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	defer store.Close()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET directory_revision=X'01' WHERE volume=? AND id=?`, store.volume, root.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stat(t.Context(), ""); !errors.Is(err, syscall.EIO) {
		t.Fatalf("malformed directory revision Stat=%v", err)
	}
	if _, err := store.List(t.Context(), ""); !errors.Is(err, syscall.EIO) {
		t.Fatalf("malformed directory revision List=%v", err)
	}
	if _, err := store.StatNode(t.Context(), uint64(root.ID)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("malformed directory revision StatNode=%v", err)
	}
	result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(nameBytes, metadataBytes)
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := store.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: uint64(root.ID)}, result)
	if !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(observation, storage.DirectoryObservation{}) {
		t.Fatalf("malformed directory revision observation=%+v error=%v", observation, err)
	}
	if entries, listErr := result.Entries(); entries != nil || !errors.Is(listErr, syscall.EIO) {
		t.Fatalf("malformed directory revision exposed entries=%+v error=%v", entries, listErr)
	}
	if snapshot, _, err := store.Snapshot(t.Context()); snapshot != nil || !errors.Is(err, syscall.EIO) {
		if snapshot != nil {
			snapshot.Close()
		}
		t.Fatalf("malformed directory revision snapshot=%v error=%v", snapshot, err)
	}
}

func TestAuthorityChangeReadRejectsMalformedDirectoryRevision(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	defer store.Close()
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE changes SET directory_revision=X'01' WHERE volume=? AND node=? AND node_kind=?`, store.volume, root.ID, storage.NodeDirectory); err != nil {
		t.Fatal(err)
	}
	result, err := metastore.NewChangeResult(1<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return 256 + lengths.Name + lengths.FromName + lengths.Content + lengths.Metadata + lengths.Target, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Since(t.Context(), 0, 100, result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("malformed directory change=%v", err)
	}
	if changes, resultErr := result.Changes(); changes != nil || !errors.Is(resultErr, syscall.EIO) {
		t.Fatalf("malformed directory change exposed page=%+v error=%v", changes, resultErr)
	}
}

func TestReferenceNameObservationDistinguishesRootLinkedAndDetached(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	openedRoot, err := store.OpenNodeRef(t.Context(), uint64(root.ID), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(root.ID)}, Action: fileAction(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := storage.ReferenceNodeID(openedRoot.Reference)
	if err != nil || rootID != uint64(root.ID) {
		t.Fatalf("node reference identity=%d error=%v", rootID, err)
	}
	rootObserver := openedRoot.Reference.(storage.ReferenceNameObserver)
	if err := rootObserver.CheckReferenceNameObservation(); err != nil {
		t.Fatalf("node reference name capability=%v", err)
	}
	if got, err := rootObserver.ObserveName(t.Context(), nil); err != nil || got.State != storage.NameRoot || got.NodeID != uint64(root.ID) {
		t.Fatalf("root observation=%+v error=%v", got, err)
	}
	if err := openedRoot.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if closedID, err := storage.ReferenceNodeID(openedRoot.Reference); err != nil || closedID != rootID {
		t.Fatalf("closed node reference identity=%d error=%v", closedID, err)
	}
	if err := rootObserver.CheckReferenceNameObservation(); err != nil {
		t.Fatalf("closed reference lost capability shape=%v", err)
	}
	if got, err := rootObserver.ObserveName(t.Context(), nil); !errors.Is(err, syscall.ESTALE) || !reflect.DeepEqual(got, storage.NameObservation{}) {
		t.Fatalf("closed node reference observation=%+v error=%v", got, err)
	}

	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	file, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(context.Background())
	id, err := storage.ReferenceNodeID(file)
	if err != nil {
		t.Fatal(err)
	}
	observer := file.(storage.ReferenceNameObserver)
	if got, err := observer.ObserveName(t.Context(), nil); err != nil || got.State != storage.NameLinked || got.NodeID != id || got.ParentID != uint64(root.ID) || string(got.RawLeaf) != "file" {
		t.Fatalf("linked observation=%+v error=%v", got, err)
	}
	if err := store.Rename(t.Context(), "file", "renamed"); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), nil); err != nil || got.State != storage.NameLinked || string(got.RawLeaf) != "renamed" {
		t.Fatalf("renamed observation=%+v error=%v", got, err)
	}
	if err := store.Remove(t.Context(), "renamed"); err != nil {
		t.Fatal(err)
	}
	if got, err := observer.ObserveName(t.Context(), nil); err != nil || got.State != storage.NameDetached || got.NodeID != id || got.ParentID != 0 || got.RawLeaf != nil {
		t.Fatalf("detached observation=%+v error=%v", got, err)
	}
}

func TestDirectoryMetadataObservationRejectsStaleGuardsWithoutPartialEntries(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	before := observedDirectory(t, store.Store, uint64(root.ID))
	if err := store.Create(t.Context(), "new"); err != nil {
		t.Fatal(err)
	}
	result, err := storage.NewListResult(storage.MaxDirectoryBytes, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(nameBytes, metadataBytes)
	})
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DirectoryMetadataOptions{Guards: &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{before.Observation}}}
	got, err := store.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: uint64(root.ID)}, options, result)
	if !errors.Is(err, storage.ErrConditionConflict) || !reflect.DeepEqual(got, storage.DirectoryMetadataObservation{}) {
		t.Fatalf("stale observation=%+v error=%v", got, err)
	}
	if entries, listErr := result.Entries(); entries != nil || !errors.Is(listErr, storage.ErrConditionConflict) {
		t.Fatalf("failed observation exposed entries=%+v error=%v", entries, listErr)
	}
}

func TestDirectoryMetadataObservationIsIndependentOfApplicationEnumeration(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	t.Cleanup(func() { _ = store.Close() })
	if err := store.CheckDirectoryMetadataObservation(); err != nil {
		t.Fatalf("directory metadata capability=%v", err)
	}
	if err := store.CheckDirectoryRead(); err != nil {
		t.Fatalf("directory read capability=%v", err)
	}
	if err := store.Mkdir(t.Context(), "guarded"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "guarded/child"); err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "guarded")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenNodeRef(t.Context(), uint64(directory.ID), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Deny: storage.ReadEntries},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Reference.Close(context.Background())
	scope, err := opened.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(directory.ID)}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous enumeration bypassed denial: %v", err)
	}
	target := storage.DirectoryTarget{NodeID: uint64(directory.ID), Scope: &scope}
	if _, err := store.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("scope without ReadEntries enumerated: %v", err)
	}
	result, err := storage.NewListResult(storage.MaxDirectoryBytes, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(nameBytes, metadataBytes)
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := store.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil || observation.Name == nil || observation.Name.State != storage.NameLinked || string(observation.Name.RawLeaf) != "guarded" {
		t.Fatalf("metadata observation=%+v error=%v", observation, err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "child" {
		t.Fatalf("metadata entries=%+v error=%v", entries, err)
	}
}

func TestDirectoryMetadataObservationCapabilityCheckRejectsAPlainStore(t *testing.T) {
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "plain.db"), "workspace", 0, DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CheckDirectoryMetadataObservation(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("plain Store directory metadata capability=%v", err)
	}
	if err := store.CheckDirectoryRead(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("plain Store directory read capability=%v", err)
	}
}

func TestScopedDirectoryReadContinuesAfterUnlink(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	defer store.Close()
	if err := store.Mkdir(t.Context(), "gone"); err != nil {
		t.Fatal(err)
	}
	directory, err := store.Stat(t.Context(), "gone")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := store.OpenNodeRef(t.Context(), uint64(directory.ID), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: uint64(directory.ID)},
		Action: fileAction(t), Use: storage.UseClaim{Uses: storage.ReadEntries},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Reference.Close(context.Background())
	scope, err := opened.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveDir(t.Context(), "gone"); err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: uint64(directory.ID), Scope: &scope}
	observed, err := store.ReadDirNode(t.Context(), target)
	if err != nil || observed.Observation.ParentID != uint64(directory.ID) || len(observed.Entries) != 0 {
		t.Fatalf("scoped detached directory=%+v error=%v", observed, err)
	}
	if _, err := store.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(directory.ID)}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("bare detached directory read=%v", err)
	}
	wrong := storage.UseScope{Token: "00000000000000000000000000000000"}
	if _, err := store.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(directory.ID), Scope: &wrong}); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("wrong-scope detached directory read=%v", err)
	}
	result, err := storage.NewListResult(storage.MaxDirectoryBytes, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(nameBytes, metadataBytes)
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{}, result)
	if !errors.Is(err, syscall.ESTALE) || !reflect.DeepEqual(metadata, storage.DirectoryMetadataObservation{}) {
		t.Fatalf("detached metadata observation=%+v error=%v", metadata, err)
	}
}

func TestDirectoryListingsRejectDanglingAndDetachedChildren(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func(*Store, int64) error
	}{
		{"dangling", func(store *Store, id int64) error {
			connection, err := store.write.Conn(t.Context())
			if err != nil {
				return err
			}
			defer connection.Close()
			if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys=OFF`); err != nil {
				return err
			}
			_, err = connection.ExecContext(t.Context(), `DELETE FROM nodes WHERE volume=? AND id=?`, store.volume, id)
			return err
		}},
		{"root alias", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `UPDATE entries SET node=? WHERE volume=? AND node=?`, store.root, store.volume, id)
			return err
		}},
		{"detached", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET detached=1 WHERE volume=? AND id=?`, store.volume, id)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := openNameObservationStore(t, nil)
			defer store.Close()
			if err := store.Create(t.Context(), "good"); err != nil {
				t.Fatal(err)
			}
			if err := store.Create(t.Context(), "bad"); err != nil {
				t.Fatal(err)
			}
			bad, err := store.Stat(t.Context(), "bad")
			if err != nil {
				t.Fatal(err)
			}
			root, err := store.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := test.damage(store.Store, bad.ID); err != nil {
				t.Fatal(err)
			}
			if children, err := store.List(t.Context(), ""); !errors.Is(err, syscall.EIO) || children != nil {
				t.Fatalf("ordinary listing exposed children=%+v error=%v", children, err)
			}
			assertBoundedFailure := func(name string, run func(*storage.ListResult) error) {
				t.Helper()
				result, err := storage.NewListResult(storage.MaxDirectoryBytes, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
					return storage.ObservedEntryBytes(nameBytes, metadataBytes)
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := run(result); !errors.Is(err, syscall.EIO) {
					t.Fatalf("%s=%v", name, err)
				}
				if entries, err := result.Entries(); entries != nil || !errors.Is(err, syscall.EIO) {
					t.Fatalf("%s exposed entries=%+v error=%v", name, entries, err)
				}
			}
			assertBoundedFailure("ListBounded", func(result *storage.ListResult) error {
				return store.ListBounded(t.Context(), "", result)
			})
			assertBoundedFailure("ReadDirNodeBounded", func(result *storage.ListResult) error {
				_, err := store.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: uint64(root.ID)}, result)
				return err
			})
			assertBoundedFailure("ObserveDirectoryMetadata", func(result *storage.ListResult) error {
				_, err := store.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: uint64(root.ID)}, storage.DirectoryMetadataOptions{}, result)
				return err
			})
		})
	}
}

func TestReferenceNameObservationRejectsMissingAndMultipleBindings(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func(*Store, int64) error
	}{
		{"missing", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `DELETE FROM entries WHERE volume=? AND node=?`, store.volume, id)
			return err
		}},
		{"multiple", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `INSERT INTO entries(volume,parent,name,node) VALUES(?,?,?,?)`, store.volume, store.root, []byte("second"), id)
			return err
		}},
		{"unknown kind", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET kind=99 WHERE volume=? AND id=?`, store.volume, id)
			return err
		}},
		{"non-blob name", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `UPDATE entries SET name=CAST('file' AS TEXT) WHERE volume=? AND node=?`, store.volume, id)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := openNameObservationStore(t, nil)
			defer store.Close()
			if err := store.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			file, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close(context.Background())
			id, err := storage.ReferenceNodeID(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.damage(store.Store, int64(id)); err != nil {
				t.Fatal(err)
			}
			got, err := file.(storage.ReferenceNameObserver).ObserveName(t.Context(), nil)
			if !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, storage.NameObservation{}) {
				t.Fatalf("corrupt binding observation=%+v error=%v", got, err)
			}
		})
	}
}

func TestReferenceNameObservationGuardsRejectMalformedExistingEdges(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func(*Store, int64) error
	}{
		{"detached but linked", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `UPDATE nodes SET detached=1 WHERE volume=? AND id=?`, store.volume, id)
			return err
		}},
		{"dangling", func(store *Store, id int64) error {
			connection, err := store.write.Conn(t.Context())
			if err != nil {
				return err
			}
			defer connection.Close()
			if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys=OFF`); err != nil {
				return err
			}
			_, err = connection.ExecContext(t.Context(), `DELETE FROM nodes WHERE volume=? AND id=?`, store.volume, id)
			return err
		}},
		{"root alias", func(store *Store, id int64) error {
			_, err := store.write.ExecContext(t.Context(), `UPDATE entries SET node=? WHERE volume=? AND node=?`, store.root, store.volume, id)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := openNameObservationStore(t, nil)
			defer store.Close()
			if err := store.Create(t.Context(), "held"); err != nil {
				t.Fatal(err)
			}
			if err := store.Create(t.Context(), "guarded"); err != nil {
				t.Fatal(err)
			}
			held, err := store.OpenFile(t.Context(), "held", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close(context.Background())
			guarded, err := store.Stat(t.Context(), "guarded")
			if err != nil {
				t.Fatal(err)
			}
			if err := test.damage(store.Store, guarded.ID); err != nil {
				t.Fatal(err)
			}
			guards := &storage.NamespaceGuards{Edges: []storage.ObservedEdge{{
				ParentID: uint64(store.root), RawLeaf: []byte("guarded"), ChildID: uint64(guarded.ID),
			}}}
			got, err := held.(storage.ReferenceNameObserver).ObserveName(t.Context(), guards)
			if !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, storage.NameObservation{}) {
				t.Fatalf("malformed guarded edge observation=%+v error=%v", got, err)
			}
		})
	}
}

func TestReferenceNameObservationRejectsOversizedLeafBeforeBudgetingPayload(t *testing.T) {
	store, _ := openNameObservationStore(t, nil)
	defer store.Close()
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	file, err := store.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(context.Background())
	id, err := storage.ReferenceNodeID(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE entries SET name=zeroblob(?) WHERE volume=? AND node=?`, storage.MaxLeafBytes+1, store.volume, id); err != nil {
		t.Fatal(err)
	}
	calls := 0
	ctx := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) {
		calls++
		return 0, nil
	})
	got, err := file.(storage.ReferenceNameObserver).ObserveName(ctx, nil)
	if !errors.Is(err, syscall.ENAMETOOLONG) || calls != 0 || !reflect.DeepEqual(got, storage.NameObservation{}) {
		t.Fatalf("oversized binding observation=%+v error=%v budget calls=%d", got, err, calls)
	}
}

func TestDirectoryObservationHonorsConfiguredEntryAndByteLimits(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Options)
	}{
		{"entries", func(options *Options) { options.MaxDirectoryEntries = 1 }},
		{"bytes", func(options *Options) { options.MaxDirectoryBytes = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := openNameObservationStore(t, test.edit)
			defer store.Close()
			if err := store.Create(t.Context(), "a"); err != nil {
				t.Fatal(err)
			}
			if test.name == "entries" {
				if err := store.Create(t.Context(), "b"); err != nil {
					t.Fatal(err)
				}
			}
			root, err := store.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if got, err := store.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(root.ID)}); !errors.Is(err, syscall.EFBIG) || got.Entries != nil {
				t.Fatalf("bounded observation=%+v error=%v", got, err)
			}
		})
	}
}
