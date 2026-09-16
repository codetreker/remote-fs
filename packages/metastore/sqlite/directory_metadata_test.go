package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func directoryMetadataResult(t *testing.T, maximum, fixed int64, charge func(int, int64, int64, storage.Attr) (int64, error)) *storage.ListResult {
	t.Helper()
	result, err := storage.NewListResult(maximum, fixed, charge)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func directoryMetadataEntryCharge(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
	return storage.ObservedEntryBytes(nameBytes, metadataBytes)
}

func directoryMetadataZeroCharge(int, int64, int64, storage.Attr) (int64, error) { return 0, nil }

func observeDirectoryMetadata(t *testing.T, s *Store, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions) (storage.DirectoryMetadataObservation, []storage.Entry) {
	t.Helper()
	result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	observation, err := s.ObserveDirectoryMetadata(t.Context(), target, options, result)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	return observation, entries
}

func requireDirectoryMetadataFailure(t *testing.T, observation storage.DirectoryMetadataObservation, result *storage.ListResult, err, cause error) {
	t.Helper()
	if err == nil || cause != nil && !errors.Is(err, cause) {
		t.Fatalf("directory metadata failure=%v want=%v", err, cause)
	}
	if observation.Observation.ParentID != 0 || len(observation.Observation.Revision) != 0 || observation.Name != nil {
		t.Fatalf("failed observation exposed a name or token: %+v", observation)
	}
	entries, listErr := result.Entries()
	if listErr == nil || entries != nil || cause != nil && !errors.Is(listErr, cause) {
		t.Fatalf("failed observation retained entries=%+v error=%v want=%v", entries, listErr, cause)
	}
}

func TestDirectoryMetadataObservationDoesNotGrantOrRequireEnumerationAndMetadataRead(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "guarded", storage.NameMkdir)
	child := namespaceCreate(t, s.Store, int64(directory.ID), "child", storage.NameCreate)
	opened, err := s.OpenNodeRef(t.Context(), directory.ID, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any},
		Use: storage.UseClaim{Deny: storage.ReadEntries},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scope, err := opened.Reference.(storage.ScopedReference).Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.Reference.Node(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata-only observation reference acquired attribute-read permission=%v", err)
	}
	if _, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("ordinary enumeration bypassed ReadEntries denial=%v", err)
	}
	target := storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}
	if _, err := s.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("reference scope acquired ReadEntries=%v", err)
	}
	var observer storage.DirectoryMetadataObserver = s.Store
	if err := observer.CheckDirectoryMetadataObservation(); err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	files := s.fileDomain.files
	observation, entries := observeDirectoryMetadata(t, s.Store, target, storage.DirectoryMetadataOptions{IncludeName: true})
	if observation.Observation.ParentID != directory.ID || observation.Name == nil || observation.Name.State != storage.NameLinked || observation.Name.NodeID != directory.ID || observation.Name.ParentID != uint64(s.root) || !bytes.Equal(observation.Name.RawLeaf, []byte("guarded")) || len(entries) != 1 || entries[0].Name != "child" || entries[0].Attr.ID != child.ID {
		t.Fatalf("metadata observation=%+v entries=%+v", observation, entries)
	}
	again, _ := observeDirectoryMetadata(t, s.Store, target, storage.DirectoryMetadataOptions{})
	if again.Name != nil || !bytes.Equal(again.Observation.Revision, observation.Observation.Revision) || s.fileDomain.files != files {
		t.Fatalf("observation changed identity retention or directory revision: %+v", again)
	}
	if after, err := s.CommittedPosition(t.Context()); err != nil || after != position {
		t.Fatalf("observation published history=%v error=%v", after, err)
	}
}

func TestDirectoryMetadataObservationOwnNamesPreserveRootAndRawLinkedBytes(t *testing.T) {
	s := pendingUnlinkStore(t)
	raw := []byte{0xff, 'x'}
	created, err := s.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameMkdir, Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: uint64(s.root)}, RawLeaf: raw},
		Target: storage.ChildCondition{State: storage.Absent},
	})
	if err != nil || created.Attr == nil {
		t.Fatalf("raw directory creation=%+v error=%v", created, err)
	}
	root, entries := observeDirectoryMetadata(t, s.Store, storage.DirectoryTarget{NodeID: uint64(s.root)}, storage.DirectoryMetadataOptions{IncludeName: true})
	if root.Name == nil || root.Name.State != storage.NameRoot || root.Name.NodeID != uint64(s.root) || root.Name.ParentID != 0 || root.Name.RawLeaf != nil || len(entries) != 1 || !bytes.Equal([]byte(entries[0].Name), raw) {
		t.Fatalf("root binding=%+v entries=%+v", root, entries)
	}
	target := storage.DirectoryTarget{NodeID: created.Attr.ID}
	linked, entries := observeDirectoryMetadata(t, s.Store, target, storage.DirectoryMetadataOptions{IncludeName: true})
	if linked.Name == nil || linked.Name.State != storage.NameLinked || linked.Name.NodeID != target.NodeID || linked.Name.ParentID != uint64(s.root) || !bytes.Equal(linked.Name.RawLeaf, raw) || len(entries) != 0 {
		t.Fatalf("linked binding=%+v entries=%+v", linked, entries)
	}
	linked.Name.RawLeaf[0] = 'z'
	again, _ := observeDirectoryMetadata(t, s.Store, target, storage.DirectoryMetadataOptions{IncludeName: true})
	if again.Name == nil || !bytes.Equal(again.Name.RawLeaf, raw) {
		t.Fatalf("returned name did not own its bytes: %+v", again)
	}
}

func TestDirectoryMetadataObservationChecksTargetPrefixAndRootGuards(t *testing.T) {
	s := pendingUnlinkStore(t)
	parent := namespaceCreate(t, s.Store, s.root, "parent", storage.NameMkdir)
	directory := namespaceCreate(t, s.Store, int64(parent.ID), "directory", storage.NameMkdir)
	child := namespaceCreate(t, s.Store, int64(directory.ID), "child", storage.NameCreate)
	rootView, _ := observeDirectoryMetadata(t, s.Store, storage.DirectoryTarget{NodeID: uint64(s.root)}, storage.DirectoryMetadataOptions{})
	parentView, _ := observeDirectoryMetadata(t, s.Store, storage.DirectoryTarget{NodeID: parent.ID}, storage.DirectoryMetadataOptions{})
	guards := &storage.NamespaceGuards{
		RootID: uint64(s.root), Directories: []storage.DirectoryObservation{rootView.Observation, parentView.Observation},
		Edges: []storage.ObservedEdge{{ParentID: uint64(s.root), RawLeaf: []byte("parent"), ChildID: parent.ID}, {ParentID: parent.ID, RawLeaf: []byte("directory"), ChildID: directory.ID}},
	}
	target := storage.DirectoryTarget{NodeID: directory.ID}
	view, entries := observeDirectoryMetadata(t, s.Store, target, storage.DirectoryMetadataOptions{Guards: guards, IncludeName: true})
	if view.Name == nil || view.Name.ParentID != parent.ID || !bytes.Equal(view.Name.RawLeaf, []byte("directory")) || len(entries) != 1 || entries[0].Attr.ID != child.ID {
		t.Fatalf("guarded prefix capture=%+v entries=%+v", view, entries)
	}
	namespaceCreate(t, s.Store, int64(directory.ID), "sibling", storage.NameCreate)
	stale := &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{view.Observation}}
	result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err := s.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{Guards: stale, IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, storage.ErrConditionConflict)
	if _, err := s.MutateName(t.Context(), namespaceRename(s.root, "parent", parent.ID, "moved", 0, "moved")); err != nil {
		t.Fatal(err)
	}
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err = s.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{Guards: guards, IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, storage.ErrConditionConflict)
	for _, rootID := range []uint64{child.ID, 999999} {
		result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
		failed, err := s.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{Guards: &storage.NamespaceGuards{RootID: rootID}, IncludeName: true}, result)
		requireDirectoryMetadataFailure(t, failed, result, err, storage.ErrConditionConflict)
	}
}

func TestDirectoryMetadataObservationChargesActualNamePrefixAndOmitsUnrequestedName(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "xy", storage.NameMkdir)
	target := storage.DirectoryTarget{NodeID: directory.ID}
	residency, err := storage.NameObservationRetentionBytes(2)
	if err != nil {
		t.Fatal(err)
	}
	const fixed = int64(7)
	prefix := residency + 13
	calls := 0
	ctx := storage.WithNameObservationBudget(t.Context(), func(scalar storage.NameObservation, leafBytes int64) (int64, error) {
		calls++
		if scalar.NodeID != directory.ID || scalar.State != storage.NameLinked || scalar.ParentID != uint64(s.root) || scalar.RawLeaf != nil || leafBytes != 2 {
			t.Fatalf("prefix admission received loaded or maximum-sized name: %+v bytes=%d", scalar, leafBytes)
		}
		return prefix, nil
	})
	result := directoryMetadataResult(t, fixed+prefix, fixed, directoryMetadataZeroCharge)
	observation, err := s.ObserveDirectoryMetadata(ctx, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil || observation.Name == nil || !bytes.Equal(observation.Name.RawLeaf, []byte("xy")) || calls != 1 {
		t.Fatalf("actual-size prefix=%+v error=%v calls=%d", observation, err, calls)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("own name became a directory entry: %+v error=%v", entries, err)
	}
	result = directoryMetadataResult(t, fixed+prefix-1, fixed, directoryMetadataZeroCharge)
	failed, err := s.ObserveDirectoryMetadata(ctx, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, nil)
	refusal := errors.New("unexpected name observation")
	nameCalls := 0
	withoutName := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) {
		nameCalls++
		return 0, refusal
	})
	result = directoryMetadataResult(t, 0, 0, directoryMetadataZeroCharge)
	observation, err = s.ObserveDirectoryMetadata(withoutName, target, storage.DirectoryMetadataOptions{}, result)
	if err != nil || observation.Name != nil || observation.Observation.ParentID != directory.ID || len(observation.Observation.Revision) == 0 || nameCalls != 0 {
		t.Fatalf("name-less empty observation=%+v error=%v name calls=%d", observation, err, nameCalls)
	}
	if entries, err := result.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("name-less empty result=%+v error=%v", entries, err)
	}
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataZeroCharge)
	failed, err = s.ObserveDirectoryMetadata(withoutName, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, refusal)
}

func TestDirectoryMetadataObservationDropsPrefixAndPartialEntriesOnFailure(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	namespaceCreate(t, s.Store, int64(directory.ID), "a", storage.NameCreate)
	last := namespaceCreate(t, s.Store, int64(directory.ID), "b", storage.NameCreate)
	target := storage.DirectoryTarget{NodeID: directory.ID}
	refusal := errors.New("second entry refused")
	reservations := 0
	result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		reservations++
		if attr.Metadata != nil || nameBytes != 1 || metadataBytes < 6 {
			t.Fatalf("entry reservation received payload: %+v name=%d metadata=%d", attr, nameBytes, metadataBytes)
		}
		if index == 1 {
			return 0, refusal
		}
		return 1, nil
	})
	failed, err := s.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, refusal)
	if reservations != 2 {
		t.Fatalf("expected refusal after first admitted entry, reservations=%d", reservations)
	}
	metadata, err := storage.EncodeMetadata(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.write.ExecContext(t.Context(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, []byte("broken"), s.volume, int64(last.ID)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.write.ExecContext(context.Background(), `UPDATE nodes SET metadata=? WHERE volume=? AND id=?`, metadata, s.volume, int64(last.ID)); err != nil {
			t.Error(err)
		}
	})
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err = s.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, syscall.EIO)
}

func TestDirectoryMetadataObservationHardByteBoundIncludesOwnNamePrefix(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	namespaceCreate(t, s.Store, int64(directory.ID), "child", storage.NameCreate)
	entryBytes, err := storage.ObservedEntryBytes(5, 6)
	if err != nil {
		t.Fatal(err)
	}
	ctx := storage.WithNameObservationBudget(t.Context(), func(storage.NameObservation, int64) (int64, error) {
		return storage.MaxDirectoryBytes - entryBytes + 1, nil
	})
	result := directoryMetadataResult(t, 2*storage.MaxDirectoryBytes, 0, directoryMetadataZeroCharge)
	failed, err := s.ObserveDirectoryMetadata(ctx, storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, syscall.EFBIG)
}

func TestDirectoryMetadataObservationHardBytesSurviveZeroCallerCharges(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "large", storage.NameMkdir)
	payload := bytes.Repeat([]byte{'x'}, storage.MaxMetadataValueBytes)
	metadata := map[string]storage.OpaquePayload{"test.payload": {Version: []byte{0, 0, 0, 0, 0, 0, 0, 1}, Data: payload}}
	encoded, err := storage.EncodeMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	entryBytes, err := storage.ObservedEntryBytes(4, int64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	fit := storage.MaxDirectoryBytes / entryBytes
	for i := int64(0); i <= fit; i++ {
		_, err := s.MutateName(t.Context(), storage.NameCommand{
			Kind: storage.NameCreate, Name: namespaceName(int64(directory.ID), fmt.Sprintf("n%03d", i)),
			Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{Metadata: map[string][]byte{"test.payload": payload}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	reservations := int64(0)
	result := directoryMetadataResult(t, 2*storage.MaxDirectoryBytes, 0, func(_ int, _, _ int64, attr storage.Attr) (int64, error) {
		reservations++
		if attr.Metadata != nil {
			t.Fatal("hard-byte admission loaded metadata before reservation")
		}
		return 0, nil
	})
	failed, err := s.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, syscall.EFBIG)
	if reservations > fit {
		t.Fatalf("native bound ran after excess caller reservation: %d > %d", reservations, fit)
	}
}

func TestDirectoryMetadataObservationRejectsForeignClosedAndDetachedScopes(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	first := nodeReferenceSessionContext(t, s.Store)
	second := nodeReferenceSessionContext(t, s.Store)
	opened, err := s.OpenNodeRef(first, directory.ID, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	scope, err := opened.Reference.(storage.ScopedReference).Scope(first)
	if err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: directory.ID, Scope: &scope}
	result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	observation, err := s.ObserveDirectoryMetadata(first, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	if err != nil || observation.Name == nil || observation.Name.NodeID != directory.ID {
		t.Fatalf("live scoped observation=%+v error=%v", observation, err)
	}
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err := s.ObserveDirectoryMetadata(second, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, storage.ErrInvalidScope)
	if err := opened.Reference.Close(first); err != nil {
		t.Fatal(err)
	}
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err = s.ObserveDirectoryMetadata(first, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, storage.ErrInvalidScope)
	if live, _ := observeDirectoryMetadata(t, s.Store, storage.DirectoryTarget{NodeID: directory.ID}, storage.DirectoryMetadataOptions{IncludeName: true}); live.Name == nil || live.Name.State != storage.NameLinked {
		t.Fatalf("closing a reference changed the linked directory=%+v", live)
	}
	detached := namespaceCreate(t, s.Store, s.root, "detached", storage.NameMkdir)
	retained, err := s.OpenNodeRef(first, detached.ID, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := retained.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	detachedScope, err := retained.Reference.(storage.ScopedReference).Scope(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameRemoveDir, Name: namespaceName(s.root, "detached"), Target: storage.ChildCondition{State: storage.SameNode, NodeID: detached.ID},
	}); err != nil {
		t.Fatal(err)
	}
	if state, err := retained.Reference.Node(first); err != nil || !state.Detached {
		t.Fatalf("retained detached directory=%+v error=%v", state, err)
	}
	for _, target := range []storage.DirectoryTarget{{NodeID: detached.ID}, {NodeID: detached.ID, Scope: &detachedScope}} {
		result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
		failed, err := s.ObserveDirectoryMetadata(first, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
		requireDirectoryMetadataFailure(t, failed, result, err, syscall.ESTALE)
	}
}

func TestDirectoryMetadataObservationInvalidatesResultsOnCancellationAndLifetimeFailure(t *testing.T) {
	s := pendingUnlinkStore(t)
	directory := namespaceCreate(t, s.Store, s.root, "dir", storage.NameMkdir)
	namespaceCreate(t, s.Store, int64(directory.ID), "child", storage.NameCreate)
	target := storage.DirectoryTarget{NodeID: directory.ID}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	result := directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err := s.ObserveDirectoryMetadata(cancelled, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, context.Canceled)
	fenced := errors.New("reference lifetime ended during capture")
	checks := 0
	reserved := false
	guarded := metastore.WithFilePublicationGuard(t.Context(), func() error {
		checks++
		if reserved {
			return fenced
		}
		return nil
	})
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
		reserved = true
		return directoryMetadataEntryCharge(index, nameBytes, metadataBytes, attr)
	})
	failed, err = s.ObserveDirectoryMetadata(guarded, target, storage.DirectoryMetadataOptions{IncludeName: true}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, fenced)
	if checks < 2 || !reserved {
		t.Fatal("completed capture was not checked against current lifetime")
	}
	result = directoryMetadataResult(t, storage.MaxDirectoryBytes, 0, directoryMetadataEntryCharge)
	failed, err = s.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{}, storage.DirectoryMetadataOptions{}, result)
	requireDirectoryMetadataFailure(t, failed, result, err, syscall.EINVAL)
	if failed, err := s.ObserveDirectoryMetadata(t.Context(), target, storage.DirectoryMetadataOptions{}, nil); !errors.Is(err, syscall.EINVAL) || failed.Name != nil || failed.Observation.ParentID != 0 {
		t.Fatalf("nil collector observation=%+v error=%v", failed, err)
	}
}
