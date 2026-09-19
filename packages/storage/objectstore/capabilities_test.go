package objectstore_test

import (
	"bytes"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func objectCapability[T any](t *testing.T, value any) T {
	t.Helper()
	capability, ok := value.(T)
	if !ok {
		t.Fatalf("%T does not provide %T", value, (*T)(nil))
	}
	return capability
}

func objectChild(parent uint64, name string) storage.ChildName {
	return storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent}, RawLeaf: []byte(name)}
}

func objectScope(t *testing.T, reference any) storage.UseScope {
	t.Helper()
	capability := objectCapability[storage.ScopedReference](t, reference)
	if err := capability.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	scope, err := capability.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func objectState(t *testing.T, reference any) storage.ReferenceState {
	t.Helper()
	capability := objectCapability[storage.ReferenceStateAccess](t, reference)
	if err := capability.CheckReferenceState(); err != nil {
		t.Fatal(err)
	}
	state, err := capability.State(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestObjectAtomicOpenCapturesOnlyItsSelectedInitialState(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	opener := objectCapability[storage.AtomicFileOpener](t, session)
	if err := opener.CheckAtomicFileOpen(); err != nil {
		t.Fatal(err)
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	createdTime, resetTime, replacedTime := time.Unix(100, 0).UTC(), time.Unix(200, 0).UTC(), time.Unix(300, 0).UTC()
	initial := func(value string, stamp *time.Time) storage.InitialFields {
		return storage.InitialFields{Attr: storage.AttrChange{ModTime: stamp}, Metadata: map[string][]byte{"test.phase": []byte(value)}}
	}
	options := storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Existing: storage.ResetContent,
		Target: storage.ChildCondition{State: storage.Any}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName},
		Initial: storage.InitialState{OnCreate: initial("created", &createdTime), OnReset: initial("reset", &resetTime)},
	}
	created, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || created.File == nil || created.Outcome != storage.Created || created.Attr.Size != 0 || !created.Attr.ModTime.Equal(createdTime) || string(created.Attr.Metadata["test.phase"].Data) != "created" {
		t.Fatalf("create = %+v, %v", created, err)
	}
	if _, err := created.File.WriteAt(t.Context(), 0, []byte("original")); err != nil {
		t.Fatal(err)
	}
	options.Existing = storage.Keep
	options.Initial = storage.InitialState{OnCreate: initial("unused", &replacedTime)}
	kept, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || kept.File == nil || kept.Outcome != storage.Opened || kept.Attr.ID != created.Attr.ID || kept.Attr.Size != 8 || string(kept.Attr.Metadata["test.phase"].Data) != "created" {
		t.Fatalf("keep = %+v, %v", kept, err)
	}
	options.Existing = storage.ResetContent
	options.Initial = storage.InitialState{OnCreate: initial("unused", &replacedTime), OnReset: initial("reset", &resetTime)}
	reset, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || reset.File == nil || reset.Outcome != storage.Reset || reset.Attr.ID != created.Attr.ID || reset.Attr.Size != 0 || !reset.Attr.ModTime.Equal(resetTime) || string(reset.Attr.Metadata["test.phase"].Data) != "reset" {
		t.Fatalf("reset = %+v, %v", reset, err)
	}
	readFileFor(t, created.File, "")
	if _, err := reset.File.WriteAt(t.Context(), 0, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	options.Existing = storage.ReplaceNode
	options.Initial = storage.InitialState{OnCreate: initial("unused", &createdTime), OnReplace: initial("replaced", &replacedTime)}
	replaced, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || replaced.File == nil || replaced.Outcome != storage.Replaced || replaced.Attr.ID == created.Attr.ID || replaced.Attr.Size != 0 || !replaced.Attr.ModTime.Equal(replacedTime) || string(replaced.Attr.Metadata["test.phase"].Data) != "replaced" {
		t.Fatalf("replace = %+v, %v", replaced, err)
	}
	readFileFor(t, created.File, "retained")
	if !objectState(t, created.File).Detached {
		t.Fatal("replacement did not detach the retained identity")
	}
	if created.Attr.Size != 0 || !created.Attr.ModTime.Equal(createdTime) || string(created.Attr.Metadata["test.phase"].Data) != "created" || kept.Attr.Size != 8 || string(kept.Attr.Metadata["test.phase"].Data) != "created" || reset.Attr.Size != 0 || string(reset.Attr.Metadata["test.phase"].Data) != "reset" {
		t.Fatalf("later mutations changed captured open results: created=%+v kept=%+v reset=%+v", created.Attr, kept.Attr, reset.Attr)
	}
}

func TestObjectNodeReferencesShareFileAdmissionAndRetention(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 2
	session := fileSessionFor(t, volume, options)
	references := objectCapability[storage.NodeReferences](t, session)
	if err := references.CheckNodeReferences(); err != nil {
		t.Fatal(err)
	}
	file := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
	attr, err := file.WriteAt(t.Context(), 0, []byte("retained"))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := references.OpenNodeRef(t.Context(), attr.ID, storage.NodeRefOptions{Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: attr.ID}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata})
	if err != nil || metadata.Reference == nil {
		t.Fatalf("metadata reference = %+v, %v", metadata, err)
	}
	if _, ok := metadata.Reference.(storage.File); ok {
		t.Fatal("metadata reference exposes byte I/O")
	}
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	directoryOptions := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Absent}, Create: true, MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries}}
	if _, err := references.OpenChildRef(t.Context(), objectChild(root.ID, "d"), directoryOptions); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("shared reference limit = %v", err)
	}
	if _, err := volume.Stat(t.Context(), "d"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused directory open created a name: %v", err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	directory, err := references.OpenChildRef(t.Context(), objectChild(root.ID, "d"), directoryOptions)
	if err != nil || directory.Reference == nil || directory.Outcome != storage.Created {
		t.Fatalf("directory reference = %+v, %v", directory, err)
	}
	if _, ok := directory.Reference.(storage.File); ok {
		t.Fatal("directory reference exposes byte I/O")
	}
	if err := volume.Remove(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := volume.RemoveDir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	if state := objectState(t, metadata.Reference); !state.Detached || state.Attr.ID != attr.ID || state.Attr.Size != 8 {
		t.Fatalf("retained metadata state = %+v", state)
	}
	if state := objectState(t, directory.Reference); !state.Detached || state.Attr.ID != directory.Attr.ID || state.Attr.Kind != storage.NodeDirectory {
		t.Fatalf("retained directory state = %+v", state)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 8 {
		t.Fatalf("retained content usage = %d, %v", used, err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("closed reference usage = %d, %v", used, err)
	}
	for _, id := range []uint64{attr.ID, directory.Attr.ID} {
		if _, err := meta.StatNode(t.Context(), id); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("closed identity %d remains: %v", id, err)
		}
	}
}

func TestObjectDirectoryScopesRemainBoundToTheirLiveReference(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	foreign := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	namespace := objectCapability[storage.NamespaceAccess](t, session)
	if err := namespace.CheckNamespaceAccess(); err != nil {
		t.Fatal(err)
	}
	references := objectCapability[storage.NodeReferences](t, session)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	opened, err := references.OpenChildRef(t.Context(), objectChild(root.ID, "d"), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Create: true, Target: storage.ChildCondition{State: storage.Absent},
		MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries, Deny: storage.ReadEntries},
	})
	if err != nil || opened.Reference == nil {
		t.Fatalf("directory open = %+v, %v", opened, err)
	}
	if err := volume.Write(t.Context(), "d/child", []byte("bytes")); err != nil {
		t.Fatal(err)
	}
	scope := objectScope(t, opened.Reference)
	target := storage.DirectoryTarget{NodeID: opened.Attr.ID, Scope: &scope}
	listing, err := namespace.ReadDirNode(t.Context(), target)
	if err != nil || listing.Observation.ParentID != opened.Attr.ID || len(listing.Observation.Revision) == 0 || len(listing.Entries) != 1 || string(listing.Entries[0].RawLeaf) != "child" || listing.Entries[0].Attr.Size != 5 {
		t.Fatalf("scoped listing = %+v, %v", listing, err)
	}
	for _, budget := range []int64{10, 11} {
		reserved := 0
		result, err := storage.NewListResult(budget, 0, func(index int, nameBytes, metadataBytes int64, attr storage.Attr) (int64, error) {
			reserved++
			if index != 0 || nameBytes != 5 || metadataBytes != 6 || attr.ID != listing.Entries[0].Attr.ID || len(attr.Metadata) != 0 {
				t.Fatalf("bounded entry reservation = %d, %d, %d, %+v", index, nameBytes, metadataBytes, attr)
			}
			return nameBytes + metadataBytes, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		observation, readErr := namespace.ReadDirNodeBounded(t.Context(), target, result)
		entries, entriesErr := result.Entries()
		if reserved != 1 {
			t.Fatalf("bounded listing reserved %d entries", reserved)
		}
		if budget == 10 {
			if !errors.Is(readErr, syscall.EIO) || !errors.Is(entriesErr, syscall.EIO) || entries != nil || observation.ParentID != 0 || len(observation.Revision) != 0 {
				t.Fatalf("oversized scoped listing = %+v, %+v, %v, %v", observation, entries, readErr, entriesErr)
			}
		} else if readErr != nil || entriesErr != nil || !reflect.DeepEqual(observation, listing.Observation) || len(entries) != 1 || entries[0].Name != "child" || !reflect.DeepEqual(entries[0].Attr, listing.Entries[0].Attr) {
			t.Fatalf("bounded scoped listing = %+v, %+v, %v, %v", observation, entries, readErr, entriesErr)
		}
	}
	if _, err := namespace.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: opened.Attr.ID}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("unscoped listing bypassed the reference claim: %v", err)
	}
	if _, err := objectCapability[storage.NamespaceAccess](t, foreign).ReadDirNode(t.Context(), target); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("foreign scope = %v", err)
	}
	if _, err := namespace.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: root.ID, Scope: &scope}); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("scope with another identity = %v", err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := namespace.ReadDirNode(t.Context(), target); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("closed scope = %v", err)
	}
	if _, err := opened.Reference.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed reference stat = %v", err)
	}
	retained, err := references.OpenNodeRef(t.Context(), opened.Attr.ID, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: opened.Attr.ID},
		MetadataAccess: storage.ReadMetadata, Use: storage.UseClaim{Uses: storage.ReadEntries},
	})
	if err != nil || retained.Reference == nil {
		t.Fatalf("retained directory = %+v, %v", retained, err)
	}
	if err := volume.Remove(t.Context(), "d/child"); err != nil {
		t.Fatal(err)
	}
	if err := volume.RemoveDir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	scope = objectScope(t, retained.Reference)
	for _, target := range []storage.DirectoryTarget{{NodeID: opened.Attr.ID}, {NodeID: opened.Attr.ID, Scope: &scope}} {
		if _, err := namespace.ReadDirNode(t.Context(), target); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("detached directory listing = %v", err)
		}
	}
	if state := objectState(t, retained.Reference); !state.Detached || state.Attr.ID != opened.Attr.ID || state.Attr.Kind != storage.NodeDirectory {
		t.Fatalf("detached directory state = %+v", state)
	}
}

func TestObjectSymlinkReferenceStateOwnsItsCapturedRawTarget(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	namespace := objectCapability[storage.NamespaceAccess](t, session)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	target := []byte{'d', '/', 't', 0xff}
	want := bytes.Clone(target)
	name := objectChild(root.ID, "link")
	created, err := namespace.MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Name: name, Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: target},
	})
	if err != nil || created.Attr == nil || created.Attr.Kind != storage.NodeSymlink {
		t.Fatalf("symlink creation = %+v, %v", created, err)
	}
	target[0] = 'x'
	opened, err := objectCapability[storage.NodeReferences](t, session).OpenNodeRef(t.Context(), created.Attr.ID, storage.NodeRefOptions{
		Kind: storage.NodeSymlink, Target: storage.ChildCondition{State: storage.SameNode, NodeID: created.Attr.ID}, MetadataAccess: storage.ReadMetadata,
	})
	if err != nil || opened.Reference == nil {
		t.Fatalf("symlink metadata reference = %+v, %v", opened, err)
	}
	state := objectState(t, opened.Reference)
	if state.Attr.Kind != storage.NodeSymlink || state.Attr.ID != created.Attr.ID || state.Attr.Size != int64(len(want)) || state.Detached || state.PendingUnlink || !bytes.Equal(state.LinkTarget, want) {
		t.Fatalf("linked symlink state = %+v", state)
	}
	state.LinkTarget[0] = 'y'
	if current := objectState(t, opened.Reference); !bytes.Equal(current.LinkTarget, want) || current.Attr.ID != created.Attr.ID || current.Detached {
		t.Fatalf("mutating a returned target changed the reference: %+v", current)
	}
	if _, err := namespace.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemove, Name: name, Target: storage.ChildCondition{State: storage.SameNode, NodeID: created.Attr.ID}}); err != nil {
		t.Fatal(err)
	}
	if detached := objectState(t, opened.Reference); !detached.Detached || detached.Attr.ID != created.Attr.ID || detached.Attr.Kind != storage.NodeSymlink || !bytes.Equal(detached.LinkTarget, want) {
		t.Fatalf("detached symlink state = %+v", detached)
	}
}

func TestObjectDirectoryReferenceControlsPreserveIdentityAndCleanup(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	namespace := objectCapability[storage.NamespaceAccess](t, session)
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	name := objectChild(root.ID, "directory")
	opened, err := objectCapability[storage.NodeReferences](t, session).OpenChildRef(t.Context(), name, storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Create: true, Target: storage.ChildCondition{State: storage.Absent},
		Use: storage.UseClaim{Uses: storage.ReadEntries | storage.DeleteName}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
	})
	if err != nil || opened.Reference == nil || opened.Outcome != storage.Created || opened.Attr.Kind != storage.NodeDirectory {
		t.Fatalf("directory reference = %+v, %v", opened, err)
	}
	scope := objectScope(t, opened.Reference)
	child := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: opened.Attr.ID, Scope: &scope}, RawLeaf: []byte{'c', 0xff}}
	created, err := namespace.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameCreate, Name: child, Target: storage.ChildCondition{State: storage.Absent}})
	if err != nil || created.Attr == nil || created.Attr.Kind != storage.NodeRegular {
		t.Fatalf("scoped child create = %+v, %v", created, err)
	}
	if found, err := namespace.LookupAt(t.Context(), child); err != nil || !reflect.DeepEqual(found, *created.Attr) {
		t.Fatalf("raw scoped child lookup = %+v, %v", found, err)
	}
	deletion := objectCapability[storage.DeleteIntent](t, opened.Reference)
	if err := deletion.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	if _, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("pending nonempty directory = %v", err)
	}
	if _, err := namespace.MutateName(t.Context(), storage.NameCommand{Kind: storage.NameRemove, Name: child, Target: storage.ChildCondition{State: storage.SameNode, NodeID: created.Attr.ID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := namespace.LookupAt(t.Context(), child); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("removed scoped child lookup = %v", err)
	}
	stamp := time.Unix(600, 0).UTC()
	changed, err := opened.Reference.SetAttr(t.Context(), storage.AttrChange{ModTime: &stamp})
	if err != nil || changed.ID != opened.Attr.ID || changed.Kind != storage.NodeDirectory || !changed.ModTime.Equal(stamp) || !reflect.DeepEqual(changed.BirthTime, opened.Attr.BirthTime) {
		t.Fatalf("directory reference attributes = %+v, %v", changed, err)
	}
	owners := objectCapability[storage.UseOwners](t, session)
	owner, err := owners.NewUseOwner(t.Context(), opened.Attr.ID, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference})
	if err != nil || owner == 0 {
		t.Fatalf("directory reference owner = %d, %v", owner, err)
	}
	ranges := objectCapability[storage.RangeControl](t, session)
	command := storage.RangeCommand{Domain: storage.DomainRecord, Mode: storage.RangeShared, Edit: storage.Replace, Range: storage.Range{Kind: storage.Bytes, Start: 0, Length: 1}}
	if conflict, err := ranges.GetConflict(t.Context(), owner, command); err != nil || conflict.Found {
		t.Fatalf("directory owner conflict = %+v, %v", conflict, err)
	}
	if err := owners.RetireUseOwner(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if _, err := ranges.GetConflict(t.Context(), owner, command); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired directory owner remains usable: %v", err)
	}
	first, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty})
	if err != nil || !first.PendingUnlink || first.Detached || first.Attr.ID != opened.Attr.ID || first.Attr.Kind != storage.NodeDirectory || len(first.PendingGeneration) == 0 {
		t.Fatalf("pending empty directory = %+v, %v", first, err)
	}
	cleared, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: first.PendingGeneration})
	if err != nil || cleared.PendingUnlink || cleared.Detached || cleared.Attr.ID != opened.Attr.ID || len(cleared.PendingGeneration) != 0 {
		t.Fatalf("clear directory pending generation = %+v, %v", cleared, err)
	}
	second, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty})
	if err != nil || !second.PendingUnlink || bytes.Equal(second.PendingGeneration, first.PendingGeneration) {
		t.Fatalf("new directory pending generation = %+v, %v", second, err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := namespace.LookupAt(t.Context(), name); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("directory name after last close = %v", err)
	}
	if _, err := meta.StatNode(t.Context(), opened.Attr.ID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("directory identity after last close = %v", err)
	}
}

func TestObjectMetadataCASAndConditionalAttributesPreserveOtherNamespaces(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	file := openFileFor(t, session, "f", storage.FileOpenOptions{
		OpenAccess:      storage.OpenAccess{Read: true, Write: true, Create: true},
		InitialMetadata: map[string][]byte{"test.one": []byte("one"), "test.other": {0, 0xff, 7}},
	})
	initial, err := file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	metadata := objectCapability[storage.MetadataAccess](t, session)
	if err := metadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	updated, err := metadata.SetMetadata(t.Context(), initial.ID, "test.one", initial.Metadata["test.one"].Version, []byte("two"))
	if err != nil || string(updated.Data) != "two" || len(updated.Version) == 0 || bytes.Equal(updated.Version, initial.Metadata["test.one"].Version) {
		t.Fatalf("session metadata update = %+v, %v", updated, err)
	}
	reference, err := objectCapability[storage.NodeReferences](t, session).OpenNodeRef(t.Context(), initial.ID, storage.NodeRefOptions{
		Kind: storage.NodeRegular, Target: storage.ChildCondition{State: storage.SameNode, NodeID: initial.ID}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
	})
	if err != nil || reference.Reference == nil {
		t.Fatalf("metadata reference = %+v, %v", reference, err)
	}
	refMetadata := objectCapability[storage.ReferenceMetadataAccess](t, reference.Reference)
	if err := refMetadata.CheckMetadataAccess(); err != nil {
		t.Fatal(err)
	}
	current, err := refMetadata.SetMetadata(t.Context(), "test.one", updated.Version, []byte("three"))
	if err != nil || string(current.Data) != "three" || bytes.Equal(current.Version, updated.Version) {
		t.Fatalf("reference metadata update = %+v, %v", current, err)
	}
	if _, err := metadata.SetMetadata(t.Context(), initial.ID, "test.one", updated.Version, []byte("stale")); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale metadata version = %v", err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("current body")); err != nil {
		t.Fatal(err)
	}
	mutation := objectCapability[storage.ConditionalFileMutation](t, file)
	if err := mutation.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(400, 0).UTC()
	change := storage.FileMutation{Kind: storage.MutateAttributes, Attr: storage.AttrChange{ModTime: &stamp}, Metadata: map[string]storage.OpaquePayload{"test.one": {Version: current.Version, Data: []byte("four")}}}
	changed, err := mutation.MutateFile(t.Context(), change)
	if err != nil || changed.Size != 12 || !changed.ModTime.Equal(stamp) || string(changed.Metadata["test.one"].Data) != "four" || bytes.Equal(changed.Metadata["test.one"].Version, current.Version) {
		t.Fatalf("conditional attributes = %+v, %v", changed, err)
	}
	staleStamp := time.Unix(500, 0).UTC()
	change.Attr.ModTime = &staleStamp
	change.Metadata["test.one"] = storage.OpaquePayload{Version: current.Version, Data: []byte("stale")}
	if _, err := mutation.MutateFile(t.Context(), change); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale combined metadata condition = %v", err)
	}
	after, err := reference.Reference.Stat(t.Context())
	if err != nil || !after.ModTime.Equal(stamp) || string(after.Metadata["test.one"].Data) != "four" || !bytes.Equal(after.Metadata["test.other"].Data, initial.Metadata["test.other"].Data) || !bytes.Equal(after.Metadata["test.other"].Version, initial.Metadata["test.other"].Version) {
		t.Fatalf("metadata after rejected mutation = %+v, %v", after, err)
	}
	readFileFor(t, file, "current body")
}

func TestObjectUseClaimsProtectOrdinaryFilesAndPathIO(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "f", []byte("body")); err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ordinary := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	reader := openFileFor(t, ordinary, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	opener := objectCapability[storage.AtomicFileOpener](t, session)
	options := storage.OpenAtOptions{
		Read: true, Write: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep,
		Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName, Deny: storage.ReadData | storage.WriteData | storage.DeleteName},
	}
	if _, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("new denial ignored an existing ordinary reader: %v", err)
	}
	if err := reader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	protected, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || protected.File == nil {
		t.Fatalf("protected open = %+v, %v", protected, err)
	}
	for _, access := range []storage.OpenAccess{{Read: true}, {Write: true}} {
		if _, err := ordinary.OpenFile(t.Context(), "f", storage.FileOpenOptions{OpenAccess: access}); !errors.Is(err, storage.ErrUseConflict) {
			t.Fatalf("ordinary path open %+v = %v", access, err)
		}
		if _, err := ordinary.OpenNode(t.Context(), protected.Attr.ID, storage.FileOpenOptions{OpenAccess: access}); !errors.Is(err, storage.ErrUseConflict) {
			t.Fatalf("ordinary identity open %+v = %v", access, err)
		}
	}
	if _, err := volume.Read(t.Context(), "f"); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous read = %v", err)
	}
	if err := volume.Write(t.Context(), "f", []byte("forbidden")); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous write = %v", err)
	}
	if err := volume.Remove(t.Context(), "f"); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous remove = %v", err)
	}
	if err := volume.Rename(t.Context(), "f", "moved"); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("anonymous rename = %v", err)
	}
	readFileFor(t, protected.File, "body")
	if _, err := protected.File.WriteAt(t.Context(), 0, []byte("own!")); err != nil {
		t.Fatal(err)
	}
	if _, err := protected.File.Truncate(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	readFileFor(t, protected.File, "own")
	if err := protected.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if body, err := volume.Read(t.Context(), "f"); err != nil || string(body) != "own" {
		t.Fatalf("read after claim close = %q, %v", body, err)
	}
}

func TestObjectEnforcedRangesUseLogicalIOAndCurrentTruncateRevision(t *testing.T) {
	objects := newPausedFilePut(t)
	volume, _ := fileVolume(t, objects, 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	otherSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	t.Cleanup(objects.release)
	ownerFile := openFileFor(t, session, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
	initial, err := ownerFile.WriteAt(t.Context(), 0, []byte("abcdefgh"))
	if err != nil {
		t.Fatal(err)
	}
	other := openFileFor(t, otherSession, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	owners := objectCapability[storage.UseOwners](t, session)
	if err := owners.CheckUseOwners(); err != nil {
		t.Fatal(err)
	}
	owner, err := owners.NewUseOwner(t.Context(), initial.ID, objectScope(t, ownerFile), storage.OwnerOptions{Lifetime: storage.OwnerReference})
	if err != nil {
		t.Fatal(err)
	}
	ranges := objectCapability[storage.RangeControl](t, session)
	if err := ranges.CheckRangeControl(); err != nil {
		t.Fatal(err)
	}
	command := storage.RangeCommand{
		Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Edit: storage.AddExact,
		Range: storage.Range{Kind: storage.Bytes, Start: 2, Length: 3}, Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData},
	}
	attempt, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{command}, retainedLockRequest(t, session))
	if err != nil || attempt.State != storage.Granted || len(attempt.Claims) != 1 {
		t.Fatalf("enforced grant = %+v, %v", attempt, err)
	}
	if _, err := other.ReadAt(t.Context(), 2, 1); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("overlapping read = %v", err)
	}
	if _, err := other.WriteAt(t.Context(), 3, []byte("!")); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("overlapping write = %v", err)
	}
	if _, err := other.WriteAt(t.Context(), 0, []byte("A")); err != nil {
		t.Fatalf("disjoint write requiring full object materialization = %v", err)
	}
	if err := volume.Write(t.Context(), "f", []byte("replacement")); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("whole path write = %v", err)
	}
	if _, err := ownerFile.WriteAt(t.Context(), 2, []byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	mutation := objectCapability[storage.ConditionalFileMutation](t, other)
	if err := mutation.CheckConditionalFileMutation(); err != nil {
		t.Fatal(err)
	}
	if attr, err := mutation.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateTruncate, Size: 6}); err != nil || attr.Size != 6 {
		t.Fatalf("disjoint conditional truncate = %+v, %v", attr, err)
	}
	if _, err := mutation.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateTruncate, Size: 3}); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("overlapping conditional truncate = %v", err)
	}
	readFileFor(t, ownerFile, "AbXYZf")
	command.Edit, command.Claim = storage.RemoveExact, attempt.Claims[0]
	if released, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{command}, retainedLockRequest(t, session)); err != nil || released.State != storage.Released || len(released.Claims) != 0 || released.EverGranted || len(released.Effects) != 1 || released.Effects[0] != (storage.RangeEffect{Claim: command.Claim, Command: command, Released: true}) {
		t.Fatalf("exact release = %+v, %v", released, err)
	}
	command.Edit, command.Claim = storage.AddExact, ""
	command.Range = storage.Range{Kind: storage.Bytes, Start: 100, Length: 10}
	if beyondEOF, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{command}, retainedLockRequest(t, session)); err != nil || beyondEOF.State != storage.Granted {
		t.Fatalf("enforced grant beyond EOF = %+v, %v", beyondEOF, err)
	}
	if data, err := volume.Read(t.Context(), "f"); err != nil || string(data) != "AbXYZf" {
		t.Fatalf("path read with a lock beyond EOF = %q, %v", data, err)
	}
	if data, err := volume.ReadBounded(t.Context(), "f", 6); err != nil || string(data) != "AbXYZf" {
		t.Fatalf("bounded path read with a lock beyond EOF = %q, %v", data, err)
	}
	if read, err := other.ReadAt(t.Context(), 0, 200); err != nil || string(read.Data) != "AbXYZf" || read.Attr.Size != 6 {
		t.Fatalf("retained read extending beyond EOF = %+v, %v", read, err)
	}
	objects.pause.Store(true)
	truncated := make(chan error, 1)
	go func() {
		_, err := mutation.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateTruncate, Size: 4})
		truncated <- err
	}()
	select {
	case <-objects.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("conditional truncate did not stage")
	}
	if _, err := ownerFile.WriteAt(t.Context(), 0, []byte("Q")); err != nil {
		t.Fatal(err)
	}
	objects.release()
	if err := <-truncated; err != nil {
		t.Fatal(err)
	}
	readFileFor(t, other, "QbXY")
}

func TestObjectConditionalAppendRechecksEOFAndGuardsAfterStaging(t *testing.T) {
	for _, scenario := range []string{"latest EOF", "expected size changed", "expected metadata changed"} {
		t.Run(scenario, func(t *testing.T) {
			objects := newPausedFilePut(t)
			volume, meta := fileVolume(t, objects, 4096, nil)
			session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			otherSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
			t.Cleanup(objects.release)
			file := openFileFor(t, session, "f", storage.FileOpenOptions{
				OpenAccess:      storage.OpenAccess{Read: true, Write: true, Create: true},
				InitialMetadata: map[string][]byte{"test.guard": []byte("initial"), "test.other": {0xff, 0, 7}},
			})
			initial, err := file.WriteAt(t.Context(), 0, []byte("base"))
			if err != nil {
				t.Fatal(err)
			}
			other := openFileFor(t, otherSession, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
			mutation := objectCapability[storage.ConditionalFileMutation](t, file)
			if err := mutation.CheckConditionalFileMutation(); err != nil {
				t.Fatal(err)
			}
			command := storage.FileMutation{
				Kind: storage.MutateAppend, Data: []byte("-tail"),
				ExpectedMetadata: map[string][]byte{"test.guard": initial.Metadata["test.guard"].Version},
			}
			if scenario == "expected size changed" {
				expected := initial.Size
				command.ExpectedSize = &expected
			}
			type appendResult struct {
				attr storage.Attr
				err  error
			}
			appended := make(chan appendResult, 1)
			objects.pause.Store(true)
			go func() {
				attr, err := mutation.MutateFile(t.Context(), command)
				appended <- appendResult{attr: attr, err: err}
			}()
			select {
			case <-objects.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("conditional append did not stage")
			}
			if _, err := other.WriteAt(t.Context(), initial.Size, []byte("-other")); err != nil {
				t.Fatal(err)
			}
			if scenario == "expected metadata changed" {
				metadata := objectCapability[storage.MetadataAccess](t, otherSession)
				if _, err := metadata.SetMetadata(t.Context(), initial.ID, "test.guard", initial.Metadata["test.guard"].Version, []byte("changed")); err != nil {
					t.Fatal(err)
				}
			}
			competing, err := other.Stat(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			objects.release()
			result := <-appended
			want := "base-other"
			if scenario == "latest EOF" {
				want += "-tail"
				if result.err != nil || result.attr.ID != initial.ID || result.attr.Size != int64(len(want)) {
					t.Fatalf("append after concurrent extension = %+v, %v", result.attr, result.err)
				}
			} else if !errors.Is(result.err, storage.ErrConditionConflict) {
				t.Fatalf("append with changed condition = %+v, %v", result.attr, result.err)
			}
			current := readFileFor(t, file, want)
			if scenario != "latest EOF" && !reflect.DeepEqual(current, competing) {
				t.Fatalf("rejected append changed attributes: before=%+v after=%+v", competing, current)
			}
			if !reflect.DeepEqual(current.Metadata, competing.Metadata) {
				t.Fatalf("append changed opaque metadata: before=%+v after=%+v", competing.Metadata, current.Metadata)
			}
			if used, err := meta.Usage(t.Context()); err != nil || used != int64(len(want)) {
				t.Fatalf("append usage = %d, %v; want %d", used, err, len(want))
			}
		})
	}
}

func TestObjectPendingUnlinkGenerationAndArmedCloseRetainCleanup(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	session := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	opener := objectCapability[storage.AtomicFileOpener](t, session)
	options := storage.OpenAtOptions{
		Read: true, Write: true, Create: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep,
		Use:         storage.UseClaim{Uses: storage.ReadData | storage.WriteData | storage.DeleteName},
		CloseIntent: &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
	}
	armed, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || armed.File == nil {
		t.Fatalf("armed open = %+v, %v", armed, err)
	}
	if _, err := armed.File.WriteAt(t.Context(), 0, []byte("retained")); err != nil {
		t.Fatal(err)
	}
	if state := objectState(t, armed.File); state.PendingUnlink || len(state.PendingGeneration) != 0 {
		t.Fatalf("armed close is already pending: %+v", state)
	}
	options.Create, options.CloseIntent = false, nil
	survivor, err := opener.OpenAt(t.Context(), objectChild(root.ID, "f"), options)
	if err != nil || survivor.File == nil {
		t.Fatalf("compatible open after armed intent = %+v, %v", survivor, err)
	}
	deletion := objectCapability[storage.DeleteIntent](t, survivor.File)
	if err := deletion.CheckDeleteIntent(); err != nil {
		t.Fatal(err)
	}
	first, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
	if err != nil || !first.PendingUnlink || len(first.PendingGeneration) == 0 || first.Attr.ID != armed.Attr.ID {
		t.Fatalf("first pending generation = %+v, %v", first, err)
	}
	if cleared, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: first.PendingGeneration}); err != nil || cleared.PendingUnlink {
		t.Fatalf("clear first generation = %+v, %v", cleared, err)
	}
	second, err := deletion.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
	if err != nil || !second.PendingUnlink || len(second.PendingGeneration) == 0 || bytes.Equal(second.PendingGeneration, first.PendingGeneration) {
		t.Fatalf("second pending generation = %+v, %v", second, err)
	}
	if _, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: first.PendingGeneration}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale generation clear = %v", err)
	}
	if state := objectState(t, survivor.File); !state.PendingUnlink || !bytes.Equal(state.PendingGeneration, second.PendingGeneration) {
		t.Fatalf("stale clear changed current pending state: %+v", state)
	}
	if _, err := deletion.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: second.PendingGeneration}); err != nil {
		t.Fatal(err)
	}
	if err := armed.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := objectState(t, survivor.File); !state.PendingUnlink || bytes.Equal(state.PendingGeneration, second.PendingGeneration) || state.Attr.Size != 8 {
		t.Fatalf("armed close did not reactivate pending state: %+v", state)
	}
	if _, err := session.OpenFile(t.Context(), "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, storage.ErrPendingDelete) {
		t.Fatalf("new open during pending unlink = %v", err)
	}
	if _, err := survivor.File.WriteAt(t.Context(), 0, []byte("current!")); err != nil {
		t.Fatal(err)
	}
	readFileFor(t, survivor.File, "current!")
	if err := survivor.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := volume.Stat(t.Context(), "f"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("pending name after last close = %v", err)
	}
	if _, err := meta.StatNode(t.Context(), armed.Attr.ID); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("pending identity after last close = %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("pending cleanup usage = %d, %v", used, err)
	}
}
