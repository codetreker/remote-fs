package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type retainedTestSession struct {
	storage.FileSession
	rootID uint64
}

type retainedOpen struct {
	File     storage.File
	Attr     storage.Attr
	Location *storage.EntryLocation
	Receipt  storage.FileActionReceipt
}

func retainedAction(t *testing.T, session storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func retainedSessionFor(t *testing.T, volume storage.FileStorage) *retainedTestSession {
	t.Helper()
	return fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
}

func retainedResult(t *testing.T, session storage.FileSession, result storage.FileActionReceipt, err error) retainedOpen {
	t.Helper()
	if err != nil || result.State != storage.FileActionCompleted || result.Reference == 0 {
		t.Fatalf("retain = %+v, %v", result, err)
	}
	file, err := session.Reference(t.Context(), result.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return retainedOpen{File: file, Attr: result.Observation.Attr, Location: result.Observation.Location, Receipt: result}
}

func retainedRootFor(t *testing.T, session *retainedTestSession) retainedOpen {
	t.Helper()
	result, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: session.rootID}, retainedAction(t, session))
	opened := retainedResult(t, session, result, err)
	observation, err := opened.File.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	opened.Location = observation.Location
	return opened
}

func retainedTarget(t *testing.T, parent storage.File, name string) storage.EntryTarget {
	t.Helper()
	observation, err := parent.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	if observation.Location == nil {
		t.Fatal("requested parent location is missing")
	}
	target := storage.EntryTarget{Parent: parent.Reference(), ParentID: observation.Attr.ID, Name: []byte(name), DirectoryRevision: observation.Attr.DirectoryRevision, Witness: observation.Location}
	cursor := storage.DirectoryCursor{}
	for {
		page, err := parent.ListAt(t.Context(), storage.DirectoryPageRequest{Revision: target.DirectoryRevision, Cursor: cursor, MaxEntries: 256, MaxBytes: storage.MaxDirectoryPageBytes})
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range page.Entries {
			if string(entry.Name) == name {
				target.ExpectedEntryID = entry.EntryID
				target.ExpectedNodeID = entry.Attr.ID
				target.ExpectedMetadataRevision = entry.Attr.MetadataRevision
			}
		}
		if page.Done {
			break
		}
		cursor = page.Next
	}
	return target
}

func retainedCreateFor(t *testing.T, session *retainedTestSession, parent storage.File, name string, kind storage.NodeKind) retainedOpen {
	t.Helper()
	target := retainedTarget(t, parent, name)
	var result storage.FileActionReceipt
	var err error
	claim := storage.AccessClaim{Uses: storage.AllAccessUses}
	if kind != storage.NodeRegular {
		claim = storage.AccessClaim{Uses: storage.RemoveEntry}
	}
	if target.ExpectedNodeID != 0 {
		result, err = session.RetainAt(t.Context(), storage.RetainAtRequest{Target: target, Claim: claim}, retainedAction(t, session))
	} else {
		result, err = session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: kind}, Claim: claim}, retainedAction(t, session))
	}
	return retainedResult(t, session, result, err)
}

func retainedLocationPath(location *storage.EntryLocation) string {
	if location == nil {
		return ""
	}
	names := make([]string, len(location.Ancestors))
	for i, entry := range location.Ancestors {
		names[i] = string(entry.Name)
	}
	return strings.Join(names, "/")
}

func TestRetainedObjectReferencesSupportDirectoriesAndMetadataOnlyAccess(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	dir := retainedCreateFor(t, session, root.File, "dir", storage.NodeDirectory)
	child := retainedCreateFor(t, session, dir.File, "child", storage.NodeRegular)
	if (root.Location == nil || root.Location.State != storage.LocationRoot) || !dir.Attr.IsDir() {
		t.Fatalf("root/directory: %+v, %+v", root, dir)
	}
	contentReceipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: dir.Attr.ID, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, retainedAction(t, session))
	contentDirectory := retainedResult(t, session, contentReceipt, err)
	if _, err := contentDirectory.File.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("directory read: %v", err)
	}
	if _, err := contentDirectory.File.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, retainedAction(t, session)); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("directory write: %v", err)
	}
	result, err := session.RetainAt(t.Context(), storage.RetainAtRequest{Target: retainedTarget(t, dir.File, "child")}, retainedAction(t, session))
	meta := retainedResult(t, session, result, err)
	if observation, err := meta.File.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); err != nil || observation.Attr.ID != child.Attr.ID {
		t.Fatalf("metadata reference = %+v, %v", observation, err)
	}
	if _, err := meta.File.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("metadata read: %v", err)
	}
	if err := meta.File.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := volume.Rename(t.Context(), "dir", "renamed"); err != nil {
		t.Fatal(err)
	}
	listing, err := dir.File.ListAt(t.Context(), storage.DirectoryPageRequest{MaxEntries: 256, MaxBytes: storage.MaxDirectoryPageBytes})
	if err != nil || !listing.Done || len(listing.Entries) != 1 || string(listing.Entries[0].Name) != "child" || listing.Entries[0].Attr.ID != child.Attr.ID {
		t.Fatalf("retained directory: %+v, %v", listing, err)
	}
	if observation, err := meta.File.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); err != nil || retainedLocationPath(observation.Location) != "renamed/child" {
		t.Fatalf("renamed metadata: %+v, %v", observation, err)
	}
}

func TestRetainedObjectLocksCheckOrdinaryLogicalRanges(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	opened := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular)
	owner := storage.RangeOwnerID(1)
	scope := storage.RangeScope{Enforced: true}
	snapshot, err := opened.File.RangeSnapshot(t.Context(), owner, scope)
	if err != nil {
		t.Fatal(err)
	}
	result, err := opened.File.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 4, End: 5, Exclusive: true}}}, retainedAction(t, session))
	if err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("range acquisition: %+v, %v", result, err)
	}
	ordinarySession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ordinary := openFileFor(t, ordinarySession, "file", retainedOpenOptions{Read: true, Write: true})
	if _, err := ordinary.Stat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if read, err := ordinary.ReadAt(t.Context(), 0, 2); err != nil || string(read.Data) != "01" {
		t.Fatalf("disjoint read = %q, %v", read.Data, err)
	}
	for name, operation := range map[string]func() error{
		"overlapping read":     func() error { _, err := ordinary.ReadAt(t.Context(), 4, 1); return err },
		"whole read":           func() error { _, err := volume.Read(t.Context(), "file"); return err },
		"overlapping write":    func() error { _, err := ordinary.WriteAt(t.Context(), 4, []byte("x")); return err },
		"whole replacement":    func() error { return volume.Write(t.Context(), "file", []byte("replacement")) },
		"overlapping truncate": func() error { _, err := ordinary.Truncate(t.Context(), 5); return err },
	} {
		err := operation()
		var failure *storage.FileError
		if !errors.Is(err, syscall.EAGAIN) || !errors.As(err, &failure) || failure.Conflict == nil || failure.Conflict.Kind != storage.ConflictRange {
			t.Fatalf("%s = %v; want range conflict", name, err)
		}
	}
	if _, err := ordinary.WriteAt(t.Context(), 0, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if _, err := ordinary.Truncate(t.Context(), 8); err != nil {
		t.Fatal(err)
	}
	if read, err := opened.File.ReadAt(t.Context(), storage.FileReadRequest{Offset: 4, Length: 2, Owner: &owner}); err != nil || string(read.Data) != "45" {
		t.Fatalf("owner read = %q, %v", read.Data, err)
	}
	snapshot, err = opened.File.RangeSnapshot(t.Context(), owner, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: snapshot.Revision}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if body, err := volume.Read(t.Context(), "file"); err != nil || string(body) != "ab234567" {
		t.Fatalf("final content = %q, %v", body, err)
	}
}

type retainedPutProbe struct {
	*memory.Objects
	puts             atomic.Int64
	gate             atomic.Bool
	entered, release chan struct{}
}

func (p *retainedPutProbe) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	p.puts.Add(1)
	digest, err := p.Objects.Put(ctx, key, body)
	if err != nil {
		return nil, err
	}
	if p.gate.CompareAndSwap(true, false) {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return digest, nil
}

func TestRetainedObjectContentReplayKeepsOneUploadAndOriginalAction(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "cancelled"}[cancelled], func(t *testing.T) {
			objects := &retainedPutProbe{Objects: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
			volume, _ := fileVolume(t, objects, 4096, nil)
			session := retainedSessionFor(t, volume)
			root := retainedRootFor(t, session)
			file := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular).File
			id := retainedAction(t, session)
			objects.gate.Store(true)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type outcome struct {
				result storage.FileActionReceipt
				err    error
			}
			result := make(chan outcome, 1)
			finished := make(chan struct{})
			var once sync.Once
			resume := func() { once.Do(func() { close(objects.release) }) }
			go func() {
				defer close(finished)
				r, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("payload")}, id)
				result <- outcome{r, err}
			}()
			t.Cleanup(func() { resume(); cancel(); <-finished })
			select {
			case <-objects.entered:
			case <-ctx.Done():
				t.Fatal("write did not reach the actual Put gate")
			}
			if r, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("payload")}, id); err != nil || r.State != storage.FileActionPending {
				t.Fatalf("pending replay = %+v, %v", r, err)
			}
			if r, err := session.QueryAction(ctx, id); err != nil || r.State != storage.FileActionPending {
				t.Fatalf("pending query = %+v, %v", r, err)
			}
			if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("different")}, id); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("changed replay fingerprint = %v", err)
			}
			if objects.puts.Load() != 1 {
				t.Fatalf("replay uploaded %d objects, want 1", objects.puts.Load())
			}
			wantState := storage.FileActionCompleted
			wantBody := []byte("payload")
			var wantError error
			if cancelled {
				wantState, wantBody = storage.FileActionNotApplied, nil
				wantError = syscall.EINTR
				if r, err := session.CancelAction(ctx, id); !errors.Is(err, wantError) || r.State != wantState {
					t.Fatalf("cancel = %+v, %v", r, err)
				}
			}
			resume()
			got := <-result
			if !errors.Is(got.err, wantError) || got.result.State != wantState || got.result.Action != id {
				t.Fatalf("original write = %+v, %v", got.result, got.err)
			}
			if r, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("payload")}, id); !errors.Is(err, wantError) || r.State != wantState || r.Action != id {
				t.Fatalf("settled replay = %+v, %v", r, err)
			}
			if objects.puts.Load() != 1 {
				t.Fatalf("settled replay uploaded %d objects, want 1", objects.puts.Load())
			}
			if read, err := file.ReadAt(ctx, storage.FileReadRequest{Offset: 0, Length: 20}); err != nil || !bytes.Equal(read.Data, wantBody) {
				t.Fatalf("content = %q, %v; want %q", read.Data, err, wantBody)
			}
		})
	}
}

type retainedReadFailure struct {
	*memory.Objects
	fail atomic.Bool
}

func (o *retainedReadFailure) GetBounded(ctx context.Context, key string, size int64) ([]byte, error) {
	if o.fail.Load() {
		return nil, syscall.EIO
	}
	return o.Objects.GetBounded(ctx, key, size)
}

func TestRetainedObjectZeroWriteAndTruncateDoNotReadDiscardedBytes(t *testing.T) {
	objects := &retainedReadFailure{Objects: memory.New()}
	volume, _ := fileVolume(t, objects, 4096, nil)
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	file := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular).File
	initial, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: []byte("stored")}, retainedAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	objects.fail.Store(true)
	empty, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: nil}, retainedAction(t, session))
	if err != nil || empty.State != storage.FileActionCompleted || !reflect.DeepEqual(empty.Observation.Attr, initial.Observation.Attr) {
		t.Fatalf("zero write = %+v, %v; want unchanged %+v", empty, err, initial.Observation.Attr)
	}
	truncated, err := file.Truncate(t.Context(), storage.FileTruncateRequest{Size: 0}, retainedAction(t, session))
	if err != nil || truncated.State != storage.FileActionCompleted || truncated.Observation.Attr.Size != 0 {
		t.Fatalf("empty truncate = %+v, %v", truncated, err)
	}
}

func TestRetainedObjectRevisionRetryPreservesUntouchedCurrentBytes(t *testing.T) {
	objects := &retainedPutProbe{Objects: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	volume, _ := fileVolume(t, objects, 4096, nil)
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	file := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular).File
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: []byte("abcdef")}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	id := retainedAction(t, session)
	objects.gate.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	finished := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(objects.release) }) }
	go func() {
		defer close(finished)
		_, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("X")}, id)
		result <- err
	}()
	t.Cleanup(func() { resume(); cancel(); <-finished })
	select {
	case <-objects.entered:
	case <-ctx.Done():
		t.Fatal("patch did not reach the actual Put gate")
	}
	if err := volume.Write(ctx, "file", []byte("abcdeY")); err != nil {
		t.Fatal(err)
	}
	resume()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if read, err := file.ReadAt(ctx, storage.FileReadRequest{Offset: 0, Length: 6}); err != nil || string(read.Data) != "XbcdeY" {
		t.Fatalf("CAS retry content = %q, %v", read.Data, err)
	}
	before := objects.puts.Load()
	if r, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("X")}, id); err != nil || r.State != storage.FileActionCompleted {
		t.Fatalf("CAS receipt replay = %+v, %v", r, err)
	}
	if after := objects.puts.Load(); after != before {
		t.Fatalf("CAS receipt replay uploaded again: %d -> %d", before, after)
	}
}

type retainedFailedPut struct {
	*memory.Objects
	puts    atomic.Int64
	failure error
}

func (o *retainedFailedPut) Put(ctx context.Context, key string, body []byte) ([]byte, error) {
	o.puts.Add(1)
	if _, err := o.Objects.Put(ctx, key, body); err != nil {
		return nil, err
	}
	return nil, o.failure
}

func TestRetainedObjectFailedPutRetainsOneRejectedActionAndQuarantinedObject(t *testing.T) {
	failure := errors.New("lost object Put confirmation")
	objects := &retainedFailedPut{Objects: memory.New(), failure: failure}
	volume, meta := fileVolume(t, objects, 4096, nil)
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	file := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular).File
	id := retainedAction(t, session)
	for range 2 {
		result, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: []byte("uncertain")}, id)
		if !errors.Is(err, failure) || result.State != storage.FileActionNotApplied || result.Action != id {
			t.Fatalf("failed Put receipt = %+v, %v", result, err)
		}
	}
	if objects.puts.Load() != 1 {
		t.Fatalf("failed Put replay uploaded %d objects", objects.puts.Load())
	}
	if read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Offset: 0, Length: 20}); err != nil || len(read.Data) != 0 {
		t.Fatalf("failed Put published bytes = %q, %v", read.Data, err)
	}
	status, err := meta.ObjectStatus(t.Context())
	if err != nil || status.UnresolvedCount != 1 {
		t.Fatalf("quarantine status = %+v, %v", status, err)
	}
}

func TestRetainedObjectSessionRetainsRoot(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.CheckFileStorage(); err != nil {
		t.Fatal(err)
	}
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	if root.Attr.Kind != storage.NodeDirectory || (root.Location == nil || root.Location.State != storage.LocationRoot) {
		t.Fatalf("root = %+v", root)
	}
}

func retainedDrain(t *testing.T, session storage.FileSession, file storage.File) storage.FileActionReceipt {
	t.Helper()
	observation, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	if observation.Location == nil || len(observation.Location.Ancestors) == 0 {
		t.Fatal("drain needs a linked entry")
	}
	condition := storage.RemovalFile
	if observation.Attr.IsDir() {
		condition = storage.RemovalIfEmpty
	}
	result, err := file.DrainEntry(t.Context(), storage.DrainEntryRequest{ExpectedMetadataRevision: observation.Attr.MetadataRevision, Entry: observation.Location.Ancestors[len(observation.Location.Ancestors)-1], Witness: *observation.Location, Condition: condition}, retainedAction(t, session))
	if err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("drain = %+v, %v", result, err)
	}
	return result
}

func TestRetainedObjectActionsRetainIdentityThroughMetadataLinksAndClose(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	openID := retainedAction(t, session)
	receipt, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: retainedTarget(t, root.File, "file"), Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, openID)
	opened := retainedResult(t, session, receipt, err)
	file := opened.File
	if file.Reference() == 0 {
		t.Fatal("empty reference identity")
	}
	replay, err := session.QueryAction(t.Context(), openID)
	if err != nil || replay.Reference != file.Reference() || replay.Observation.Attr.ID != opened.Attr.ID {
		t.Fatalf("retain replay = %+v, %v", replay, err)
	}
	replayed, err := session.Reference(t.Context(), replay.Reference)
	if err != nil || replayed.Reference() != file.Reference() {
		t.Fatalf("reference replay = %v, %v", replayed, err)
	}
	metadata := storage.Metadata{{Key: "app.tag", Version: 1, Data: []byte("hidden-from-index")}}
	changed, err := file.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: opened.Attr.MetadataRevision, Metadata: &metadata}, retainedAction(t, session))
	if err != nil || !reflect.DeepEqual(changed.Observation.Attr.Metadata, metadata) {
		t.Fatalf("opaque metadata = %+v, %v", changed, err)
	}
	renamed, err := file.Rename(t.Context(), storage.RenameRequest{Source: retainedTarget(t, root.File, "file"), Destination: retainedTarget(t, root.File, "moved")}, retainedAction(t, session))
	if err != nil || renamed.Observation.Attr.ID != opened.Attr.ID {
		t.Fatalf("identity rename = %+v, %v", renamed, err)
	}
	renamedObservation, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil || renamedObservation.Attr.ID != opened.Attr.ID || retainedLocationPath(renamedObservation.Location) != "moved" {
		t.Fatalf("renamed identity location = %+v, %v", renamedObservation, err)
	}
	drained := retainedDrain(t, session, file)
	if _, err := file.CancelDrain(t.Context(), storage.CancelDrainRequest{EntryID: drained.Removal.EntryID, Generation: drained.Removal.Generation}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	observation, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || len(observation.LinkTarget) != 0 {
		t.Fatalf("regular observation = %+v, %v", observation, err)
	}
	linked, err := file.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: observation.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Metadata: observation.Attr.Metadata}, retainedAction(t, session))
	if err != nil || linked.Observation.Attr.ID != opened.Attr.ID {
		t.Fatalf("kind conversion = %+v, %v", linked, err)
	}
	if observation, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); err != nil || string(observation.LinkTarget) != "target" || retainedLocationPath(observation.Location) != "moved" || observation.Location.State != storage.LocationLinked {
		t.Fatalf("atomic link observation = %+v, %v", observation, err)
	}
	if _, err := volume.Read(t.Context(), "moved"); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("named symlink read = %v", err)
	}
	observation, err = file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil || observation.Location == nil || len(observation.Location.Ancestors) == 0 {
		t.Fatalf("removal observation = %+v, %v", observation, err)
	}
	prepared, err := file.PrepareRemoval(t.Context(), storage.PrepareRemovalRequest{ExpectedMetadataRevision: observation.Attr.MetadataRevision, Entry: observation.Location.Ancestors[len(observation.Location.Ancestors)-1], Witness: *observation.Location, Condition: storage.RemovalFile}, retainedAction(t, session))
	if err != nil || !prepared.Removal.Prepared || prepared.Removal.IntentID == 0 || prepared.Removal.State != storage.EntryActive {
		t.Fatalf("prepared retirement = %+v, %v", prepared, err)
	}
	if _, err := file.CancelPrepared(t.Context(), prepared.Removal.IntentID, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	retainedDrain(t, session, file)
	closeID := retainedAction(t, session)
	closed, err := file.Close(t.Context(), closeID)
	if err != nil || closed.State != storage.FileActionCompleted {
		t.Fatalf("close = %+v, %v", closed, err)
	}
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed reference = %v", err)
	}
	if result, err := session.QueryAction(t.Context(), closeID); err != nil || result.State != storage.FileActionCompleted {
		t.Fatalf("close receipt = %+v, %v", result, err)
	}
	if _, err := volume.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("drained entry remains = %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("final usage = %d, %v", used, err)
	}
}

func TestRetainedObjectControlAdmissionRemainsAvailableDuringStaging(t *testing.T) {
	objects := &retainedPutProbe{Objects: memory.New(), entered: make(chan struct{}), release: make(chan struct{})}
	volume, _ := fileVolume(t, objects, 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.MaxOperations = 1
	session := fileSessionFor(t, volume, options)
	root := retainedRootFor(t, session)
	file := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular).File
	id := retainedAction(t, session)
	objects.gate.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	resume := func() { once.Do(func() { close(objects.release) }) }
	go func() {
		defer close(finished)
		_, err := file.WriteAt(ctx, storage.FileWriteRequest{Offset: 0, Data: []byte("payload")}, id)
		result <- err
	}()
	t.Cleanup(func() { resume(); cancel(); <-finished })
	select {
	case <-objects.entered:
	case <-ctx.Done():
		t.Fatal("write did not reach Put gate")
	}
	if _, err := file.Stat(ctx, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("saturated data admission = %v", err)
	}
	before, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, err := session.Renew(ctx)
	if err != nil || after.Epoch != before.Epoch || after.Revision <= before.Revision || after.Remaining <= 0 {
		t.Fatalf("renew during staging = %+v, %v", after, err)
	}
	if pending, err := session.QueryAction(ctx, id); err != nil || pending.State != storage.FileActionPending {
		t.Fatalf("query during staging = %+v, %v", pending, err)
	}
	resume()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(ctx, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); err != nil {
		t.Fatalf("released data admission = %v", err)
	}
}

func TestRetainedObjectExpirySettlesDetachedLifetimeAccounting(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	cleaned := make(chan struct{})
	var once sync.Once
	creation, cancel := context.WithCancel(storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if previous == 4 && next == 0 && result == storage.PublicationApplied {
				once.Do(func() { close(cleaned) })
			}
			return nil
		}, nil
	}))
	defer cancel()
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Second
	rawSession, initialStatus, err := volume.NewFileSession(creation, options)
	if err != nil {
		t.Fatal(err)
	}
	state, err := volume.FileState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session := &retainedTestSession{FileSession: rawSession, rootID: state.RootID}
	closeID, err := storage.NewFileActionID(initialStatus.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), closeID); err != nil {
			t.Errorf("close expiring session: %v", err)
		}
	})
	root := retainedRootFor(t, session)
	file := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular).File
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: []byte("kept")}, retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := volume.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Offset: 0, Length: 4}); err != nil || string(read.Data) != "kept" {
		t.Fatalf("detached read = %q, %v", read.Data, err)
	}
	cancel()
	select {
	case <-cleaned:
	case <-time.After(5 * time.Second):
		t.Fatal("expiry did not settle detached cleanup")
	}
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired reference = %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("expired usage = %d, %v", used, err)
	}
	if _, err := session.Close(t.Context(), closeID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(t.Context(), closeID); err != nil {
		t.Fatalf("repeated close = %v", err)
	}
}

func TestRetainedObjectAlreadyElapsedLeaseCanClose(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Nanosecond
	session, initialStatus, err := volume.NewFileSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := session.Status(t.Context()); err != nil || !status.Retired || status.Remaining != 0 {
		t.Fatalf("already elapsed lease = %+v, %v", status, err)
	}
	closeID, err := storage.NewFileActionID(initialStatus.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := session.Close(t.Context(), closeID); err != nil || (result.State != storage.FileActionRetired && result.State != storage.FileActionCompleted) || result.Errno != 0 {
		t.Fatalf("elapsed-session cleanup = %+v, %v", result, err)
	}
}

func TestRetainedObjectDrainRefusesNamedIOAndPreservesExistingReferences(t *testing.T) {
	volume, meta := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "file", []byte("initial")); err != nil {
		t.Fatal(err)
	}
	session := retainedSessionFor(t, volume)
	root := retainedRootFor(t, session)
	opened := retainedCreateFor(t, session, root.File, "file", storage.NodeRegular)
	ordinarySession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ordinary := openFileFor(t, ordinarySession, "file", retainedOpenOptions{Read: true, Write: true})
	retainedDrain(t, session, opened.File)
	if body, err := volume.Read(t.Context(), "file"); body != nil || !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("named read under draining = %q, %v", body, err)
	}
	if err := volume.Write(t.Context(), "file", []byte("wrong replacement")); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("named write under draining = %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 7 {
		t.Fatalf("rejected named write changed usage = %d, %v", used, err)
	}
	for name, read := range map[string]func(context.Context, int64, int) (storage.FileRead, error){
		"draining reference": func(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
			return opened.File.ReadAt(ctx, storage.FileReadRequest{Offset: offset, Length: length})
		}, "ordinary": ordinary.ReadAt,
	} {
		if got, err := read(t.Context(), 0, 20); err != nil || string(got.Data) != "initial" || got.Attr.ID != opened.Attr.ID {
			t.Fatalf("%s retained read = %q, ID %d, %v", name, got.Data, got.Attr.ID, err)
		}
	}
	if _, err := opened.File.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: []byte("W")}, retainedAction(t, session)); err != nil {
		t.Fatalf("draining reference write: %v", err)
	}
	if _, err := ordinary.WriteAt(t.Context(), 1, []byte("O")); err != nil {
		t.Fatalf("ordinary retained write: %v", err)
	}
	for name, read := range map[string]func(context.Context, int64, int) (storage.FileRead, error){
		"draining reference": func(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
			return opened.File.ReadAt(ctx, storage.FileReadRequest{Offset: offset, Length: length})
		}, "ordinary": ordinary.ReadAt,
	} {
		if got, err := read(t.Context(), 0, 20); err != nil || string(got.Data) != "WOitial" || got.Attr.ID != opened.Attr.ID {
			t.Fatalf("%s retained current bytes = %q, ID %d, %v", name, got.Data, got.Attr.ID, err)
		}
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 7 {
		t.Fatalf("retained writes changed usage = %d, %v", used, err)
	}
	if err := ordinary.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.Close(t.Context(), retainedAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := volume.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("last close left the pending name: %v", err)
	}
	if used, err := meta.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("last close retained usage = %d, %v", used, err)
	}
}
