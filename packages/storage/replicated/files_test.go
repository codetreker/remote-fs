package replicated_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func retainedAction(t *testing.T, epoch uint64) storage.FileActionID {
	t.Helper()
	action, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func retainedSession(t *testing.T, volume storage.FileStorage) (storage.FileSession, uint64) {
	t.Helper()
	if err := volume.CheckFileStorage(); err != nil {
		t.Fatal(err)
	}
	session, status, err := volume.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	epoch := status.ActionEpoch
	closeAction := retainedAction(t, epoch)
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), closeAction); err != nil {
			t.Error(err)
		}
	})
	return session, epoch
}

func retainedTarget(t *testing.T, volume storage.FileStorage, session storage.FileSession, epoch uint64, name string) (storage.EntryTarget, func()) {
	t.Helper()
	state, err := volume.FileState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	root, err := session.StatNode(t.Context(), state.RootID, storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: state.RootID, ExpectedMetadataRevision: root.Attr.MetadataRevision, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, epoch))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	closeAction := retainedAction(t, epoch)
	cleanup := func() {
		if _, err := parent.Close(context.Background(), closeAction); err != nil {
			t.Error(err)
		}
	}
	observation, err := parent.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if parent.NodeID() != state.RootID {
		cleanup()
		t.Fatal("retained parent changed identity")
	}
	lookup, err := parent.LookupAt(t.Context(), []byte(name))
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if observation.Location == nil {
		cleanup()
		t.Fatal("parent observation omitted its requested location")
	}
	target := storage.EntryTarget{Parent: parent.Reference(), ParentID: state.RootID, Name: []byte(name), DirectoryRevision: lookup.DirectoryRevision, Witness: observation.Location}
	if lookup.Found {
		target.ExpectedEntryID, target.ExpectedNodeID, target.ExpectedMetadataRevision = lookup.EntryID, lookup.Attr.ID, lookup.Attr.MetadataRevision
	}
	return target, cleanup
}

func retainedOpen(t *testing.T, volume storage.FileStorage, session storage.FileSession, epoch uint64, name string, create bool) storage.File {
	t.Helper()
	target, cleanup := retainedTarget(t, volume, session, epoch, name)
	defer cleanup()
	claim := storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}
	var receipt storage.FileActionReceipt
	var err error
	if create {
		receipt, err = session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: claim}, retainedAction(t, epoch))
	} else {
		receipt, err = session.RetainAt(t.Context(), storage.RetainAtRequest{Target: target, Claim: claim}, retainedAction(t, epoch))
	}
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	closeAction := retainedAction(t, epoch)
	t.Cleanup(func() {
		if _, err := file.Close(context.Background(), closeAction); err != nil {
			t.Error(err)
		}
	})
	return file
}

func TestRetainedFileQueriesTheAuthorityAfterRenameAndUnlink(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, local := mount(t, s)
	session, epoch := retainedSession(t, mounted)
	file := retainedOpen(t, mounted, session, epoch, "file", true)
	created, err := mounted.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal("atomic create did not confirm its replica entry:", err)
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("original")}, retainedAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	if attr, err := mounted.Stat(t.Context(), "file"); err != nil || attr.Size != 8 {
		t.Fatalf("linked write returned before its barrier: %+v, %v", attr, err)
	}
	if err := s.elsewhere.Write(t.Context(), "file", []byte("current")); err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Offset: 0, Length: 100})
	if err != nil || string(read.Data) != "current" || read.Attr.Size != 7 || read.Attr.ID != created.ID {
		t.Fatalf("retained read did not capture current authority content: %+v, %v", read, err)
	}
	if err := s.elsewhere.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.elsewhere.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 1, Data: []byte("!")}, retainedAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	if content, err := s.elsewhere.Read(t.Context(), "moved"); err != nil || string(content) != "c!rrent" {
		t.Fatalf("range write followed the former name: %q, %v", content, err)
	}
	if err := s.elsewhere.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	requireCaughtUp(t, s, local)
	position := local.Position()
	if attr, err := file.Stat(t.Context(), storage.ObservationOptions{}); err != nil || attr.Attr.ID != created.ID || attr.Attr.Size != 7 {
		t.Fatalf("detached file stat: %+v, %v", attr, err)
	}
	detached, err := session.StatNode(t.Context(), created.ID, storage.ObservationOptions{})
	if err != nil || detached.Attr.ID != created.ID {
		t.Fatalf("detached node stat: %+v, %v", detached, err)
	}
	retain, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: created.ID, ExpectedMetadataRevision: detached.Attr.MetadataRevision, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, retainedAction(t, epoch))
	if err != nil {
		t.Fatal("retaining the detached identity required its former name:", err)
	}
	byID, err := session.Reference(t.Context(), retain.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if read, err := byID.ReadAt(t.Context(), storage.FileReadRequest{Length: 7}); err != nil || string(read.Data) != "c!rrent" || read.Attr.ID != created.ID {
		t.Fatalf("identity retain selected a replacement: %+v, %v", read, err)
	}
	if _, err := byID.Close(t.Context(), retainedAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	if receipt, err := file.Truncate(t.Context(), storage.FileTruncateRequest{Size: 10}, retainedAction(t, epoch)); err != nil || receipt.Observation.Attr.Size != 10 || receipt.Effects&storage.EffectContentChanged == 0 {
		t.Fatalf("detached truncate: %+v, %v", receipt, err)
	}
	read, err = file.ReadAt(t.Context(), storage.FileReadRequest{Offset: 5, Length: 10})
	if err != nil || !bytes.Equal(read.Data, []byte{'n', 't', 0, 0, 0}) || read.Attr.Size != 10 {
		t.Fatalf("detached read/EOF lost its content revision: %+v, %v", read, err)
	}
	metadata, err := read.Attr.Metadata.With(storage.OpaqueMetadata{Key: "test.value", Version: 1, Data: []byte("detached")})
	if err != nil {
		t.Fatal(err)
	}
	if attr, err := session.SetNodeAttr(t.Context(), created.ID, storage.AttrChange{ExpectedRevision: read.Attr.MetadataRevision, Metadata: &metadata}, retainedAction(t, epoch)); err != nil || !reflect.DeepEqual(attr.Observation.Attr.Metadata, metadata) {
		t.Fatalf("detached identity setattr: %+v, %v", attr, err)
	}
	moment := time.Unix(1000, 123)
	if attr, err := file.SetAttr(t.Context(), storage.AttrChange{ModTime: &moment}, retainedAction(t, epoch)); err != nil || !attr.Observation.Attr.ModTime.Equal(moment) {
		t.Fatalf("detached file setattr: %+v, %v", attr, err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if local.Position() != position {
		t.Fatal("detached mutations fabricated named replica events")
	}
	if _, err := mounted.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("detached mutation recreated its removed name: %v", err)
	}
	if content, err := s.elsewhere.Read(t.Context(), "file"); err != nil || string(content) != "replacement" {
		t.Fatalf("detached mutation changed the replacement: %q, %v", content, err)
	}
}

func TestRetainedSessionPreservesMutationScope(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)
	if err := mounted.Write(t.Context(), "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	owner := replicaLockOwner(t, mounted)
	grant := replicaLockGrant(t, mounted, owner)
	proofs := []locking.GrantRef{grant}
	view, err := mounted.Scope(locking.MutationScope{Owner: owner, Grants: proofs})
	if err != nil {
		t.Fatal(err)
	}
	session, epoch := retainedSession(t, view.(storage.FileStorage))
	proofs[0].Generation++
	file := retainedOpen(t, mounted, session, epoch, "file", false)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("scoped")}, retainedAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(locking.WithScope(t.Context(), locking.MutationScope{}), storage.FileTruncateRequest{Size: 0}, retainedAction(t, epoch)); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("explicit anonymous file mutation inherited a grant: %v", err)
	}
	if _, err := mounted.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{}, retainedAction(t, epoch)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("file mutation discarded its expired proof: %v", err)
	}
	if read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Offset: 0, Length: 6}); err != nil || string(read.Data) != "scoped" {
		t.Fatalf("ordinary retained read asserted a live strong grant: %+v, %v", read, err)
	}
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
		t.Fatal("ordinary retained stat asserted a live strong grant:", err)
	}
}

func TestRetainedControlsRemainAvailableWhenTheStreamFails(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)
	session, epoch := retainedSession(t, mounted)
	file := retainedOpen(t, mounted, session, epoch, "file", true)
	other := retainedOpen(t, mounted, session, epoch, "file", false)
	s.events.cut()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := mounted.Stat(t.Context(), "file"); errors.Is(err, syscall.EIO) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica did not observe its disconnected stream")
		}
		time.Sleep(time.Millisecond)
	}
	writeAction, truncateAction, attrAction := retainedAction(t, epoch), retainedAction(t, epoch), retainedAction(t, epoch)
	before := s.calls.total()
	for name, call := range map[string]func() error{
		"stat": func() error { _, err := file.Stat(t.Context(), storage.ObservationOptions{}); return err },
		"read": func() error {
			_, err := file.ReadAt(t.Context(), storage.FileReadRequest{Offset: 0, Length: 1})
			return err
		},
		"write": func() error {
			_, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte{'x'}}, writeAction)
			return err
		},
		"truncate": func() error {
			_, err := file.Truncate(t.Context(), storage.FileTruncateRequest{Size: 0}, truncateAction)
			return err
		},
		"setattr": func() error {
			_, err := file.SetAttr(t.Context(), storage.AttrChange{}, attrAction)
			return err
		},
		"sync": func() error { return file.Sync(t.Context()) },
	} {
		if err := call(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s fabricated a healthy answer: %v", name, err)
		}
	}
	if s.calls.total() != before {
		t.Fatal("unhealthy retained I/O reached the authority")
	}
	scope := storage.RangeScope{Domain: 1}
	snapshot, err := file.RangeSnapshot(t.Context(), 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	action := retainedAction(t, epoch)
	request := storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 0, End: ^uint64(0), Exclusive: true}}}
	if receipt, err := file.ReplaceRanges(t.Context(), request, action); err != nil || receipt.State != storage.FileActionCompleted || receipt.Effects&storage.EffectRangesChanged == 0 {
		t.Fatalf("range replacement was not authoritative: %+v, %v", receipt, err)
	}
	if snapshot, err := other.RangeSnapshot(t.Context(), 2, scope); err != nil || len(snapshot.Other) != 1 || snapshot.Other[0].Owner.ID != 1 {
		t.Fatalf("range conflict was not authoritative: %+v, %v", snapshot, err)
	}
	if receipt, err := session.QueryAction(t.Context(), action); err != nil || receipt.State != storage.FileActionCompleted {
		t.Fatalf("range receipt unavailable: %+v, %v", receipt, err)
	}
	if receipt, err := session.CancelAction(t.Context(), action); err != nil || receipt.State != storage.FileActionCompleted {
		t.Fatalf("range cancellation unavailable: %+v, %v", receipt, err)
	}
	if _, err := session.RetireRangeOwner(t.Context(), 1, retainedAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := other.RangeSnapshot(t.Context(), 2, scope); err != nil || len(snapshot.Other) != 0 {
		t.Fatalf("range cleanup did not release the acquisition: %+v, %v", snapshot, err)
	}
	if _, err := session.Renew(t.Context()); err != nil {
		t.Fatal(err)
	}
}
