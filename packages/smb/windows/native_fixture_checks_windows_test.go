package windows

import (
	"context"
	"errors"
	"fmt"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func nativeTestSession(t *testing.T, a storage.FileStorage) storage.FileSession {
	t.Helper()
	s, status, err := a.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		id, err := storage.NewFileActionID(status.ActionEpoch)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := s.Close(context.Background(), id); err != nil {
			t.Error(err)
		}
	})
	return s
}
func nativeTestAction(t *testing.T, s storage.FileSession) storage.FileActionID {
	t.Helper()
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func nativeTestReference(t *testing.T, s storage.FileSession, r storage.FileActionReceipt) storage.File {
	t.Helper()
	f, err := s.Reference(t.Context(), r.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func nativeTestRoot(t *testing.T, s storage.FileSession) storage.File {
	t.Helper()
	r, err := s.Retain(t.Context(), storage.RetainRequest{NodeID: 1}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return nativeTestReference(t, s, r)
}
func nativeTestTarget(t *testing.T, parent storage.File, name string) storage.EntryTarget {
	t.Helper()
	o, err := parent.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	target := storage.EntryTarget{Parent: parent.Reference(), ParentID: o.Attr.ID, Name: []byte(name), DirectoryRevision: o.Attr.DirectoryRevision, Witness: o.Location}
	request := storage.DirectoryPageRequest{Revision: o.Attr.DirectoryRevision, MaxEntries: 64, MaxBytes: storage.MaxDirectoryPageBytes}
	for {
		page, err := parent.ListAt(t.Context(), request)
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
		request.Cursor = page.Next
	}
	return target
}
func nativeTestCreate(t *testing.T, s storage.FileSession, root storage.File, name string) storage.File {
	t.Helper()
	r, err := s.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: nativeTestTarget(t, root, name), Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return nativeTestReference(t, s, r)
}
func TestNativeAuthorityOpenRequiresExpectedIdentity(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "identity.bin")
	target := nativeTestTarget(t, root, "identity.bin")
	r, err := s.RetainAt(t.Context(), storage.RetainAtRequest{Target: target}, nativeTestAction(t, s))
	if err != nil || r.Observation.Attr.ID != target.ExpectedNodeID {
		t.Fatalf("matching identity=%+v %v", r, err)
	}
	before := a.openReferences()
	target.ExpectedNodeID++
	_, err = s.RetainAt(t.Context(), storage.RetainAtRequest{Target: target}, nativeTestAction(t, s))
	var conflict *storage.FileError
	if !errors.As(err, &conflict) || conflict.Conflict == nil || conflict.Conflict.Kind != storage.ConflictIdentity || a.openReferences() != before {
		t.Fatalf("wrong identity: %v", err)
	}
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
		t.Fatal(err)
	}
	missing := nativeTestTarget(t, root, "absent.bin")
	missing.ExpectedNodeID = target.ExpectedNodeID
	missing.ExpectedEntryID = target.ExpectedEntryID
	_, err = s.RetainAt(t.Context(), storage.RetainAtRequest{Target: missing}, nativeTestAction(t, s))
	if !errors.As(err, &conflict) || conflict.Conflict.Kind != storage.ConflictIdentity {
		t.Fatalf("missing identity: %v", err)
	}
}
func TestNativeAuthorityEnforcesClaimsAndIndependentRanges(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "ranges")
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("original")}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	o, err := file.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other := nativeTestSession(t, a)
	retained, err := other.Retain(t.Context(), storage.RetainRequest{NodeID: o.Attr.ID, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, other))
	if err != nil {
		t.Fatal(err)
	}
	writer := nativeTestReference(t, other, retained)
	scope := storage.RangeScope{Enforced: true}
	snap, err := file.RangeSnapshot(t.Context(), 7, scope)
	if err != nil {
		t.Fatal(err)
	}
	held := []storage.RangeAcquisition{{ID: 1, Start: 0, End: 2}, {ID: 2, Start: 0, End: 2}}
	receipt, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 7, Scope: scope, ExpectedRevision: snap.Revision, Ranges: held}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	resetTarget := nativeTestTarget(t, root, "ranges")
	reset, resetErr := s.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: resetTarget, ExpectedRevision: resetTarget.ExpectedMetadataRevision, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, s))
	var resetConflict *storage.FileError
	if !errors.As(resetErr, &resetConflict) || resetConflict.Conflict == nil || resetConflict.Conflict.Kind != storage.ConflictRange || reset.Effects != 0 {
		t.Fatalf("reset bypassed ranges: %+v %v", reset, resetErr)
	}
	for count := 2; count > 0; count-- {
		if _, err := writer.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("bad")}, nativeTestAction(t, other)); err == nil {
			t.Fatalf("write bypassed %d shared acquisitions", count)
		}
		receipt, err = file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 7, Scope: scope, ExpectedRevision: receipt.RangeRevision, Ranges: held[:count-1]}, nativeTestAction(t, s))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("new")}, nativeTestAction(t, other)); err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 8})
	if err != nil || string(read.Data) != "newginal" {
		t.Fatalf("write after ranges release=%q %v", read.Data, err)
	}
	if _, err := writer.Close(t.Context(), nativeTestAction(t, other)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: storage.ReadContent, Excludes: storage.WriteContent}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Retain(t.Context(), storage.RetainRequest{NodeID: o.Attr.ID, Claim: storage.AccessClaim{Uses: storage.WriteContent}}, nativeTestAction(t, other)); err == nil {
		t.Fatal("write claim bypassed exclusion")
	}
	if _, err := other.Retain(t.Context(), storage.RetainRequest{NodeID: o.Attr.ID}, nativeTestAction(t, other)); err != nil {
		t.Fatalf("metadata-only retention conflicted: %v", err)
	}
}
func TestNativeAuthorityKeepsExactNamesAndOpaqueMetadata(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	for _, name := range []string{"Case", "case", "a:b"} {
		nativeTestCreate(t, s, root, name)
	}
	page, err := root.ListAt(t.Context(), storage.DirectoryPageRequest{MaxEntries: 64, MaxBytes: storage.MaxDirectoryPageBytes})
	if err != nil || len(page.Entries) != 3 {
		t.Fatalf("exact names=%+v %v", page, err)
	}
	for i, name := range []string{"Case", "a:b", "case"} {
		if string(page.Entries[i].Name) != name {
			t.Fatalf("name=%q want%q", page.Entries[i].Name, name)
		}
	}
	meta := storage.Metadata{{Key: "application", Version: 93, Data: []byte{0xff, 0, 1}}}
	target := nativeTestTarget(t, root, "Case")
	r, err := s.RetainAt(t.Context(), storage.RetainAtRequest{Target: target}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	f := nativeTestReference(t, s, r)
	updated, err := f.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: r.Observation.Attr.MetadataRevision, Metadata: &meta}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Observation.Attr.Metadata[0].Version != 93 || string(updated.Observation.Attr.Metadata[0].Data) != string(meta[0].Data) {
		t.Fatal("opaque metadata was interpreted or changed")
	}
}

func TestNativeAuthorityReplaysReceiptsWithoutResurrectingReferences(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	target := nativeTestTarget(t, root, "receipt")
	retainID := nativeTestAction(t, s)
	request := storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}
	created, err := s.CreateAndRetainAt(t.Context(), request, retainID)
	if err != nil {
		t.Fatal(err)
	}
	f := nativeTestReference(t, s, created)
	writeID := nativeTestAction(t, s)
	write := storage.FileWriteRequest{Data: []byte("first")}
	first, err := f.WriteAt(t.Context(), write, writeID)
	if err != nil {
		t.Fatal(err)
	}
	creation := *first.Observation.Attr.CreationTime
	*first.Observation.Attr.CreationTime = creation.Add(time.Hour)
	queried, queryErr := s.QueryAction(t.Context(), writeID)
	if queryErr != nil || !queried.Observation.Attr.CreationTime.Equal(creation) {
		t.Fatalf("caller changed stored receipt time: %+v %v", queried, queryErr)
	}
	replay, err := f.WriteAt(t.Context(), write, writeID)
	if err != nil || replay.Observation.Attr.MetadataRevision != first.Observation.Attr.MetadataRevision {
		t.Fatalf("write replay=%+v %v", replay, err)
	}
	if _, err := f.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("other")}, writeID); !storage.IsFileCallNotAdmitted(err) {
		t.Fatal("same action accepted different input")
	}
	prior, err := s.QueryAction(t.Context(), writeID)
	if err != nil || prior.State != storage.FileActionCompleted || prior.Operation != storage.OpFileWrite || prior.Effects&storage.EffectContentChanged == 0 {
		t.Fatalf("refused invocation erased the earlier action outcome: %+v %v", prior, err)
	}
	unknown, err := s.QueryAction(t.Context(), nativeTestAction(t, s))
	if unknown.State != storage.FileActionUnknown || unknown.Operation != "" || unknown.Effects != 0 || err == nil {
		t.Fatalf("unknown action=%+v %v", unknown, err)
	}
	if _, err := f.Close(t.Context(), nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	replayed, err := s.CreateAndRetainAt(t.Context(), request, retainID)
	if err != nil || replayed.Reference != created.Reference {
		t.Fatalf("historical retain receipt=%+v %v", replayed, err)
	}
	if _, err := s.Reference(t.Context(), replayed.Reference); err == nil {
		t.Fatal("historical retain resurrected closed reference")
	}
}

func TestNativeAuthorityPreparedRemovalDrainsWithoutChangingTarget(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	first := nativeTestCreate(t, s, root, "remove")
	target := nativeTestTarget(t, root, "remove")
	retained, err := s.RetainAt(t.Context(), storage.RetainAtRequest{Target: target, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	second := nativeTestReference(t, s, retained)
	o, err := first.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	entry := o.Location.Ancestors[len(o.Location.Ancestors)-1]
	prepared, err := first.PrepareRemoval(t.Context(), storage.PrepareRemovalRequest{Entry: entry, Witness: *o.Location, ExpectedMetadataRevision: o.Attr.MetadataRevision, Condition: storage.RemovalFile}, nativeTestAction(t, s))
	if err != nil || !prepared.Removal.Prepared {
		t.Fatalf("prepare=%+v %v", prepared, err)
	}
	if _, err := first.Close(t.Context(), nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetainAt(t.Context(), storage.RetainAtRequest{Target: nativeTestTarget(t, root, "remove")}, nativeTestAction(t, s)); err == nil {
		t.Fatal("draining entry accepted new reference")
	}
	if _, err := second.Stat(t.Context(), storage.ObservationOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Close(t.Context(), nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Stat(t.Context(), "remove"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("last reference did not remove prepared entry: %v", err)
	}
}

func TestNativeAuthorityCancelsWaitingRangeActionWithoutDroppingHeldRanges(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "wait")
	o, err := file.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scope := storage.RangeScope{Enforced: true}
	snap, err := file.RangeSnapshot(t.Context(), 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snap.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 0, End: 1, Exclusive: true}}}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	other := nativeTestSession(t, a)
	r, err := other.Retain(t.Context(), storage.RetainRequest{NodeID: o.Attr.ID, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, other))
	if err != nil {
		t.Fatal(err)
	}
	waiting := nativeTestReference(t, other, r)
	snapshot, err := waiting.RangeSnapshot(t.Context(), 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	id := nativeTestAction(t, other)
	request := storage.RangeWaitRequest{Owner: 0, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 2, Start: 0, End: 1, Exclusive: true}}, DetectDeadlock: true}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type outcome struct {
		receipt storage.FileActionReceipt
		err     error
	}
	done := make(chan outcome, 1)
	joined := make(chan struct{})
	go func() {
		receipt, err := waiting.WaitRanges(ctx, request, id)
		done <- outcome{receipt, err}
		close(joined)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("range waiter survived cancellation")
		}
	})
	for {
		pending, err := other.QueryAction(ctx, id)
		if pending.State == storage.FileActionPending {
			break
		}
		if err != nil && !errors.Is(err, syscall.EIO) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("wait was not registered")
		case <-time.After(time.Millisecond):
		}
	}
	canceled, err := other.CancelAction(ctx, id)
	if canceled.State != storage.FileActionNotApplied || !errors.Is(err, syscall.EINTR) && !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%+v %v", canceled, err)
	}
	result := <-done
	if result.receipt.Action != id || result.receipt.Operation != storage.OpFileWaitRanges || result.receipt.State != storage.FileActionNotApplied || result.err == nil {
		t.Fatalf("wait result=%+v %v", result.receipt, result.err)
	}
	held, err := file.RangeSnapshot(t.Context(), 1, scope)
	if err != nil || len(held.Own) != 1 {
		t.Fatalf("cancel lost existing range: %+v %v", held, err)
	}
}
func TestNativeAuthorityExpiryRetiresClaimsBeforeNewAdmission(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "expiry")
	if _, err := file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: storage.ReadContent, Excludes: storage.WriteContent}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	o, err := file.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other := nativeTestSession(t, a)
	if _, err := other.Retain(t.Context(), storage.RetainRequest{NodeID: o.Attr.ID, Claim: storage.AccessClaim{Uses: storage.WriteContent}}, nativeTestAction(t, other)); err == nil {
		t.Fatal("live claim did not exclude writer")
	}
	a.mu.Lock()
	s.(*nativeSession).expires = time.Now().Add(-time.Second)
	a.mu.Unlock()
	if _, err := other.Retain(t.Context(), storage.RetainRequest{NodeID: o.Attr.ID, Claim: storage.AccessClaim{Uses: storage.WriteContent}}, nativeTestAction(t, other)); err != nil {
		t.Fatalf("expired claim blocked writer: %v", err)
	}
	if _, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 1}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expired reference read=%v", err)
	}
}

func TestNativeAuthoritySetKindValidatesAncestorWitness(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	parentReceipt, err := s.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: nativeTestTarget(t, root, "parent"), Initial: storage.NodeInitial{Kind: storage.NodeDirectory}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	parent := nativeTestReference(t, s, parentReceipt)
	file := nativeTestCreate(t, s, parent, "link")
	before, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Rename(t.Context(), storage.RenameRequest{Source: nativeTestTarget(t, root, "parent"), Destination: nativeTestTarget(t, root, "moved")}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	request := storage.SetKindRequest{Witness: before.Location, ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("sibling")}
	refused, err := file.SetKind(t.Context(), request, nativeTestAction(t, s))
	var conflict *storage.FileError
	if !errors.As(err, &conflict) || conflict.Conflict == nil || conflict.Conflict.Kind != storage.ConflictRevision || refused.Effects != 0 {
		t.Fatalf("stale ancestor witness=%+v %v", refused, err)
	}
	current, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || current.Attr.Kind != storage.NodeRegular || current.Attr.MetadataRevision != before.Attr.MetadataRevision || len(current.LinkTarget) != 0 {
		t.Fatalf("refused conversion changed node=%+v %v", current, err)
	}
	request.Witness = current.Location
	converted, err := file.SetKind(t.Context(), request, nativeTestAction(t, s))
	if err != nil || converted.Observation.Attr.Kind != storage.NodeSymlink || string(converted.Observation.LinkTarget) != "sibling" || converted.Observation.Attr.ID != before.Attr.ID {
		t.Fatalf("current witness conversion=%+v %v", converted, err)
	}
	rootState, err := root.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := file.SetKind(t.Context(), storage.SetKindRequest{Witness: rootState.Location, ExpectedRevision: converted.Observation.Attr.MetadataRevision, Kind: storage.NodeRegular}, nativeTestAction(t, s))
	if !errors.Is(err, syscall.EINVAL) || wrong.Effects != 0 {
		t.Fatalf("witness for another identity=%+v %v", wrong, err)
	}
}

func TestNativeAuthorityLookupAtPreservesExactAbsenceAndOptionalWitness(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "Exact")
	present, err := root.LookupAt(t.Context(), []byte("Exact"))
	if err != nil || !present.Found || present.EntryID == 0 || present.Attr.ID != file.NodeID() || present.Check([]byte("Exact")) != nil {
		t.Fatalf("exact lookup=%+v %v", present, err)
	}
	absent, err := root.LookupAt(t.Context(), []byte("exact"))
	if err != nil || absent.Found || absent.EntryID != 0 || absent.Check([]byte("exact")) != nil || absent.ParentID != present.ParentID || absent.DirectoryRevision != present.DirectoryRevision {
		t.Fatalf("exact absence=%+v %v", absent, err)
	}
	request := storage.RetainAtRequest{Target: storage.EntryTarget{Parent: root.Reference(), ParentID: present.ParentID, DirectoryRevision: present.DirectoryRevision, Name: present.Name, ExpectedEntryID: present.EntryID, ExpectedNodeID: present.Attr.ID}}
	retained, err := s.RetainAt(t.Context(), request, nativeTestAction(t, s))
	if err != nil || retained.Observation.Attr.ID != file.NodeID() {
		t.Fatalf("relative retain without witness=%+v %v", retained, err)
	}
	nativeTestCreate(t, s, root, "other")
	if _, err := s.RetainAt(t.Context(), request, nativeTestAction(t, s)); err == nil {
		t.Fatal("nil witness bypassed directory revision")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	failed, err := root.LookupAt(ctx, []byte("absent"))
	if err == nil || failed.ParentID != 0 {
		t.Fatalf("canceled lookup invented absence=%+v %v", failed, err)
	}
}

func TestNativeAuthorityReportsPreparedAndDrainConditionsIndependently(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "conditions")
	observation, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	entry := observation.Location.Ancestors[len(observation.Location.Ancestors)-1]
	prepared, err := file.PrepareRemoval(t.Context(), storage.PrepareRemovalRequest{Entry: entry, Witness: *observation.Location, ExpectedMetadataRevision: observation.Attr.MetadataRevision, Condition: storage.RemovalFile}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: observation.Attr.MetadataRevision, Kind: storage.NodeDirectory}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	observation, err = file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	entry = observation.Location.Ancestors[len(observation.Location.Ancestors)-1]
	drained, err := file.DrainEntry(t.Context(), storage.DrainEntryRequest{Entry: entry, Witness: *observation.Location, ExpectedMetadataRevision: observation.Attr.MetadataRevision, Condition: storage.RemovalIfEmpty}, nativeTestAction(t, s))
	if err != nil {
		t.Fatal(err)
	}
	status, err := file.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil || !status.Removal.Prepared || status.Removal.PreparedCondition != storage.RemovalFile || status.Removal.DrainCondition != storage.RemovalIfEmpty || status.Removal.State != storage.EntryDraining || status.Removal.Check() != nil {
		t.Fatalf("independent conditions=%+v %v", status.Removal, err)
	}
	canceled, err := file.CancelPrepared(t.Context(), prepared.Removal.IntentID, nativeTestAction(t, s))
	if err != nil || canceled.Removal.Prepared || canceled.Removal.PreparedCondition != 0 || canceled.Removal.DrainCondition != storage.RemovalIfEmpty || canceled.Removal.Check() != nil {
		t.Fatalf("cancel prepared=%+v %v", canceled, err)
	}
	active, err := file.CancelDrain(t.Context(), storage.CancelDrainRequest{EntryID: entry.EntryID, Generation: drained.Removal.Generation}, nativeTestAction(t, s))
	if err != nil || active.Removal.State != storage.EntryActive || active.Removal.Generation == 0 || active.Removal.DrainCondition != 0 || active.Removal.Check() != nil {
		t.Fatalf("cancel drain=%+v %v", active, err)
	}
}

func TestNativeAuthorityChecksAppendSizeAndTerminalCloseFacts(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "append")
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("a")}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	size := int64(1)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 1, Data: []byte("b"), ExpectedSize: &size}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	stale, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 1, Data: []byte("x"), ExpectedSize: &size}, nativeTestAction(t, s))
	if err == nil || stale.Effects != 0 {
		t.Fatalf("stale append applied=%+v %v", stale, err)
	}
	read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 3})
	if err != nil || string(read.Data) != "ab" {
		t.Fatalf("stale append changed bytes=%q %v", read.Data, err)
	}
	if _, err := file.Close(t.Context(), nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	id := nativeTestAction(t, s)
	before := len(s.(*nativeSession).actions)
	retired, err := file.Close(t.Context(), id)
	if err != nil || retired.State != storage.FileActionRetired || retired.Effects != 0 || len(s.(*nativeSession).actions) != before {
		t.Fatalf("retired close invented history=%+v %v", retired, err)
	}
	if _, err := file.Close(t.Context(), "invalid"); !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("terminal close skipped syntax check=%v", err)
	}
	if _, err := s.Close(t.Context(), nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	id, err = storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	before = len(s.(*nativeSession).actions)
	closed, err := s.Close(t.Context(), id)
	if err != nil || closed.State != storage.FileActionRetired || closed.Effects != 0 || len(s.(*nativeSession).actions) != before {
		t.Fatalf("closed session invented history=%+v %v", closed, err)
	}
}

func TestNativeAuthorityRenameSeparatesObservedAndOutputNames(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		t.Run(fmt.Sprintf("output occupied=%v", occupied), func(t *testing.T) {
			a := newNativeAuthority()
			s := nativeTestSession(t, a)
			root := nativeTestRoot(t, s)
			source := nativeTestCreate(t, s, root, "Source.txt")
			destination := nativeTestCreate(t, s, root, "Other.txt")
			for file, content := range map[storage.File]string{source: "source", destination: "destination"} {
				if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte(content)}, nativeTestAction(t, s)); err != nil {
					t.Fatal(err)
				}
			}
			var third storage.File
			if occupied {
				third = nativeTestCreate(t, s, root, "OTHER.TXT")
			}
			request := storage.RenameRequest{Source: nativeTestTarget(t, root, "Source.txt"), Destination: nativeTestTarget(t, root, "Other.txt"), NewName: []byte("OTHER.TXT")}
			position, err := a.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			result, err := source.Rename(t.Context(), request, nativeTestAction(t, s))
			if occupied {
				var conflict *storage.FileError
				if !errors.As(err, &conflict) || conflict.Conflict == nil || conflict.Conflict.Kind != storage.ConflictIdentity || result.Effects != 0 {
					t.Fatalf("third output occupant=%+v %v", result, err)
				}
				for name, file := range map[string]storage.File{"Source.txt": source, "Other.txt": destination, "OTHER.TXT": third} {
					lookup, err := root.LookupAt(t.Context(), []byte(name))
					if err != nil || !lookup.Found || lookup.Attr.ID != file.NodeID() || lookup.DirectoryRevision != request.Source.DirectoryRevision {
						t.Fatalf("failed rename changed %q: %+v %v", name, lookup, err)
					}
				}
				after, err := a.CommittedPosition(t.Context())
				if err != nil || after != position {
					t.Fatalf("failed rename published history: %d -> %d, %v", position, after, err)
				}
			} else {
				if err != nil || result.Effects&storage.EffectEntryMoved == 0 {
					t.Fatalf("rename replacement=%+v %v", result, err)
				}
				lookup, err := root.LookupAt(t.Context(), []byte("OTHER.TXT"))
				if err != nil || !lookup.Found || lookup.Attr.ID != source.NodeID() || lookup.EntryID != request.Source.ExpectedEntryID {
					t.Fatalf("output spelling or entry identity changed: %+v %v", lookup, err)
				}
				for _, name := range []string{"Source.txt", "Other.txt"} {
					lookup, err := root.LookupAt(t.Context(), []byte(name))
					if err != nil || lookup.Found {
						t.Fatalf("old slot %q survived: %+v %v", name, lookup, err)
					}
				}
			}
			for file, content := range map[storage.File]string{source: "source", destination: "destination"} {
				read, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 32})
				if err != nil || string(read.Data) != content {
					t.Fatalf("rename changed retained content=%q want%q error=%v", read.Data, content, err)
				}
			}
		})
	}
}

func TestNativeAuthoritySetKindScopesRangeOwnerAndPreservesAcquisitions(t *testing.T) {
	a := newNativeAuthority()
	s := nativeTestSession(t, a)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "owner-link")
	scope := storage.RangeScope{Enforced: true}
	owner := storage.RangeOwnerID(0)
	own := []storage.RangeAcquisition{{ID: 1, Start: 0, End: 3, Exclusive: true}, {ID: 2, Start: 10, End: 11, Exclusive: true}}
	snapshot, err := file.RangeSnapshot(t.Context(), owner, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: own}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	snapshot, err = file.RangeSnapshot(t.Context(), 7, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 7, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 3, Start: 5, End: 7, Exclusive: true}}}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	before, err := file.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []storage.SetKindRequest{{ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("name")}, {Owner: &owner, ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("target")}} {
		refused, err := file.SetKind(t.Context(), request, nativeTestAction(t, s))
		var conflict *storage.FileError
		if !errors.As(err, &conflict) || conflict.Conflict == nil || conflict.Conflict.Kind != storage.ConflictRange || refused.Effects != 0 {
			t.Fatalf("anonymous/other-owner range refusal=%+v %v", refused, err)
		}
	}
	if _, err := file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: storage.ReadContent}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	request := storage.SetKindRequest{Owner: &owner, ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("name")}
	if refused, err := file.SetKind(t.Context(), request, nativeTestAction(t, s)); !errors.Is(err, syscall.EBADF) || refused.Effects != 0 {
		t.Fatalf("conversion without declared WriteContent=%+v %v", refused, err)
	}
	if _, err := file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: storage.AllAccessUses}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	changed, err := file.SetKind(t.Context(), request, nativeTestAction(t, s))
	if err != nil || changed.Observation.Attr.Kind != storage.NodeSymlink || string(changed.Observation.LinkTarget) != "name" {
		t.Fatalf("own range conversion=%+v %v", changed, err)
	}
	snapshot, err = file.RangeSnapshot(t.Context(), owner, scope)
	if err != nil || len(snapshot.Own) != 2 || snapshot.Own[0] != own[0] || snapshot.Own[1] != own[1] {
		t.Fatalf("conversion changed current/future acquisitions=%+v %v", snapshot, err)
	}
	other, err := file.RangeSnapshot(t.Context(), 7, scope)
	if err != nil || len(other.Own) != 1 || other.Own[0].ID != 3 {
		t.Fatalf("conversion changed other owner=%+v %v", other, err)
	}
}

func TestNativeAuthoritySetKindKeepsDirectoryGeneration(t *testing.T) {
	for _, churn := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory churn=%v", churn), func(t *testing.T) {
			a := newNativeAuthority()
			s := nativeTestSession(t, a)
			root := nativeTestRoot(t, s)
			receipt, err := s.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: nativeTestTarget(t, root, "directory"), Initial: storage.NodeInitial{Kind: storage.NodeDirectory}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nativeTestAction(t, s))
			if err != nil {
				t.Fatal(err)
			}
			directory := nativeTestReference(t, s, receipt)
			if churn {
				child := nativeTestCreate(t, s, directory, "child")
				o, err := child.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
				if err != nil {
					t.Fatal(err)
				}
				entry := o.Location.Ancestors[len(o.Location.Ancestors)-1]
				if _, err := child.PrepareRemoval(t.Context(), storage.PrepareRemovalRequest{Entry: entry, Witness: *o.Location, ExpectedMetadataRevision: o.Attr.MetadataRevision, Condition: storage.RemovalFile}, nativeTestAction(t, s)); err != nil {
					t.Fatal(err)
				}
				if _, err := child.Close(t.Context(), nativeTestAction(t, s)); err != nil {
					t.Fatal(err)
				}
			}
			for _, away := range []bool{false, true} {
				before, err := directory.Stat(t.Context(), storage.ObservationOptions{})
				if err != nil {
					t.Fatal(err)
				}
				target := nativeTestTarget(t, directory, "new-entry")
				target.Witness = nil
				cursor := storage.DirectoryPageRequest{Revision: before.Attr.DirectoryRevision, Cursor: storage.DirectoryCursor{ParentID: directory.NodeID(), Revision: before.Attr.DirectoryRevision, After: []byte("old-cursor")}, MaxEntries: 1, MaxBytes: storage.MaxDirectoryPageBytes}
				expected := before.Attr.MetadataRevision
				if away {
					changed, err := directory.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: expected, Kind: storage.NodeRegular}, nativeTestAction(t, s))
					if err != nil || changed.Observation.Attr.DirectoryRevision != 0 || uint64(changed.Observation.Attr.MetadataRevision) <= uint64(before.Attr.DirectoryRevision) {
						t.Fatalf("conversion lost directory generation: %+v %v", changed, err)
					}
					expected = changed.Observation.Attr.MetadataRevision
				}
				changed, err := directory.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: expected, Kind: storage.NodeDirectory}, nativeTestAction(t, s))
				if err != nil || changed.Observation.Attr.DirectoryRevision <= before.Attr.DirectoryRevision || uint64(changed.Observation.Attr.MetadataRevision) != uint64(changed.Observation.Attr.DirectoryRevision) {
					t.Fatalf("directory generation reused: %+v %v", changed, err)
				}
				if page, err := directory.ListAt(t.Context(), cursor); err == nil || len(page.Entries) != 0 {
					t.Fatalf("old cursor accepted: %+v %v", page, err)
				}
				refused, err := s.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}}, nativeTestAction(t, s))
				var conflict *storage.FileError
				if !errors.As(err, &conflict) || conflict.Conflict == nil || conflict.Conflict.Kind != storage.ConflictRevision || refused.Effects != 0 {
					t.Fatalf("old entry target accepted: %+v %v", refused, err)
				}
				lookup, err := directory.LookupAt(t.Context(), []byte("new-entry"))
				if err != nil || lookup.Found {
					t.Fatalf("old target created a name: %+v %v", lookup, err)
				}
			}
		})
	}
}

func TestNativeAuthoritySetKindRefusesRevisionOverflowBeforeEffects(t *testing.T) {
	for _, directoryCeiling := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory ceiling=%v", directoryCeiling), func(t *testing.T) {
			a := newNativeAuthority()
			s := nativeTestSession(t, a)
			root := nativeTestRoot(t, s)
			file := nativeTestCreate(t, s, root, "ceiling")
			a.mu.Lock()
			n := a.nodes[file.NodeID()]
			if directoryCeiling {
				n.attr.Kind = storage.NodeDirectory
				n.attr.DirectoryRevision = math.MaxInt64
			} else {
				n.attr.MetadataRevision = math.MaxInt64
			}
			a.mu.Unlock()
			before, err := file.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			position, err := a.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			result, err := file.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeRegular}, nativeTestAction(t, s))
			if !errors.Is(err, syscall.EOVERFLOW) || result.Effects != 0 {
				t.Fatalf("overflow published effects: %+v %v", result, err)
			}
			after, err := file.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil || after.Attr.Kind != before.Attr.Kind || after.Attr.MetadataRevision != before.Attr.MetadataRevision || after.Attr.DirectoryRevision != before.Attr.DirectoryRevision {
				t.Fatalf("overflow changed node: before=%+v after=%+v error=%v", before, after, err)
			}
			tail, err := a.CommittedPosition(t.Context())
			if err != nil || tail != position {
				t.Fatalf("overflow appended history: %d -> %d %v", position, tail, err)
			}
		})
	}
}
