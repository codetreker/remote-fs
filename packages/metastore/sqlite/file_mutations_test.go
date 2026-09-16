package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func mutationReference(t *testing.T, session *fileSession, parent *fileReference, name string, kind storage.NodeKind, claim storage.AccessClaim) *fileReference {
	t.Helper()
	r, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: nativeTarget(t, parent, []byte(name)), Initial: storage.NodeInitial{Kind: kind, Metadata: storage.Metadata{{Key: "test.tag", Version: 1, Data: []byte(name)}}}, Claim: claim}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	ref, live, err := session.Reference(t.Context(), r.Reference)
	if err != nil || !live {
		t.Fatalf("created reference=%v,%v", live, err)
	}
	return ref.(*fileReference)
}

func TestNativeRenameSeparatesExpectedSlotFromOutputSpelling(t *testing.T) {
	for _, scenario := range []string{"same entry", "replace different spelling", "output stays at source", "ordinary output"} {
		t.Run(scenario, func(t *testing.T) {
			s, session := newFileAuthority(t)
			root := nativeRootReference(t, session)
			source := mutationReference(t, session, root, "source", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
			var displaced *fileReference
			canonical, output := "source", []byte("SOURCE")
			if scenario != "same entry" {
				displaced = mutationReference(t, session, root, "target", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
				canonical, output = "target", []byte("TARGET")
				if scenario == "output stays at source" {
					output = []byte("source")
				}
				if scenario == "ordinary output" {
					output = nil
				}
			}
			before, err := source.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			request := storage.RenameRequest{Source: nativeTarget(t, root, []byte("source")), Destination: nativeTarget(t, root, []byte(canonical)), NewName: output}
			action := fileActionID(t, session)
			result, err := source.Rename(t.Context(), request, action)
			if err != nil || result.State != storage.FileActionCompleted {
				t.Fatalf("rename=%+v,%v", result, err)
			}
			name := string(output)
			if output == nil {
				name = canonical
			}
			current, err := root.LookupAt(t.Context(), []byte(name))
			if err != nil || !current.Found || current.EntryID != source.entryID || current.Attr.ID != uint64(source.id) {
				t.Fatalf("output slot=%+v,%v", current, err)
			}
			if !reflect.DeepEqual(current.Attr.Metadata, before.Attr.Metadata) {
				t.Fatal("rename changed source opaque metadata")
			}
			if scenario == "output stays at source" {
				if result.Effects != storage.EffectEntryDetached || current.Attr.MetadataRevision != before.Attr.MetadataRevision {
					t.Fatalf("stationary source reported a move: %+v", result)
				}
			} else if result.Effects&storage.EffectEntryMoved == 0 {
				t.Fatalf("move effect missing: %+v", result)
			}
			if displaced != nil {
				observed, err := displaced.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
				if err != nil || observed.Location == nil || observed.Location.State != storage.LocationDetached {
					t.Fatalf("displaced reference was replaced: %+v,%v", observed, err)
				}
				if result.Effects&storage.EffectEntryDetached == 0 {
					t.Fatal("displacement effect missing")
				}
			}
			replay, err := session.QueryAction(t.Context(), action)
			if err != nil || replay.Effects != result.Effects || replay.Observation.Attr.ID != uint64(source.id) {
				t.Fatalf("rename receipt=%+v,%v", replay, err)
			}
			children, err := s.List(t.Context(), "")
			if err != nil || len(children) != 1 || string(children[0].Name) != name {
				t.Fatalf("unexpected namespace after rename: %+v,%v", children, err)
			}
		})
	}
}

func TestNativeRenameRejectsAnUnrelatedOutputOccupantWithoutEffects(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	refs := make(map[string]*fileReference)
	for _, name := range []string{"source", "target", "TARGET"} {
		refs[name] = mutationReference(t, session, root, name, storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	}
	before, err := root.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	request := storage.RenameRequest{Source: nativeTarget(t, root, []byte("source")), Destination: nativeTarget(t, root, []byte("target")), NewName: []byte("TARGET")}
	result, err := refs["source"].Rename(t.Context(), request, fileActionID(t, session))
	if !errors.Is(err, syscall.EEXIST) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("third occupant=%+v,%v", result, err)
	}
	for name, ref := range refs {
		lookup, err := root.LookupAt(t.Context(), []byte(name))
		if err != nil || !lookup.Found || lookup.EntryID != ref.entryID || lookup.Attr.ID != uint64(ref.id) {
			t.Fatalf("refusal changed %s: %+v,%v", name, lookup, err)
		}
	}
	after, err := root.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil || !reflect.DeepEqual(after.Attr.Clone(), before.Attr.Clone()) {
		t.Fatalf("refusal changed parent: %+v,%v", after, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("refusal changed usage=%d,%v", used, err)
	}
}

func TestNativeSetKindChecksContentUseAndScopedRangeOwner(t *testing.T) {
	for _, scenario := range []string{"metadata only", "anonymous", "own zero owner", "other session same owner"} {
		t.Run(scenario, func(t *testing.T) {
			s, session := newFileAuthority(t)
			root := nativeRootReference(t, session)
			claim := storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}
			if scenario == "metadata only" {
				claim = storage.AccessClaim{}
			}
			file := mutationReference(t, session, root, "file", storage.NodeRegular, claim)
			if scenario != "metadata only" {
				holder, holderSession := file, session
				if scenario == "other session same owner" {
					inner, _, err := s.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
					if err != nil {
						t.Fatal(err)
					}
					holderSession = inner.(*fileSession)
					t.Cleanup(func() {
						if err := holderSession.Dispose(context.Background()); err != nil {
							t.Error(err)
						}
					})
					holder = retainRangeFile(t, holderSession, uint64(file.id))
				}
				snapshot, err := holder.RangeSnapshot(t.Context(), 0, storage.RangeScope{Enforced: true})
				if err != nil {
					t.Fatal(err)
				}
				_, err = holder.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 0, Scope: storage.RangeScope{Enforced: true}, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 0, End: 63, Exclusive: true}}}, fileActionID(t, holderSession))
				if err != nil {
					t.Fatal(err)
				}
				if holder != file {
					if _, err := holder.Close(t.Context(), fileActionID(t, holderSession)); err != nil {
						t.Fatal(err)
					}
					snapshot, err = file.RangeSnapshot(t.Context(), 0, storage.RangeScope{Enforced: true})
					if err != nil || len(snapshot.Other) != 1 {
						t.Fatalf("foreign owner did not retain its acquisition: %+v,%v", snapshot, err)
					}
				}
			}
			before, err := file.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			request := storage.SetKindRequest{ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Metadata: before.Attr.Metadata}
			owner := storage.RangeOwnerID(0)
			if scenario == "own zero owner" || scenario == "other session same owner" {
				request.Owner = &owner
			}
			result, err := file.SetKind(t.Context(), request, fileActionID(t, session))
			if scenario == "own zero owner" {
				if err != nil || result.State != storage.FileActionCompleted || result.Observation.Attr.Kind != storage.NodeSymlink {
					t.Fatalf("own future lock refused kind change: %+v,%v", result, err)
				}
				snapshot, err := file.RangeSnapshot(t.Context(), 0, storage.RangeScope{Enforced: true})
				if err != nil || len(snapshot.Own) != 1 || snapshot.Own[0].ID != 1 {
					t.Fatalf("kind change lost range identity: %+v,%v", snapshot, err)
				}
				if used, err := s.Usage(t.Context()); err != nil || used != 6 {
					t.Fatalf("target accounting=%d,%v", used, err)
				}
				return
			}
			expected := syscall.EAGAIN
			if scenario == "metadata only" {
				expected = syscall.EBADF
			}
			if !errors.Is(err, expected) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
				t.Fatalf("kind access rejection=%+v,%v", result, err)
			}
			if scenario != "metadata only" && (result.Conflict == nil || result.Conflict.Kind != storage.ConflictRange) {
				t.Fatalf("range conflict missing: %+v", result)
			}
			after, err := file.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil || !reflect.DeepEqual(after.Attr.Clone(), before.Attr.Clone()) {
				t.Fatalf("rejected kind change altered facts: %+v,%v", after, err)
			}
		})
	}
}

func TestNativeSetKindPreservesDirectoryRevisionAcrossKindTransitions(t *testing.T) {
	for _, away := range []bool{false, true} {
		t.Run(map[bool]string{false: "same kind", true: "away and back"}[away], func(t *testing.T) {
			s, session := newFileAuthority(t)
			root := nativeRootReference(t, session)
			dir := mutationReference(t, session, root, "dir", storage.NodeDirectory, storage.AccessClaim{Uses: storage.AllAccessUses})
			initial, err := dir.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			oldTarget := nativeTarget(t, dir, []byte("fresh"))
			for _, name := range []string{"dir/a", "dir/b"} {
				if err := s.Create(t.Context(), name); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Rename(t.Context(), "dir/a", "dir/b"); err != nil {
				t.Fatal(err)
			}
			if err := s.Remove(t.Context(), "dir/b"); err != nil {
				t.Fatal(err)
			}
			before, err := dir.Stat(t.Context(), storage.ObservationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if uint64(before.Attr.DirectoryRevision) <= uint64(before.Attr.MetadataRevision) {
				t.Fatalf("fixture did not establish a directory counter floor: %+v", before)
			}
			request := storage.SetKindRequest{ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeDirectory, Metadata: before.Attr.Metadata}
			if away {
				request.Kind = storage.NodeRegular
			}
			result, err := dir.SetKind(t.Context(), request, fileActionID(t, session))
			if err != nil {
				t.Fatal(err)
			}
			expected := storage.NodeMetadataRevision(uint64(before.Attr.DirectoryRevision) + 1)
			if result.Observation.Attr.MetadataRevision != expected {
				t.Fatalf("kind transition lost revision floor: %+v", result)
			}
			if away {
				if result.Observation.Attr.DirectoryRevision != 0 {
					t.Fatal("non-directory exposes a directory revision")
				}
				request.ExpectedRevision, request.Kind = result.Observation.Attr.MetadataRevision, storage.NodeDirectory
				result, err = dir.SetKind(t.Context(), request, fileActionID(t, session))
				if err != nil {
					t.Fatal(err)
				}
				expected++
			}
			if uint64(result.Observation.Attr.DirectoryRevision) != uint64(expected) {
				t.Fatalf("directory revision repeated: %+v", result)
			}
			for _, revision := range []storage.DirectoryRevision{initial.Attr.DirectoryRevision, before.Attr.DirectoryRevision} {
				_, err := dir.ListAt(t.Context(), storage.DirectoryPageRequest{Revision: revision, Cursor: storage.DirectoryCursor{ParentID: uint64(dir.id), Revision: revision, After: []byte("old")}, MaxEntries: 1, MaxBytes: storage.MaxDirectoryPageBytes})
				if !errors.Is(err, syscall.EAGAIN) {
					t.Fatalf("stale directory cursor accepted revision %d: %v", revision, err)
				}
			}
			rejected, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: oldTarget, Initial: storage.NodeInitial{Kind: storage.NodeRegular}}, fileActionID(t, session))
			if !errors.Is(err, syscall.EAGAIN) || rejected.Effects != 0 {
				t.Fatalf("stale relative target accepted: %+v,%v", rejected, err)
			}
			if _, err := s.Stat(t.Context(), "dir/fresh"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("stale target created child: %v", err)
			}
		})
	}
}

func TestNativeSetKindRejectsExhaustedDirectoryCounterBeforeEffects(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	dir := mutationReference(t, session, root, "dir", storage.NodeDirectory, storage.AccessClaim{Uses: storage.AllAccessUses})
	if err := s.mutate(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE nodes SET directory_revision=? WHERE id=?`, int64(math.MaxInt64), dir.id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before, err := dir.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dir.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeRegular, Metadata: before.Attr.Metadata}, fileActionID(t, session))
	if !errors.Is(err, syscall.EOVERFLOW) || result.State != storage.FileActionNotApplied || result.Effects != 0 {
		t.Fatalf("counter exhaustion=%+v,%v", result, err)
	}
	after, err := dir.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil || !reflect.DeepEqual(after.Attr.Clone(), before.Attr.Clone()) {
		t.Fatalf("overflow altered node facts: %+v,%v", after, err)
	}
}

func removalRequest(t *testing.T, file *fileReference, condition storage.RemovalCondition) storage.PrepareRemovalRequest {
	t.Helper()
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	location := observed.Location.Clone()
	return storage.PrepareRemovalRequest{ExpectedMetadataRevision: observed.Attr.MetadataRevision, Entry: location.Ancestors[len(location.Ancestors)-1], Witness: location, Condition: condition}
}

func TestNativePreparedIntentAndDrainCancellationKeepIndependentOwnership(t *testing.T) {
	_, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	file := mutationReference(t, session, root, "file", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	foreign := mutationReference(t, session, root, "foreign", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	r, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: file.NodeID(), Claim: storage.AccessClaim{Uses: storage.RemoveEntry}}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	shared, _, err := session.Reference(t.Context(), r.Reference)
	if err != nil {
		t.Fatal(err)
	}
	observer := shared.(*fileReference)
	request := removalRequest(t, file, storage.RemovalFile)
	prepared, err := file.PrepareRemoval(t.Context(), request, fileActionID(t, session))
	if err != nil || !prepared.Removal.Prepared || prepared.Removal.IntentID == 0 || prepared.Removal.State != storage.EntryActive {
		t.Fatalf("prepare=%+v,%v", prepared, err)
	}
	token := prepared.Removal.IntentID
	for _, attempt := range []struct {
		file  *fileReference
		token storage.RemovalIntentID
	}{{file, token + 1}, {observer, token}, {foreign, token}} {
		refused, err := attempt.file.CancelPrepared(t.Context(), attempt.token, fileActionID(t, session))
		if !errors.Is(err, syscall.ESTALE) || refused.State != storage.FileActionNotApplied || refused.Effects != 0 {
			t.Fatalf("foreign prepared cancellation=%+v,%v", refused, err)
		}
	}
	drain := storage.DrainEntryRequest{ExpectedMetadataRevision: request.ExpectedMetadataRevision, Entry: request.Entry, Witness: request.Witness, Condition: request.Condition}
	drained, err := file.DrainEntry(t.Context(), drain, fileActionID(t, session))
	if err != nil || drained.Removal.State != storage.EntryDraining || !drained.Removal.Prepared || drained.Removal.IntentID != token || drained.Removal.Generation == 0 {
		t.Fatalf("drain=%+v,%v", drained, err)
	}
	generation := drained.Removal.Generation
	refused, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: file.NodeID()}, fileActionID(t, session))
	if !errors.Is(err, syscall.EBUSY) || refused.State != storage.FileActionNotApplied || refused.Reference != 0 {
		t.Fatalf("drain admitted new reference=%+v,%v", refused, err)
	}
	cancelled, err := file.CancelPrepared(t.Context(), token, fileActionID(t, session))
	if err != nil || cancelled.Removal.Prepared || cancelled.Removal.State != storage.EntryDraining || cancelled.Removal.Generation != generation {
		t.Fatalf("prepared cancellation cleared drain=%+v,%v", cancelled, err)
	}
	if _, err := file.CancelPrepared(t.Context(), 0, fileActionID(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero intent token=%v", err)
	}
	wrong := storage.CancelDrainRequest{EntryID: file.entryID, Generation: generation + 1}
	if r, err := observer.CancelDrain(t.Context(), wrong, fileActionID(t, session)); !errors.Is(err, syscall.EAGAIN) || r.Effects != 0 {
		t.Fatalf("stale generation=%+v,%v", r, err)
	}
	actual := storage.CancelDrainRequest{EntryID: file.entryID, Generation: generation}
	if r, err := foreign.CancelDrain(t.Context(), actual, fileActionID(t, session)); !errors.Is(err, syscall.ESTALE) || r.Effects != 0 {
		t.Fatalf("foreign entry cancellation=%+v,%v", r, err)
	}
	cleared, err := observer.CancelDrain(t.Context(), actual, fileActionID(t, session))
	if err != nil || cleared.Removal.State != storage.EntryActive || cleared.Removal.Prepared || cleared.Removal.Generation != generation {
		t.Fatalf("shared authorized cancellation=%+v,%v", cleared, err)
	}
	next, err := file.DrainEntry(t.Context(), drain, fileActionID(t, session))
	if err != nil || next.Removal.Generation <= generation {
		t.Fatalf("new drain reused generation=%+v,%v", next, err)
	}
	if r, err := file.CancelDrain(t.Context(), actual, fileActionID(t, session)); !errors.Is(err, syscall.EAGAIN) || r.Effects != 0 {
		t.Fatalf("old cancel cleared new drain=%+v,%v", r, err)
	}
	if _, err := file.CancelDrain(t.Context(), storage.CancelDrainRequest{EntryID: file.entryID, Generation: next.Removal.Generation}, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.CancelDrain(t.Context(), storage.CancelDrainRequest{}, fileActionID(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed drain cancellation=%v", err)
	}
	if _, err := file.DrainEntry(t.Context(), storage.DrainEntryRequest{}, fileActionID(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed drain=%v", err)
	}
}

func TestNativeDirectoryDrainRejectsNonemptyAndBlocksOnlyAfterActivation(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	dir := mutationReference(t, session, root, "dir", storage.NodeDirectory, storage.AccessClaim{Uses: storage.AllAccessUses})
	if err := s.Create(t.Context(), "dir/child"); err != nil {
		t.Fatal(err)
	}
	request := removalRequest(t, dir, storage.RemovalIfEmpty)
	drain := storage.DrainEntryRequest{ExpectedMetadataRevision: request.ExpectedMetadataRevision, Entry: request.Entry, Witness: request.Witness, Condition: request.Condition}
	rejected, err := dir.DrainEntry(t.Context(), drain, fileActionID(t, session))
	if !errors.Is(err, syscall.ENOTEMPTY) || rejected.State != storage.FileActionNotApplied || rejected.Effects != 0 {
		t.Fatalf("nonempty drain=%+v,%v", rejected, err)
	}
	if err := s.Remove(t.Context(), "dir/child"); err != nil {
		t.Fatal(err)
	}
	request = removalRequest(t, dir, storage.RemovalIfEmpty)
	prepared, err := dir.PrepareRemoval(t.Context(), request, fileActionID(t, session))
	if err != nil || !prepared.Removal.Prepared {
		t.Fatalf("directory prepare=%+v,%v", prepared, err)
	}
	if err := s.Create(t.Context(), "dir/allowed"); err != nil {
		t.Fatalf("prepared intent blocked insertion: %v", err)
	}
	if _, err := dir.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	named, err := s.Stat(t.Context(), "dir")
	if err != nil || named.ID != dir.id {
		t.Fatalf("false IfEmpty removed directory=%+v,%v", named, err)
	}
	empty := mutationReference(t, session, root, "empty", storage.NodeDirectory, storage.AccessClaim{Uses: storage.AllAccessUses})
	request = removalRequest(t, empty, storage.RemovalIfEmpty)
	drain = storage.DrainEntryRequest{ExpectedMetadataRevision: request.ExpectedMetadataRevision, Entry: request.Entry, Witness: request.Witness, Condition: request.Condition}
	active, err := empty.DrainEntry(t.Context(), drain, fileActionID(t, session))
	if err != nil || active.Removal.State != storage.EntryDraining {
		t.Fatalf("empty drain=%+v,%v", active, err)
	}
	create := storage.CreateAndRetainRequest{Target: nativeTarget(t, empty, []byte("blocked")), Initial: storage.NodeInitial{Kind: storage.NodeRegular}}
	refused, err := session.CreateAndRetainAt(t.Context(), create, fileActionID(t, session))
	if err == nil || refused.State != storage.FileActionNotApplied || refused.Effects != 0 || refused.Reference != 0 {
		t.Fatalf("draining parent admitted child=%+v,%v", refused, err)
	}
	lookup, err := empty.LookupAt(t.Context(), []byte("blocked"))
	if err != nil || lookup.Found {
		t.Fatalf("rejected child exists=%+v,%v", lookup, err)
	}
	if _, err := empty.CancelDrain(t.Context(), storage.CancelDrainRequest{EntryID: empty.entryID, Generation: active.Removal.Generation}, fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.CreateAndRetainAt(t.Context(), create, fileActionID(t, session)); err != nil {
		t.Fatalf("cancelled drain still blocks child: %v", err)
	}
}
