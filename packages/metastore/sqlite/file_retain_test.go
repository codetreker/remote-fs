package sqlite

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"

	"github.com/codetreker/remote-fs/packages/storage"
)

func nativeRootReference(t *testing.T, session *fileSession) *fileReference {
	t.Helper()
	result, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(session.store.root)}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	ref, live, err := session.Reference(t.Context(), result.Reference)
	if err != nil || !live {
		t.Fatalf("root reference=%v,%v", live, err)
	}
	return ref.(*fileReference)
}

func nativeTarget(t *testing.T, parent *fileReference, name []byte) storage.EntryTarget {
	t.Helper()
	lookup, err := parent.LookupAt(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	return storage.EntryTarget{Parent: parent.reference, ParentID: uint64(parent.id), Name: bytes.Clone(name), DirectoryRevision: lookup.DirectoryRevision, ExpectedEntryID: lookup.EntryID, ExpectedNodeID: lookup.Attr.ID, ExpectedMetadataRevision: lookup.Attr.MetadataRevision}
}

func TestNativeCreateRetainsOpaqueFactsAndExactNames(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	for _, name := range [][]byte{[]byte("CON"), {0xff}, []byte("ReadMe"), []byte("readme")} {
		metadata := storage.Metadata{{Key: "application", Version: 99, Data: []byte{0xff, 0, 1}}}
		id := fileActionID(t, session)
		request := storage.CreateAndRetainRequest{Target: nativeTarget(t, root, name), Initial: storage.NodeInitial{Kind: storage.NodeRegular, Metadata: metadata}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}
		result, err := session.CreateAndRetainAt(t.Context(), request, id)
		if err != nil {
			t.Fatal(err)
		}
		if result.State != storage.FileActionCompleted || result.Effects&(storage.EffectCreated|storage.EffectRetained) != (storage.EffectCreated|storage.EffectRetained) {
			t.Fatalf("create=%+v", result)
		}
		queried, err := session.QueryAction(t.Context(), id)
		if err != nil || queried.Reference != result.Reference || queried.Observation.Attr.ID != result.Observation.Attr.ID {
			t.Fatalf("query=%+v,%v", queried, err)
		}
		metadata[0].Data[0] = 0
		stored, err := s.Stat(t.Context(), string(name))
		if err != nil || stored.Kind != storage.NodeRegular || stored.MetadataRevision != 1 || stored.DirectoryRevision != 0 || !bytes.Equal(stored.Metadata[0].Data, []byte{0xff, 0, 1}) {
			t.Fatalf("stored=%+v,%v", stored, err)
		}
	}
}

func TestNativeRetainClaimConflictHasNoReferenceEffect(t *testing.T) {
	s, first := newFileAuthority(t)
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(node.ID), Claim: storage.AccessClaim{Uses: storage.ReadContent, Excludes: storage.RemoveEntry}}, fileActionID(t, first)); err != nil {
		t.Fatal(err)
	}
	native, _, err := s.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	second := native.(*fileSession)
	t.Cleanup(func() {
		if err := second.Dispose(context.Background()); err != nil {
			t.Error(err)
		}
	})
	before := s.fileDomain.files
	id := fileActionID(t, second)
	r, err := second.Retain(t.Context(), storage.RetainRequest{NodeID: uint64(node.ID), Claim: storage.AccessClaim{Uses: storage.RemoveEntry}}, id)
	if !errors.Is(err, syscall.EACCES) || r.State != storage.FileActionNotApplied || r.Conflict == nil || r.Conflict.Kind != storage.ConflictClaim || r.Reference != 0 || s.fileDomain.files != before {
		t.Fatalf("claim refusal=%+v,%v files=%d", r, err, s.fileDomain.files)
	}
	q, err := second.QueryAction(t.Context(), id)
	if !errors.Is(err, syscall.EACCES) || q.Conflict == nil || q.Conflict.Kind != storage.ConflictClaim {
		t.Fatalf("claim receipt=%+v,%v", q, err)
	}
}

func TestNativeKindConversionRejectsChangedAncestorWitness(t *testing.T) {
	s, session := newFileAuthority(t)
	if err := s.Mkdir(t.Context(), "parent"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "parent/file"); err != nil {
		t.Fatal(err)
	}
	node, err := s.Stat(t.Context(), "parent/file")
	if err != nil {
		t.Fatal(err)
	}
	f := retainRangeFile(t, session, uint64(node.ID))
	observed, err := f.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "parent", "moved"); err != nil {
		t.Fatal(err)
	}
	r, err := f.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: observed.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("../target"), Witness: observed.Location}, fileActionID(t, session))
	if !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("stale witness=%+v,%v", r, err)
	}
	fresh, err := f.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Attr.Kind != storage.NodeRegular || fresh.Attr.MetadataRevision != observed.Attr.MetadataRevision {
		t.Fatalf("refused kind conversion changed node: %+v", fresh)
	}
	r, err = f.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: fresh.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("../target"), Witness: fresh.Location}, fileActionID(t, session))
	if err != nil || r.Observation.Attr.Kind != storage.NodeSymlink {
		t.Fatalf("fresh conversion=%+v,%v", r, err)
	}
}

func TestNativeRetainAtRequiresTheExactObservedEntryAndReplaysOnePin(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	original := mutationReference(t, session, root, "file", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	target := nativeTarget(t, root, []byte("file"))
	before := s.fileDomain.files
	action := fileActionID(t, session)
	request := storage.RetainAtRequest{Target: target, Claim: storage.AccessClaim{Uses: storage.ReadContent}}
	result, err := session.RetainAt(t.Context(), request, action)
	if err != nil || result.State != storage.FileActionCompleted || result.Effects != storage.EffectRetained || result.Observation.Attr.ID != original.NodeID() || s.fileDomain.files != before+1 {
		t.Fatalf("retain-at=%+v,%v", result, err)
	}
	replay, err := session.RetainAt(t.Context(), request, action)
	if err != nil || replay.Reference != result.Reference || s.fileDomain.files != before+1 {
		t.Fatalf("retain replay acquired another pin=%+v,%v", replay, err)
	}
	if err := s.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	refused, err := session.RetainAt(t.Context(), request, fileActionID(t, session))
	if !errors.Is(err, syscall.EAGAIN) || refused.State != storage.FileActionNotApplied || refused.Reference != 0 || s.fileDomain.files != before+1 {
		t.Fatalf("stale parent accepted=%+v,%v", refused, err)
	}
	staleID := nativeTarget(t, root, []byte("file"))
	staleID.ExpectedEntryID = target.ExpectedEntryID
	staleID.ExpectedNodeID = target.ExpectedNodeID
	staleID.ExpectedMetadataRevision = 0
	refused, err = session.RetainAt(t.Context(), storage.RetainAtRequest{Target: staleID}, fileActionID(t, session))
	if !errors.Is(err, syscall.ESTALE) || refused.Reference != 0 {
		t.Fatalf("same name rebound old identity=%+v,%v", refused, err)
	}
	if _, err := session.RetainAt(t.Context(), storage.RetainAtRequest{}, fileActionID(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed retain-at=%v", err)
	}
}

func TestNativeReplaceAndRetainPublishesNewIdentityAndRetainsDisplacedObject(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	old := mutationReference(t, session, root, "file", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	key, err := s.Reserve(t.Context(), "file", 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(t.Context(), "file", metastore.Object{Key: key, Size: 3, ModTime: time.Unix(1700000000, 1)}); err != nil {
		t.Fatal(err)
	}
	request := storage.CreateAndRetainRequest{Target: nativeTarget(t, root, []byte("file")), Initial: storage.NodeInitial{Kind: storage.NodeRegular, Metadata: storage.Metadata{{Key: "replacement", Version: 7, Data: []byte("new")}}}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}
	observed, err := old.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	refusedCtx := metastore.WithFilePublicationGuard(t.Context(), func() error { return syscall.ESTALE })
	failed, err := session.ReplaceAndRetainAt(refusedCtx, request, fileActionID(t, session))
	if !errors.Is(err, syscall.ESTALE) || failed.State != storage.FileActionNotApplied || failed.Effects != 0 || failed.Reference != 0 {
		t.Fatalf("refused replacement=%+v,%v", failed, err)
	}
	unchanged, err := root.LookupAt(t.Context(), []byte("file"))
	if err != nil || unchanged.Attr.ID != old.NodeID() || unchanged.EntryID != old.entryID || unchanged.Attr.MetadataRevision != observed.Attr.MetadataRevision {
		t.Fatalf("refusal changed target=%+v,%v", unchanged, err)
	}
	action := fileActionID(t, session)
	result, err := session.ReplaceAndRetainAt(t.Context(), request, action)
	if err != nil || result.State != storage.FileActionCompleted || result.Effects&(storage.EffectCreated|storage.EffectRetained|storage.EffectEntryDetached) != (storage.EffectCreated|storage.EffectRetained|storage.EffectEntryDetached) || result.Observation.Attr.ID == old.NodeID() {
		t.Fatalf("replacement=%+v,%v", result, err)
	}
	current, err := root.LookupAt(t.Context(), []byte("file"))
	if err != nil || current.Attr.ID != result.Observation.Attr.ID || current.EntryID == old.entryID || string(current.Attr.Metadata[0].Data) != "new" {
		t.Fatalf("new target=%+v,%v", current, err)
	}
	state, err := old.Sync(t.Context())
	if err != nil || !state.Detached || state.Content != key || state.Size != 3 {
		t.Fatalf("old object lost=%+v,%v", state, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 3 {
		t.Fatalf("replacement refunded retained bytes=%d,%v", used, err)
	}
	replay, err := session.ReplaceAndRetainAt(t.Context(), request, action)
	if err != nil || replay.Reference != result.Reference || replay.Observation.Attr.ID != result.Observation.Attr.ID {
		t.Fatalf("replacement replay=%+v,%v", replay, err)
	}
	if _, err := old.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("displaced cleanup=%d,%v", used, err)
	}
	if _, err := session.ReplaceAndRetainAt(t.Context(), storage.CreateAndRetainRequest{}, fileActionID(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed replacement=%v", err)
	}
}
