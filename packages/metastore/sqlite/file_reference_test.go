package sqlite

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNativeReferenceIdentitySurvivesNamesAndTerminalResolution(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	file := mutationReference(t, session, root, "original", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	reference, node := file.Reference(), file.NodeID()
	if reference == 0 || node == 0 {
		t.Fatal("retention returned zero identity")
	}
	if err := s.Rename(t.Context(), "original", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "original"); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.Stat(t.Context(), "original")
	if err != nil || uint64(replacement.ID) == node {
		t.Fatalf("replacement identity=%+v,%v", replacement, err)
	}
	resolved, live, err := session.Reference(t.Context(), reference)
	if err != nil || !live || resolved != file || resolved.NodeID() != node || resolved.Reference() != reference {
		t.Fatalf("identity resolution=%v,%v,%v", resolved, live, err)
	}
	observed, err := resolved.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil || observed.Attr.ID != node || observed.Location.State != storage.LocationDetached {
		t.Fatalf("retained observation=%+v,%v", observed, err)
	}
	if _, err := file.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	resolved, live, err = session.Reference(t.Context(), reference)
	if err != nil || live || resolved.NodeID() != node || resolved.Reference() != reference {
		t.Fatalf("terminal identity=%v,%v,%v", resolved, live, err)
	}
	if _, err := resolved.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("terminal reference revived: %v", err)
	}
	if _, _, err := session.Reference(t.Context(), 0); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("zero reference=%v", err)
	}
}

func TestNativeCheckObservationBindsMetadataTargetAndAllAncestors(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	parent := mutationReference(t, session, root, "parent", storage.NodeDirectory, storage.AccessClaim{Uses: storage.AllAccessUses})
	result, err := session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: nativeTarget(t, parent, []byte("link")), Initial: storage.NodeInitial{Kind: storage.NodeSymlink, LinkTarget: []byte("../target"), Metadata: storage.Metadata{{Key: "app", Version: 2, Data: []byte{7}}}}}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := session.Reference(t.Context(), result.Reference)
	if err != nil {
		t.Fatal(err)
	}
	file := raw.(*fileReference)
	options := storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}
	observed, err := file.Stat(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	condition := storage.ObservationCondition{MetadataRevision: observed.Attr.MetadataRevision, DirectoryRevision: observed.Attr.DirectoryRevision, Location: observed.Location.Clone()}
	checked, err := file.CheckObservation(t.Context(), condition)
	if err != nil || !reflect.DeepEqual(checked.Clone(), observed.Clone()) || string(checked.LinkTarget) != "../target" {
		t.Fatalf("coherent capture=%+v,%v", checked, err)
	}
	for _, change := range []func(*storage.ObservationCondition){func(c *storage.ObservationCondition) { c.MetadataRevision++ }, func(c *storage.ObservationCondition) { c.DirectoryRevision++ }} {
		stale := condition
		change(&stale)
		if _, err := file.CheckObservation(t.Context(), stale); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("stale node fact=%v", err)
		}
	}
	if err := s.Create(t.Context(), "parent/sibling"); err != nil {
		t.Fatal(err)
	}
	if _, err := file.CheckObservation(t.Context(), condition); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("changed ancestor revision accepted: %v", err)
	}
	fresh, err := file.Stat(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	condition.Location = fresh.Location.Clone()
	if _, err := file.CheckObservation(t.Context(), condition); err != nil {
		t.Fatal(err)
	}
	foreign := condition
	foreign.Location = storage.EntryLocation{State: storage.LocationRoot, RootNodeID: uint64(s.root), NodeID: uint64(s.root)}
	if _, err := file.CheckObservation(t.Context(), foreign); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("foreign identity accepted: %v", err)
	}
	if _, err := file.CheckObservation(t.Context(), storage.ObservationCondition{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("malformed condition=%v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := file.CheckObservation(canceled, condition); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observation=%v", err)
	}
	if _, err := file.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.CheckObservation(t.Context(), condition); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed observation=%v", err)
	}
}

func TestNativeReplaceClaimPreservesOldClaimOnConflictAndGuardFailure(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	initial := storage.AccessClaim{Uses: storage.ReadContent}
	file := mutationReference(t, session, root, "file", storage.NodeRegular, initial)
	secondResult, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: file.NodeID(), Claim: storage.AccessClaim{Uses: storage.WriteContent}}, fileActionID(t, session))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := session.Reference(t.Context(), secondResult.Reference)
	if err != nil {
		t.Fatal(err)
	}
	desired := storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent, Excludes: storage.WriteContent}
	before, _ := s.fileDomain.access.Revision()
	failedID := fileActionID(t, session)
	refused, err := file.ReplaceClaim(t.Context(), desired, failedID)
	after, _ := s.fileDomain.access.Revision()
	if !errors.Is(err, syscall.EACCES) || refused.State != storage.FileActionNotApplied || refused.Effects != 0 || refused.Conflict == nil || refused.Conflict.Kind != storage.ConflictClaim || file.claim != initial || after != before {
		t.Fatalf("claim conflict=%+v,%v claim=%+v revisions=%d/%d", refused, err, file.claim, before, after)
	}
	if _, err := file.Capture(t.Context(), storage.FileIO{Write: true}); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("rejected claim added write use: %v", err)
	}
	if _, err := second.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	rejected := metastore.WithFilePublicationGuard(t.Context(), func() error { return syscall.ESTALE })
	refused, err = file.ReplaceClaim(rejected, desired, fileActionID(t, session))
	if !errors.Is(err, syscall.ESTALE) || refused.State != storage.FileActionNotApplied || file.claim != initial {
		t.Fatalf("guard refusal=%+v,%v claim=%+v", refused, err, file.claim)
	}
	id := fileActionID(t, session)
	result, err := file.ReplaceClaim(t.Context(), desired, id)
	if err != nil || result.Effects != storage.EffectClaimChanged || file.claim != desired {
		t.Fatalf("claim replacement=%+v,%v", result, err)
	}
	revision, _ := s.fileDomain.access.Revision()
	replay, err := file.ReplaceClaim(t.Context(), desired, id)
	current, _ := s.fileDomain.access.Revision()
	if err != nil || replay.Effects != result.Effects || current != revision {
		t.Fatalf("claim replay changed state=%+v,%v", replay, err)
	}
	if _, err := file.ReplaceClaim(t.Context(), initial, id); !errors.Is(err, syscall.EINVAL) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("changed claim replay=%v", err)
	}
	if old, err := session.QueryAction(t.Context(), failedID); !errors.Is(err, syscall.EACCES) || old.State != storage.FileActionNotApplied {
		t.Fatalf("old refusal changed=%+v,%v", old, err)
	}
	if _, err := file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: 8}, fileActionID(t, session)); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unknown use=%v", err)
	}
}

func TestNativeSyncCapturesCurrentObjectWithoutPublishing(t *testing.T) {
	s, session := newFileAuthority(t)
	root := nativeRootReference(t, session)
	file := mutationReference(t, session, root, "file", storage.NodeRegular, storage.AccessClaim{Uses: storage.AllAccessUses})
	key, err := s.Reserve(t.Context(), "file", 3)
	if err != nil {
		t.Fatal(err)
	}
	modified := time.Unix(1700000000, 123)
	digest := []byte{1, 2, 3}
	if err := s.Commit(t.Context(), "file", metastore.Object{Key: key, Size: 3, Digest: digest, ModTime: modified}); err != nil {
		t.Fatal(err)
	}
	before, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	state, err := file.Sync(t.Context())
	if err != nil || state.ID != int64(file.NodeID()) || state.Content != key || state.Size != 3 || !state.ModTime.Equal(modified) || state.Revision == 0 {
		t.Fatalf("sync capture=%+v,%v", state, err)
	}
	after, err := s.CommittedPosition(t.Context())
	if err != nil || after != before {
		t.Fatalf("sync published metadata: %v,%v before=%v", after, err, before)
	}
	var storedDigest []byte
	if err := s.read.QueryRowContext(t.Context(), `SELECT digest FROM objects WHERE volume=? AND key=?`, s.volume, string(key)).Scan(&storedDigest); err != nil || !bytes.Equal(storedDigest, digest) {
		t.Fatalf("sync changed immutable object evidence: %x,%v", storedDigest, err)
	}
	if _, err := root.Sync(t.Context()); err != nil {
		t.Fatalf("metadata-only directory sync: %v", err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	detached, err := file.Sync(t.Context())
	if err != nil || !detached.Detached || detached.Content != key || detached.ID != state.ID {
		t.Fatalf("detached sync=%+v,%v", detached, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := file.Sync(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sync=%v", err)
	}
	if _, err := file.Close(t.Context(), fileActionID(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Sync(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closed sync=%v", err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("final retained cleanup=%d,%v", used, err)
	}
}
