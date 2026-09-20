package objectstore_test

import (
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

func TestUseClaimsProtectRetainedAndPathOperations(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	if err := volume.Write(t.Context(), "f", []byte("body")); err != nil {
		t.Fatal(err)
	}
	protectedSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ordinarySession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	reader := openFileFor(t, ordinarySession, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	claim := storage.UseClaim{
		Uses: storage.ReadData | storage.WriteData | storage.DeleteName,
		Deny: storage.ReadData | storage.WriteData | storage.DeleteName,
	}
	if _, err := protectedSession.OpenFile(t.Context(), "f", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true}, Use: claim,
	}); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("new denial ignored an existing reader: %v", err)
	}
	if err := reader.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	protected := openFileFor(t, protectedSession, "f", storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true}, Use: claim,
	})
	attr, err := protected.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, access := range []storage.OpenAccess{{Read: true}, {Write: true}} {
		if _, err := ordinarySession.OpenFile(t.Context(), "f", storage.FileOpenOptions{OpenAccess: access}); !errors.Is(err, storage.ErrUseConflict) {
			t.Fatalf("ordinary path open %+v = %v", access, err)
		}
		if _, err := ordinarySession.OpenNode(t.Context(), attr.ID, storage.FileOpenOptions{OpenAccess: access}); !errors.Is(err, storage.ErrUseConflict) {
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
	readFileFor(t, protected, "body")
	if _, err := protected.WriteAt(t.Context(), 0, []byte("own!")); err != nil {
		t.Fatal(err)
	}
	if _, err := protected.Truncate(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	readFileFor(t, protected, "own")
	if err := protected.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if body, err := volume.Read(t.Context(), "f"); err != nil || string(body) != "own" {
		t.Fatalf("read after claim close = %q, %v", body, err)
	}
}

func TestEnforcedRangesUseLogicalIOWhileAdvisoryDomainsDoNot(t *testing.T) {
	volume, _ := fileVolume(t, memory.New(), 4096, nil)
	ownerSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	otherSession := fileSessionFor(t, volume, storage.DefaultFileSessionOptions())
	ownerFile := openFileFor(t, ownerSession, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true}})
	initial, err := ownerFile.WriteAt(t.Context(), 0, []byte("abcdefgh"))
	if err != nil {
		t.Fatal(err)
	}
	other := openFileFor(t, otherSession, "f", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	owner := retainedOwnerFor(t, ownerSession, ownerFile, storage.OwnerReference, 1)
	ranges := ownerSession.(storage.RangeControl)
	command := storage.RangeCommand{
		Domain: storage.DomainEnforced, Mode: storage.RangeExclusive, Edit: storage.AddExact,
		Range:  storage.Range{Kind: storage.Bytes, Start: 2, Length: 3},
		Policy: storage.RangePolicy{DenyOthers: storage.ReadData | storage.WriteData},
	}
	attempt, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{command}, retainedLockRequest(t, ownerSession))
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
		t.Fatalf("disjoint write requiring whole-object materialization = %v", err)
	}
	if err := volume.Write(t.Context(), "f", []byte("replacement")); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("whole path write = %v", err)
	}
	if _, err := ownerFile.WriteAt(t.Context(), 2, []byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	if attr, err := other.Truncate(t.Context(), 6); err != nil || attr.Size != 6 {
		t.Fatalf("disjoint truncate = %+v, %v", attr, err)
	}
	if _, err := other.Truncate(t.Context(), 3); !errors.Is(err, storage.ErrRangeConflict) {
		t.Fatalf("overlapping truncate = %v", err)
	}
	readFileFor(t, ownerFile, "AbXYZf")
	command.Edit, command.Claim = storage.RemoveExact, attempt.Claims[0]
	if released, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{command}, retainedLockRequest(t, ownerSession)); err != nil || released.State != storage.Released {
		t.Fatalf("exact release = %+v, %v", released, err)
	}

	for _, domain := range []storage.ConflictDomain{storage.DomainRecord, storage.DomainWholeFile} {
		owner := retainedOwnerFor(t, ownerSession, ownerFile, storage.OwnerExplicit, uint64(domain)+10)
		advisory := storage.RangeCommand{Domain: domain, Mode: storage.RangeExclusive, Edit: storage.Replace,
			Range: storage.Range{Kind: storage.Bytes, Start: 1, Length: 4}}
		if result, err := ranges.Apply(t.Context(), owner, []storage.RangeCommand{advisory}, retainedLockRequest(t, ownerSession)); err != nil || result.State != storage.Granted {
			t.Fatalf("advisory domain %d grant = %+v, %v", domain, result, err)
		}
		if _, err := other.ReadAt(t.Context(), 1, 1); err != nil {
			t.Fatalf("advisory domain %d enforced a read: %v", domain, err)
		}
		if _, err := other.WriteAt(t.Context(), 1, []byte("!")); err != nil {
			t.Fatalf("advisory domain %d enforced a write: %v", domain, err)
		}
		if err := ranges.Drop(t.Context(), owner, domain); err != nil {
			t.Fatal(err)
		}
	}
	if initial.ID == 0 {
		t.Fatal("created file has no identity")
	}
}
