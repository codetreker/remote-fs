package sqlite

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRawRenameCannotInsertIntoPendingDirectory(t *testing.T) {
	s := pendingUnlinkStore(t)
	opened, err := s.OpenChildRef(t.Context(), namespaceName(s.root, "directory"), storage.NodeRefOptions{
		Kind: storage.NodeDirectory, Create: true, Target: storage.ChildCondition{State: storage.Absent},
		Use: storage.UseClaim{Uses: storage.DeleteName}, MetadataAccess: storage.ReadMetadata | storage.WriteMetadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := opened.Reference.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := s.Create(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	if _, err := opened.Reference.(metastore.DeleteIntent).SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkIfEmpty}); err != nil {
		t.Fatal(err)
	}
	before, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(opened.State.ID)})
	if err != nil {
		t.Fatal(err)
	}
	source, err := s.Stat(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	position, err := s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	err = s.Rename(t.Context(), "source", "directory/source")
	if !errors.Is(err, storage.ErrPendingDelete) {
		t.Errorf("rename into pending directory = %v", err)
	}
	if err == nil {
		// Restore the invalid insertion so a negative control can release its pin.
		if err := s.Rename(t.Context(), "directory/source", "source"); err != nil {
			t.Fatal(err)
		}
	}
	after, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(opened.State.ID)})
	if err != nil || len(after.Entries) != 0 || !bytes.Equal(after.Observation.Revision, before.Observation.Revision) {
		t.Errorf("refused rename changed directory = %+v, %v", after, err)
	}
	if node, err := s.Stat(t.Context(), "source"); err != nil || node.ID != source.ID {
		t.Errorf("refused rename changed source identity = %+v, %v", node, err)
	}
	if after, err := s.CommittedPosition(t.Context()); err != nil || after != position {
		t.Errorf("refused rename published changes: %d -> %d, %v", position, after, err)
	}
	if err := opened.Reference.Close(t.Context()); err != nil {
		t.Fatalf("accepted directory deletion did not complete: %v", err)
	}
}
