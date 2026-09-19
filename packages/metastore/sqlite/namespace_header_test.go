package sqlite

import (
	"bytes"
	"context"
	"runtime"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestNameObservationAdmissionDoesNotLoadUnreturnedMetadata(t *testing.T) {
	s := pendingUnlinkStore(t)
	emptyDir := namespaceCreate(t, s.Store, s.root, "empty", storage.NameMkdir)
	largeDir := namespaceCreate(t, s.Store, s.root, "large", storage.NameMkdir)
	emptyFile := namespaceCreate(t, s.Store, s.root, "plain", storage.NameCreate)
	largeFile := namespaceCreate(t, s.Store, s.root, "heavy", storage.NameCreate)
	held := namespaceCreate(t, s.Store, s.root, "held", storage.NameCreate)
	ref := nameReference(t, s.Store, held, 0).(storage.ReferenceNameObserver)
	const payloadBytes = 62000
	for _, id := range []uint64{largeDir.ID, largeFile.ID} {
		for _, namespace := range []string{"test.left", "test.right"} {
			if _, err := s.SetMetadata(t.Context(), id, namespace, nil, bytes.Repeat([]byte("m"), payloadBytes/2)); err != nil {
				t.Fatal(err)
			}
		}
	}
	observeDirectory := func(id uint64) func(context.Context) error {
		return func(ctx context.Context) error {
			result, err := storage.NewListResult(1024, 0, func(int, int64, int64, storage.Attr) (int64, error) { return 0, nil })
			if err != nil {
				return err
			}
			_, err = s.ObserveDirectoryMetadata(ctx, storage.DirectoryTarget{NodeID: id}, storage.DirectoryMetadataOptions{IncludeName: true}, result)
			return err
		}
	}
	observeGuardedDirectory := func(id uint64) func(context.Context) error {
		node, err := s.StatNode(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		guards := &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{{ParentID: id, Revision: node.DirectoryRevision}}}
		return func(ctx context.Context) error { _, err := ref.ObserveName(ctx, guards); return err }
	}
	observeGuardedEdge := func(id uint64, leaf string) func(context.Context) error {
		guards := &storage.NamespaceGuards{Edges: []storage.ObservedEdge{{ParentID: uint64(s.root), RawLeaf: []byte(leaf), ChildID: id}}}
		return func(ctx context.Context) error { _, err := ref.ObserveName(ctx, guards); return err }
	}
	for _, test := range []struct {
		name         string
		empty, large func(context.Context) error
	}{
		{"parent", observeDirectory(emptyDir.ID), observeDirectory(largeDir.ID)},
		{"directory guard", observeGuardedDirectory(emptyDir.ID), observeGuardedDirectory(largeDir.ID)},
		{"edge guard", observeGuardedEdge(emptyFile.ID, "plain"), observeGuardedEdge(largeFile.ID, "heavy")},
	} {
		t.Run(test.name, func(t *testing.T) {
			measure := func(observe func(context.Context) error) uint64 {
				if err := observe(t.Context()); err != nil {
					t.Fatal(err)
				}
				runtime.GC()
				var before, atAdmission runtime.MemStats
				runtime.ReadMemStats(&before)
				calls := 0
				ctx := storage.WithNameObservationBudget(t.Context(), func(_ storage.NameObservation, leafBytes int64) (int64, error) {
					calls++
					runtime.ReadMemStats(&atAdmission)
					return storage.NameObservationRetentionBytes(leafBytes)
				})
				if err := observe(ctx); err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatalf("name admission calls=%d", calls)
				}
				return atAdmission.TotalAlloc - before.TotalAlloc
			}
			empty, large := measure(test.empty), measure(test.large)
			t.Logf("allocation before output admission: empty=%d large=%d", empty, large)
			if large > empty+payloadBytes/2 {
				t.Fatalf("unreturned %d-byte metadata was loaded before output admission: empty=%d large=%d", payloadBytes, empty, large)
			}
		})
	}
}
