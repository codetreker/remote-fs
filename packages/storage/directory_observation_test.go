package storage_test

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDirectoryObservationsAndGuardsOwnMutableValues(t *testing.T) {
	guards := &storage.NamespaceGuards{
		RootID: 1,
		Directories: []storage.DirectoryObservation{
			{ParentID: 1, Revision: []byte{1}},
			{ParentID: 2, Revision: []byte{2}},
		},
		Edges: []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte{0xff, 'd'}, ChildID: 2}},
	}
	if err := guards.Check(); err != nil {
		t.Fatal(err)
	}
	clone := guards.Clone()
	if unsafe.SliceData(clone.Directories[0].Revision) == unsafe.SliceData(guards.Directories[0].Revision) ||
		unsafe.SliceData(clone.Edges[0].RawLeaf) == unsafe.SliceData(guards.Edges[0].RawLeaf) {
		t.Fatal("guard clone retained caller-owned bytes")
	}
	guards.Directories[0].Revision[0] = 9
	guards.Edges[0].RawLeaf[0] = 'x'
	if clone.Directories[0].Revision[0] != 1 || clone.Edges[0].RawLeaf[0] != 0xff {
		t.Fatal("guard clone changed with source bytes")
	}
	if (*storage.NamespaceGuards)(nil).Clone() != nil || (*storage.NamespaceGuards)(nil).Check() != nil {
		t.Fatal("nil optional guards did not remain absent and valid")
	}
}

func TestNamespaceGuardsRejectContradictoryOrUnboundedEvidence(t *testing.T) {
	validEdge := storage.ObservedEdge{ParentID: 1, RawLeaf: []byte("dir"), ChildID: 2}
	for _, guards := range []storage.NamespaceGuards{
		{Directories: make([]storage.DirectoryObservation, storage.MaxNamespaceGuards+1)},
		{Edges: make([]storage.ObservedEdge, storage.MaxNamespaceGuards+1)},
		{Directories: []storage.DirectoryObservation{{}}},
		{Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1}}, {ParentID: 1, Revision: []byte{2}}}},
		{Edges: []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte("x"), ChildID: 1}}},
		{Edges: []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte(".."), ChildID: 2}}},
		{Edges: []storage.ObservedEdge{validEdge, {ParentID: 3, RawLeaf: []byte("other"), ChildID: 2}}},
		{Edges: []storage.ObservedEdge{validEdge, {ParentID: 2, RawLeaf: []byte("back"), ChildID: 1}}},
		{RootID: 4, Edges: []storage.ObservedEdge{validEdge}},
		{RootID: 1, Directories: []storage.DirectoryObservation{{ParentID: 3, Revision: []byte{1}}}},
	} {
		if err := guards.Check(); err == nil {
			t.Fatalf("invalid guards accepted: %+v", guards)
		}
	}
	huge := storage.NamespaceGuards{}
	for i := 0; i < 17; i++ {
		leaf := bytes.Repeat([]byte{'x'}, storage.MaxLeafBytes)
		leaf[0] = byte('a' + i)
		huge.Edges = append(huge.Edges, storage.ObservedEdge{ParentID: 1, RawLeaf: leaf, ChildID: uint64(i + 2)})
	}
	if !errors.Is(huge.Check(), syscall.EFBIG) {
		t.Fatalf("oversized guards returned %v, want EFBIG", huge.Check())
	}
}

func TestObservedDirectoryIsCompleteBoundedAndOwned(t *testing.T) {
	original := storage.ObservedDirectory{
		Observation: storage.DirectoryObservation{ParentID: 1, Revision: []byte{1}},
		Entries: []storage.ObservedEntry{{
			RawLeaf: []byte{0xff},
			Attr: storage.Attr{
				ID: 2, Kind: storage.NodeRegular,
				Metadata: map[string]storage.OpaquePayload{"client": {Version: []byte{1}, Data: []byte{2}}},
			},
		}},
	}
	if err := original.Check(); err != nil {
		t.Fatal(err)
	}
	clone := original.Clone()
	if unsafe.SliceData(clone.Observation.Revision) == unsafe.SliceData(original.Observation.Revision) ||
		unsafe.SliceData(clone.Entries[0].RawLeaf) == unsafe.SliceData(original.Entries[0].RawLeaf) ||
		unsafe.SliceData(clone.Entries[0].Attr.Metadata["client"].Data) == unsafe.SliceData(original.Entries[0].Attr.Metadata["client"].Data) {
		t.Fatal("directory clone retained caller-owned bytes")
	}
	original.Observation.Revision[0] = 9
	original.Entries[0].RawLeaf[0] = 'x'
	payload := original.Entries[0].Attr.Metadata["client"]
	payload.Data[0] = 9
	if clone.Observation.Revision[0] != 1 || clone.Entries[0].RawLeaf[0] != 0xff || clone.Entries[0].Attr.Metadata["client"].Data[0] != 2 {
		t.Fatal("directory clone changed with source bytes")
	}

	observation := storage.DirectoryObservation{ParentID: 1, Revision: []byte{1}}
	validEntry := storage.ObservedEntry{RawLeaf: []byte("x"), Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular}}
	for _, directory := range []storage.ObservedDirectory{
		{},
		{Observation: observation, Entries: make([]storage.ObservedEntry, storage.MaxDirectoryEntries+1)},
		{Observation: observation, Entries: []storage.ObservedEntry{{RawLeaf: []byte("..")}}},
		{Observation: observation, Entries: []storage.ObservedEntry{{RawLeaf: []byte("x"), Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory}}}},
		{Observation: observation, Entries: []storage.ObservedEntry{validEntry, validEntry}},
		{Observation: observation, Entries: []storage.ObservedEntry{validEntry, {RawLeaf: []byte("y"), Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular}}}},
		{Observation: observation, Entries: []storage.ObservedEntry{{RawLeaf: []byte("x"), Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{"bad key": {Version: []byte{1}}}}}}},
	} {
		if err := directory.Check(); err == nil {
			t.Fatalf("invalid directory observation accepted: %+v", directory)
		}
	}
}

func TestObservedEntryRetentionChargeRejectsInvalidLengths(t *testing.T) {
	for _, lengths := range [][2]int64{{0, 6}, {storage.MaxLeafBytes + 1, 6}, {1, 5}, {1, storage.MaxMetadataBytes + 1}} {
		if _, err := storage.ObservedEntryBytes(lengths[0], lengths[1]); err == nil {
			t.Fatalf("invalid charge lengths accepted: %v", lengths)
		}
	}
	if charge, err := storage.ObservedEntryBytes(1, 6); err != nil || charge <= 7 {
		t.Fatalf("entry charge = %d, %v", charge, err)
	}
}
