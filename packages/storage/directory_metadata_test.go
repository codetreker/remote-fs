package storage_test

import (
	"testing"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestDirectoryMetadataObservationMatchesTargetAndOutputSelection(t *testing.T) {
	target := storage.DirectoryTarget{NodeID: 2}
	observation := storage.DirectoryObservation{ParentID: 2, Revision: []byte{1}}
	linked := storage.NameObservation{NodeID: 2, State: storage.NameLinked, ParentID: 1, RawLeaf: []byte{0xff, 'x'}}
	for _, test := range []struct {
		result  storage.DirectoryMetadataObservation
		options storage.DirectoryMetadataOptions
	}{
		{storage.DirectoryMetadataObservation{Observation: observation}, storage.DirectoryMetadataOptions{}},
		{storage.DirectoryMetadataObservation{Observation: observation, Name: &linked}, storage.DirectoryMetadataOptions{IncludeName: true}},
		{storage.DirectoryMetadataObservation{Observation: observation, Name: &storage.NameObservation{NodeID: 2, State: storage.NameRoot}}, storage.DirectoryMetadataOptions{IncludeName: true}},
	} {
		if err := test.result.Check(target, test.options); err != nil {
			t.Fatalf("valid directory metadata observation: %v", err)
		}
	}

	original := storage.DirectoryMetadataObservation{Observation: observation, Name: &linked}
	clone := original.Clone()
	if unsafe.SliceData(clone.Observation.Revision) == unsafe.SliceData(original.Observation.Revision) ||
		unsafe.SliceData(clone.Name.RawLeaf) == unsafe.SliceData(original.Name.RawLeaf) {
		t.Fatal("directory metadata clone retained caller-owned bytes")
	}
}

func TestDirectoryMetadataOptionsCloneOwnsGuardsAndPreservesAbsence(t *testing.T) {
	withoutGuards := (storage.DirectoryMetadataOptions{IncludeName: true}).Clone()
	if withoutGuards.Guards != nil || !withoutGuards.IncludeName {
		t.Fatalf("cloning absent guards changed options: %+v", withoutGuards)
	}

	original := storage.DirectoryMetadataOptions{
		IncludeName: true,
		Guards: &storage.NamespaceGuards{
			RootID:      1,
			Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1}}},
			Edges:       []storage.ObservedEdge{{ParentID: 1, RawLeaf: []byte{0xff, 'x'}, ChildID: 2}},
		},
	}
	clone := original.Clone()
	if clone.Guards == original.Guards ||
		unsafe.SliceData(clone.Guards.Directories[0].Revision) == unsafe.SliceData(original.Guards.Directories[0].Revision) ||
		unsafe.SliceData(clone.Guards.Edges[0].RawLeaf) == unsafe.SliceData(original.Guards.Edges[0].RawLeaf) {
		t.Fatal("options clone retained caller-owned guard state")
	}
	original.Guards.Directories[0].Revision[0] = 9
	original.Guards.Edges[0].RawLeaf[0] = 'y'
	original.Guards.RootID = 3
	if clone.Guards.Directories[0].Revision[0] != 1 || clone.Guards.Edges[0].RawLeaf[0] != 0xff || clone.Guards.RootID != 1 {
		t.Fatalf("options clone changed with source guards: %+v", clone)
	}
}

func TestDirectoryMetadataObservationRejectsSubstitutionAndMissingName(t *testing.T) {
	target := storage.DirectoryTarget{NodeID: 2}
	observation := storage.DirectoryObservation{ParentID: 2, Revision: []byte{1}}
	root := storage.NameObservation{NodeID: 2, State: storage.NameRoot}
	for _, test := range []struct {
		result  storage.DirectoryMetadataObservation
		target  storage.DirectoryTarget
		options storage.DirectoryMetadataOptions
	}{
		{storage.DirectoryMetadataObservation{}, target, storage.DirectoryMetadataOptions{}},
		{storage.DirectoryMetadataObservation{Observation: observation}, storage.DirectoryTarget{}, storage.DirectoryMetadataOptions{}},
		{storage.DirectoryMetadataObservation{Observation: observation}, target, storage.DirectoryMetadataOptions{Guards: &storage.NamespaceGuards{Edges: []storage.ObservedEdge{{}}}}},
		{storage.DirectoryMetadataObservation{Observation: storage.DirectoryObservation{ParentID: 3, Revision: []byte{1}}}, target, storage.DirectoryMetadataOptions{}},
		{storage.DirectoryMetadataObservation{Observation: observation}, target, storage.DirectoryMetadataOptions{IncludeName: true}},
		{storage.DirectoryMetadataObservation{Observation: observation, Name: &root}, target, storage.DirectoryMetadataOptions{}},
		{storage.DirectoryMetadataObservation{Observation: observation, Name: &storage.NameObservation{}}, target, storage.DirectoryMetadataOptions{IncludeName: true}},
		{storage.DirectoryMetadataObservation{Observation: observation, Name: &storage.NameObservation{NodeID: 3, State: storage.NameRoot}}, target, storage.DirectoryMetadataOptions{IncludeName: true}},
		{storage.DirectoryMetadataObservation{Observation: observation, Name: &storage.NameObservation{NodeID: 2, State: storage.NameDetached}}, target, storage.DirectoryMetadataOptions{IncludeName: true}},
	} {
		if err := test.result.Check(test.target, test.options); err == nil {
			t.Fatalf("invalid directory observation accepted: %+v", test)
		}
	}
}
