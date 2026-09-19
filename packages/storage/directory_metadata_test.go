package storage_test

import (
	"testing"

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
	guards := &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{observation}}
	if err := (storage.DirectoryMetadataOptions{Guards: guards}).Check(); err != nil {
		t.Fatal(err)
	}
	if err := (storage.DirectoryMetadataOptions{Guards: &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{{}}}}).Check(); err == nil {
		t.Fatal("invalid prefix guard accepted")
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
