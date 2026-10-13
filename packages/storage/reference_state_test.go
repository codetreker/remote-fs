package storage_test

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestReferenceStateScalarsKeepIdentityAndExcludeProtectedFields(t *testing.T) {
	valid := storage.ReferenceState{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 3, AllocationKnown: true, AllocationSize: 4096}}
	for _, state := range []storage.ReferenceState{
		valid,
		{Attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory, Size: -1}, Detached: true, PendingUnlink: true, PendingGeneration: []byte{1}},
		{Attr: storage.Attr{ID: 7, Kind: storage.NodeSymlink, Size: 8}},
		{Attr: valid.Attr, PendingUnlink: true, PendingGeneration: bytes.Repeat([]byte{1}, storage.MaxObservationTokenBytes)},
	} {
		if err := state.CheckProjection(7, storage.StateScalars); err != nil {
			t.Fatalf("valid scalar state %+v = %v", state, err)
		}
	}
	for _, mutate := range []func(*storage.ReferenceState){
		func(s *storage.ReferenceState) { s.Attr.ID = 8 },
		func(s *storage.ReferenceState) { s.Attr.Kind = 0 },
		func(s *storage.ReferenceState) { s.Attr.Size = -1 },
		func(s *storage.ReferenceState) { s.Attr.AllocationKnown = false },
		func(s *storage.ReferenceState) { s.Attr.AllocationSize = -1 },
		func(s *storage.ReferenceState) { s.Attr.Metadata = map[string]storage.OpaquePayload{} },
		func(s *storage.ReferenceState) { s.Attr.BirthTime = new(time.Time) },
		func(s *storage.ReferenceState) { s.Attr.ChangeTime = new(time.Time) },
		func(s *storage.ReferenceState) { s.Attr.AccessTime = time.Unix(1, 0) },
		func(s *storage.ReferenceState) { s.Attr.ModTime = time.Unix(1, 0) },
		func(s *storage.ReferenceState) { s.LinkTarget = []byte{} },
		func(s *storage.ReferenceState) { s.PendingGeneration = []byte{1} },
		func(s *storage.ReferenceState) { s.PendingUnlink = true },
		func(s *storage.ReferenceState) {
			s.PendingUnlink = true
			s.PendingGeneration = make([]byte, storage.MaxObservationTokenBytes+1)
		},
	} {
		state := valid
		mutate(&state)
		if err := state.CheckProjection(7, storage.StateScalars); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid scalar state %+v = %v", state, err)
		}
	}
	if err := valid.CheckProjection(0, storage.StateScalars); !errors.Is(err, syscall.EIO) {
		t.Fatalf("zero expected identity = %v", err)
	}
}

func TestReferenceStateRequiresExplicitProjectionAndPreservesFullFacts(t *testing.T) {
	for _, projection := range []storage.StateProjection{0, 3, 255} {
		if err := projection.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("unknown projection %d = %v", projection, err)
		}
		if err := (storage.ReferenceState{}).CheckProjection(7, projection); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("unknown result projection %d = %v", projection, err)
		}
	}
	for _, state := range []storage.ReferenceState{
		{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular}},
		{Attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory, Size: -1}},
		{Attr: storage.Attr{ID: 7, Kind: storage.NodeSymlink, Size: 4}, LinkTarget: []byte("dest")},
		{Attr: storage.Attr{ID: 7, Kind: storage.NodeRegular}, PendingUnlink: true, PendingGeneration: []byte{1}},
	} {
		if err := state.CheckProjection(7, storage.StateFull); err != nil {
			t.Fatalf("full query %+v = %v", state, err)
		}
	}
}
