package storage_test

import (
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMetadataObservationEnrollmentBoundsAndPermissions(t *testing.T) {
	valid := []storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: 8}, {Namespace: "app.empty"}}
	if err := storage.CheckMetadataObservationScopes(valid); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		scopes []storage.MetadataObservationScope
		want   error
	}{
		{[]storage.MetadataObservationScope{{Namespace: "bad space", MaxPayloadBytes: 1}}, syscall.EINVAL},
		{[]storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: -1}}, syscall.EINVAL},
		{[]storage.MetadataObservationScope{{Namespace: "app.flags", MaxPayloadBytes: storage.MaxMetadataValueBytes + 1}}, syscall.EFBIG},
		{[]storage.MetadataObservationScope{valid[0], valid[0]}, syscall.EINVAL},
		{make([]storage.MetadataObservationScope, storage.MaxMetadataNamespaces+1), syscall.EFBIG},
		{[]storage.MetadataObservationScope{{Namespace: "a", MaxPayloadBytes: storage.MaxMetadataValueBytes}, {Namespace: "b", MaxPayloadBytes: storage.MaxMetadataValueBytes}}, syscall.EFBIG},
	} {
		if err := storage.CheckMetadataObservationScopes(test.scopes); !errors.Is(err, test.want) {
			t.Fatalf("enrollment %+v = %v, want %v", test.scopes, err, test.want)
		}
	}
	cloned := storage.CloneMetadataObservationScopes(valid)
	cloned[0].Namespace, cloned[0].MaxPayloadBytes = "other", 0
	if valid[0].Namespace != "app.flags" || valid[0].MaxPayloadBytes != 8 {
		t.Fatal("scope clone changed enrollment")
	}
	if storage.CloneMetadataObservationScopes(nil) != nil || storage.CloneMetadataObservationScopes([]storage.MetadataObservationScope{}) != nil {
		t.Fatal("empty enrollment gained scopes")
	}
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	file := storage.OpenAtOptions{Read: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep,
		Action: action, Use: storage.UseClaim{Uses: storage.ReadData}, MetadataAccess: storage.WriteMetadata, MetadataObservations: valid}
	node := storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.Any}, Action: action,
		MetadataAccess: storage.WriteMetadata, MetadataObservations: valid}
	if err := file.Check(); err != nil {
		t.Fatal(err)
	}
	if err := node.Check(); err != nil {
		t.Fatal(err)
	}
	file.MetadataAccess, node.MetadataAccess = storage.ReadMetadata, 0
	if err := file.Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("read-only file enrollment = %v", err)
	}
	if err := node.Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("unprivileged node enrollment = %v", err)
	}
	if err := (storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, MetadataObservations: valid}}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("legacy open enrollment = %v", err)
	}
}

func TestMetadataScopeObservationKeepsPresenceAndOwnedBytes(t *testing.T) {
	scope := storage.MetadataObservationScope{Namespace: "app.flags", MaxPayloadBytes: 1}
	for _, observed := range []storage.ContentMetadataObservation{
		{NodeID: 7},
		{NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}}},
		{NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}},
	} {
		if err := observed.CheckScope(7, scope); err != nil {
			t.Fatal(err)
		}
		copied := observed.Clone()
		if !reflect.DeepEqual(copied, observed) {
			t.Fatal("clone changed namespace fact")
		}
		if copied.Value != nil {
			copied.Value.Version[0] = 99
			if observed.Value.Version[0] != 1 {
				t.Fatal("clone changed namespace token")
			}
		}
	}
	valid := storage.ContentMetadataObservation{NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}}
	if err := valid.CheckScope(7, storage.MetadataObservationScope{Namespace: "app.empty"}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("payload exceeds enrollment = %v", err)
	}
	if err := valid.CheckScope(0, scope); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown expected identity = %v", err)
	}
	if err := valid.CheckScope(8, scope); !errors.Is(err, syscall.EIO) {
		t.Fatalf("substituted object = %v", err)
	}
	if err := (storage.ContentMetadataObservation{NodeID: 7, Value: &storage.OpaquePayload{}}).CheckScope(7, scope); !errors.Is(err, syscall.EIO) {
		t.Fatalf("present without token = %v", err)
	}
	if err := valid.CheckScope(7, storage.MetadataObservationScope{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid scope = %v", err)
	}
	access := storage.OpenAccess{MetadataObservations: []storage.MetadataObservationScope{scope}}
	cloned := access.Clone()
	cloned.MetadataObservations[0].Namespace = "other"
	if access.MetadataObservations[0].Namespace != "app.flags" {
		t.Fatal("policy clone changed sealed scope")
	}
}
