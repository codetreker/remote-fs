package storage_test

import (
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMetadataObservationRequestsKeepPermissionsAndBoundsSeparate(t *testing.T) {
	for _, request := range []storage.MetadataObservationRequest{
		{Purpose: storage.MetadataObservationContentEffect},
		{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 65535},
		{Purpose: storage.MetadataObservationNamespace, Namespace: "app.flags", MaxPayloadBytes: 8},
		{Purpose: storage.MetadataObservationNamespace, Namespace: "app.empty"},
		{Purpose: storage.MetadataObservationNamespace, Namespace: "app.large", MaxPayloadBytes: storage.MaxMetadataValueBytes},
	} {
		if err := request.Check(); err != nil {
			t.Fatalf("valid request %+v = %v", request, err)
		}
		if request.Clone() != request {
			t.Fatal("clone changed namespace request")
		}
	}
	for _, test := range []struct {
		request storage.MetadataObservationRequest
		want    error
	}{
		{storage.MetadataObservationRequest{}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: 3}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, Namespace: "app.flags"}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, MaxPayloadBytes: 1}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, EffectIndex: 1, Namespace: "app.flags"}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "bad space"}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "app.flags", MaxPayloadBytes: -1}, syscall.EINVAL},
		{storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "app.flags", MaxPayloadBytes: storage.MaxMetadataValueBytes + 1}, syscall.EFBIG},
	} {
		if err := test.request.Check(); !errors.Is(err, test.want) {
			t.Fatalf("request %+v = %v, want %v", test.request, err, test.want)
		}
	}
}

func TestMetadataNamespaceObservationKeepsPresenceAndOwnedBytes(t *testing.T) {
	for _, observed := range []storage.MetadataObservation{
		{NodeID: 7},
		{NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}}},
		{NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}},
	} {
		if err := observed.CheckNamespace(7, "app.flags", 1); err != nil {
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
			if len(copied.Value.Data) != 0 {
				copied.Value.Data[0] = 99
				if observed.Value.Data[0] != 2 {
					t.Fatal("clone changed namespace payload")
				}
			}
		}
	}
	valid := storage.MetadataObservation{NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2}}}
	for _, test := range []struct {
		observation storage.MetadataObservation
		id          uint64
		namespace   string
		bound       int
		want        error
	}{
		{valid, 7, "app.empty", 0, syscall.EFBIG},
		{valid, 0, "app.flags", 1, syscall.EIO},
		{valid, 8, "app.flags", 1, syscall.EIO},
		{storage.MetadataObservation{NodeID: 7, Value: &storage.OpaquePayload{}}, 7, "app.flags", 1, syscall.EIO},
		{valid, 7, "", 1, syscall.EINVAL},
	} {
		if err := test.observation.CheckNamespace(test.id, test.namespace, test.bound); !errors.Is(err, test.want) {
			t.Fatalf("bounded observation = %v, want %v", err, test.want)
		}
	}
}
