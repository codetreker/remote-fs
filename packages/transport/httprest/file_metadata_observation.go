package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type metadataObservationRequest struct {
	Purpose         storage.MetadataObservationPurpose `json:"purpose"`
	EffectIndex     uint16                             `json:"effectIndex"`
	Namespace       string                             `json:"namespace"`
	MaxPayloadBytes int                                `json:"maxPayloadBytes"`
}

func metadataObservationRequestOf(value storage.MetadataObservationRequest) *metadataObservationRequest {
	return &metadataObservationRequest{value.Purpose, value.EffectIndex, value.Namespace, value.MaxPayloadBytes}
}
func (value metadataObservationRequest) storage() storage.MetadataObservationRequest {
	return storage.MetadataObservationRequest{Purpose: value.Purpose, EffectIndex: value.EffectIndex, Namespace: value.Namespace, MaxPayloadBytes: value.MaxPayloadBytes}
}

type metadataObservation struct {
	NodeID uint64         `json:"nodeId"`
	Value  *OpaquePayload `json:"value,omitempty"`
}

func metadataObservationOf(value storage.MetadataObservation) *metadataObservation {
	result := &metadataObservation{NodeID: value.NodeID}
	if value.Value != nil {
		result.Value = &OpaquePayload{Version: bytes.Clone(value.Value.Version), Data: append([]byte{}, value.Value.Data...)}
	}
	return result
}
func (value metadataObservation) storage() storage.MetadataObservation {
	result := storage.MetadataObservation{NodeID: value.NodeID}
	if value.Value != nil {
		result.Value = &storage.OpaquePayload{Version: bytes.Clone(value.Value.Version), Data: bytes.Clone(value.Value.Data)}
	}
	return result
}

func (f *remoteFile) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	if err := request.Check(); err != nil {
		return storage.MetadataObservation{}, err
	}
	if err := f.CheckMetadataAccess(); err != nil {
		return storage.MetadataObservation{}, err
	}
	var effect *storage.ContentMetadataEffect
	if request.Purpose == storage.MetadataObservationContentEffect {
		effects, err := storage.ResolveContentMetadataEffects(f.contentEffects, []uint16{request.EffectIndex})
		if err != nil {
			return storage.MetadataObservation{}, err
		}
		effect = &effects[0]
	}
	request = request.Clone()
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileObserveMetadata, MetadataObservation: metadataObservationRequestOf(request)})
	if err != nil {
		return storage.MetadataObservation{}, err
	}
	value := response.ObservedMetadata.storage()
	if effect != nil {
		err = value.CheckEffect(f.node, *effect)
	} else {
		err = value.CheckNamespace(f.node, request.Namespace, request.MaxPayloadBytes)
	}
	if err != nil {
		return storage.MetadataObservation{}, unreachable(Request{Op: OpFile}, err)
	}
	return value, nil
}
func (r *remoteNodeReference) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	return r.file.ObserveMetadata(ctx, request)
}

func performMetadataObservation(ctx context.Context, reference retainedReference, req fileRequest, effects []storage.ContentMetadataEffect) (fileResponse, error) {
	request := req.MetadataObservation.storage()
	if err := request.Check(); err != nil {
		return fileResponse{}, err
	}
	maximumPayload := request.MaxPayloadBytes
	var effect *storage.ContentMetadataEffect
	if request.Purpose == storage.MetadataObservationContentEffect {
		selected, err := storage.ResolveContentMetadataEffects(effects, []uint16{request.EffectIndex})
		if err != nil {
			return fileResponse{}, err
		}
		effect = &selected[0]
		maximumPayload = effect.PayloadBytes
	}
	maximum := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, ObservedMetadata: metadataObservationOf(storage.MetadataObservation{NodeID: math.MaxUint64, Value: &storage.OpaquePayload{Version: make([]byte, storage.MaxObservationTokenBytes), Data: []byte{}}})}
	encoded, err := json.Marshal(maximum)
	if err != nil {
		return fileResponse{}, err
	}
	if int64(len(encoded))+int64(base64.StdEncoding.EncodedLen(maximumPayload)) > req.ResultBytes {
		return fileResponse{}, syscall.EFBIG
	}
	capability, ok := reference.(storage.ReferenceMetadataAccess)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := capability.CheckMetadataAccess(); err != nil {
		return fileResponse{}, err
	}
	value, err := capability.ObserveMetadata(ctx, request)
	if err != nil {
		return fileResponse{}, err
	}
	node, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return fileResponse{}, err
	}
	if effect != nil {
		err = value.CheckEffect(node, *effect)
	} else {
		err = value.CheckNamespace(node, request.Namespace, request.MaxPayloadBytes)
	}
	if err != nil {
		return fileResponse{}, err
	}
	return boundedObservationResponse(fileResponse{ObservedMetadata: metadataObservationOf(value)}, req.ResultBytes)
}
