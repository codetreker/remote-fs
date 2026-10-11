package httprest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type metadataObservationScope struct {
	Namespace       string `json:"namespace"`
	MaxPayloadBytes int    `json:"maxPayloadBytes"`
}

func metadataObservationScopesOf(values []storage.MetadataObservationScope) []metadataObservationScope {
	if len(values) == 0 {
		return nil
	}
	result := make([]metadataObservationScope, len(values))
	for i, v := range values {
		result[i] = metadataObservationScope{v.Namespace, v.MaxPayloadBytes}
	}
	return result
}
func metadataObservationScopesStorage(values []metadataObservationScope) []storage.MetadataObservationScope {
	if len(values) == 0 {
		return nil
	}
	result := make([]storage.MetadataObservationScope, len(values))
	for i, v := range values {
		result[i] = storage.MetadataObservationScope{Namespace: v.Namespace, MaxPayloadBytes: v.MaxPayloadBytes}
	}
	return storage.CloneMetadataObservationScopes(result)
}
func (s *remoteFileSession) CheckOpenMetadataObservation() error {
	return checkFileCapability(s.capabilities.OpenMetadataObservation)
}
func (f *remoteFile) CheckMetadataObservation() error {
	return checkFileCapability(f.capabilities.MetadataObservation)
}
func (f *remoteFile) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	if err := f.CheckMetadataObservation(); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if int(index) >= len(f.metadataObservations) {
		return storage.ContentMetadataObservation{}, syscall.EINVAL
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileObserveMetadata, MetadataObservation: index})
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	value := response.ObservedMetadata.storage()
	if err := value.CheckScope(f.node, f.metadataObservations[index]); err != nil {
		return storage.ContentMetadataObservation{}, unreachable(Request{Op: OpFile}, err)
	}
	return value, nil
}
func (r *remoteNodeReference) CheckMetadataObservation() error {
	return r.file.CheckMetadataObservation()
}
func (r *remoteNodeReference) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	return r.file.ObserveMetadata(ctx, index)
}

func (h *Handler) fileMetadataObservation(request fileRequest) ([]storage.MetadataObservationScope, error) {
	if request.Op != storage.OpFileObserveMetadata {
		return nil, nil
	}
	h.files.mu.Lock()
	session := h.files.sessions[request.Session]
	h.files.mu.Unlock()
	if session == nil {
		return nil, syscall.ESTALE
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	reference := session.files[request.File]
	if reference == nil {
		return nil, syscall.ESTALE
	}
	if int(request.MetadataObservation) >= len(reference.metadataObservations) {
		return nil, syscall.EINVAL
	}
	return storage.CloneMetadataObservationScopes(reference.metadataObservations[request.MetadataObservation : request.MetadataObservation+1]), nil
}
func performMetadataObservation(ctx context.Context, reference retainedReference, req fileRequest, scope storage.MetadataObservationScope) (fileResponse, error) {
	if err := scope.Check(); err != nil {
		return fileResponse{}, err
	}
	maximum := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, ObservedMetadata: contentMetadataObservationOf(storage.ContentMetadataObservation{NodeID: math.MaxUint64, Value: &storage.OpaquePayload{Version: make([]byte, storage.MaxObservationTokenBytes), Data: []byte{}}})}
	encoded, err := json.Marshal(maximum)
	if err != nil {
		return fileResponse{}, err
	}
	if int64(len(encoded))+int64(base64.StdEncoding.EncodedLen(scope.MaxPayloadBytes)) > req.ResultBytes {
		return fileResponse{}, syscall.EFBIG
	}
	capability, ok := reference.(storage.ReferenceMetadataObserver)
	if !ok {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	if err := capability.CheckMetadataObservation(); err != nil {
		return fileResponse{}, err
	}
	value, err := capability.ObserveMetadata(ctx, req.MetadataObservation)
	if err != nil {
		return fileResponse{}, err
	}
	node, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return fileResponse{}, err
	}
	if err := value.CheckScope(node, scope); err != nil {
		return fileResponse{}, err
	}
	return boundedObservationResponse(fileResponse{ObservedMetadata: contentMetadataObservationOf(value)}, req.ResultBytes)
}

var (
	_ storage.OpenMetadataObservation   = (*remoteFileSession)(nil)
	_ storage.ReferenceMetadataObserver = (*remoteFile)(nil)
	_ storage.ReferenceMetadataObserver = (*remoteNodeReference)(nil)
)
