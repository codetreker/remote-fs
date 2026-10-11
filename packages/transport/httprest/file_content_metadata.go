package httprest

import (
	"bytes"
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type contentMetadataEffect struct {
	Namespace     string         `json:"namespace"`
	PayloadBytes  int            `json:"payloadBytes"`
	PresentPrefix canonicalBytes `json:"presentPrefix"`
	AbsentPayload canonicalBytes `json:"absentPayload"`
	ClearMask     canonicalBytes `json:"clearMask"`
	SetMask       canonicalBytes `json:"setMask"`
}

func contentMetadataEffectsOf(values []storage.ContentMetadataEffect) []contentMetadataEffect {
	if len(values) == 0 {
		return nil
	}
	result := make([]contentMetadataEffect, len(values))
	for i, e := range values {
		result[i] = contentMetadataEffect{e.Namespace, e.PayloadBytes, canonicalBytes(bytes.Clone(e.PresentPrefix)), canonicalBytes(bytes.Clone(e.AbsentPayload)), canonicalBytes(bytes.Clone(e.ClearMask)), canonicalBytes(bytes.Clone(e.SetMask))}
	}
	return result
}
func contentMetadataEffectsStorage(values []contentMetadataEffect) []storage.ContentMetadataEffect {
	if len(values) == 0 {
		return nil
	}
	result := make([]storage.ContentMetadataEffect, len(values))
	for i, e := range values {
		result[i] = storage.ContentMetadataEffect{Namespace: e.Namespace, PayloadBytes: e.PayloadBytes, PresentPrefix: bytes.Clone(e.PresentPrefix), AbsentPayload: bytes.Clone(e.AbsentPayload), ClearMask: bytes.Clone(e.ClearMask), SetMask: bytes.Clone(e.SetMask)}
	}
	return result
}

type contentMetadataObservation struct {
	NodeID uint64         `json:"nodeId"`
	Value  *OpaquePayload `json:"value,omitempty"`
}

func contentMetadataObservationOf(value storage.ContentMetadataObservation) *contentMetadataObservation {
	result := &contentMetadataObservation{NodeID: value.NodeID}
	if value.Value != nil {
		result.Value = &OpaquePayload{Version: bytes.Clone(value.Value.Version), Data: append([]byte{}, value.Value.Data...)}
	}
	return result
}
func (value contentMetadataObservation) storage() storage.ContentMetadataObservation {
	result := storage.ContentMetadataObservation{NodeID: value.NodeID}
	if value.Value != nil {
		result.Value = &storage.OpaquePayload{Version: bytes.Clone(value.Value.Version), Data: bytes.Clone(value.Value.Data)}
	}
	return result
}

func (h *Handler) fileContentEffects(request fileRequest) ([]storage.ContentMetadataEffect, error) {
	if request.Op != storage.OpFileObserveContentMetadata && (request.Op != storage.OpFileMutate || request.Mutation == nil || len(request.Mutation.ContentEffects) == 0) {
		return nil, nil
	}
	h.files.mu.Lock()
	session := h.files.sessions[request.Session]
	h.files.mu.Unlock()
	if session == nil {
		return nil, syscall.ESTALE
	}
	session.mu.Lock()
	reference := session.files[request.File]
	if reference == nil {
		session.mu.Unlock()
		return nil, syscall.ESTALE
	}
	effects := storage.CloneContentMetadataEffects(reference.contentEffects)
	session.mu.Unlock()
	indices := []uint16{request.ContentEffect}
	if request.Op == storage.OpFileMutate {
		indices = request.Mutation.ContentEffects
	}
	return storage.ResolveContentMetadataEffects(effects, indices)
}

func (s *remoteFileSession) CheckOpenContentMetadata() error {
	if !s.capabilities.OpenContentMetadata {
		return syscall.EOPNOTSUPP
	}
	return nil
}
func (f *remoteFile) CheckContentMetadata() error {
	if !f.capabilities.ContentMetadata {
		return syscall.EOPNOTSUPP
	}
	return nil
}
func (f *remoteFile) ObserveContentMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	if err := f.CheckContentMetadata(); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	effects, err := storage.ResolveContentMetadataEffects(f.contentEffects, []uint16{index})
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileObserveContentMetadata, ContentEffect: index})
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	value := response.ContentMetadata.storage()
	if err := value.CheckEffect(f.node, effects[0]); err != nil {
		return storage.ContentMetadataObservation{}, unreachable(Request{Op: OpFile}, err)
	}
	return value, nil
}

var _ storage.OpenContentMetadata = (*remoteFileSession)(nil)
var _ storage.ReferenceContentMetadata = (*remoteFile)(nil)
