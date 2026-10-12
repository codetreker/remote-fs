package httprest

import (
	"bytes"
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

func (h *Handler) fileContentEffects(request fileRequest) ([]storage.ContentMetadataEffect, error) {
	if (request.Op != storage.OpFileObserveMetadata || request.MetadataObservation == nil || request.MetadataObservation.Purpose != storage.MetadataObservationContentEffect) && (request.Op != storage.OpFileMutate || request.Mutation == nil || len(request.Mutation.ContentEffects) == 0) {
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
	indices := []uint16{}
	if request.MetadataObservation != nil {
		indices = append(indices, request.MetadataObservation.EffectIndex)
	}
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

var _ storage.OpenContentMetadata = (*remoteFileSession)(nil)
