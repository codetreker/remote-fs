package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsContentEffects() []storage.ContentMetadataEffect {
	clearMask, setMask := make([]byte, 8), make([]byte, 8)
	binary.LittleEndian.PutUint32(clearMask[4:], dosNormal)
	binary.LittleEndian.PutUint32(setMask[4:], dosArchive)
	absent := make([]byte, 8)
	copy(absent, "SMW\x01")
	return []storage.ContentMetadataEffect{{Namespace: windowsMetadataKey, PayloadBytes: 8,
		PresentPrefix: []byte("SMW\x01"), AbsentPayload: absent, ClearMask: clearMask, SetMask: setMask}}
}

func (c *connection) observeWriteMetadata(ctx context.Context, s *session, t *tree, h *fileHandle) (map[string][]byte, error) {
	if len(h.contentEffects) != 1 {
		return nil, syscall.EIO
	}
	if err := c.authorizeFileIO(ctx, s, t, h, storage.OpFileObserveContentMetadata, h.contentEffects); err != nil {
		return nil, err
	}
	observer, ok := h.file.(storage.ReferenceContentMetadata)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := observer.CheckContentMetadata(); err != nil {
		return nil, err
	}
	observation, err := observer.ObserveContentMetadata(ctx, 0)
	if err != nil {
		return nil, err
	}
	if err := observation.CheckEffect(h.nodeID, h.contentEffects[0]); err != nil {
		return nil, err
	}
	values := map[string]storage.OpaquePayload{}
	var token []byte
	if observation.Value != nil {
		values[windowsMetadataKey] = *observation.Value
		token = bytes.Clone(observation.Value.Version)
	}
	metadata, err := decodeWindowsMetadata(values)
	if err != nil {
		return nil, err
	}
	if metadata.Attributes&dosReadOnly != 0 {
		return nil, syscall.EACCES
	}
	return map[string][]byte{windowsMetadataKey: token}, nil
}

func (c *connection) authorizeFileIO(ctx context.Context, s *session, t *tree, h *fileHandle, operation storage.Operation, effects []storage.ContentMetadataEffect) error {
	if err := checkFileIOIdentity(ctx, s, t, h); err != nil {
		return err
	}
	s.identityMu.RLock()
	principal := s.principal
	s.identityMu.RUnlock()
	call := WithPrincipal(ctx, principal)
	request := authz.AccessRequest{Volume: t.export.share.Volume, Operation: operation,
		ContentMetadataEffects: storage.CloneContentMetadataEffects(effects)}
	if err := c.server.config.Authorize.Authorize(call, request.Clone()); err != nil {
		return err
	}
	if operation == storage.OpFileWrite && len(effects) != 0 {
		request.Operation = storage.OpFileSetMetadata
		request.ContentMetadataEffects = storage.CloneContentMetadataEffects(effects)
		if err := c.server.config.Authorize.Authorize(call, request.Clone()); err != nil {
			return err
		}
	}
	return checkFileIOIdentity(ctx, s, t, h)
}
