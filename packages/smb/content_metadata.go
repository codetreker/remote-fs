package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsMetadataCondition struct {
	Version        []byte
	RootAttributes uint32
}

// A retry can refresh only the authority's CAS token and the root attributes
// Windows preserves. The owner retains all operation data, times and flags.
func applyWindowsMetadataCondition(command storage.FileMutation, condition windowsMetadataCondition, root bool) (storage.FileMutation, error) {
	if len(condition.Version) > storage.MaxObservationTokenBytes || condition.RootAttributes&^(dosHidden|dosSystem) != 0 || !root && condition.RootAttributes != 0 {
		return storage.FileMutation{}, syscall.EIO
	}
	if command.Kind == storage.MutateAttributes {
		update, present := command.Metadata[windowsMetadataKey]
		if !present || len(command.Metadata) != 1 || len(command.ExpectedMetadata) != 0 {
			return storage.FileMutation{}, syscall.EIO
		}
		metadata, err := decodeWindowsMetadataPayload(update.Data)
		if err != nil {
			return storage.FileMutation{}, err
		}
		if root {
			metadata.Attributes = metadata.Attributes&^(dosHidden|dosSystem) | condition.RootAttributes
		}
		update.Data, err = encodeWindowsMetadata(metadata)
		if err != nil {
			return storage.FileMutation{}, err
		}
		update.Version = bytes.Clone(condition.Version)
		command.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: update}
	} else {
		if condition.RootAttributes != 0 || len(command.ExpectedMetadata) != 1 {
			return storage.FileMutation{}, syscall.EIO
		}
		if _, present := command.ExpectedMetadata[windowsMetadataKey]; !present {
			return storage.FileMutation{}, syscall.EIO
		}
		command.ExpectedMetadata = map[string][]byte{windowsMetadataKey: bytes.Clone(condition.Version)}
	}
	return command, nil
}

func windowsContentEffects() []storage.ContentMetadataEffect {
	clearMask, setMask := make([]byte, 8), make([]byte, 8)
	binary.LittleEndian.PutUint32(clearMask[4:], dosNormal)
	binary.LittleEndian.PutUint32(setMask[4:], dosArchive)
	absent := make([]byte, 8)
	copy(absent, "SMW\x01")
	return []storage.ContentMetadataEffect{{Namespace: windowsMetadataKey, PayloadBytes: 8,
		PresentPrefix: []byte("SMW\x01"), AbsentPayload: absent, ClearMask: clearMask, SetMask: setMask}}
}

func (c *connection) observeWriteMetadata(ctx context.Context, s *session, t *tree, h *fileHandle) (windowsMetadataCondition, error) {
	if len(h.contentEffects) != 1 {
		return windowsMetadataCondition{}, syscall.EIO
	}
	request := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationContentEffect, EffectIndex: 0}
	if err := c.authorizeReference(ctx, s, t, h, storage.OpFileObserveMetadata, &request); err != nil {
		return windowsMetadataCondition{}, err
	}
	observer, ok := h.reference().(storage.ReferenceMetadataAccess)
	if !ok {
		return windowsMetadataCondition{}, syscall.EOPNOTSUPP
	}
	if err := observer.CheckMetadataAccess(); err != nil {
		return windowsMetadataCondition{}, err
	}
	observation, err := observer.ObserveMetadata(ctx, request)
	if err != nil {
		return windowsMetadataCondition{}, err
	}
	if err := observation.CheckEffect(h.nodeID, h.contentEffects[0]); err != nil {
		return windowsMetadataCondition{}, err
	}
	values := map[string]storage.OpaquePayload{}
	var token []byte
	if observation.Value != nil {
		values[windowsMetadataKey] = *observation.Value
		token = bytes.Clone(observation.Value.Version)
	}
	metadata, err := decodeWindowsMetadata(values)
	if err != nil {
		return windowsMetadataCondition{}, err
	}
	if metadata.Attributes&dosReadOnly != 0 {
		return windowsMetadataCondition{}, syscall.EACCES
	}
	return windowsMetadataCondition{Version: token}, nil
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
	if (operation == storage.OpFileWrite || operation == storage.OpFileTruncate) && len(effects) != 0 {
		request.Operation = storage.OpFileSetMetadata
		request.ContentMetadataEffects = storage.CloneContentMetadataEffects(effects)
		if err := c.server.config.Authorize.Authorize(call, request.Clone()); err != nil {
			return err
		}
	}
	return checkFileIOIdentity(ctx, s, t, h)
}
