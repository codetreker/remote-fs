package smb

import "github.com/codetreker/remote-fs/packages/storage"

func fileEffects(op storage.Operation) storage.FileEffects {
	switch op {
	case storage.OpFileRetain, storage.OpFileRetainAt:
		return storage.EffectRetained | storage.EffectClaimChanged
	case storage.OpFileCreateAndRetainAt:
		return storage.EffectRetained | storage.EffectClaimChanged | storage.EffectCreated
	case storage.OpFileReplaceAndRetainAt:
		return storage.EffectRetained | storage.EffectClaimChanged | storage.EffectCreated | storage.EffectEntryDetached
	case storage.OpFileResetAndRetainAt:
		return storage.EffectRetained | storage.EffectClaimChanged | storage.EffectContentChanged | storage.EffectMetadataChanged
	case storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetKind:
		return storage.EffectContentChanged | storage.EffectMetadataChanged
	case storage.OpFileSetAttr, storage.OpFileSetNodeAttr:
		return storage.EffectMetadataChanged
	case storage.OpFileRename:
		return storage.EffectEntryMoved | storage.EffectEntryDetached
	case storage.OpFilePrepareRemoval, storage.OpFileCancelPrepared:
		return storage.EffectPreparedChanged
	case storage.OpFileDrainEntry, storage.OpFileCancelDrain:
		return storage.EffectDrainChanged
	case storage.OpFileReplaceRanges, storage.OpFileRetireRanges, storage.OpFileRetireRangeOwner:
		return storage.EffectRangesChanged
	case storage.OpFileClose, storage.OpFileSessionClose:
		return storage.EffectReferenceRetired | storage.EffectClaimChanged | storage.EffectRangesChanged | storage.EffectPreparedChanged | storage.EffectDrainChanged | storage.EffectEntryDetached
	default:
		return 0
	}
}
