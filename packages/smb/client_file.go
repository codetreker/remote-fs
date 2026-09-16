package smb

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (f *clientFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	if err := f.require(windowsReadData); err != nil {
		return storage.FileRead{}, err
	}
	if err := f.authorize(ctx, storage.OpFileRead, 0); err != nil {
		return storage.FileRead{}, err
	}
	return f.raw.ReadAt(ctx, storage.FileReadRequest{Offset: offset, Length: length, Owner: &f.owner})
}
func (f *clientFile) WriteAt(ctx context.Context, offset int64, data []byte, id windowsActionID) (windowsActionResult, error) {
	if f.access&(windowsWriteData|windowsAppendData) == 0 {
		return windowsActionResult{}, syscall.EACCES
	}
	if err := f.authorize(ctx, storage.OpFileWrite, storage.EffectContentChanged|storage.EffectMetadataChanged); err != nil {
		return windowsActionResult{}, err
	}
	request := storage.FileWriteRequest{Offset: offset, Data: data, Owner: &f.owner}
	if f.access&windowsWriteData == 0 {
		request.ExpectedSize = &offset
		if err := f.session.remember(id, clientAction{appendOnly: true}); err != nil {
			return windowsActionResult{}, err
		}
	}
	r, err := f.raw.WriteAt(ctx, request, id)
	if request.ExpectedSize != nil {
		if storage.IsFileCallNotAdmitted(err) {
			f.session.forgetAction(id, id)
		} else {
			f.session.recordOpen(id, r)
		}
	}
	return f.result(ctx, id, r, err)
}
func (f *clientFile) Truncate(ctx context.Context, size int64, id windowsActionID) (windowsActionResult, error) {
	if err := f.require(windowsWriteData); err != nil {
		return windowsActionResult{}, err
	}
	if err := f.authorize(ctx, storage.OpFileTruncate, storage.EffectContentChanged|storage.EffectMetadataChanged); err != nil {
		return windowsActionResult{}, err
	}
	r, err := f.raw.Truncate(ctx, storage.FileTruncateRequest{Size: size, Owner: &f.owner}, id)
	return f.result(ctx, id, r, err)
}
func (f *clientFile) setAttrAttempt(ctx context.Context, change windowsAttrChange, id windowsActionID) (windowsActionResult, error) {
	if err := f.require(windowsWriteAttributes); err != nil {
		return windowsActionResult{}, err
	}
	if err := f.authorize(ctx, storage.OpFileSetAttr, storage.EffectMetadataChanged); err != nil {
		return windowsActionResult{}, err
	}
	observation, err := f.raw.Stat(ctx, storage.ObservationOptions{})
	if err != nil {
		return windowsActionResult{}, err
	}
	request := change.AttrChange
	request.ExpectedRevision = observation.Attr.MetadataRevision
	request.CreationTime = change.CreationTime
	request.ChangeTime = change.ChangeTime
	if change.DOSAttributes != nil {
		attr, err := projectWindowsAttr(observation, f.session.backend.defaults)
		if err != nil {
			return windowsActionResult{}, err
		}
		payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: *change.DOSAttributes, DirectorySymlink: observation.Attr.Kind == storage.NodeSymlink && attr.DOSAttributes&dosDirectory != 0})
		if err != nil {
			return windowsActionResult{}, err
		}
		metadata, err := observation.Attr.Metadata.With(storage.OpaqueMetadata{Key: windowsMetadataKey, Version: windowsMetadataVersion, Data: payload})
		if err != nil {
			return windowsActionResult{}, err
		}
		request.Metadata = &metadata
	}
	r, err := f.raw.SetAttr(ctx, request, id)
	return f.result(ctx, id, r, err)
}
func (f *clientFile) namedObservation(ctx context.Context) (storage.FileObservation, error) {
	if err := f.authorize(ctx, storage.OpFileStat, 0); err != nil {
		return storage.FileObservation{}, err
	}
	observation, err := f.raw.Stat(ctx, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		return observation, err
	}
	if observation.Location == nil {
		return observation, syscall.EIO
	}
	if err = f.session.validateLocation(ctx, *observation.Location); err != nil {
		return observation, err
	}
	return observation, nil
}
func (f *clientFile) setDeletePendingAttempt(ctx context.Context, pending bool, id windowsActionID) (windowsActionResult, error) {
	if err := f.require(windowsDelete); err != nil {
		return windowsActionResult{}, err
	}
	observation, err := f.namedObservation(ctx)
	if err != nil {
		return windowsActionResult{}, err
	}
	if observation.Location.State != storage.LocationLinked {
		return windowsActionResult{}, syscall.EACCES
	}
	attr, err := projectWindowsAttr(observation, f.session.backend.defaults)
	if err != nil {
		return windowsActionResult{}, err
	}
	edge := observation.Location.Ancestors[len(observation.Location.Ancestors)-1]
	var r storage.FileActionReceipt
	if pending {
		if attr.DOSAttributes&dosDirectory == 0 && attr.DOSAttributes&dosReadOnly != 0 {
			return windowsActionResult{}, syscall.EACCES
		}
		if err = f.authorize(ctx, storage.OpFileDrainEntry, fileEffects(storage.OpFileDrainEntry)); err != nil {
			return windowsActionResult{}, err
		}
		condition := storage.RemovalFile
		if observation.Attr.Kind == storage.NodeDirectory {
			condition = storage.RemovalIfEmpty
		}
		r, err = f.raw.DrainEntry(ctx, storage.DrainEntryRequest{ExpectedMetadataRevision: observation.Attr.MetadataRevision, Entry: edge, Witness: *observation.Location, Condition: condition}, id)
	} else {
		if err = f.authorize(ctx, storage.OpFileCancelDrain, storage.EffectDrainChanged); err != nil {
			return windowsActionResult{}, err
		}
		checked, checkErr := f.raw.CheckObservation(ctx, storage.ObservationCondition{MetadataRevision: observation.Attr.MetadataRevision, DirectoryRevision: observation.Attr.DirectoryRevision, Location: *observation.Location})
		if checkErr != nil {
			return windowsActionResult{}, checkErr
		}
		if checked.Removal.State != storage.EntryDraining {
			if checked.Removal.State != storage.EntryActive {
				return windowsActionResult{}, syscall.ESTALE
			}
			// An authoritative observation completes this no-op. Prepared
			// ownership is independent; no mutation receipt is invented.
			attr, err := projectWindowsAttr(checked, f.session.backend.defaults)
			return windowsActionResult{Action: id, State: windowsActionCompleted, Attr: attr}, err
		}
		r, err = f.raw.CancelDrain(ctx, storage.CancelDrainRequest{EntryID: edge.EntryID, Generation: checked.Removal.Generation}, id)
	}
	return f.result(ctx, id, r, err)
}
func (f *clientFile) renameAttempt(ctx context.Context, request windowsRenameRequest, id windowsActionID) (windowsActionResult, error) {
	if err := f.require(windowsDelete); err != nil {
		return windowsActionResult{}, err
	}
	observation, err := f.namedObservation(ctx)
	if err != nil {
		return windowsActionResult{}, err
	}
	if observation.Location.State != storage.LocationLinked {
		return windowsActionResult{}, syscall.EACCES
	}
	destination, old, exists, err := f.session.lookup(ctx, request.Destination)
	if err != nil {
		return windowsActionResult{}, err
	}
	if exists && !request.Replace && old.Attr.ID != f.raw.NodeID() {
		return windowsActionResult{}, syscall.EEXIST
	}
	if exists {
		attr, err := projectWindowsAttr(old, f.session.backend.defaults)
		if err != nil {
			return windowsActionResult{}, err
		}
		if attr.DOSAttributes&dosReadOnly != 0 && old.Attr.ID != f.raw.NodeID() {
			return windowsActionResult{}, syscall.EACCES
		}
	}
	edge := observation.Location.Ancestors[len(observation.Location.Ancestors)-1]
	// The source target is supplied for identity and ancestry validation. Rename
	// uses this retained reference; the destination owns its parent's reference.
	source := storage.EntryTarget{ParentID: edge.ParentID, Name: edge.Name, DirectoryRevision: edge.DirectoryRevision, ExpectedEntryID: edge.EntryID, ExpectedNodeID: edge.NodeID, ExpectedMetadataRevision: observation.Attr.MetadataRevision}
	parentLocation := observation.Location.Clone()
	parentLocation.NodeID = edge.ParentID
	parentLocation.Ancestors = parentLocation.Ancestors[:len(parentLocation.Ancestors)-1]
	if len(parentLocation.Ancestors) == 0 {
		parentLocation.State = storage.LocationRoot
	}
	source.Witness = &parentLocation
	var temporary storage.File
	if source.Parent == 0 {
		retainID, err := f.session.next(ctx)
		if err != nil {
			return windowsActionResult{}, err
		}
		if err = f.session.authorize(ctx, storage.OpFileRetain, fileEffects(storage.OpFileRetain), storage.AccessClaim{}, 0, edge.ParentID, 0, 0); err != nil {
			return windowsActionResult{}, err
		}
		receipt, err := f.session.raw.Retain(ctx, storage.RetainRequest{NodeID: edge.ParentID, Witness: &parentLocation}, retainID)
		if err != nil {
			return windowsActionResult{}, f.session.retentionFailure(ctx, receipt, err)
		}
		temporary, err = f.session.raw.Reference(ctx, receipt.Reference)
		if err != nil {
			return windowsActionResult{}, f.session.cleanupOwnership(ctx, err)
		}
		if temporary == nil || temporary.NodeID() != edge.ParentID || temporary.Reference() != receipt.Reference {
			return windowsActionResult{}, f.session.cleanupOwnership(ctx, syscall.EIO)
		}
		source.Parent = temporary.Reference()
	}
	if err = f.session.authorize(ctx, storage.OpFileRename, storage.EffectEntryMoved|storage.EffectEntryDetached, storage.AccessClaim{}, f.Reference(), f.raw.NodeID(), source.Parent, destination.Parent); err != nil {
		if temporary != nil {
			if closeErr := f.session.closeTemporary(ctx, temporary); closeErr != nil {
				return windowsActionResult{}, closeErr
			}
		}
		return windowsActionResult{}, err
	}
	r, err := f.raw.Rename(ctx, storage.RenameRequest{Source: source, Destination: destination, NewName: []byte(request.Destination.Name)}, id)
	if temporary != nil {
		if closeErr := f.session.closeTemporary(ctx, temporary); closeErr != nil {
			return f.result(ctx, id, r, closeErr)
		}
	}
	return f.result(ctx, id, r, err)
}
func (f *clientFile) ReadLink(ctx context.Context) (windowsSymlinkInfo, error) {
	if err := f.require(windowsReadAttributes); err != nil {
		return windowsSymlinkInfo{}, err
	}
	observation, err := f.namedObservation(ctx)
	if err != nil {
		return windowsSymlinkInfo{}, err
	}
	if observation.Attr.Kind != storage.NodeSymlink {
		return windowsSymlinkInfo{}, &windowsError{Failure: windowsNotReparsePoint, Err: syscall.EINVAL}
	}
	target := string(observation.LinkTarget)
	if err = checkWindowsLinkTarget(target); err != nil {
		return windowsSymlinkInfo{}, err
	}
	attr, err := projectWindowsAttr(observation, f.session.backend.defaults)
	if err != nil {
		return windowsSymlinkInfo{}, err
	}
	if _, err = relativeLink(target, attr.NameInfo); err != nil {
		return windowsSymlinkInfo{}, err
	}
	if _, err = f.raw.CheckObservation(ctx, storage.ObservationCondition{MetadataRevision: observation.Attr.MetadataRevision, Location: *observation.Location}); err != nil {
		return windowsSymlinkInfo{}, err
	}
	return windowsSymlinkInfo{Target: target, Location: attr.NameInfo}, nil
}
func (f *clientFile) setLinkAttempt(ctx context.Context, target string, id windowsActionID) (windowsActionResult, error) {
	if f.access&(windowsWriteData|windowsWriteAttributes) == 0 {
		return windowsActionResult{}, syscall.EACCES
	}
	if err := checkWindowsLinkTarget(target); err != nil {
		return windowsActionResult{}, err
	}
	observation, err := f.namedObservation(ctx)
	if err != nil {
		return windowsActionResult{}, err
	}
	attr, err := projectWindowsAttr(observation, f.session.backend.defaults)
	if err != nil {
		return windowsActionResult{}, err
	}
	if _, err = relativeLink(target, attr.NameInfo); err != nil {
		return windowsActionResult{}, err
	}
	payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: attr.DOSAttributes & dosSettableAttributes, DirectorySymlink: attr.DOSAttributes&dosDirectory != 0})
	if err != nil {
		return windowsActionResult{}, err
	}
	metadata, err := observation.Attr.Metadata.With(storage.OpaqueMetadata{Key: windowsMetadataKey, Version: windowsMetadataVersion, Data: payload})
	if err != nil {
		return windowsActionResult{}, err
	}
	if err = f.authorize(ctx, storage.OpFileSetKind, storage.EffectMetadataChanged|storage.EffectContentChanged); err != nil {
		return windowsActionResult{}, err
	}
	r, err := f.raw.SetKind(ctx, storage.SetKindRequest{Owner: &f.owner, Witness: observation.Location, ExpectedRevision: observation.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte(target), Metadata: metadata}, id)
	return f.result(ctx, id, r, err)
}
