package limited

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func capability[T any](backing any, check func(T) error) (T, error) {
	value, ok := backing.(T)
	if !ok {
		var zero T
		return zero, syscall.EOPNOTSUPP
	}
	return value, check(value)
}

func (s *fileSession) CheckAtomicFileOpen() error {
	_, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	return err
}

func (s *fileSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	backing, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	if err != nil {
		return storage.OpenResult{}, err
	}
	result, err := fileMutation(s.storage, ctx, "atomic open", func(ctx context.Context) (storage.OpenResult, error) {
		return backing.OpenAt(ctx, selection, options)
	})
	result.File = wrapFile(s.storage, result.File)
	return result, err
}

func (s *fileSession) CheckNamespaceAccess() error {
	_, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	return err
}

func (s *fileSession) CheckDirectoryRead() error {
	_, err := capability(s.FileSession, storage.DirectoryReader.CheckDirectoryRead)
	return err
}

func (s *fileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	backing, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.Attr{}, err
	}
	return backing.LookupAt(ctx, name)
}

func (s *fileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	backing, err := capability(s.FileSession, storage.DirectoryReader.CheckDirectoryRead)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	observed, err := backing.ReadDirNode(ctx, target)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	if err := observed.Check(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	if observed.Observation.ParentID != target.NodeID {
		return storage.ObservedDirectory{}, syscall.EIO
	}
	return observed, nil
}

func (s *fileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			observation = storage.DirectoryObservation{}
			result.Fail(returned)
		}
	}()
	backing, err := capability(s.FileSession, storage.DirectoryReader.CheckDirectoryRead)
	if err != nil {
		return storage.DirectoryObservation{}, err
	}
	observation, returned = backing.ReadDirNodeBounded(ctx, target, result)
	if returned == nil {
		returned = observation.Check()
	}
	if returned == nil && observation.ParentID != target.NodeID {
		returned = syscall.EIO
	}
	return observation, returned
}

func (s *fileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	backing, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.NameResult{}, err
	}
	return fileMutation(s.storage, ctx, "namespace mutation", func(ctx context.Context) (storage.NameResult, error) {
		return backing.MutateName(ctx, command)
	})
}

func (s *fileSession) CheckNodeReferences() error {
	_, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	return err
}

func (s *fileSession) CheckFileActions() error {
	_, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	return err
}

func (s *fileSession) QueryFileAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	backing, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	return backing.QueryFileAction(ctx, id)
}

func (s *fileSession) QueryDeleteIntent(ctx context.Context, owner storage.DeleteIntentOwner, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	backing, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	return backing.QueryDeleteIntent(ctx, owner, id)
}

func (s *fileSession) ListDeleteIntents(ctx context.Context, owner storage.DeleteIntentOwner, after storage.DeleteIntentCursor, limit int) (storage.DeleteIntentPage, error) {
	backing, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return storage.DeleteIntentPage{}, err
	}
	return backing.ListDeleteIntents(ctx, owner, after, limit)
}

func (s *fileSession) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	backing, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return err
	}
	_, err = fileMutation(s.storage, ctx, "delete intent acknowledgement", func(ctx context.Context) (struct{}, error) {
		return struct{}{}, backing.AcknowledgeDeleteIntent(ctx, command)
	})
	return err
}

func (s *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backing, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	result, err := fileMutation(s.storage, ctx, "node reference", func(ctx context.Context) (storage.NodeOpenResult, error) {
		return backing.OpenNodeRef(ctx, id, options)
	})
	result.Reference = wrapNodeReference(s.storage, result.Reference)
	return result, err
}

func (s *fileSession) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backing, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	result, err := fileMutation(s.storage, ctx, "child reference", func(ctx context.Context) (storage.NodeOpenResult, error) {
		return backing.OpenChildRef(ctx, selection, options)
	})
	result.Reference = wrapNodeReference(s.storage, result.Reference)
	return result, err
}

func (s *fileSession) CheckMetadataAccess() error {
	_, err := capability(s.FileSession, storage.MetadataAccess.CheckMetadataAccess)
	return err
}

func (s *fileSession) SetMetadata(ctx context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	backing, err := capability(s.FileSession, storage.MetadataAccess.CheckMetadataAccess)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	return fileMutation(s.storage, ctx, "node metadata", func(ctx context.Context) (storage.OpaquePayload, error) {
		return backing.SetMetadata(ctx, id, namespace, version, data)
	})
}

func (s *fileSession) CheckUseOwners() error {
	_, err := capability(s.FileSession, storage.UseOwners.CheckUseOwners)
	return err
}

func (s *fileSession) NewUseOwner(ctx context.Context, id uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	backing, err := capability(s.FileSession, storage.UseOwners.CheckUseOwners)
	if err != nil {
		return 0, err
	}
	return backing.NewUseOwner(ctx, id, scope, options)
}

func (s *fileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	backing, err := capability(s.FileSession, storage.UseOwners.CheckUseOwners)
	if err != nil {
		return err
	}
	return backing.RetireUseOwner(ctx, owner)
}

func (s *fileSession) CheckRangeControl() error {
	_, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	return err
}

func (s *fileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	backing, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	return backing.GetConflict(ctx, owner, command)
}

func (s *fileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backing, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backing.Apply(ctx, owner, commands, request)
}

func (s *fileSession) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backing, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backing.Query(ctx, owner, request)
}

func (s *fileSession) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backing, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backing.Cancel(ctx, owner, request)
}

func (s *fileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	backing, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return err
	}
	return backing.Drop(ctx, owner, domain)
}

type referenceCapabilities struct {
	backing any
	storage *Storage
}

func (r *referenceCapabilities) CheckScopedReference() error {
	_, err := capability(r.backing, storage.ScopedReference.CheckScopedReference)
	return err
}

func (r *referenceCapabilities) Scope(ctx context.Context) (storage.UseScope, error) {
	backing, err := capability(r.backing, storage.ScopedReference.CheckScopedReference)
	if err != nil {
		return storage.UseScope{}, err
	}
	return backing.Scope(ctx)
}

func (r *referenceCapabilities) CheckMetadataAccess() error {
	_, err := capability(r.backing, storage.ReferenceMetadataAccess.CheckMetadataAccess)
	return err
}

func (r *referenceCapabilities) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	backing, err := capability(r.backing, storage.ReferenceMetadataAccess.CheckMetadataAccess)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	return fileMutation(r.storage, ctx, "reference metadata", func(ctx context.Context) (storage.OpaquePayload, error) {
		return backing.SetMetadata(ctx, namespace, version, data)
	})
}

func (r *referenceCapabilities) CheckReferenceState() error {
	_, err := capability(r.backing, storage.ReferenceStateAccess.CheckReferenceState)
	return err
}

func (r *referenceCapabilities) State(ctx context.Context) (storage.ReferenceState, error) {
	backing, err := capability(r.backing, storage.ReferenceStateAccess.CheckReferenceState)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return backing.State(ctx)
}

func (r *referenceCapabilities) CheckDeleteIntent() error {
	_, err := capability(r.backing, storage.DeleteIntent.CheckDeleteIntent)
	return err
}

func (r *referenceCapabilities) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	backing, err := capability(r.backing, storage.DeleteIntent.CheckDeleteIntent)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return fileMutation(r.storage, ctx, "pending unlink", func(ctx context.Context) (storage.ReferenceState, error) {
		return backing.SetPendingUnlink(ctx, command)
	})
}

func (r *referenceCapabilities) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	backing, err := capability(r.backing, storage.DeleteIntent.CheckDeleteIntent)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return fileMutation(r.storage, ctx, "pending unlink", func(ctx context.Context) (storage.ReferenceState, error) {
		return backing.ClearPendingUnlink(ctx, command)
	})
}

func (r *referenceCapabilities) CheckConditionalFileMutation() error {
	_, err := capability(r.backing, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	return err
}

func (r *referenceCapabilities) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	backing, err := capability(r.backing, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	if err != nil {
		return storage.Attr{}, err
	}
	return fileMutation(r.storage, ctx, "conditional file mutation", func(ctx context.Context) (storage.Attr, error) {
		return backing.MutateFile(ctx, command)
	})
}

var (
	_ storage.AtomicFileOpener        = (*fileSession)(nil)
	_ storage.NamespaceAccess         = (*fileSession)(nil)
	_ storage.DirectoryReader         = (*fileSession)(nil)
	_ storage.NodeReferences          = (*fileSession)(nil)
	_ storage.FileActions             = (*fileSession)(nil)
	_ storage.MetadataAccess          = (*fileSession)(nil)
	_ storage.UseOwners               = (*fileSession)(nil)
	_ storage.RangeControl            = (*fileSession)(nil)
	_ storage.ReferenceStateAccess    = (*referenceCapabilities)(nil)
	_ storage.ScopedReference         = (*referenceCapabilities)(nil)
	_ storage.ReferenceMetadataAccess = (*referenceCapabilities)(nil)
	_ storage.DeleteIntent            = (*referenceCapabilities)(nil)
	_ storage.ConditionalFileMutation = (*referenceCapabilities)(nil)
)
