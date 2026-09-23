package locked

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func capability[C any](backend any, check func(C) error) (C, error) {
	value, ok := backend.(C)
	if !ok {
		var zero C
		return zero, syscall.EOPNOTSUPP
	}
	return value, check(value)
}

func (s *fileSession) CheckAtomicFileOpen() error {
	_, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	return err
}

func (s *fileSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	backend, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	if err != nil {
		return storage.OpenResult{}, err
	}
	result, err := backend.OpenAt(s.storage.mutationContext(ctx), selection, options)
	result.File = s.storage.wrapFile(result.File)
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
	backend, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.Attr{}, err
	}
	return backend.LookupAt(readContext(ctx), name)
}

func (s *fileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	backend, err := capability(s.FileSession, storage.DirectoryReader.CheckDirectoryRead)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	observed, err := backend.ReadDirNode(readContext(ctx), target)
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
	backend, err := capability(s.FileSession, storage.DirectoryReader.CheckDirectoryRead)
	if err != nil {
		return observation, err
	}
	observation, returned = backend.ReadDirNodeBounded(readContext(ctx), target, result)
	if returned == nil {
		returned = observation.Check()
	}
	if returned == nil && observation.ParentID != target.NodeID {
		returned = syscall.EIO
	}
	return observation, returned
}

func (s *fileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	backend, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.NameResult{}, err
	}
	return backend.MutateName(s.storage.mutationContext(ctx), command)
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
	backend, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	return backend.QueryFileAction(readContext(ctx), id)
}

func (s *fileSession) QueryDeleteIntent(ctx context.Context, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	backend, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	return backend.QueryDeleteIntent(readContext(ctx), id)
}

func (s *fileSession) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	backend, err := capability(s.FileSession, storage.FileActions.CheckFileActions)
	if err != nil {
		return err
	}
	return backend.AcknowledgeDeleteIntent(s.storage.mutationContext(ctx), command)
}

func (s *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backend, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	if options.Create || options.CloseIntent != nil {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	result, err := backend.OpenNodeRef(ctx, id, options)
	result.Reference = s.storage.wrapNodeReference(result.Reference)
	return result, err
}

func (s *fileSession) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backend, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	if options.Create || options.CloseIntent != nil {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	result, err := backend.OpenChildRef(ctx, selection, options)
	result.Reference = s.storage.wrapNodeReference(result.Reference)
	return result, err
}

func (s *fileSession) CheckMetadataAccess() error {
	_, err := capability(s.FileSession, storage.MetadataAccess.CheckMetadataAccess)
	return err
}

func (s *fileSession) SetMetadata(ctx context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	backend, err := capability(s.FileSession, storage.MetadataAccess.CheckMetadataAccess)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	return backend.SetMetadata(s.storage.mutationContext(ctx), id, namespace, version, data)
}

func (s *fileSession) CheckUseOwners() error {
	_, err := capability(s.FileSession, storage.UseOwners.CheckUseOwners)
	return err
}

func (s *fileSession) NewUseOwner(ctx context.Context, id uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	backend, err := capability(s.FileSession, storage.UseOwners.CheckUseOwners)
	if err != nil {
		return 0, err
	}
	return backend.NewUseOwner(readContext(ctx), id, scope, options)
}

func (s *fileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	backend, err := capability(s.FileSession, storage.UseOwners.CheckUseOwners)
	if err != nil {
		return err
	}
	return backend.RetireUseOwner(readContext(ctx), owner)
}

func (s *fileSession) CheckRangeControl() error {
	_, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	return err
}

func (s *fileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	backend, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	return backend.GetConflict(readContext(ctx), owner, command)
}

func (s *fileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backend, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backend.Apply(readContext(ctx), owner, commands, request)
}

func (s *fileSession) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backend, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backend.Query(readContext(ctx), owner, request)
}

func (s *fileSession) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backend, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backend.Cancel(readContext(ctx), owner, request)
}

func (s *fileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	backend, err := capability(s.FileSession, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return err
	}
	return backend.Drop(readContext(ctx), owner, domain)
}

type referenceCapabilities struct {
	backend any
	storage *Storage
}

func (r *referenceCapabilities) CheckScopedReference() error {
	_, err := capability(r.backend, storage.ScopedReference.CheckScopedReference)
	return err
}

func (r *referenceCapabilities) Scope(ctx context.Context) (storage.UseScope, error) {
	backend, err := capability(r.backend, storage.ScopedReference.CheckScopedReference)
	if err != nil {
		return storage.UseScope{}, err
	}
	return backend.Scope(readContext(ctx))
}

func (r *referenceCapabilities) CheckMetadataAccess() error {
	_, err := capability(r.backend, storage.ReferenceMetadataAccess.CheckMetadataAccess)
	return err
}

func (r *referenceCapabilities) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	backend, err := capability(r.backend, storage.ReferenceMetadataAccess.CheckMetadataAccess)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	return backend.SetMetadata(r.storage.mutationContext(ctx), namespace, version, data)
}

func (r *referenceCapabilities) CheckReferenceState() error {
	_, err := capability(r.backend, storage.ReferenceStateAccess.CheckReferenceState)
	return err
}

func (r *referenceCapabilities) State(ctx context.Context) (storage.ReferenceState, error) {
	backend, err := capability(r.backend, storage.ReferenceStateAccess.CheckReferenceState)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return backend.State(readContext(ctx))
}

func (r *referenceCapabilities) CheckDeleteIntent() error {
	_, err := capability(r.backend, storage.DeleteIntent.CheckDeleteIntent)
	return err
}

func (r *referenceCapabilities) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	backend, err := capability(r.backend, storage.DeleteIntent.CheckDeleteIntent)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return backend.SetPendingUnlink(r.storage.mutationContext(ctx), command)
}

func (r *referenceCapabilities) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	backend, err := capability(r.backend, storage.DeleteIntent.CheckDeleteIntent)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return backend.ClearPendingUnlink(r.storage.mutationContext(ctx), command)
}

func (r *referenceCapabilities) CheckConditionalFileMutation() error {
	_, err := capability(r.backend, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	return err
}

func (r *referenceCapabilities) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	backend, err := capability(r.backend, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	if err != nil {
		return storage.Attr{}, err
	}
	return backend.MutateFile(r.storage.mutationContext(ctx), command)
}

func (s *Storage) CheckMaintenanceAccounting() error {
	_, err := capability(s.backend, storage.MaintenanceAccounting.CheckMaintenanceAccounting)
	return err
}

func (s *Storage) BindMaintenanceAccounting(ctx context.Context, chain storage.PublicationAccountingChain, initialize func(int64)) error {
	backend, err := capability(s.backend, storage.MaintenanceAccounting.CheckMaintenanceAccounting)
	if err != nil {
		return err
	}
	return backend.BindMaintenanceAccounting(readContext(ctx), chain, initialize)
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
	_ storage.MaintenanceAccounting   = (*Storage)(nil)
)
