package locked

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func capability[C any](backend any, check func(C) error) (C, error) {
	value, ok := backend.(C)
	if !ok {
		return value, syscall.EOPNOTSUPP
	}
	return value, check(value)
}

func (s *fileSession) CheckAtomicFileOpen() error {
	_, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	return err
}
func (s *fileSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	backend, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	if err != nil {
		return storage.OpenResult{}, err
	}
	if options.Create || options.Existing == storage.ResetContent || options.Existing == storage.ReplaceNode {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	result, err := backend.OpenAt(ctx, name, options)
	result.File = s.storage.wrapFile(result.File)
	return result, err
}
func (s *fileSession) CheckNamespaceAccess() error {
	_, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	return err
}
func (s *fileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	backend, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	return backend.ReadDirNode(readContext(ctx), target)
}
func (s *fileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	backend, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.DirectoryObservation{}, result.Fail(err)
	}
	observation, err := backend.ReadDirNodeBounded(readContext(ctx), target, result)
	return observation, result.Fail(err)
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
func (s *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backend, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	if options.Create {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	result, err := backend.OpenNodeRef(ctx, id, options)
	result.Reference = s.storage.wrapReference(result.Reference)
	return result, err
}
func (s *fileSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backend, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	if options.Create {
		ctx = s.storage.mutationContext(ctx)
	} else {
		ctx = readContext(ctx)
	}
	result, err := backend.OpenChildRef(ctx, name, options)
	result.Reference = s.storage.wrapReference(result.Reference)
	return result, err
}
func (s *fileSession) CheckMetadataAccess() error {
	_, err := capability(s.FileSession, storage.MetadataAccess.CheckMetadataAccess)
	return err
}
func (s *fileSession) SetMetadata(ctx context.Context, id uint64, namespace string, version, payload []byte) (storage.OpaquePayload, error) {
	backend, err := capability(s.FileSession, storage.MetadataAccess.CheckMetadataAccess)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	return backend.SetMetadata(s.storage.mutationContext(ctx), id, namespace, version, payload)
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

func (s *Storage) wrapFile(inner storage.File) storage.File {
	if inner == nil {
		return nil
	}
	return &file{File: inner, storage: s, referenceCapabilities: referenceCapabilities{backend: inner, storage: s}}
}
func (s *Storage) wrapReference(inner storage.NodeReference) storage.NodeReference {
	if inner == nil {
		return nil
	}
	return &nodeReference{NodeReference: inner, storage: s, referenceCapabilities: referenceCapabilities{backend: inner, storage: s}}
}

type nodeReference struct {
	storage.NodeReference
	storage *Storage
	referenceCapabilities
}

func (r *nodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	return r.NodeReference.Stat(readContext(ctx))
}
func (r *nodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return r.NodeReference.SetAttr(r.storage.mutationContext(ctx), change)
}
func (r *nodeReference) Close(ctx context.Context) error {
	return r.NodeReference.Close(readContext(ctx))
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

func (s *fileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	backend, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.Attr{}, err
	}
	return backend.LookupAt(readContext(ctx), name)
}

var (
	_ storage.FileSession           = (*fileSession)(nil)
	_ storage.File                  = (*file)(nil)
	_ storage.NodeReference         = (*nodeReference)(nil)
	_ storage.AtomicFileOpener      = (*fileSession)(nil)
	_ storage.NamespaceAccess       = (*fileSession)(nil)
	_ storage.NodeReferences        = (*fileSession)(nil)
	_ storage.MetadataAccess        = (*fileSession)(nil)
	_ storage.UseOwners             = (*fileSession)(nil)
	_ storage.MaintenanceAccounting = (*Storage)(nil)
)

func (s *fileSession) CheckRangeControl() error {
	return (&referenceCapabilities{backend: s.FileSession}).CheckRangeControl()
}
func (s *fileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	return (&referenceCapabilities{backend: s.FileSession}).GetConflict(ctx, owner, command)
}
func (s *fileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return (&referenceCapabilities{backend: s.FileSession}).Apply(ctx, owner, commands, request)
}
func (s *fileSession) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return (&referenceCapabilities{backend: s.FileSession}).Query(ctx, owner, request)
}
func (s *fileSession) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return (&referenceCapabilities{backend: s.FileSession}).Cancel(ctx, owner, request)
}
func (s *fileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	return (&referenceCapabilities{backend: s.FileSession}).Drop(ctx, owner, domain)
}

var _ storage.RangeControl = (*fileSession)(nil)
