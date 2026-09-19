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

func (s *fileSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	backing, err := capability(s.FileSession, storage.AtomicFileOpener.CheckAtomicFileOpen)
	if err != nil {
		return storage.OpenResult{}, err
	}
	result, err := fileMutation(s.storage, ctx, "atomic open", func(ctx context.Context) (storage.OpenResult, error) {
		return backing.OpenAt(ctx, name, options)
	})
	result.File = wrapFile(s.storage, result.File)
	return result, err
}

func (s *fileSession) CheckNamespaceAccess() error {
	_, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
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
	backing, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	return backing.ReadDirNode(ctx, target)
}

func (s *fileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	backing, err := capability(s.FileSession, storage.NamespaceAccess.CheckNamespaceAccess)
	if err != nil {
		return storage.DirectoryObservation{}, err
	}
	return backing.ReadDirNodeBounded(ctx, target, result)
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

func (s *fileSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	backing, err := capability(s.FileSession, storage.NodeReferences.CheckNodeReferences)
	if err != nil {
		return storage.NodeOpenResult{}, err
	}
	result, err := fileMutation(s.storage, ctx, "child reference", func(ctx context.Context) (storage.NodeOpenResult, error) {
		return backing.OpenChildRef(ctx, name, options)
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

var (
	_ storage.AtomicFileOpener = (*fileSession)(nil)
	_ storage.NamespaceAccess  = (*fileSession)(nil)
	_ storage.NodeReferences   = (*fileSession)(nil)
	_ storage.MetadataAccess   = (*fileSession)(nil)
	_ storage.UseOwners        = (*fileSession)(nil)
	_ storage.RangeControl     = (*fileSession)(nil)
)
