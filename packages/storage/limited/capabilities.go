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

var (
	_ storage.MetadataAccess          = (*fileSession)(nil)
	_ storage.UseOwners               = (*fileSession)(nil)
	_ storage.RangeControl            = (*fileSession)(nil)
	_ storage.ScopedReference         = (*file)(nil)
	_ storage.ReferenceMetadataAccess = (*file)(nil)
)
