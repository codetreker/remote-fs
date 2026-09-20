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

var (
	_ storage.MetadataAccess          = (*fileSession)(nil)
	_ storage.UseOwners               = (*fileSession)(nil)
	_ storage.RangeControl            = (*fileSession)(nil)
	_ storage.ScopedReference         = (*file)(nil)
	_ storage.ReferenceMetadataAccess = (*file)(nil)
)
