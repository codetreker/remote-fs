package locked

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceCapabilities struct {
	backend any
	storage *Storage
}

func (r *referenceCapabilities) CheckScopedReference() error {
	_, err := capability(r.backend, storage.ScopedReference.CheckScopedReference)
	return err
}

func (r *referenceCapabilities) CheckReferenceState() error {
	_, err := capability(r.backend, storage.ReferenceStateAccess.CheckReferenceState)
	return err
}

func (r *referenceCapabilities) CheckMetadataAccess() error {
	_, err := capability(r.backend, storage.ReferenceMetadataAccess.CheckMetadataAccess)
	return err
}

func (r *referenceCapabilities) CheckRangeControl() error {
	_, err := capability(r.backend, storage.RangeControl.CheckRangeControl)
	return err
}

func (r *referenceCapabilities) CheckDeleteIntent() error {
	_, err := capability(r.backend, storage.DeleteIntent.CheckDeleteIntent)
	return err
}

func (r *referenceCapabilities) CheckConditionalFileMutation() error {
	_, err := capability(r.backend, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	return err
}

func (r *referenceCapabilities) Scope(ctx context.Context) (storage.UseScope, error) {
	backend, err := capability(r.backend, storage.ScopedReference.CheckScopedReference)
	if err != nil {
		return storage.UseScope{}, err
	}
	return backend.Scope(readContext(ctx))
}

func (r *referenceCapabilities) State(ctx context.Context) (storage.ReferenceState, error) {
	backend, err := capability(r.backend, storage.ReferenceStateAccess.CheckReferenceState)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return backend.State(readContext(ctx))
}

func (r *referenceCapabilities) SetMetadata(ctx context.Context, namespace string, version, payload []byte) (storage.OpaquePayload, error) {
	backend, err := capability(r.backend, storage.ReferenceMetadataAccess.CheckMetadataAccess)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	return backend.SetMetadata(r.storage.mutationContext(ctx), namespace, version, payload)
}

func (r *referenceCapabilities) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	backend, err := capability(r.backend, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	return backend.GetConflict(readContext(ctx), owner, command)
}

func (r *referenceCapabilities) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backend, err := capability(r.backend, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backend.Apply(readContext(ctx), owner, commands, request)
}

func (r *referenceCapabilities) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backend, err := capability(r.backend, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backend.Query(readContext(ctx), owner, request)
}

func (r *referenceCapabilities) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	backend, err := capability(r.backend, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return backend.Cancel(readContext(ctx), owner, request)
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

func (r *referenceCapabilities) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	backend, err := capability(r.backend, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	if err != nil {
		return storage.Attr{}, err
	}
	return backend.MutateFile(r.storage.mutationContext(ctx), command)
}

func (r *referenceCapabilities) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	backend, err := capability(r.backend, storage.RangeControl.CheckRangeControl)
	if err != nil {
		return err
	}
	return backend.Drop(readContext(ctx), owner, domain)
}

var (
	_ storage.ScopedReference         = (*referenceCapabilities)(nil)
	_ storage.ReferenceStateAccess    = (*referenceCapabilities)(nil)
	_ storage.ReferenceMetadataAccess = (*referenceCapabilities)(nil)
	_ storage.RangeControl            = (*referenceCapabilities)(nil)
	_ storage.DeleteIntent            = (*referenceCapabilities)(nil)
	_ storage.ConditionalFileMutation = (*referenceCapabilities)(nil)
)
