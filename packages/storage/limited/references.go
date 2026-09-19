package limited

import (
	"context"

	"github.com/codetreker/remote-fs/packages/storage"
)

type nodeReference struct {
	storage.NodeReference
	referenceCapabilities
}

func wrapNodeReference(s *Storage, inner storage.NodeReference) storage.NodeReference {
	if inner == nil {
		return nil
	}
	return &nodeReference{NodeReference: inner, referenceCapabilities: referenceCapabilities{backing: inner, storage: s}}
}

func (r *nodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return fileMutation(r.storage, ctx, "retained node", func(ctx context.Context) (storage.Attr, error) {
		return r.NodeReference.SetAttr(ctx, change)
	})
}

func (r *nodeReference) Close(ctx context.Context) error {
	return r.storage.publicationError(r.NodeReference.Close(ctx))
}

type referenceCapabilities struct {
	backing any
	storage *Storage
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

func (r *referenceCapabilities) MutateFile(ctx context.Context, mutation storage.FileMutation) (storage.Attr, error) {
	backing, err := capability(r.backing, storage.ConditionalFileMutation.CheckConditionalFileMutation)
	if err != nil {
		return storage.Attr{}, err
	}
	return fileMutation(r.storage, ctx, "conditional file mutation", func(ctx context.Context) (storage.Attr, error) {
		return backing.MutateFile(ctx, mutation)
	})
}

var (
	_ storage.NodeReference           = (*nodeReference)(nil)
	_ storage.ReferenceStateAccess    = (*referenceCapabilities)(nil)
	_ storage.ScopedReference         = (*referenceCapabilities)(nil)
	_ storage.ReferenceMetadataAccess = (*referenceCapabilities)(nil)
	_ storage.DeleteIntent            = (*referenceCapabilities)(nil)
	_ storage.ConditionalFileMutation = (*referenceCapabilities)(nil)
)
