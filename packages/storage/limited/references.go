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
	_, err := r.CloseWithResult(ctx)
	return err
}

func (r *nodeReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	result, err := storage.CloseReference(ctx, r.NodeReference)
	return result, r.storage.publicationError(err)
}

func (r *nodeReference) CheckScopedReference() error {
	return r.referenceCapabilities.CheckScopedReference()
}

func (r *nodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return r.referenceCapabilities.Scope(ctx)
}

func (r *nodeReference) CheckReferenceState() error {
	return r.referenceCapabilities.CheckReferenceState()
}

func (r *nodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	return r.referenceCapabilities.State(ctx)
}

var _ storage.NodeReference = (*nodeReference)(nil)
