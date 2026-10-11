package locked

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (r *referenceCapabilities) CheckReferenceInformation() error {
	if _, err := capability(r.backend, storage.ReferenceInformationAccess.CheckReferenceInformation); err != nil {
		return err
	}
	_, err := r.ReferenceNodeID()
	return err
}

func (r *referenceCapabilities) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	inner, err := capability(r.backend, storage.ReferenceInformationAccess.CheckReferenceInformation)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	result, err := inner.Information(readContext(ctx))
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	if err := result.Check(id); err != nil {
		return storage.ReferenceInformation{}, err
	}
	after, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	if after != id {
		return storage.ReferenceInformation{}, syscall.ESTALE
	}
	if err := ctx.Err(); err != nil {
		return storage.ReferenceInformation{}, err
	}
	return result, nil
}

var (
	_ storage.ReferenceInformationAccess = (*file)(nil)
	_ storage.ReferenceInformationAccess = (*nodeReference)(nil)
)
