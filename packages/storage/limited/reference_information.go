package limited

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (r *referenceCapabilities) CheckReferenceInformation() error {
	if err := r.storage.healthy(); err != nil {
		return err
	}
	if _, err := capability(r.backing, storage.ReferenceInformationAccess.CheckReferenceInformation); err != nil {
		return err
	}
	_, err := r.ReferenceNodeID()
	return err
}

func (r *referenceCapabilities) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	r.storage.gate.RLock()
	defer r.storage.gate.RUnlock()
	if err := r.storage.healthy(); err != nil {
		return storage.ReferenceInformation{}, err
	}
	inner, err := capability(r.backing, storage.ReferenceInformationAccess.CheckReferenceInformation)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	id, err := r.ReferenceNodeID()
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	result, err := inner.Information(ctx)
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
	if err := r.storage.healthy(); err != nil {
		return storage.ReferenceInformation{}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.ReferenceInformation{}, err
	}
	result.AllocationKnown = false
	result.AllocationSize = 0
	return result, nil
}

var (
	_ storage.ReferenceInformationAccess = (*file)(nil)
	_ storage.ReferenceInformationAccess = (*nodeReference)(nil)
)
