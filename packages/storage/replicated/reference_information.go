package replicated

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func (s *fileSession) CheckOpenReferenceInformation() error {
	return capabilityCheck(s.remote, storage.OpenReferenceInformation.CheckOpenReferenceInformation)
}

func checkReferenceInformation(remote any) error {
	if err := capabilityCheck(remote, storage.ReferenceInformationAccess.CheckReferenceInformation); err != nil {
		return err
	}
	_, err := storage.ReferenceNodeID(remote)
	return err
}

func (f *retainedFile) CheckReferenceInformation() error  { return checkReferenceInformation(f.remote) }
func (r *nodeReference) CheckReferenceInformation() error { return checkReferenceInformation(r.remote) }
func (f *retainedFile) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	return observeReferenceInformation(ctx, f.session, f.remote)
}
func (r *nodeReference) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	return observeReferenceInformation(ctx, r.session, r.remote)
}

func observeReferenceInformation(ctx context.Context, session *fileSession, remote any) (storage.ReferenceInformation, error) {
	return fileCall(ctx, session, true, func(ctx context.Context) (storage.ReferenceInformation, error) {
		backing, err := optional[storage.ReferenceInformationAccess](remote)
		if err != nil {
			return storage.ReferenceInformation{}, err
		}
		if err := backing.CheckReferenceInformation(); err != nil {
			return storage.ReferenceInformation{}, err
		}
		id, err := storage.ReferenceNodeID(remote)
		if err != nil {
			return storage.ReferenceInformation{}, err
		}
		result, err := backing.Information(ctx)
		if err != nil {
			return storage.ReferenceInformation{}, err
		}
		if err := result.Check(id); err != nil {
			return storage.ReferenceInformation{}, err
		}
		after, err := storage.ReferenceNodeID(remote)
		if err != nil {
			return storage.ReferenceInformation{}, err
		}
		if after != id {
			return storage.ReferenceInformation{}, syscall.ESTALE
		}
		if err := session.base.usable("file", ""); err != nil {
			return storage.ReferenceInformation{}, err
		}
		if err := ctx.Err(); err != nil {
			return storage.ReferenceInformation{}, err
		}
		return result, nil
	})
}

var (
	_ storage.OpenReferenceInformation   = (*fileSession)(nil)
	_ storage.ReferenceInformationAccess = (*retainedFile)(nil)
	_ storage.ReferenceInformationAccess = (*nodeReference)(nil)
)
