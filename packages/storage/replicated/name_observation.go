package replicated

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ storage.DirectoryMetadataObserver = (*fileSession)(nil)
	_ storage.ReferenceNameObserver     = (*retainedFile)(nil)
	_ storage.ReferenceNameObserver     = (*nodeReference)(nil)
	_ storage.ReferenceIdentity         = (*retainedFile)(nil)
	_ storage.ReferenceIdentity         = (*nodeReference)(nil)
)

func (s *fileSession) CheckDirectoryMetadataObservation() error {
	return capabilityCheck(s.remote, storage.DirectoryMetadataObserver.CheckDirectoryMetadataObservation)
}

func (s *fileSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (observation storage.DirectoryMetadataObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			observation = storage.DirectoryMetadataObservation{}
			result.Fail(returned)
		}
	}()
	observation, returned = sessionCapability(ctx, s, true, func(ctx context.Context, remote storage.DirectoryMetadataObserver) (storage.DirectoryMetadataObservation, error) {
		if err := remote.CheckDirectoryMetadataObservation(); err != nil {
			return storage.DirectoryMetadataObservation{}, err
		}
		return remote.ObserveDirectoryMetadata(ctx, target, options, result)
	})
	if returned == nil {
		returned = observation.Check(target, options)
	}
	return observation, returned
}

func (f *retainedFile) CheckReferenceNameObservation() error {
	return checkReferenceNameObserver(f.remote)
}
func (f *retainedFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return observeReferenceName(ctx, f.session, f.remote, guards)
}
func (r *nodeReference) CheckReferenceNameObservation() error {
	return checkReferenceNameObserver(r.remote)
}
func (r *nodeReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return observeReferenceName(ctx, r.session, r.remote, guards)
}
func (f *retainedFile) ReferenceNodeID() (uint64, error)  { return storage.ReferenceNodeID(f.remote) }
func (r *nodeReference) ReferenceNodeID() (uint64, error) { return storage.ReferenceNodeID(r.remote) }
func checkReferenceNameObserver(remote any) error {
	if err := capabilityCheck(remote, storage.ReferenceNameObserver.CheckReferenceNameObservation); err != nil {
		return err
	}
	_, err := storage.ReferenceNodeID(remote)
	return err
}
func observeReferenceName(ctx context.Context, session *fileSession, remote any, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return fileCall(ctx, session, true, func(ctx context.Context) (storage.NameObservation, error) {
		backing, err := optional[storage.ReferenceNameObserver](remote)
		if err != nil {
			return storage.NameObservation{}, err
		}
		if err := backing.CheckReferenceNameObservation(); err != nil {
			return storage.NameObservation{}, err
		}
		id, err := storage.ReferenceNodeID(remote)
		if err != nil {
			return storage.NameObservation{}, err
		}
		observation, err := backing.ObserveName(ctx, guards)
		if err != nil {
			return storage.NameObservation{}, err
		}
		if err := observation.Check(); err != nil {
			return storage.NameObservation{}, err
		}
		if observation.NodeID != id {
			return storage.NameObservation{}, syscall.EIO
		}
		return observation, nil
	})
}
