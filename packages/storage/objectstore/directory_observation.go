package objectstore

import (
	"bytes"
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.DirectoryMetadataObserver = (*fileSession)(nil)

func (fs *fileSession) CheckDirectoryMetadataObservation() error {
	native, ok := fs.native.(storage.DirectoryMetadataObserver)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckDirectoryMetadataObservation()
}

func (fs *fileSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (observation storage.DirectoryMetadataObservation, err error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if err != nil {
			result.Fail(err)
			observation = storage.DirectoryMetadataObservation{}
		}
	}()
	if err := fs.CheckDirectoryMetadataObservation(); err != nil {
		return observation, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.begin(ctx, true)
	if err != nil {
		return observation, err
	}
	defer done()
	observation, err = fs.native.(storage.DirectoryMetadataObserver).ObserveDirectoryMetadata(ctx, target, options, result)
	if err != nil {
		return observation, err
	}
	if err := observation.Check(target, options); err != nil {
		return observation, err
	}
	observation.Observation.Revision = bytes.Clone(observation.Observation.Revision)
	if observation.Name != nil {
		name := observation.Name.Clone()
		observation.Name = &name
	}
	return observation, nil
}
