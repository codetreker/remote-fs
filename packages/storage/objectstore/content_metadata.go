package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.OpenContentMetadata = (*fileSession)(nil)
var _ storage.ReferenceContentMetadata = (*openFile)(nil)

func (fs *fileSession) CheckOpenContentMetadata() error {
	if err := fs.CheckAtomicFileOpen(); err != nil {
		return err
	}
	capability, ok := fs.native.(storage.OpenContentMetadata)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckOpenContentMetadata()
}

func (f *openFile) CheckContentMetadata() error {
	capability, ok := f.native.(storage.ReferenceContentMetadata)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckContentMetadata()
}

func (f *openFile) ObserveContentMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	if !f.options.Write {
		return storage.ContentMetadataObservation{}, syscall.EBADF
	}
	effects, err := storage.ResolveContentMetadataEffects(f.contentEffects, []uint16{index})
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := f.CheckContentMetadata(); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	nodeID, err := storage.ReferenceNodeID(f.native)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	defer done()
	ctx = metastore.WithReferenceSession(ctx, f.session.locks)
	observed, err := f.native.(storage.ReferenceContentMetadata).ObserveContentMetadata(ctx, index)
	if err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := observed.CheckEffect(nodeID, effects[0]); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	if err := f.session.locks.IOHealth(ctx, nodeID); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	return observed.Clone(), nil
}
