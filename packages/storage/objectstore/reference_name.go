package objectstore

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.ReferenceNameObserver = (*openFile)(nil)
var _ storage.ReferenceNameObserver = (*nodeReference)(nil)
var _ storage.ReferenceIdentity = (*openFile)(nil)
var _ storage.ReferenceIdentity = (*nodeReference)(nil)

func (f *openFile) ReferenceNodeID() (uint64, error)      { return storage.ReferenceNodeID(f.native) }
func (r *nodeReference) ReferenceNodeID() (uint64, error) { return storage.ReferenceNodeID(r.native) }

func nameObserver(native metastore.NodeReference) (storage.ReferenceNameObserver, uint64, error) {
	observer, ok := native.(storage.ReferenceNameObserver)
	if !ok {
		return nil, 0, syscall.EOPNOTSUPP
	}
	if err := observer.CheckReferenceNameObservation(); err != nil {
		return nil, 0, err
	}
	id, err := storage.ReferenceNodeID(native)
	return observer, id, err
}

func (f *openFile) CheckReferenceNameObservation() error {
	_, _, err := nameObserver(f.native)
	return err
}

func (r *nodeReference) CheckReferenceNameObservation() error {
	_, _, err := nameObserver(r.native)
	return err
}

func (f *openFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	observer, id, err := nameObserver(f.native)
	if err != nil {
		return storage.NameObservation{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.NameObservation{}, err
	}
	defer done()
	return observeReferenceName(ctx, observer, id, guards)
}

func (r *nodeReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	observer, id, err := nameObserver(r.native)
	if err != nil {
		return storage.NameObservation{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.NameObservation{}, err
	}
	defer done()
	return observeReferenceName(ctx, observer, id, guards)
}

func observeReferenceName(ctx context.Context, observer storage.ReferenceNameObserver, id uint64, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	observation, err := observer.ObserveName(ctx, guards)
	if err != nil {
		return storage.NameObservation{}, err
	}
	if err := observation.Check(); err != nil {
		return storage.NameObservation{}, err
	}
	if observation.NodeID != id {
		return storage.NameObservation{}, syscall.EIO
	}
	return observation.Clone(), nil
}
