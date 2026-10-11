package sqlite

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.OpenMetadataObservation = (*Store)(nil)
var _ storage.ReferenceMetadataObserver = (*retainedFile)(nil)
var _ storage.ReferenceMetadataObserver = (*retainedNodeReference)(nil)

func (s *Store) CheckOpenMetadataObservation() error    { return s.CheckFileStore() }
func (f *retainedFile) CheckMetadataObservation() error { return f.store.CheckFileStore() }

func (f *retainedFile) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	if f.metadata&storage.WriteMetadata == 0 {
		return storage.ContentMetadataObservation{}, syscall.EBADF
	}
	if int(index) >= len(f.metadataObservations) {
		return storage.ContentMetadataObservation{}, syscall.EINVAL
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.checkReferenceObservation(ctx); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	scope := f.metadataObservations[index]
	var observed storage.ContentMetadataObservation
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		_, captured, err := f.store.captureNamespace(ctx, tx, f.id, scope.Namespace)
		if err != nil {
			return err
		}
		if err := captured.CheckScope(uint64(f.id), scope); err != nil {
			return err
		}
		observed = captured
		return nil
	})
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return storage.ContentMetadataObservation{}, sqlerr.Failure(err)
	}
	return observed.Clone(), nil
}

func (r *retainedNodeReference) CheckMetadataObservation() error {
	return r.file.CheckMetadataObservation()
}
func (r *retainedNodeReference) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	return r.file.ObserveMetadata(ctx, index)
}
