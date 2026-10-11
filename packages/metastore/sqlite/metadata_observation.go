package sqlite

import (
	"context"
	"database/sql"
	"errors"
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
	observed := storage.ContentMetadataObservation{NodeID: uint64(f.id)}
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		var kind, metadataBytes int64
		var encoded []byte
		err := tx.QueryRowContext(ctx, `SELECT
			CASE WHEN typeof(kind)='integer' THEN kind ELSE 0 END,
			CASE WHEN typeof(metadata)='blob' THEN length(metadata) ELSE -1 END,
			CASE WHEN typeof(metadata)='blob' AND length(metadata)<=? THEN metadata END
			FROM nodes WHERE volume=? AND id=?`, storage.MaxMetadataBytes, f.store.volume, f.id).Scan(&kind, &metadataBytes, &encoded)
		if errors.Is(err, sql.ErrNoRows) {
			return syscall.ESTALE
		}
		if err != nil {
			return err
		}
		if kind < int64(storage.NodeRegular) || kind > int64(storage.NodeSymlink) || metadataBytes < 0 ||
			metadataBytes > storage.MaxMetadataBytes || int64(len(encoded)) != metadataBytes {
			return syscall.EIO
		}
		metadata, err := storage.DecodeMetadata(encoded)
		if err != nil {
			return err
		}
		if value, ok := metadata[scope.Namespace]; ok {
			observed.Value = &value
		}
		if err := checkContentMetadataValue(observed.Value); err != nil {
			return err
		}
		return observed.CheckScope(uint64(f.id), scope)
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
