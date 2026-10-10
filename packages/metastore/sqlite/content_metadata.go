package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.OpenContentMetadata = (*Store)(nil)
var _ storage.ReferenceContentMetadata = (*retainedFile)(nil)

func (s *Store) CheckOpenContentMetadata() error    { return s.CheckFileStore() }
func (f *retainedFile) CheckContentMetadata() error { return f.store.CheckFileStore() }

// The caller holds commit ordering so retirement cannot interleave with the
// observation or with the final content publication.
func (f *retainedFile) checkContentReference(ctx context.Context) error {
	if !f.write {
		return syscall.EBADF
	}
	if err := f.check(); err != nil {
		return err
	}
	if err := f.store.checkFileOwnership(); err != nil {
		return err
	}
	if _, err := f.store.resolveUseScope(ctx, f.scope, uint64(f.id), storage.WriteData); err != nil {
		return err
	}
	return metastore.CheckFilePublication(ctx)
}

func checkContentMetadataValue(value *storage.OpaquePayload) error {
	if value != nil && (len(value.Version) != 8 || binary.BigEndian.Uint64(value.Version) == 0) {
		return syscall.EIO
	}
	return nil
}

func (f *retainedFile) ObserveContentMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	if !f.write {
		return storage.ContentMetadataObservation{}, syscall.EBADF
	}
	if int(index) >= len(f.contentEffects) {
		return storage.ContentMetadataObservation{}, syscall.EINVAL
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.checkContentReference(ctx); err != nil {
		return storage.ContentMetadataObservation{}, err
	}
	effect := f.contentEffects[index]
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
		if kind != int64(storage.NodeRegular) || metadataBytes < 0 || metadataBytes > storage.MaxMetadataBytes || int64(len(encoded)) != metadataBytes {
			return syscall.EIO
		}
		metadata, err := storage.DecodeMetadata(encoded)
		if err != nil {
			return errors.Join(syscall.EIO, err)
		}
		if value, ok := metadata[effect.Namespace]; ok {
			observed.Value = &value
		}
		if err := checkContentMetadataValue(observed.Value); err != nil {
			return err
		}
		return observed.CheckEffect(uint64(f.id), effect)
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
