package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
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
	var observed storage.ContentMetadataObservation
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		kind, captured, err := f.store.captureNamespace(ctx, tx, f.id, effect.Namespace)
		if err != nil {
			return err
		}
		if kind != storage.NodeRegular {
			return syscall.EIO
		}
		if err := captured.CheckEffect(uint64(f.id), effect); err != nil {
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
