package sqlite

import (
	"context"
	"database/sql"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.MetadataAccess = (*Store)(nil)
var _ storage.ReferenceMetadataAccess = (*retainedFile)(nil)

func (s *Store) CheckMetadataAccess() error        { return nil }
func (f *retainedFile) CheckMetadataAccess() error { return nil }

func checkMetadataUpdate(id uint64, namespace string, expected, data []byte) error {
	if id == 0 || id > math.MaxInt64 {
		return syscall.ESTALE
	}
	if err := storage.CheckMetadataNamespace(namespace); err != nil {
		return err
	}
	if len(expected) > storage.MaxObservationTokenBytes || len(data) > storage.MaxMetadataValueBytes {
		return syscall.EFBIG
	}
	return nil
}

func (s *Store) SetMetadata(ctx context.Context, id uint64, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := checkMetadataUpdate(id, namespace, expected, data); err != nil {
		return storage.OpaquePayload{}, err
	}
	var result storage.OpaquePayload
	err := s.mutatePublication(ctx, &volumeIntent{kind: locking.SetAttrMutation, node: int64(id)}, func(tx *sql.Tx) error {
		var err error
		result, err = s.setNodeMetadata(ctx, tx, int64(id), namespace, expected, data, time.Now())
		if err != nil {
			return err
		}
		return s.recordNamedChanged(ctx, tx, int64(id))
	})
	if err != nil {
		return storage.OpaquePayload{}, sqlerr.Failure(err)
	}
	return result, nil
}

func (f *retainedFile) SetMetadata(ctx context.Context, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := checkMetadataUpdate(uint64(f.id), namespace, expected, data); err != nil {
		return storage.OpaquePayload{}, err
	}
	if f.metadata&storage.WriteMetadata == 0 {
		return storage.OpaquePayload{}, syscall.EBADF
	}
	var result storage.OpaquePayload
	err := f.store.mutatePublication(ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		if err := f.check(); err != nil {
			return err
		}
		var err error
		result, err = f.store.setNodeMetadata(ctx, tx, f.id, namespace, expected, data, time.Now())
		if err != nil {
			return err
		}
		return f.store.recordNamedChanged(ctx, tx, f.id)
	})
	if err != nil {
		return storage.OpaquePayload{}, sqlerr.Failure(err)
	}
	return result, nil
}

func (r *retainedNodeReference) CheckMetadataAccess() error { return r.file.CheckMetadataAccess() }
func (r *retainedNodeReference) SetMetadata(ctx context.Context, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	return r.file.SetMetadata(ctx, namespace, expected, data)
}
