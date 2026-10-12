package sqlite

import (
	"context"
	"database/sql"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.MetadataAccess = (*Store)(nil)
var _ storage.ReferenceMetadataAccess = (*retainedFile)(nil)
var _ storage.ReferenceMetadataAccess = (*retainedNodeReference)(nil)

func (s *Store) CheckMetadataAccess() error        { return s.CheckFileStore() }
func (f *retainedFile) CheckMetadataAccess() error { return f.store.CheckFileStore() }

func (f *retainedFile) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	if err := request.Check(); err != nil {
		return storage.MetadataObservation{}, err
	}
	var namespace string
	check := f.checkReferenceObservation
	if request.Purpose == storage.MetadataObservationContentEffect {
		if !f.write {
			return storage.MetadataObservation{}, syscall.EBADF
		}
		if int(request.EffectIndex) >= len(f.contentEffects) {
			return storage.MetadataObservation{}, syscall.EINVAL
		}
		namespace = f.contentEffects[request.EffectIndex].Namespace
		check = f.checkContentReference
	} else {
		if f.metadata&storage.WriteMetadata == 0 {
			return storage.MetadataObservation{}, syscall.EBADF
		}
		namespace = request.Namespace
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.MetadataObservation{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := check(ctx); err != nil {
		return storage.MetadataObservation{}, err
	}
	var observed storage.MetadataObservation
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		kind, captured, err := f.store.captureNamespace(ctx, tx, f.id, namespace)
		if err != nil {
			return err
		}
		if request.Purpose == storage.MetadataObservationContentEffect {
			if kind != storage.NodeRegular {
				return syscall.EIO
			}
			err = captured.CheckEffect(uint64(f.id), f.contentEffects[request.EffectIndex])
		} else {
			err = captured.CheckNamespace(uint64(f.id), namespace, request.MaxPayloadBytes)
		}
		if err != nil {
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
		return storage.MetadataObservation{}, sqlerr.Failure(err)
	}
	return observed.Clone(), nil
}

func checkMetadataUpdate(id uint64, namespace string, expected, data []byte) error {
	if id == 0 || id > math.MaxInt64 {
		return syscall.ESTALE
	}
	return storage.CheckMetadataUpdate(namespace, expected, data)
}

func (s *Store) SetMetadata(ctx context.Context, id uint64, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := s.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, err
	}
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
	if f.metadata&storage.WriteMetadata == 0 {
		return storage.OpaquePayload{}, syscall.EBADF
	}
	if err := f.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, err
	}
	if err := checkMetadataUpdate(uint64(f.id), namespace, expected, data); err != nil {
		return storage.OpaquePayload{}, err
	}
	var result storage.OpaquePayload
	err := f.store.mutatePublication(ctx, &volumeIntent{kind: locking.SetAttrMutation, node: f.id, scope: f.scope}, func(tx *sql.Tx) error {
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
