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

var _ storage.ReferenceInformationAccess = (*retainedFile)(nil)
var _ storage.ReferenceInformationAccess = (*retainedNodeReference)(nil)

func (f *retainedFile) CheckReferenceInformation() error { return f.store.CheckFileStore() }

func (f *retainedFile) checkReferenceObservation(ctx context.Context) error {
	if err := f.check(); err != nil {
		return err
	}
	if err := f.store.checkFileOwnership(); err != nil {
		return err
	}
	if _, err := f.store.resolveUseScope(ctx, f.scope, uint64(f.id), 0); err != nil {
		return err
	}
	return metastore.CheckFilePublication(ctx)
}

func (s *Store) captureReferenceInformation(ctx context.Context, tx *sql.Tx, id int64) (storage.ReferenceInformation, error) {
	var kind, size, allocation, detached int64
	err := tx.QueryRowContext(ctx, `SELECT
		CASE WHEN typeof(n.kind)='integer' THEN n.kind ELSE 0 END,
		CASE WHEN typeof(n.size)='integer' THEN n.size ELSE -1 END,
		CASE WHEN typeof(n.allocation_size)='integer' THEN n.allocation_size ELSE -1 END,
		CASE WHEN typeof(n.detached)='integer' THEN n.detached ELSE -1 END
		FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, id).Scan(&kind, &size, &allocation, &detached)
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ReferenceInformation{}, syscall.ESTALE
	}
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	if kind < int64(storage.NodeRegular) || kind > int64(storage.NodeSymlink) || size < 0 || allocation < 0 ||
		detached < 0 || detached > 1 {
		return storage.ReferenceInformation{}, syscall.EIO
	}
	want, err := allocatedSize(storage.NodeKind(kind), size)
	if err != nil || allocation != want || kind == int64(storage.NodeDirectory) && size != 0 {
		return storage.ReferenceInformation{}, syscall.EIO
	}
	pending, err := s.capturePendingDeletion(ctx, tx, id)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	name, _, err := s.nameObservationHeader(ctx, tx, id)
	if err != nil {
		return storage.ReferenceInformation{}, err
	}
	if (name.State == storage.NameDetached) != (detached == 1) {
		return storage.ReferenceInformation{}, syscall.EIO
	}
	information := storage.ReferenceInformation{NodeID: uint64(id), Kind: storage.NodeKind(kind), Size: size,
		AllocationSize: allocation, AllocationKnown: true, Detached: detached == 1, PendingUnlink: pending.pending}
	if err := information.Check(uint64(id)); err != nil {
		return storage.ReferenceInformation{}, err
	}
	return information, nil
}

func (f *retainedFile) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.ReferenceInformation{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.checkReferenceObservation(ctx); err != nil {
		return storage.ReferenceInformation{}, err
	}
	var result storage.ReferenceInformation
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = f.store.captureReferenceInformation(ctx, tx, f.id)
		return err
	})
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return storage.ReferenceInformation{}, sqlerr.Failure(err)
	}
	return result, nil
}

func (r *retainedNodeReference) CheckReferenceInformation() error {
	return r.file.CheckReferenceInformation()
}
func (r *retainedNodeReference) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	return r.file.Information(ctx)
}
