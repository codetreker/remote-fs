package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.ReferenceNameObserver = (*retainedFile)(nil)
var _ storage.ReferenceNameObserver = (*retainedNodeReference)(nil)

func (f *retainedFile) ReferenceNodeID() (uint64, error) {
	if f.id < 1 {
		return 0, syscall.EIO
	}
	return uint64(f.id), nil
}

func (r *retainedNodeReference) ReferenceNodeID() (uint64, error) {
	return r.file.ReferenceNodeID()
}

func (f *retainedFile) CheckReferenceNameObservation() error {
	if _, err := f.ReferenceNodeID(); err != nil {
		return err
	}
	return f.store.CheckFileStore()
}

func (f *retainedFile) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	if err := guards.Check(); err != nil {
		return storage.NameObservation{}, err
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return storage.NameObservation{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.check(); err != nil {
		return storage.NameObservation{}, err
	}
	if err := f.store.checkFileOwnership(); err != nil {
		return storage.NameObservation{}, err
	}
	if _, err := f.store.resolveUseScope(ctx, f.scope, uint64(f.id), 0); err != nil {
		return storage.NameObservation{}, err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return storage.NameObservation{}, err
	}
	var observed storage.NameObservation
	err := f.store.inspect(ctx, func(tx *sql.Tx) error {
		if err := f.store.checkNamespaceGuards(ctx, tx, guards); err != nil {
			return err
		}
		var err error
		observed, err = f.store.captureNameObservation(ctx, tx, f.id, nil)
		return err
	})
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return storage.NameObservation{}, sqlerr.Failure(err)
	}
	return observed, nil
}

func (r *retainedNodeReference) CheckReferenceNameObservation() error {
	return r.file.CheckReferenceNameObservation()
}

func (r *retainedNodeReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	return r.file.ObserveName(ctx, guards)
}

func (s *Store) nameObservationHeader(ctx context.Context, tx *sql.Tx, id int64) (storage.NameObservation, int64, error) {
	var kind, detached int64
	err := tx.QueryRowContext(ctx, `SELECT
		CASE WHEN typeof(kind)='integer' THEN kind ELSE 0 END,
		CASE WHEN typeof(detached)='integer' THEN detached ELSE -1 END
		FROM nodes WHERE volume=? AND id=?`, s.volume, id).Scan(&kind, &detached)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.NameObservation{}, 0, fmt.Errorf("name observation target %d is missing: %w", id, errors.Join(syscall.EIO, err))
		}
		return storage.NameObservation{}, 0, err
	}
	if kind < int64(storage.NodeRegular) || kind > int64(storage.NodeSymlink) || detached < 0 || detached > 1 {
		return storage.NameObservation{}, 0, syscall.EIO
	}
	rows, err := tx.QueryContext(ctx, `SELECT
		CASE WHEN typeof(e.parent)='integer' THEN e.parent ELSE 0 END,
		CASE WHEN typeof(e.name)='blob' THEN length(e.name) ELSE -1 END,
		CASE WHEN typeof(p.kind)='integer' THEN p.kind ELSE 0 END,
		CASE WHEN typeof(p.volume)='integer' THEN p.volume ELSE 0 END,
		CASE WHEN typeof(p.detached)='integer' THEN p.detached ELSE -1 END
		FROM entries e INDEXED BY entries_by_node LEFT JOIN nodes p ON p.id=e.parent
		WHERE e.node=? AND e.volume=? LIMIT 2`, id, s.volume)
	if err != nil {
		return storage.NameObservation{}, 0, err
	}
	defer rows.Close()
	var parent, nameBytes int64
	count := 0
	for rows.Next() {
		var parentKind, parentVolume, parentDetached int64
		if err := rows.Scan(&parent, &nameBytes, &parentKind, &parentVolume, &parentDetached); err != nil {
			return storage.NameObservation{}, 0, err
		}
		count++
		if count > 1 || parent < 1 || parent == id || parentKind != int64(storage.NodeDirectory) || parentVolume != s.volume || parentDetached != 0 || nameBytes < 1 {
			return storage.NameObservation{}, 0, syscall.EIO
		}
	}
	if err := rows.Err(); err != nil {
		return storage.NameObservation{}, 0, err
	}
	if err := rows.Close(); err != nil {
		return storage.NameObservation{}, 0, err
	}
	observed := storage.NameObservation{NodeID: uint64(id)}
	switch {
	case id == s.root:
		if count != 0 || detached != 0 || kind != int64(storage.NodeDirectory) {
			return storage.NameObservation{}, 0, syscall.EIO
		}
		observed.State = storage.NameRoot
	case detached == 1:
		if count != 0 {
			return storage.NameObservation{}, 0, syscall.EIO
		}
		observed.State = storage.NameDetached
	default:
		if count != 1 {
			return storage.NameObservation{}, 0, syscall.EIO
		}
		observed.State, observed.ParentID = storage.NameLinked, uint64(parent)
	}
	return observed, nameBytes, nil
}

func (s *Store) captureNameObservation(ctx context.Context, tx *sql.Tx, id int64, reserve func(int64) error) (storage.NameObservation, error) {
	observed, nameBytes, err := s.nameObservationHeader(ctx, tx, id)
	if err != nil {
		return storage.NameObservation{}, err
	}
	charge, err := storage.CheckNameObservationBudget(ctx, observed, nameBytes)
	if err != nil {
		return storage.NameObservation{}, err
	}
	if reserve != nil {
		if err := reserve(charge); err != nil {
			return storage.NameObservation{}, err
		}
	}
	if observed.State == storage.NameLinked {
		if err := tx.QueryRowContext(ctx, `SELECT
			CASE WHEN typeof(name)='blob' AND length(name)=? THEN name END
			FROM entries INDEXED BY entries_by_node WHERE node=? AND volume=? AND parent=?`,
			nameBytes, id, s.volume, observed.ParentID).Scan(&observed.RawLeaf); err != nil {
			return storage.NameObservation{}, err
		}
		if int64(len(observed.RawLeaf)) != nameBytes {
			return storage.NameObservation{}, syscall.EIO
		}
	}
	if err := observed.Check(); err != nil {
		return storage.NameObservation{}, fmt.Errorf("invalid stored name observation: %w", errors.Join(syscall.EIO, err))
	}
	return observed, nil
}
