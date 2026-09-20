package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.DirectoryMetadataObserver = (*Store)(nil)

func (s *Store) CheckDirectoryMetadataObservation() error { return s.CheckFileStore() }

func (s *Store) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (observed storage.DirectoryMetadataObservation, returned error) {
	if result == nil {
		return storage.DirectoryMetadataObservation{}, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := target.Check(); err != nil {
		return storage.DirectoryMetadataObservation{}, err
	}
	if err := options.Check(); err != nil {
		return storage.DirectoryMetadataObservation{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.DirectoryMetadataObservation{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return storage.DirectoryMetadataObservation{}, err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return storage.DirectoryMetadataObservation{}, err
	}
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		if err := s.checkNamespaceGuards(ctx, tx, options.Guards); err != nil {
			return err
		}
		parent, err := s.directoryMetadataTarget(ctx, tx, target)
		if err != nil {
			return err
		}
		observed.Observation = storage.DirectoryObservation{ParentID: target.NodeID, Revision: bytes.Clone(parent.DirectoryRevision)}
		var used int64
		if options.IncludeName {
			name, err := s.captureNameObservation(ctx, tx, parent.ID, func(charge int64) error {
				if charge > s.maxDirectoryBytes {
					return syscall.EFBIG
				}
				if err := result.ReservePrefix(charge); err != nil {
					return err
				}
				used = charge
				return nil
			})
			if err != nil {
				return err
			}
			observed.Name = &name
		}
		check := func(index int, nameBytes, metadataBytes int64, _ storage.Attr) error {
			if index >= s.maxDirectoryEntries {
				return syscall.EFBIG
			}
			charge, err := storage.ObservedEntryBytes(nameBytes, metadataBytes)
			if err != nil {
				return err
			}
			if charge > s.maxDirectoryBytes-used {
				return syscall.EFBIG
			}
			used += charge
			return nil
		}
		return s.listChildrenBoundedChecked(ctx, tx, parent.ID, result, check)
	})
	if err == nil {
		err = observed.Check(target, options)
	}
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return storage.DirectoryMetadataObservation{}, sqlerr.Failure(err)
	}
	return observed, nil
}
