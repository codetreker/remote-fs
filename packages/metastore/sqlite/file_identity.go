package sqlite

import (
	"context"
	"database/sql"
	"strconv"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	_ metastore.BackendIdentity       = (*Store)(nil)
	_ storage.StableReferenceIdentity = (*Store)(nil)
	_ storage.OpenMetadataAccess      = (*Store)(nil)
	_ storage.ReferenceIdentity       = (*retainedFile)(nil)
	_ storage.ReferenceIdentity       = (*retainedNodeReference)(nil)
)

func (s *Store) CheckBackendIdentity() error         { return s.CheckFileStore() }
func (s *Store) CheckStableReferenceIdentity() error { return s.CheckFileStore() }
func (s *Store) CheckOpenMetadataAccess() error      { return s.CheckFileStore() }

func (s *Store) BackendIdentity(ctx context.Context) (storage.BackendIdentityResult, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if s.fileDomain == nil || s.root <= 0 {
		return storage.BackendIdentityResult{}, syscall.ESTALE
	}
	var result storage.BackendIdentityResult
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = s.captureBackendIdentity(ctx, tx)
		return err
	})
	if err != nil {
		return storage.BackendIdentityResult{}, sqlerr.Failure(err)
	}
	return result, nil
}

func (s *Store) captureBackendIdentity(ctx context.Context, tx *sql.Tx) (storage.BackendIdentityResult, error) {
	state, err := dbstate.Read(ctx, tx)
	if err != nil {
		return storage.BackendIdentityResult{}, err
	}
	var roots int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM volumes v JOIN nodes n ON n.id=v.root
		WHERE v.id=? AND typeof(v.root)='integer' AND v.root=? AND n.volume=v.id
		AND typeof(n.kind)='integer' AND n.kind=? AND typeof(n.detached)='integer' AND n.detached=0`,
		s.volume, s.root, storage.NodeDirectory).Scan(&roots); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	if roots != 1 || state.NodeHighWater < s.root {
		return storage.BackendIdentityResult{}, syscall.EIO
	}
	result := storage.BackendIdentityResult{
		Volume:    storage.VolumeID(state.DatabaseID + "/" + strconv.FormatInt(s.root, 10)),
		Authority: s.fileDomain.authority, RootNodeID: uint64(s.root),
	}
	if err := result.Check(); err != nil {
		return storage.BackendIdentityResult{}, err
	}
	return result, nil
}

func (s *Store) VolumeCreatedAt(ctx context.Context) (time.Time, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return time.Time{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return time.Time{}, err
	}
	if s.fileDomain == nil || s.root <= 0 {
		return time.Time{}, syscall.ESTALE
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return time.Time{}, err
	}
	var result time.Time
	known := false
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		if _, err := s.captureBackendIdentity(ctx, tx); err != nil {
			return err
		}
		var secondsClass, nanosecondsClass string
		var seconds, nanoseconds sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT typeof(created_sec),typeof(created_nsec),
			CASE WHEN typeof(created_sec)='integer' THEN created_sec END,
			CASE WHEN typeof(created_nsec)='integer' THEN created_nsec END
			FROM volumes WHERE id=? AND root=?`, s.volume, s.root).
			Scan(&secondsClass, &nanosecondsClass, &seconds, &nanoseconds); err != nil {
			return err
		}
		if secondsClass == "null" && nanosecondsClass == "null" {
			return nil
		}
		if secondsClass != "integer" || nanosecondsClass != "integer" || !seconds.Valid || !nanoseconds.Valid ||
			nanoseconds.Int64 < 0 || nanoseconds.Int64 >= int64(time.Second) {
			return syscall.EIO
		}
		result = time.Unix(seconds.Int64, nanoseconds.Int64).UTC()
		known = true
		return nil
	})
	if err == nil {
		err = metastore.CheckFilePublication(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return time.Time{}, sqlerr.Failure(err)
	}
	if !known {
		return time.Time{}, syscall.EOPNOTSUPP
	}
	return result, nil
}
