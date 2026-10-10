package sqlite

import (
	"context"
	"database/sql"
	"strconv"
	"syscall"

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
		state, err := dbstate.Read(ctx, tx)
		if err != nil {
			return err
		}
		var roots int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM volumes v JOIN nodes n ON n.id=v.root
			WHERE v.id=? AND typeof(v.root)='integer' AND v.root=? AND n.volume=v.id
			AND typeof(n.kind)='integer' AND n.kind=? AND typeof(n.detached)='integer' AND n.detached=0`,
			s.volume, s.root, storage.NodeDirectory).Scan(&roots); err != nil {
			return err
		}
		if roots != 1 || state.NodeHighWater < s.root {
			return syscall.EIO
		}
		result = storage.BackendIdentityResult{
			Volume:    storage.VolumeID(state.DatabaseID + "/" + strconv.FormatInt(s.root, 10)),
			Authority: s.fileDomain.authority, RootNodeID: uint64(s.root),
		}
		return result.Check()
	})
	if err != nil {
		return storage.BackendIdentityResult{}, sqlerr.Failure(err)
	}
	return result, nil
}
