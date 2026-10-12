package sqlite

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.VolumeSummary = (*Store)(nil)
var _ storage.QuotaControl = (*Store)(nil)
var _ storage.ActionNamespaces = (*Store)(nil)

func (s *Store) CheckVolumeSummary() error {
	if s.quotaPolicy == FixedAllowancePolicy && s.allowance == 0 {
		return syscall.ENOSYS
	}
	return s.CheckFileStore()
}
func (s *Store) CheckQuotaControl() error {
	if s.quotaPolicy != DurableQuotaPolicy {
		return syscall.ENOSYS
	}
	return s.CheckFileStore()
}
func (s *Store) CheckActionNamespaces() error { return s.CheckFileStore() }

// The commit gate is held while constructor policy is verified or seeded.
func (s *Store) openQuotaPolicy(ctx context.Context, options Options) error {
	var quota sql.NullInt64
	var revision int64
	if err := s.write.QueryRowContext(ctx, `SELECT quota_bytes,quota_revision FROM volumes WHERE id=?`, s.volume).Scan(&quota, &revision); err != nil {
		return err
	}
	if options.QuotaPolicy == FixedAllowancePolicy {
		if quota.Valid || revision != 0 {
			return syscall.EINVAL
		}
		return nil
	}
	if quota.Valid {
		if revision < 1 || quota.Int64 < 0 {
			return syscall.EIO
		}
		return nil
	}
	if revision != 0 {
		return syscall.EIO
	}
	if !options.InitializeQuota {
		return syscall.EIO
	}
	return s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE volumes SET quota_bytes=?,quota_revision=1 WHERE id=? AND quota_bytes IS NULL AND quota_revision=0`, s.allowance, s.volume)
		return err
	})
}

func (s *Store) effectiveQuota(ctx context.Context, tx *sql.Tx) (int64, error) {
	if s.quotaPolicy == FixedAllowancePolicy {
		return s.allowance, nil
	}
	var quota, revision int64
	if err := tx.QueryRowContext(ctx, `SELECT quota_bytes,quota_revision FROM volumes WHERE id=?`, s.volume).Scan(&quota, &revision); err != nil {
		return 0, err
	}
	if quota < 0 || revision < 1 {
		return 0, syscall.EIO
	}
	return quota, nil
}

func (s *Store) Summary(ctx context.Context) (storage.VolumeSummaryResult, error) {
	var result storage.VolumeSummaryResult
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		if s.quotaPolicy == FixedAllowancePolicy && s.allowance == 0 {
			return syscall.ENOSYS
		}
		var files, dirs, used, revision int64
		var quota sql.NullInt64
		err := tx.QueryRowContext(ctx, `SELECT named_files,named_directories,allocated_used,quota_bytes,quota_revision FROM volumes WHERE id=?`, s.volume).Scan(&files, &dirs, &used, &quota, &revision)
		if err != nil {
			return err
		}
		if files < 0 || dirs < 0 || used < 0 {
			return syscall.EIO
		}
		if s.quotaPolicy == DurableQuotaPolicy {
			if !quota.Valid || quota.Int64 < 0 || revision < 1 {
				return syscall.EIO
			}
		} else {
			if quota.Valid || revision != 0 {
				return syscall.EIO
			}
			quota.Int64 = s.allowance
			revision = 1
		}
		result = storage.VolumeSummaryResult{FileCount: uint64(files), DirectoryCount: uint64(dirs), AllocationUsed: used, QuotaBytes: quota.Int64, QuotaRevision: uint64(revision)}
		return nil
	})
	if err != nil {
		return storage.VolumeSummaryResult{}, err
	}
	return result, nil
}
