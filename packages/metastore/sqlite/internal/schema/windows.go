package schema

import (
	"context"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func validateWindowsNodes(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	args := []any{}
	if volume != nil {
		where = "volume=? AND "
		args = append(args, *volume)
	}
	args = append(args, int64(storage.WindowsSettableDOSAttributes|storage.WindowsDOSDirectory), int64(storage.WindowsDOSDirectory), int64(fs.ModeType), int64(fs.ModeDir), int64(fs.ModeSymlink), int64(fs.ModeType), int64(fs.ModeSymlink), int64(fs.ModeType), int64(fs.ModeSymlink), storage.WindowsMaxLinkTargetBytes)
	var invalid int64
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM nodes WHERE `+where+`(
		typeof(windows_creation_sec)!='integer' OR typeof(windows_creation_nsec)!='integer' OR
		typeof(windows_change_sec)!='integer' OR typeof(windows_change_nsec)!='integer' OR
		typeof(windows_attributes)!='integer' OR windows_attributes<0 OR windows_attributes & ~? !=0 OR
		((windows_attributes & ?)!=0 AND (mode & ?) NOT IN (?,?)) OR
		windows_creation_nsec<0 OR windows_creation_nsec>=1000000000 OR windows_change_nsec<0 OR windows_change_nsec>=1000000000 OR
		typeof(windows_link_target)!='blob' OR
		((mode & ?)!=? AND length(windows_link_target)!=0) OR
		((mode & ?)=? AND (content IS NOT NULL OR length(windows_link_target)!=size OR size=0 OR size>?))
	)`, args...).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("the database holds %d nodes with invalid Windows metadata: %w", invalid, syscall.EIO)
	}
	return validateWindowsLinkTargets(ctx, db, volume)
}

func validateWindowsLinkTargets(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	args := []any{storage.WindowsMaxLinkTargetBytes, int64(fs.ModeType), int64(fs.ModeSymlink)}
	if volume != nil {
		where = " AND volume=?"
		args = append(args, *volume)
	}
	rows, err := db.QueryContext(ctx, `SELECT length(windows_link_target),CASE WHEN length(windows_link_target)<=? THEN windows_link_target ELSE NULL END FROM nodes WHERE (mode & ?)=?`+where, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var length int64
		var target []byte
		if err := rows.Scan(&length, &target); err != nil {
			return err
		}
		if length != int64(len(target)) {
			return fmt.Errorf("Windows link exceeds its target bound: %w", syscall.EIO)
		}
		if err := storage.CheckWindowsLinkTarget(string(target)); err != nil {
			return fmt.Errorf("invalid persisted Windows link target: %w: %w", syscall.EIO, err)
		}
	}
	return rows.Err()
}
