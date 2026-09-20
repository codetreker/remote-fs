package schema

import (
	"context"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateLegacyModeMapping(ctx context.Context, db sqlvalue.Queryer, version int) error {
	allowed := int64(fs.ModeDir | fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
	tables := []string{"nodes"}
	if version >= 2 {
		tables = append(tables, "changes")
	}
	for _, table := range tables {
		where := ""
		if table == "changes" {
			where = "node IS NOT NULL AND "
		}
		var invalid int64
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` WHERE `+where+`(
			typeof(mode)!='integer' OR mode<0 OR (mode & ~?)!=0 OR
			typeof(size)!='integer' OR typeof(atime_sec)!='integer' OR typeof(atime_nsec)!='integer' OR
			typeof(mtime_sec)!='integer' OR typeof(mtime_nsec)!='integer' OR typeof(content) NOT IN ('text','null')
		)`, allowed).Scan(&invalid); err != nil {
			return err
		}
		if invalid != 0 {
			return fmt.Errorf("%d historical %s rows cannot preserve their type and permissions during migration: %w", invalid, table, syscall.EIO)
		}
	}
	return nil
}
