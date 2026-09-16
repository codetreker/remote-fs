package schema

import (
	"context"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// Migration must reject source values that would disappear when mode becomes
// a generic kind and the versioned POSIX permission payload.
func validateLegacyModeMapping(ctx context.Context, db sqlvalue.Queryer) error {
	allowed := int64(fs.ModeDir | fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
	var invalid int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM nodes WHERE
		typeof(mode) != 'integer' OR mode < 0 OR (mode & ~?) != 0 OR
		typeof(size) != 'integer' OR typeof(atime_sec) != 'integer' OR typeof(atime_nsec) != 'integer' OR
		typeof(mtime_sec) != 'integer' OR typeof(mtime_nsec) != 'integer' OR typeof(content) NOT IN ('text','null')`, allowed).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("%d historical nodes cannot be mapped without losing type or permission facts: %w", invalid, syscall.EIO)
	}
	return nil
}
