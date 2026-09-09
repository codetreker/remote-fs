// Package sqlvalue defines the SQL value conventions shared by the metadata modules.
// Queryers and transactions remain owned by the caller.
package sqlvalue

import (
	"context"
	"database/sql"
	"fmt"
	"syscall"
)

type Queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// A missing row would otherwise leave a replica silently divergent after a successful statement.
func ExactlyOne(result sql.Result, subject string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("%w: the change acts on %s", syscall.EIO, subject)
	}
	return nil
}
