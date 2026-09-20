package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

func validateDurableIdentity(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = "WHERE i.volume=? AND "
		args = []any{*volume}
	} else {
		where = "WHERE "
	}
	args = append(args,
		storage.DeleteIntentArmed, storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed,
		storage.DeleteIntentPending, storage.DeleteIntentCleanupFailed,
	)
	var invalid int64
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM delete_intents i
		LEFT JOIN volumes v ON v.id=i.volume
		LEFT JOIN nodes n ON n.id=i.node AND n.volume=i.volume `+where+`(
		v.id IS NULL OR
		(i.outcome IN (?,?,?) AND n.id IS NULL) OR
		(i.outcome IN (?,?) AND n.pending_generation=0) OR
		(i.outcome!=5 AND i.failure IS NOT NULL)
	)`, args...).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("the database holds %d deletion intents with invalid durable relationships: %w", invalid, syscall.EIO)
	}
	failureWhere := "WHERE outcome=?"
	failureArgs := []any{storage.DeleteIntentCleanupFailed}
	if volume != nil {
		failureWhere += " AND volume=?"
		failureArgs = append(failureArgs, *volume)
	}
	rows, err := db.QueryContext(ctx, `SELECT failure FROM delete_intents `+failureWhere, failureArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var failure sql.NullInt64
		if err := rows.Scan(&failure); err != nil {
			rows.Close()
			return err
		}
		if !failure.Valid || failure.Int64 <= 0 {
			rows.Close()
			return fmt.Errorf("a cleanup-failed deletion intent has no failure classification: %w", syscall.EIO)
		}
		if _, ok := storage.ErrnoName(syscall.Errno(failure.Int64)); !ok {
			rows.Close()
			return fmt.Errorf("a cleanup-failed deletion intent has unknown errno %d: %w", failure.Int64, syscall.EIO)
		}
	}
	return errors.Join(rows.Err(), rows.Close())
}
