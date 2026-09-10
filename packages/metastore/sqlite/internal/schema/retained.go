package schema

import (
	"context"
	"database/sql"
)

// The exclusive database owner retires references from every previous serving epoch. The
// complete graph and its byte totals must be validated in this transaction before reaping.
func reapDetachedFiles(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `UPDATE objects SET state = ?
		WHERE key IN (SELECT content FROM nodes WHERE detached = 1 AND content IS NOT NULL)`, StateGarbage); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE volumes SET used = used - retained.size
		FROM (SELECT volume, sum(size) AS size FROM nodes WHERE detached = 1 GROUP BY volume) retained
		WHERE volumes.id = retained.volume`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM nodes WHERE detached = 1`)
	return err
}
