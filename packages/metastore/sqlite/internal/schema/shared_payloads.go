package schema

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Shared payloads are admitted by storage class and byte length before any
// decoder reads their contents. Names and change payloads use the same budget.
func admitSharedPayloads(ctx context.Context, db sqlvalue.Queryer, volume *int64, remaining int64) (int64, error) {
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume = ?"
		args = []any{*volume}
	}
	for _, source := range []struct {
		name, table, invalid, length string
	}{
		{"node metadata and link targets", "nodes",
			`typeof(metadata) != 'blob' OR length(metadata) < 6 OR length(metadata) > 32768 OR
			 typeof(link_target) != 'blob' OR length(link_target) > 4096`,
			`length(metadata) + length(link_target)`},
		{"entry drain authorities", "entries", `typeof(drain_authority) != 'text' OR length(CAST(drain_authority AS BLOB)) > 128`,
			`length(CAST(drain_authority AS BLOB))`},
		{"removal intention identities", "removal_intents",
			`typeof(reference) != 'text' OR typeof(token) != 'text' OR typeof(authority) != 'text' OR
			 length(CAST(reference AS BLOB)) NOT BETWEEN 1 AND 128 OR
			 length(CAST(token AS BLOB)) NOT BETWEEN 1 AND 128 OR
			 length(CAST(authority AS BLOB)) NOT BETWEEN 1 AND 128`,
			`length(CAST(reference AS BLOB)) + length(CAST(token AS BLOB)) + length(CAST(authority AS BLOB))`},
	} {
		sourceWhere, sourceArgs := where, args
		if volume != nil && source.table == "entries" {
			sourceWhere = ` WHERE volume=? OR parent IN (SELECT id FROM nodes WHERE volume=?) OR node IN (SELECT id FROM nodes WHERE volume=?)`
			sourceArgs = []any{*volume, *volume, *volume}
		}
		if volume != nil && source.table == "removal_intents" {
			sourceWhere = ` WHERE volume=? OR entry IN (SELECT id FROM entries WHERE volume=?)`
			sourceArgs = []any{*volume, *volume}
		}
		var invalid, size int64
		query := `SELECT count(CASE WHEN ` + source.invalid + ` THEN 1 END),
			coalesce(sum(` + source.length + `), 0) FROM ` + source.table + sourceWhere
		if err := db.QueryRowContext(ctx, query, sourceArgs...).Scan(&invalid, &size); err != nil {
			return 0, err
		}
		if invalid != 0 || size < 0 {
			return 0, fmt.Errorf("invalid %s storage or payload bounds: %w", source.name, syscall.EIO)
		}
		if size > remaining {
			return 0, fmt.Errorf("%s exceed the remaining integrity byte limit: %w", source.name, syscall.EFBIG)
		}
		remaining -= size
	}
	return remaining, nil
}

func validateSharedNodePayloads(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume = ?"
		args = []any{*volume}
	}
	rows, err := db.QueryContext(ctx, `SELECT length(metadata), CASE
		WHEN typeof(metadata) = 'blob' AND length(metadata) BETWEEN 6 AND 32768 THEN metadata END
		FROM nodes`+where, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var length int64
		var encoded []byte
		if err := rows.Scan(&length, &encoded); err != nil {
			rows.Close()
			return err
		}
		if length != int64(len(encoded)) {
			rows.Close()
			return fmt.Errorf("node metadata cannot be read within its declared bound: %w", syscall.EIO)
		}
		if _, err := storage.DecodeMetadata(encoded); err != nil {
			rows.Close()
			return err
		}
	}
	return errors.Join(rows.Err(), rows.Close())
}
