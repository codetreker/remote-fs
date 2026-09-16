package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// Recorded versions select exact known column layouts. Extra columns are not a
// second interpretation of the same version and are never repaired in place.
func validateColumnLayout(ctx context.Context, db sqlvalue.Queryer, version int) error {
	tables := []struct{ name, columns string }{
		{"schema_version", "version"},
		{"volumes", "id name:TEXT root used"},
		{"objects", "key:TEXT volume state size digest:BLOB created_sec created_nsec"},
	}
	nodes := "id volume mode size atime_sec atime_nsec mtime_sec mtime_nsec content:TEXT"
	entries := "parent name:BLOB node"
	if version >= 2 {
		entries = "volume " + entries
	}
	if version >= firstRetainedFileSchemaVersion {
		nodes += " detached content_revision"
	}
	if version >= firstSharedFileSchemaVersion {
		nodes = strings.Replace(nodes, " mode ", " ", 1) +
			" kind metadata_revision directory_revision creation_sec creation_nsec change_sec change_nsec metadata:BLOB link_target:BLOB"
		entries += " id draining drain_generation drain_if_empty drain_authority:TEXT"
	}
	tables = append(tables, struct{ name, columns string }{"nodes", nodes}, struct{ name, columns string }{"entries", entries})
	if version >= 2 {
		changeColumns := "position volume kind parent name:BLOB from_parent from_name:BLOB node mode size atime_sec atime_nsec mtime_sec mtime_nsec content:TEXT recorded_sec recorded_nsec"
		if version >= firstOwnershipAwareSchemaVersion {
			changeColumns = strings.Replace(changeColumns, "position ", "position previous_position ", 1)
		}
		if version >= firstSharedFileSchemaVersion {
			changeColumns = "position previous_position volume kind parent name:BLOB from_parent from_name:BLOB node node_kind size " +
				"atime_sec atime_nsec mtime_sec mtime_nsec creation_sec creation_nsec change_sec change_nsec " +
				"metadata_revision directory_revision content:TEXT metadata:BLOB link_target:BLOB recorded_sec recorded_nsec identity_high_water notification:BLOB"
		}
		tables = append(tables,
			struct{ name, columns string }{"changes", changeColumns},
			struct{ name, columns string }{"logs", "volume incarnation:TEXT committed_position trimmed_through trimmed_by_age"},
		)
	}
	if version >= firstOwnershipAwareSchemaVersion {
		tables = append(tables,
			struct{ name, columns string }{"backing_store", "singleton store_id:TEXT"},
			struct{ name, columns string }{"database_state", "singleton database_id:TEXT generation node_high_water change_high_water"},
		)
	}
	if version >= 4 {
		tables = append(tables, struct{ name, columns string }{"lease_recovery",
			"singleton database_id:TEXT state_id:TEXT accepted_generation accepted_nanos prepared_generation prepared_nanos"})
	}
	if version >= firstSharedFileSchemaVersion {
		tables = append(tables,
			struct{ name, columns string }{"removal_intents", "volume reference:TEXT token:TEXT entry if_empty authority:TEXT"},
			struct{ name, columns string }{"file_lease_recovery", "singleton database_id:TEXT state_id:TEXT accepted_generation accepted_nanos accepted_quiescent prepared_generation prepared_nanos prepared_quiescent"},
			struct{ name, columns string }{"file_lease_initialization", "singleton state"},
		)
	}
	for _, table := range tables {
		if err := validateTableColumns(ctx, db, table.name, strings.Fields(table.columns), version); err != nil {
			return err
		}
	}
	return nil
}

func validateTableColumns(ctx context.Context, db sqlvalue.Queryer, table string, columns []string, version int) error {
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?`, table).Scan(&tables); err != nil {
		return err
	}
	if tables != 1 {
		return fmt.Errorf("recorded version %d requires table %s: %w", version, table, syscall.EIO)
	}
	rows, err := db.QueryContext(ctx, `SELECT
		CASE WHEN length(CAST(name AS BLOB)) <= 64 THEN name END,
		CASE WHEN length(CAST(type AS BLOB)) <= 16 THEN type END
		FROM pragma_table_info(?) ORDER BY cid LIMIT ?`, table, len(columns)+1)
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		var name, declared sql.NullString
		if err := rows.Scan(&name, &declared); err != nil {
			rows.Close()
			return err
		}
		if count >= len(columns) || !name.Valid || !declared.Valid {
			rows.Close()
			return fmt.Errorf("table %s does not have the recorded version %d column layout: %w", table, version, syscall.EIO)
		}
		wantName, wantType, typed := strings.Cut(columns[count], ":")
		if !typed {
			wantType = "INTEGER"
		}
		if name.String != wantName || strings.ToUpper(declared.String) != wantType {
			rows.Close()
			return fmt.Errorf("table %s column %d is %s %s, expected %s %s for recorded version %d: %w",
				table, count, name.String, declared.String, wantName, wantType, version, syscall.EIO)
		}
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if count != len(columns) {
		return fmt.Errorf("table %s has %d columns, expected %d for recorded version %d: %w", table, count, len(columns), version, syscall.EIO)
	}
	return nil
}
