package schema

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	"github.com/codetreker/remote-fs/packages/storage"
)

func validateMetadataIntegrity(ctx context.Context, db sqlvalue.Queryer, volume *int64, limit int64) error {
	if err := validateMetadataAccounting(ctx, db); err != nil {
		return err
	}
	if err := validateMetadataLengths(ctx, db, volume); err != nil {
		return err
	}
	where := ""
	var args []any
	if volume != nil {
		where = "WHERE v.id=?"
		args = []any{*volume}
	}
	rows, err := db.QueryContext(ctx, `SELECT v.id,
		CASE WHEN typeof(v.metadata_used)='integer' THEN v.metadata_used END,typeof(v.metadata_used),
		coalesce((SELECT sum(length(metadata)+length(link_target)) FROM nodes n WHERE n.volume=v.id),0),
		coalesce((SELECT sum(coalesce(length(metadata),0)+coalesce(length(link_target),0)) FROM changes c WHERE c.volume=v.id),0)
		FROM volumes v `+where, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, nodes, history int64
		var raw any
		var class string
		if err := rows.Scan(&id, &raw, &class, &nodes, &history); err != nil {
			rows.Close()
			return err
		}
		used, valid := sqlvalue.StoredInteger(raw, class)
		if !valid || used < 0 || nodes < 0 || history < 0 || history > math.MaxInt64-nodes || used != nodes+history {
			rows.Close()
			return fmt.Errorf("volume %d has inconsistent stored metadata accounting: %w", id, syscall.EIO)
		}
		if used > limit {
			rows.Close()
			return fmt.Errorf("volume %d metadata exceeds its %d-byte limit: %w", id, limit, syscall.EFBIG)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return validateMetadataPayloads(ctx, db, volume)
}

func validateMetadataLengths(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume=?"
		args = []any{*volume}
	}
	for _, table := range []string{"nodes", "changes"} {
		types := `typeof(metadata)='blob' AND typeof(link_target)='blob'`
		if table == "changes" {
			types = `(node IS NULL AND metadata IS NULL AND link_target IS NULL) OR (node IS NOT NULL AND ` + types + `)`
		}
		var invalid, metadataBytes, targetBytes int64
		if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(CASE WHEN `+types+` THEN 0 ELSE 1 END),0),
			coalesce(max(CASE WHEN typeof(metadata)='blob' THEN length(metadata) ELSE 0 END),0),
			coalesce(max(CASE WHEN typeof(link_target)='blob' THEN length(link_target) ELSE 0 END),0)
			FROM `+table+where, args...).Scan(&invalid, &metadataBytes, &targetBytes); err != nil {
			return err
		}
		if invalid != 0 {
			return fmt.Errorf("invalid metadata payload storage class: %w", syscall.EIO)
		}
		if metadataBytes > storage.MaxMetadataBytes || targetBytes > storage.MaxLinkTargetBytes {
			return fmt.Errorf("stored metadata payload exceeds its bound: %w", syscall.EFBIG)
		}
	}
	return nil
}

func validateMetadataPayloads(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume=?"
		args = []any{*volume, *volume}
	}
	projection := fmt.Sprintf(`
		CASE WHEN typeof(metadata)='blob' AND length(metadata)<=%d THEN metadata END,
		typeof(metadata),coalesce(length(metadata),0),
		typeof(link_target),coalesce(length(link_target),0)`, storage.MaxMetadataBytes)
	query := `SELECT 1,` + projection + ` FROM nodes` + where +
		` UNION ALL SELECT CASE WHEN node IS NULL THEN 0 ELSE 1 END,` + projection + ` FROM changes` + where
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var present int
		var encoded []byte
		var metadataType, targetType string
		var metadataBytes, targetBytes int64
		if err := rows.Scan(&present, &encoded, &metadataType, &metadataBytes, &targetType, &targetBytes); err != nil {
			return err
		}
		if present == 0 {
			if metadataType != "null" || targetType != "null" {
				return fmt.Errorf("removed history retains node payload: %w", syscall.EIO)
			}
			continue
		}
		if metadataType != "blob" || targetType != "blob" {
			return fmt.Errorf("invalid metadata payload storage class: %w", syscall.EIO)
		}
		if metadataBytes > storage.MaxMetadataBytes || targetBytes > storage.MaxLinkTargetBytes {
			return fmt.Errorf("stored metadata payload exceeds its bound: %w", syscall.EFBIG)
		}
		values, err := storage.DecodeMetadata(encoded)
		if err != nil {
			return err
		}
		for _, value := range values {
			if len(value.Version) != 8 || binary.BigEndian.Uint64(value.Version) == 0 {
				return fmt.Errorf("stored metadata has an invalid native version counter: %w", syscall.EIO)
			}
		}
	}
	return rows.Err()
}

// Accounting is part of the stored format: a removed or changed trigger must be
// rejected before a writer can publish a delta that no longer updates its counter.
func validateMetadataAccounting(ctx context.Context, db sqlvalue.Queryer) error {
	body, err := migrationFiles.ReadFile("migrations/0006_client_capabilities.sql")
	if err != nil {
		return err
	}
	source := string(body)
	for _, table := range []string{"nodes", "changes"} {
		for _, action := range []string{"insert", "update", "delete"} {
			name := table + "_metadata_" + action
			start := strings.Index(source, "CREATE TRIGGER "+name+" ")
			if start < 0 {
				return fmt.Errorf("metadata accounting definition %s is missing: %w", name, syscall.EIO)
			}
			definition := source[start:]
			end := strings.Index(definition, "\nEND;")
			if end < 0 {
				return fmt.Errorf("metadata accounting definition %s is incomplete: %w", name, syscall.EIO)
			}
			definition = definition[:end+4]
			var actual sql.NullString
			err := db.QueryRowContext(ctx, `SELECT CASE WHEN length(CAST(sql AS BLOB))<=8192 THEN sql END FROM sqlite_schema WHERE type='trigger' AND name=?`, name).Scan(&actual)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("metadata accounting trigger %s is missing: %w", name, syscall.EIO)
			}
			if err != nil {
				return fmt.Errorf("reading metadata accounting trigger %s: %w", name, err)
			}
			if !actual.Valid || sqliteschema.Structure(actual.String) != sqliteschema.Structure(definition) {
				return fmt.Errorf("metadata accounting trigger %s does not match its schema: %w", name, syscall.EIO)
			}
		}
	}
	return nil
}
