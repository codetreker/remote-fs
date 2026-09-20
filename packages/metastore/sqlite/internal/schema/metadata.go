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

func validateMetadataIntegrity(ctx context.Context, db sqlvalue.Queryer, volume *int64, limit int64, opaqueVersions bool) error {
	if err := validateMetadataPayloads(ctx, db, volume, opaqueVersions); err != nil {
		return err
	}
	if err := validateMetadataAccounting(ctx, db); err != nil {
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
		coalesce((SELECT sum(length(metadata)) FROM nodes n WHERE n.volume=v.id),0),
		coalesce((SELECT sum(coalesce(length(metadata),0)) FROM changes c WHERE c.volume=v.id),0)
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
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	return nil
}

func validateMetadataPayloads(ctx context.Context, db sqlvalue.Queryer, volume *int64, opaqueVersions bool) error {
	where := ""
	var args []any
	if volume != nil {
		where = " WHERE volume=?"
		args = []any{*volume, *volume}
	}
	shape := `SELECT 1,typeof(metadata),CASE WHEN typeof(metadata)='blob' THEN length(metadata) END FROM nodes` + where +
		` UNION ALL SELECT CASE WHEN node IS NULL THEN 0 ELSE 1 END,typeof(metadata),
		CASE WHEN typeof(metadata)='blob' THEN length(metadata) END FROM changes` + where
	rows, err := db.QueryContext(ctx, shape, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var present int
		var metadataType string
		var metadataBytes sql.NullInt64
		if err := rows.Scan(&present, &metadataType, &metadataBytes); err != nil {
			rows.Close()
			return err
		}
		if present == 0 {
			if metadataType != "null" {
				rows.Close()
				return fmt.Errorf("removed history retains node metadata: %w", syscall.EIO)
			}
			continue
		}
		if metadataType != "blob" || !metadataBytes.Valid {
			rows.Close()
			return fmt.Errorf("invalid metadata payload storage class: %w", syscall.EIO)
		}
		if metadataBytes.Int64 < 6 || metadataBytes.Int64 > storage.MaxMetadataBytes {
			rows.Close()
			return fmt.Errorf("stored metadata payload exceeds its bound: %w", syscall.EFBIG)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}

	query := `SELECT metadata FROM nodes` + where +
		` UNION ALL SELECT metadata FROM changes` + where + ` AND node IS NOT NULL`
	if volume == nil {
		query = `SELECT metadata FROM nodes UNION ALL SELECT metadata FROM changes WHERE node IS NOT NULL`
	}
	rows, err = db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return err
		}
		values, err := storage.DecodeMetadata(encoded)
		if err != nil {
			return err
		}
		for _, value := range values {
			valid := len(value.Version) != 0 && len(value.Version) <= storage.MaxObservationTokenBytes
			if !opaqueVersions {
				valid = len(value.Version) == 8 && binary.BigEndian.Uint64(value.Version) != 0
			}
			if !valid {
				return fmt.Errorf("stored metadata has an invalid version token: %w", syscall.EIO)
			}
		}
	}
	return rows.Err()
}

func validateMetadataAccounting(ctx context.Context, db sqlvalue.Queryer) error {
	body, err := migrationFiles.ReadFile("migrations/0006_neutral_metadata.sql")
	if err != nil {
		return err
	}
	source := string(body)
	var unexpected int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name NOT IN (
		'nodes_metadata_insert','nodes_metadata_update','nodes_metadata_delete',
		'changes_metadata_insert','changes_metadata_update','changes_metadata_delete'
	)`).Scan(&unexpected); err != nil {
		return err
	}
	if unexpected != 0 {
		return fmt.Errorf("the database holds %d unexpected persistent triggers: %w", unexpected, syscall.EIO)
	}
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
