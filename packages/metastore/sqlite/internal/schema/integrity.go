package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// validateIntegrityWork counts the retained graph with scalar aggregates before any recursive
// traversal. sqlvalue.WouldExceed performs the addition without overflow. A scoped count includes every
// entry whose label, parent, or child touches the volume, so a corrupt label cannot hide
// work from the configured bound.
func validateIntegrityWork(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
	maxIntegrityRecords int64,
) error {
	var logTables int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'table' AND name IN ('logs', 'changes')`).Scan(&logTables); err != nil {
		return fmt.Errorf("reading the integrity-work schema: %w", sqlerr.ReadFailure(ctx, err))
	}
	if logTables != 0 && logTables != 2 {
		return fmt.Errorf("the database has %d of the 2 required log tables: %w", logTables, syscall.EIO)
	}

	var volumes, nodes, objects, entries, logs, changes, deleteIntents int64
	var err error
	if volume == nil {
		if logTables == 0 {
			err = db.QueryRowContext(ctx, `
				SELECT
					(SELECT count(*) FROM volumes),
					(SELECT count(*) FROM nodes),
					(SELECT count(*) FROM objects),
					(SELECT count(*) FROM entries)`).Scan(&volumes, &nodes, &objects, &entries)
		} else {
			err = db.QueryRowContext(ctx, `
				SELECT
					(SELECT count(*) FROM volumes),
					(SELECT count(*) FROM nodes),
					(SELECT count(*) FROM objects),
					(SELECT count(*) FROM entries),
					(SELECT count(*) FROM logs),
					(SELECT count(*) FROM changes)`).Scan(
				&volumes, &nodes, &objects, &entries, &logs, &changes,
			)
		}
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT
				(SELECT count(*) FROM volumes WHERE id = ?),
				(SELECT count(*) FROM nodes WHERE volume = ?),
				(SELECT count(*) FROM objects o
				 WHERE o.volume = ? OR EXISTS (
					 SELECT 1 FROM nodes n WHERE n.volume = ? AND n.content = o.key
				 )),
				(SELECT count(*)
				 FROM entries e
				 LEFT JOIN nodes parent ON parent.id = e.parent
				 LEFT JOIN nodes child ON child.id = e.node
				 WHERE e.volume = ? OR parent.volume = ? OR child.volume = ?),
				(SELECT count(*) FROM logs WHERE volume = ?),
				(SELECT count(*) FROM changes WHERE volume = ?)`,
			*volume, *volume, *volume, *volume,
			*volume, *volume, *volume, *volume, *volume).Scan(
			&volumes, &nodes, &objects, &entries, &logs, &changes,
		)
	}
	if err != nil {
		return fmt.Errorf("counting volume integrity work: %w", sqlerr.ReadFailure(ctx, err))
	}
	var identityTables int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='delete_intents'`).Scan(&identityTables); err != nil {
		return fmt.Errorf("reading the durable-identity schema: %w", sqlerr.ReadFailure(ctx, err))
	}
	if identityTables == 1 {
		query := `SELECT count(*) FROM delete_intents`
		var args []any
		if volume != nil {
			query += ` WHERE volume=?`
			args = []any{*volume}
		}
		if err := db.QueryRowContext(ctx, query, args...).Scan(&deleteIntents); err != nil {
			return fmt.Errorf("counting deletion-intent integrity work: %w", sqlerr.ReadFailure(ctx, err))
		}
	}
	if volumes < 0 || nodes < 0 || objects < 0 || entries < 0 || logs < 0 || changes < 0 || deleteIntents < 0 {
		return fmt.Errorf("the database returned a negative volume integrity count: %w", syscall.EIO)
	}
	if sqlvalue.WouldExceed(maxIntegrityRecords, volumes, nodes, objects, entries, logs, changes, deleteIntents) {
		return fmt.Errorf(
			"volume integrity requires %d volumes, %d nodes, %d objects, %d entries, %d logs, %d changes, and %d deletion intents, above the configured work limit of %d; raise MaxIntegrityRecords to open it: %w",
			volumes, nodes, objects, entries, logs, changes, deleteIntents, maxIntegrityRecords, syscall.EFBIG)
	}
	return nil
}

// validateIntegrityBytes admits variable-length names using SQLite's O(1) BLOB length before
// any content-sensitive predicate such as instr examines them. The row count has already been
// admitted, so streaming these fixed-width lengths is bounded in both records and bytes.
func validateIntegrityBytes(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
	maxIntegrityBytes int64,
	version int,
) error {
	remaining := maxIntegrityBytes
	entryWhere := ""
	changeWhere := ""
	entryArgs := []any{}
	changeArgs := []any{}
	if volume != nil {
		entryWhere = `
			LEFT JOIN nodes parent ON parent.id = e.parent
			LEFT JOIN nodes child ON child.id = e.node
			WHERE e.volume = ? OR parent.volume = ? OR child.volume = ?`
		changeWhere = "WHERE volume = ?"
		entryArgs = []any{*volume, *volume, *volume}
		changeArgs = []any{*volume}
	}
	rows, err := db.QueryContext(ctx, `
		SELECT typeof(e.name), CASE WHEN typeof(e.name) = 'blob' THEN length(e.name) END
		FROM entries e `+entryWhere, entryArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var storageClass string
		var length sql.NullInt64
		if err := rows.Scan(&storageClass, &length); err != nil {
			rows.Close()
			return err
		}
		if storageClass != "blob" || !length.Valid || length.Int64 < 0 {
			rows.Close()
			return fmt.Errorf("an entry name is stored as %s rather than a BLOB: %w", storageClass, syscall.EIO)
		}
		if length.Int64 > remaining {
			rows.Close()
			return fmt.Errorf("entry and change names exceed the %d-byte integrity limit: %w",
				maxIntegrityBytes, syscall.EFBIG)
		}
		remaining -= length.Int64
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if version < 2 {
		return nil
	}

	rows, err = db.QueryContext(ctx, `
		SELECT
			typeof(name), CASE WHEN typeof(name) = 'blob' THEN length(name) END,
			typeof(from_name), CASE WHEN typeof(from_name) = 'blob' THEN length(from_name) END
		FROM changes `+changeWhere, changeArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nameType, fromNameType string
		var nameLength, fromNameLength sql.NullInt64
		if err := rows.Scan(&nameType, &nameLength, &fromNameType, &fromNameLength); err != nil {
			rows.Close()
			return err
		}
		for _, field := range []struct {
			name         string
			storageClass string
			length       sql.NullInt64
		}{
			{"name", nameType, nameLength},
			{"from_name", fromNameType, fromNameLength},
		} {
			if field.storageClass == "null" {
				continue
			}
			if field.storageClass != "blob" || !field.length.Valid || field.length.Int64 < 0 {
				rows.Close()
				return fmt.Errorf("a retained change %s is stored as %s rather than a BLOB or NULL: %w",
					field.name, field.storageClass, syscall.EIO)
			}
			if field.length.Int64 > remaining {
				rows.Close()
				return fmt.Errorf("entry and change names exceed the %d-byte integrity limit: %w",
					maxIntegrityBytes, syscall.EFBIG)
			}
			remaining -= field.length.Int64
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if version < firstDurableIdentitySchemaVersion {
		return nil
	}
	intentWhere := ""
	var intentArgs []any
	if volume != nil {
		intentWhere = "WHERE volume=?"
		intentArgs = []any{*volume}
	}
	rows, err = db.QueryContext(ctx, `SELECT typeof(name),CASE WHEN typeof(name)='blob' THEN length(name) END
		FROM delete_intents `+intentWhere, intentArgs...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var class string
		var length sql.NullInt64
		if err := rows.Scan(&class, &length); err != nil {
			rows.Close()
			return err
		}
		if class != "blob" || !length.Valid || length.Int64 < 1 || length.Int64 > storage.MaxLeafBytes {
			rows.Close()
			return fmt.Errorf("a deletion-intent name has an invalid representation: %w", syscall.EIO)
		}
		if length.Int64 > remaining {
			rows.Close()
			return fmt.Errorf("entry, change, and deletion-intent names exceed the %d-byte integrity limit: %w",
				maxIntegrityBytes, syscall.EFBIG)
		}
		remaining -= length.Int64
	}
	return errors.Join(rows.Err(), rows.Close())
}

// validateStorageClasses rejects SQLite's dynamically typed values before any cursor or
// payload reader can coerce them into a plausible row or order them in another storage class.
func validateStorageClasses(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	return validateStorageClassesVersion(ctx, db, volume, schema.Version())
}

func validateStorageClassesVersion(ctx context.Context, db sqlvalue.Queryer, volume *int64, version int) error {
	volumeWhere := ""
	nodeWhere := ""
	objectWhere := ""
	entryWhere := ""
	logWhere := ""
	changeWhere := ""
	var scopeArgs []any
	if volume != nil {
		volumeWhere = "WHERE id = ?"
		nodeWhere = "WHERE volume = ?"
		objectWhere = `WHERE (o.volume = ? OR EXISTS (
			SELECT 1 FROM nodes n WHERE n.volume = ? AND n.content = o.key))`
		entryWhere = `WHERE (e.volume = ? OR parent.volume = ? OR child.volume = ?)`
		logWhere = "WHERE volume = ?"
		changeWhere = "WHERE volume = ?"
		scopeArgs = []any{*volume}
	}

	var invalidVolumes int64
	query := `SELECT count(*) FROM volumes ` + volumeWhere
	if volumeWhere == "" {
		query += " WHERE "
	} else {
		query += " AND "
	}
	query += `(
		typeof(id) != 'integer' OR id <= 0 OR typeof(name) != 'text' OR name = '' OR
		typeof(root) != 'integer' OR root <= 0 OR typeof(used) != 'integer'`
	if version >= firstNeutralMetadataSchemaVersion {
		query += ` OR typeof(metadata_used) != 'integer'`
	}
	query += `)`
	if err := db.QueryRowContext(ctx, query, scopeArgs...).Scan(&invalidVolumes); err != nil {
		return err
	}

	objectArgs := []any{}
	entryArgs := []any{}
	if volume != nil {
		objectArgs = []any{*volume, *volume}
		entryArgs = []any{*volume, *volume, *volume}
	}
	retainedNodeClasses := ""
	if version >= firstRetainedFileSchemaVersion {
		retainedNodeClasses = ` OR typeof(detached) != 'integer' OR typeof(content_revision) != 'integer'`
	}
	nodeKindColumn, changeKindColumn := "mode", "mode"
	neutralNodeClasses, neutralChangeClasses := "", ""
	if version >= firstNeutralMetadataSchemaVersion {
		nodeKindColumn, changeKindColumn = "kind", "node_kind"
		neutralNodeClasses = ` OR typeof(birth_sec) NOT IN ('integer','null') OR typeof(birth_nsec) NOT IN ('integer','null')
			OR typeof(change_sec) NOT IN ('integer','null') OR typeof(change_nsec) NOT IN ('integer','null')
			OR typeof(metadata)!='blob'`
		neutralChangeClasses = ` OR typeof(birth_sec) NOT IN ('integer','null') OR typeof(birth_nsec) NOT IN ('integer','null')
			OR typeof(change_sec) NOT IN ('integer','null') OR typeof(change_nsec) NOT IN ('integer','null')
			OR typeof(metadata) NOT IN ('blob','null')`
	}
	if version >= firstDurableIdentitySchemaVersion {
		neutralNodeClasses += ` OR typeof(link_target)!='blob' OR typeof(pending_unlink)!='integer'
			OR typeof(pending_generation)!='integer'`
		neutralChangeClasses += ` OR typeof(link_target) NOT IN ('blob','null')`
	}
	if version >= firstDirectoryRevisionSchemaVersion {
		neutralNodeClasses += ` OR typeof(directory_revision)!='blob'`
		neutralChangeClasses += ` OR typeof(directory_revision) NOT IN ('blob','null')`
	}
	queries := []struct {
		name  string
		query string
		args  []any
	}{
		{"nodes", `SELECT count(*) FROM nodes ` + nodeWhere + predicateJoin(nodeWhere) + `(
			typeof(id) != 'integer' OR id <= 0 OR
			typeof(volume) != 'integer' OR volume <= 0 OR
			typeof(` + nodeKindColumn + `) != 'integer' OR typeof(size) != 'integer' OR
			typeof(atime_sec) != 'integer' OR typeof(atime_nsec) != 'integer' OR
			typeof(mtime_sec) != 'integer' OR typeof(mtime_nsec) != 'integer' OR
			typeof(content) NOT IN ('text', 'null')` + retainedNodeClasses + neutralNodeClasses + `)`, scopeArgs},
		{"objects", `SELECT count(*) FROM objects o ` + objectWhere + predicateJoin(objectWhere) + `(
			typeof(o.key) != 'text' OR o.key = '' OR
			typeof(o.volume) != 'integer' OR o.volume <= 0 OR
			typeof(o.state) != 'integer' OR typeof(o.size) != 'integer' OR
			typeof(o.digest) NOT IN ('blob', 'null') OR
			typeof(o.created_sec) != 'integer' OR typeof(o.created_nsec) != 'integer')`,
			objectArgs},
		{"entries", `SELECT count(*) FROM entries e
		 LEFT JOIN nodes parent ON parent.id = e.parent
		 LEFT JOIN nodes child ON child.id = e.node ` + entryWhere + predicateJoin(entryWhere) + `(
			typeof(e.volume) != 'integer' OR e.volume <= 0 OR
			typeof(e.parent) != 'integer' OR e.parent <= 0 OR
			typeof(e.name) != 'blob' OR length(e.name) = 0 OR length(e.name) > 4096 OR
			e.name IN (X'2e', X'2e2e') OR instr(e.name, X'2f') != 0 OR instr(e.name, X'00') != 0 OR
			typeof(e.node) != 'integer' OR e.node <= 0)`,
			entryArgs},
		{"logs", `SELECT count(*) FROM logs ` + logWhere + predicateJoin(logWhere) + `(
			typeof(volume) != 'integer' OR volume <= 0 OR
			typeof(incarnation) != 'text' OR incarnation = '' OR
			typeof(committed_position) != 'integer' OR typeof(trimmed_through) != 'integer' OR
			typeof(trimmed_by_age) != 'integer')`, scopeArgs},
		{"changes", `SELECT count(*) FROM changes ` + changeWhere + predicateJoin(changeWhere) + `(
			typeof(position) != 'integer' OR position <= 0 OR
			typeof(previous_position) != 'integer' OR previous_position < 0 OR
			typeof(volume) != 'integer' OR volume <= 0 OR
			typeof(kind) != 'integer' OR typeof(parent) != 'integer' OR
			typeof(name) NOT IN ('blob', 'null') OR
			length(name)>4096 OR
			typeof(from_parent) NOT IN ('integer', 'null') OR
			typeof(from_name) NOT IN ('blob', 'null') OR length(from_name)>4096 OR
			typeof(node) NOT IN ('integer', 'null') OR typeof(` + changeKindColumn + `) NOT IN ('integer', 'null') OR
			typeof(size) NOT IN ('integer', 'null') OR typeof(atime_sec) NOT IN ('integer', 'null') OR
			typeof(atime_nsec) NOT IN ('integer', 'null') OR typeof(mtime_sec) NOT IN ('integer', 'null') OR
			typeof(mtime_nsec) NOT IN ('integer', 'null') OR typeof(content) NOT IN ('text', 'null') OR
			typeof(recorded_sec) != 'integer' OR typeof(recorded_nsec) != 'integer'` + neutralChangeClasses + `)`, scopeArgs},
		{"backing store", `SELECT count(*) FROM backing_store WHERE
			typeof(singleton) != 'integer' OR singleton != 1 OR
			typeof(store_id) != 'text' OR store_id = ''`, nil},
		{"durable state", `SELECT count(*) FROM database_state WHERE
			typeof(singleton) != 'integer' OR singleton != 1 OR
			typeof(database_id) != 'text' OR length(database_id) != 32 OR
			typeof(generation) != 'integer' OR generation < 0 OR
			typeof(node_high_water) != 'integer' OR node_high_water < 0 OR
			typeof(change_high_water) != 'integer' OR change_high_water < 0`, nil},
	}
	if version >= firstDurableIdentitySchemaVersion {
		intentWhere := ""
		intentArgs := []any{}
		if volume != nil {
			intentWhere = "WHERE volume=? AND "
			intentArgs = []any{*volume}
		} else {
			intentWhere = "WHERE "
		}
		queries = append(queries, struct {
			name  string
			query string
			args  []any
		}{"deletion intents", `SELECT count(*) FROM delete_intents ` + intentWhere + `(
			typeof(intent)!='text' OR length(CAST(intent AS BLOB))!=32 OR intent GLOB '*[^0-9a-f]*' OR
			typeof(volume)!='integer' OR volume<=0 OR typeof(node)!='integer' OR node<=0 OR
			typeof(parent) NOT IN ('integer','null') OR typeof(name) NOT IN ('blob','null') OR
			(parent IS NULL)!=(name IS NULL) OR length(reference)!=16 OR length(request_hash)!=32 OR
			typeof(if_empty)!='integer' OR if_empty NOT IN (0,1) OR
			typeof(outcome)!='integer' OR outcome NOT BETWEEN 1 AND 5 OR
			typeof(failure) NOT IN ('integer','null') OR (outcome=5)!=(failure IS NOT NULL) OR
			typeof(updated_sec)!='integer' OR typeof(updated_nsec)!='integer')`, intentArgs})
	}
	if invalidVolumes != 0 {
		return fmt.Errorf("the database holds %d volume rows in an invalid SQLite storage class: %w",
			invalidVolumes, syscall.EIO)
	}
	for _, check := range queries {
		var count int64
		if err := db.QueryRowContext(ctx, check.query, check.args...).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("the database holds %d %s rows in an invalid SQLite storage class: %w",
				count, check.name, syscall.EIO)
		}
	}
	return nil
}

func predicateJoin(where string) string {
	if where == "" {
		return " WHERE "
	}
	return " AND "
}

// ValidateVolumeIntegrity is the full volume pass run at open and by ObjectStatus.
// The state field is a promise to the sweeper: only an object referenced by exactly one file
// in its own volume may be protected from deletion, and every other state must be
// unreferenced. Both directions are checked because either half can be damaged while the
// foreign-key constraints are disabled by an external writer.
func ValidateVolumeIntegrity(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume int64,
	maxIntegrityRecords, maxIntegrityBytes int64,
	metadataLimits ...int64,
) error {
	return validateIntegrity(ctx, db, &volume, maxIntegrityRecords, maxIntegrityBytes, schema.Version(), metadataLimits...)
}

// A nil volume validates the complete database for migration or exclusive-owner recovery.
// Version selects the stored layout explicitly; ordinary readers validate the current schema.
func validateIntegrity(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
	maxIntegrityRecords, maxIntegrityBytes int64,
	version int,
	metadataLimits ...int64,
) error {
	maxMetadataBytes := int64(64 << 20)
	if len(metadataLimits) != 0 {
		maxMetadataBytes = metadataLimits[0]
	}
	return validateIntegrityWithMetadataPolicy(ctx, db, volume, maxIntegrityRecords, maxIntegrityBytes,
		version, maxMetadataBytes, false)
}

func validateIntegrityWithMetadataPolicy(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
	maxIntegrityRecords, maxIntegrityBytes int64,
	version int,
	maxMetadataBytes int64,
	opaqueMetadataVersions bool,
) error {
	if err := validateIntegrityWork(ctx, db, volume, maxIntegrityRecords); err != nil {
		return err
	}
	if err := validateIntegrityBytes(ctx, db, volume, maxIntegrityBytes, version); err != nil {
		return err
	}
	if version >= firstNeutralMetadataSchemaVersion {
		if err := validateMetadataIntegrity(ctx, db, volume, maxMetadataBytes, opaqueMetadataVersions, version); err != nil {
			return err
		}
	}
	if err := validateStorageClassesVersion(ctx, db, volume, version); err != nil {
		return err
	}
	if volume == nil {
		if _, err := dbstate.Validate(ctx, db); err != nil {
			return err
		}
	} else if err := dbstate.ValidateIdentityBounds(ctx, db, *volume); err != nil {
		return err
	}
	if err := validateNodeValuesVersion(ctx, db, volume, version); err != nil {
		return err
	}
	where := ""
	args := []any{StateReserved, StateReferenced, StateGarbage, StateUnresolved}
	if volume != nil {
		where = "WHERE volume = ?"
		args = append(args, *volume)
	}
	var invalidStates, invalidSizes int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			coalesce(sum(CASE WHEN typeof(state) != 'integer' OR state NOT IN (?, ?, ?, ?) THEN 1 ELSE 0 END), 0),
			coalesce(sum(CASE WHEN typeof(size) != 'integer' OR size < 0 THEN 1 ELSE 0 END), 0)
		FROM objects `+where, args...).Scan(
		&invalidStates, &invalidSizes,
	); err != nil {
		return err
	}
	if invalidStates != 0 || invalidSizes != 0 {
		return fmt.Errorf("the database holds %d objects in an unknown state and %d objects with an invalid size: %w",
			invalidStates, invalidSizes, syscall.EIO)
	}
	if err := validateObjectRelationshipsVersion(ctx, db, volume, version); err != nil {
		return err
	}
	if err := validateNodeRelationshipsVersion(ctx, db, volume, version); err != nil {
		return err
	}
	if version >= firstDurableIdentitySchemaVersion {
		if err := validateDurableIdentity(ctx, db, volume); err != nil {
			return err
		}
	}
	if err := validateUsedAccountingVersion(ctx, db, volume, version); err != nil {
		return err
	}
	return validateLogIntegrityVersion(ctx, db, volume, version, true)
}
