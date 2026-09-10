package schema

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// validateVersionTwoLogIntegrity checks the historical log format before migration resets
// its incarnation. Version 2 has no predecessor chain, so this proves row shape and tail only.
func validateVersionTwoLogIntegrity(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	if err := validateVersionTwoLogStorageClasses(ctx, db, volume); err != nil {
		return err
	}
	return validateLogIntegrityVersion(ctx, db, volume, false)
}

func validateVersionTwoLogStorageClasses(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = "WHERE volume = ? AND "
		args = []any{*volume}
	} else {
		where = "WHERE "
	}
	var invalidLogs, invalidChanges int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM logs `+where+`(
		typeof(volume) != 'integer' OR typeof(incarnation) != 'text' OR
		typeof(committed_position) != 'integer' OR typeof(trimmed_through) != 'integer' OR
		typeof(trimmed_by_age) != 'integer')`, args...).Scan(&invalidLogs); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM changes `+where+`(
		typeof(position) != 'integer' OR typeof(volume) != 'integer' OR
		typeof(kind) != 'integer' OR typeof(parent) != 'integer' OR
		typeof(name) NOT IN ('blob', 'null') OR
		typeof(from_parent) NOT IN ('integer', 'null') OR
		typeof(from_name) NOT IN ('blob', 'null') OR
		typeof(node) NOT IN ('integer', 'null') OR typeof(mode) NOT IN ('integer', 'null') OR
		typeof(size) NOT IN ('integer', 'null') OR typeof(atime_sec) NOT IN ('integer', 'null') OR
		typeof(atime_nsec) NOT IN ('integer', 'null') OR typeof(mtime_sec) NOT IN ('integer', 'null') OR
		typeof(mtime_nsec) NOT IN ('integer', 'null') OR typeof(content) NOT IN ('text', 'null') OR
		typeof(recorded_sec) != 'integer' OR typeof(recorded_nsec) != 'integer')`, args...).Scan(&invalidChanges); err != nil {
		return err
	}
	if invalidLogs != 0 || invalidChanges != 0 {
		return fmt.Errorf("schema version 2 holds %d log rows and %d change rows in invalid SQLite storage classes: %w",
			invalidLogs, invalidChanges, syscall.EIO)
	}
	return nil
}

// validateLogIntegrity checks the durable tail, predecessor chain, and operation-dependent
// shape of every retained change before Snapshot or Since may expose it as history.
func validateLogIntegrity(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	return validateLogIntegrityVersion(ctx, db, volume, true)
}

func validateLogIntegrityVersion(ctx context.Context, db sqlvalue.Queryer, volume *int64, predecessors bool) error {
	volumeWhere := ""
	changeWhere := ""
	var args []any
	if volume != nil {
		volumeWhere = "WHERE ns.id = ? AND "
		changeWhere = "WHERE c.volume = ? AND "
		args = []any{*volume}
	} else {
		volumeWhere = "WHERE "
		changeWhere = "WHERE "
	}

	tailPredicate := `l.committed_position != coalesce((
				SELECT max(c.position) FROM changes c WHERE c.volume = ns.id
			), l.trimmed_through)`
	if predecessors {
		tailPredicate = `l.committed_position != coalesce((
				SELECT max(c.position) FROM changes c WHERE c.volume = ns.id
			), l.trimmed_through) OR
			EXISTS (
				SELECT 1 FROM (
					SELECT position, previous_position,
						row_number() OVER (ORDER BY position) AS ordinal,
						lag(position) OVER (ORDER BY position) AS preceding
					FROM changes WHERE volume = ns.id
				) chain
				WHERE chain.previous_position != CASE
					WHEN chain.ordinal = 1 THEN l.trimmed_through
					ELSE chain.preceding
				END
			)`
	}
	var invalidLogs int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM volumes ns
		LEFT JOIN logs l ON l.volume = ns.id
		`+volumeWhere+`(
			l.volume IS NULL OR l.incarnation = '' OR
			l.committed_position < 0 OR l.trimmed_through < 0 OR
			l.trimmed_by_age NOT IN (0, 1) OR l.trimmed_through > l.committed_position OR
			`+tailPredicate+` OR
			EXISTS (
				SELECT 1 FROM changes c
				WHERE c.volume = ns.id AND c.position <= l.trimmed_through
			)
		)`, args...).Scan(&invalidLogs); err != nil {
		return err
	}

	predecessorPredicate := ""
	if predecessors {
		predecessorPredicate = `
			OR typeof(c.previous_position) != 'integer'
			OR c.previous_position < 0 OR c.previous_position >= c.position`
	}
	var invalidChanges int64
	changeArgs := append([]any{}, args...)
	changeArgs = append(changeArgs,
		changes.KindCreated, changes.KindRemoved, changes.KindModified, changes.KindRenamed,
		changes.KindCreated, changes.KindRemoved, changes.KindRenamed, changes.KindModified,
		changes.KindRenamed, changes.KindRenamed,
		changes.KindRemoved, changes.KindRemoved,
		int64(math.MaxUint32), int64(fs.ModeType), int64(fs.ModeDir),
		int64(fs.ModeType), int64(fs.ModeDir), int64(fs.ModeType),
	)
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM changes c
		LEFT JOIN volumes ns ON ns.id = c.volume
		LEFT JOIN nodes current_node ON current_node.id = c.node
		LEFT JOIN nodes current_parent ON current_parent.id = c.parent
		LEFT JOIN nodes current_from_parent ON current_from_parent.id = c.from_parent
		`+changeWhere+`(
			ns.id IS NULL OR c.position <= 0 OR c.kind NOT IN (?, ?, ?, ?) OR c.parent < 0 OR
			(c.kind IN (?, ?, ?) AND (c.parent <= 0 OR c.name IS NULL OR length(c.name) = 0)) OR
			(c.kind = ? AND NOT (
				(c.parent = 0 AND c.name IS NULL) OR
				(c.parent > 0 AND c.name IS NOT NULL AND length(c.name) > 0)
			)) OR
			(c.kind = ? AND (c.from_parent IS NULL OR c.from_parent <= 0 OR
				c.from_name IS NULL OR length(c.from_name) = 0)) OR
			(c.kind != ? AND (c.from_parent IS NOT NULL OR c.from_name IS NOT NULL)) OR
			(c.name IS NOT NULL AND (
				c.name IN (X'2e', X'2e2e') OR instr(c.name, X'2f') != 0 OR instr(c.name, X'00') != 0
			)) OR
			(c.from_name IS NOT NULL AND (
				c.from_name IN (X'2e', X'2e2e') OR
				instr(c.from_name, X'2f') != 0 OR instr(c.from_name, X'00') != 0
			)) OR
			(c.kind = ? AND (c.node IS NOT NULL OR c.mode IS NOT NULL OR c.size IS NOT NULL OR
				c.atime_sec IS NOT NULL OR c.atime_nsec IS NOT NULL OR c.mtime_sec IS NOT NULL OR
				c.mtime_nsec IS NOT NULL OR c.content IS NOT NULL)) OR
			(c.kind != ? AND (c.node IS NULL OR c.mode IS NULL OR c.size IS NULL OR
				c.atime_sec IS NULL OR c.atime_nsec IS NULL OR c.mtime_sec IS NULL OR
				c.mtime_nsec IS NULL)) OR
			(c.node IS NOT NULL AND (
				c.node <= 0 OR c.mode < 0 OR c.mode > ? OR c.size < 0 OR
				c.atime_nsec < 0 OR c.atime_nsec >= 1000000000 OR
				c.mtime_nsec < 0 OR c.mtime_nsec >= 1000000000 OR
				(c.mode & ?) NOT IN (0, ?) OR
				((c.mode & ?) = ? AND (c.size != 0 OR c.content IS NOT NULL)) OR
				((c.mode & ?) = 0 AND c.content IS NULL AND c.size != 0) OR
				(c.content IS NOT NULL AND c.content = '')
			)) OR
			(current_node.id IS NOT NULL AND current_node.volume != c.volume) OR
			(current_parent.id IS NOT NULL AND c.parent != 0 AND current_parent.volume != c.volume) OR
			(current_from_parent.id IS NOT NULL AND current_from_parent.volume != c.volume) OR
			c.recorded_nsec < 0 OR c.recorded_nsec >= 1000000000
			`+predecessorPredicate+`
		)`, changeArgs...).Scan(&invalidChanges); err != nil {
		return err
	}
	if invalidLogs != 0 || invalidChanges != 0 {
		return fmt.Errorf("the database holds %d invalid volume logs and %d invalid change rows: %w",
			invalidLogs, invalidChanges, syscall.EIO)
	}
	return nil
}
