package schema

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateNodeValues(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	return validateNodeValuesVersion(ctx, db, volume, schema.Version())
}

func validateNodeValuesVersion(ctx context.Context, db sqlvalue.Queryer, volume *int64, version int) error {
	where := ""
	var args []any
	if volume != nil {
		where = "WHERE volume = ? AND "
		args = []any{*volume}
	} else {
		where = "WHERE "
	}
	args = append(args,
		int64(math.MaxUint32), int64(fs.ModeType), int64(fs.ModeDir),
		int64(fs.ModeType), int64(fs.ModeDir), int64(fs.ModeType),
	)
	retainedNodeValues := ""
	if version >= firstRetainedFileSchemaVersion {
		retainedNodeValues = ` OR detached NOT IN (0, 1) OR content_revision < 1`
	}
	var invalid int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM nodes `+where+`(
			mode < 0 OR mode > ? OR (mode & ?) NOT IN (0, ?) OR size < 0 OR
			atime_nsec < 0 OR atime_nsec >= 1000000000 OR
			mtime_nsec < 0 OR mtime_nsec >= 1000000000 OR
			((mode & ?) = ? AND (size != 0 OR content IS NOT NULL)) OR
			(content IS NOT NULL AND content = '')`+retainedNodeValues+`
		)`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("the database holds %d nodes with invalid metadata values: %w", invalid, syscall.EIO)
	}
	return nil
}

// validateVersionOneNodeRelationships checks the entry table before migration 0002 replaces
// it. Version 1 has no entry volume column, so the parent and child nodes are the only proof
// that an entry stays within one volume. Running this before the rebuild prevents its inner
// join from silently discarding an entry whose endpoint is missing.
func validateVersionOneNodeRelationships(ctx context.Context, db sqlvalue.Queryer) error {
	var invalidRoots int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN typeof(ns.root) != 'integer'
				OR root.id IS NULL
				OR root.volume != ns.id
				OR typeof(root.mode) != 'integer'
				OR (root.mode & ?) = 0
			THEN 1 ELSE 0
		END), 0)
		FROM volumes ns
		LEFT JOIN nodes root ON root.id = ns.root`, int64(fs.ModeDir)).Scan(&invalidRoots); err != nil {
		return err
	}

	var invalidNodes int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(invalid), 0)
		FROM (
			SELECT CASE
				WHEN ns.id IS NULL THEN 1
				WHEN n.id = ns.root AND count(e.node) != 0 THEN 1
				WHEN n.id != ns.root AND count(e.node) != 1 THEN 1
				ELSE 0
			END AS invalid
			FROM nodes n
			LEFT JOIN volumes ns ON ns.id = n.volume
			LEFT JOIN entries e ON e.node = n.id
			GROUP BY n.id, n.volume, ns.id, ns.root
		)`).Scan(&invalidNodes); err != nil {
		return err
	}

	var invalidEntries int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN parent.id IS NULL
				OR child.id IS NULL
				OR parent.volume != child.volume
				OR typeof(parent.mode) != 'integer'
				OR (parent.mode & ?) = 0
			THEN 1 ELSE 0
		END), 0)
		FROM entries e
		LEFT JOIN nodes parent ON parent.id = e.parent
		LEFT JOIN nodes child ON child.id = e.node`, int64(fs.ModeDir)).Scan(&invalidEntries); err != nil {
		return err
	}
	if invalidRoots != 0 || invalidNodes != 0 || invalidEntries != 0 {
		return fmt.Errorf(
			"schema version 1 holds %d invalid volume roots, %d nodes with invalid entry cardinality, and %d entries with invalid endpoints: %w",
			invalidRoots, invalidNodes, invalidEntries, syscall.EIO)
	}

	var unreachableNodes int64
	if err := db.QueryRowContext(ctx, `
		WITH RECURSIVE reachable(volume, node) AS (
			SELECT id, root FROM volumes
			UNION
			SELECT reachable.volume, e.node
			FROM reachable
			JOIN entries e ON e.parent = reachable.node
			JOIN nodes child
				ON child.id = e.node
				AND child.volume = reachable.volume
		)
		SELECT count(*)
		FROM nodes n
		LEFT JOIN reachable
			ON reachable.volume = n.volume
			AND reachable.node = n.id
		WHERE reachable.node IS NULL`).Scan(&unreachableNodes); err != nil {
		return err
	}
	if unreachableNodes != 0 {
		return fmt.Errorf("schema version 1 holds %d nodes outside their volume root's tree: %w",
			unreachableNodes, syscall.EIO)
	}
	return nil
}

// Linked nodes form one tree per volume. Detached nodes are regular non-root files with no
// incoming or outgoing entries. A nil volume validates every volume.
func validateNodeRelationships(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
) error {
	return validateNodeRelationshipsVersion(ctx, db, volume, schema.Version())
}

func validateNodeRelationshipsVersion(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
	version int,
) error {
	volumeWhere := ""
	nodeWhere := ""
	entryWhere := ""
	var scopeArgs []any
	if volume != nil {
		volumeWhere = "WHERE ns.id = ?"
		nodeWhere = "WHERE n.volume = ?"
		entryWhere = "WHERE e.volume = ? OR parent.volume = ? OR child.volume = ?"
		scopeArgs = []any{*volume}
	}

	retainedRoot := ""
	retainedNode := ""
	retainedEntry := ""
	nodeArgs := []any{}
	if version >= firstRetainedFileSchemaVersion {
		retainedRoot = ` OR root.detached != 0`
		retainedNode = `
				WHEN n.detached = 1 THEN CASE
					WHEN n.id = ns.root OR (n.mode & ?) != 0 OR count(e.node) != 0
					THEN 1 ELSE 0 END`
		retainedEntry = ` OR parent.detached != 0 OR child.detached != 0`
		nodeArgs = append(nodeArgs, int64(fs.ModeType))
	}
	nodeArgs = append(nodeArgs, scopeArgs...)
	rootArgs := append([]any{int64(fs.ModeDir)}, scopeArgs...)
	var invalidRoots int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN typeof(ns.root) != 'integer'
				OR root.id IS NULL
				OR root.volume != ns.id
				OR typeof(root.mode) != 'integer'
				OR (root.mode & ?) = 0`+retainedRoot+`
			THEN 1 ELSE 0
		END), 0)
		FROM volumes ns
		LEFT JOIN nodes root ON root.id = ns.root
		`+volumeWhere, rootArgs...).Scan(&invalidRoots); err != nil {
		return err
	}

	var invalidNodes int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(invalid), 0)
		FROM (
			SELECT CASE
				WHEN ns.id IS NULL THEN 1`+retainedNode+`
				WHEN n.id = ns.root AND count(e.node) != 0 THEN 1
				WHEN n.id != ns.root AND (
					count(e.node) != 1
					OR count(CASE WHEN e.volume = n.volume THEN 1 END) != 1
				) THEN 1
				ELSE 0
			END AS invalid
			FROM nodes n
			LEFT JOIN volumes ns ON ns.id = n.volume
			LEFT JOIN entries e ON e.node = n.id
			`+nodeWhere+`
			GROUP BY n.id, n.volume, ns.id, ns.root
		)`, nodeArgs...).Scan(&invalidNodes); err != nil {
		return err
	}

	entryArgs := append([]any{int64(fs.ModeDir)}, scopeArgs...)
	if volume != nil {
		entryArgs = append(entryArgs, *volume, *volume)
	}
	var invalidEntries int64
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN typeof(e.volume) != 'integer'
				OR parent.id IS NULL
				OR child.id IS NULL
				OR e.volume != parent.volume
				OR e.volume != child.volume
				OR typeof(parent.mode) != 'integer'
				OR (parent.mode & ?) = 0`+retainedEntry+`
			THEN 1 ELSE 0
		END), 0)
		FROM entries e
		LEFT JOIN nodes parent ON parent.id = e.parent
		LEFT JOIN nodes child ON child.id = e.node
		`+entryWhere, entryArgs...).Scan(&invalidEntries); err != nil {
		return err
	}

	if invalidRoots != 0 || invalidNodes != 0 || invalidEntries != 0 {
		return fmt.Errorf(
			"the database holds %d invalid volume roots, %d nodes with invalid entry cardinality, and %d entries crossing an invalid relationship: %w",
			invalidRoots, invalidNodes, invalidEntries, syscall.EIO)
	}
	return validateNodeReachability(ctx, db, volume, version)
}

// validateNodeReachability proves that the cardinality-checked entry graph is one tree rooted
// at each volume root. UNION, rather than UNION ALL, admits each stored volume/node pair
// once, so a disconnected cycle terminates and the query's work is bounded by stored state.
func validateNodeReachability(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
	version int,
) error {
	seedWhere := ""
	nodeWhere := ""
	var args []any
	if volume != nil {
		seedWhere = "WHERE id = ?"
		nodeWhere = " AND n.volume = ?"
		args = []any{*volume, *volume}
	}
	if version >= firstRetainedFileSchemaVersion {
		nodeWhere += " AND n.detached = 0"
	}

	var unreachableNodes int64
	if err := db.QueryRowContext(ctx, `
		WITH RECURSIVE reachable(volume, node) AS (
			SELECT id, root FROM volumes `+seedWhere+`
			UNION
			SELECT reachable.volume, e.node
			FROM reachable
			JOIN entries e
				ON e.volume = reachable.volume
				AND e.parent = reachable.node
		)
		SELECT count(*)
		FROM nodes n
		LEFT JOIN reachable
			ON reachable.volume = n.volume
			AND reachable.node = n.id
		WHERE reachable.node IS NULL`+nodeWhere, args...).Scan(&unreachableNodes); err != nil {
		return err
	}
	if unreachableNodes != 0 {
		return fmt.Errorf("the database holds %d nodes outside their volume root's tree: %w",
			unreachableNodes, syscall.EIO)
	}
	return nil
}

// validateUsedAccounting streams each volume and its nodes in key order. File sizes are
// added only after checking that the next addition fits in int64, so a corrupt database cannot
// wrap an aggregate into a plausible counter.
func validateUsedAccounting(
	ctx context.Context,
	db sqlvalue.Queryer,
	volume *int64,
) error {
	where := ""
	var args []any
	if volume != nil {
		where = "WHERE ns.id = ?"
		args = []any{*volume}
	}
	rows, err := db.QueryContext(ctx, `
		SELECT
			ns.id, ns.used, typeof(ns.used),
			n.id, n.mode, typeof(n.mode), n.size, typeof(n.size)
		FROM volumes ns
		LEFT JOIN nodes n ON n.volume = ns.id
		`+where+`
		ORDER BY ns.id, n.id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		haveVolume         bool
		currentVolume      int64
		recordedUsed       int64
		recordedUsedValid  bool
		calculatedUsed     int64
		calculatedOverflow bool
		invalidUsed        int64
		invalidNodeValues  int64
		overflowed         int64
		mismatched         int64
	)
	finishVolume := func() {
		if !haveVolume {
			return
		}
		if calculatedOverflow {
			overflowed++
			return
		}
		if recordedUsedValid && recordedUsed != calculatedUsed {
			mismatched++
		}
	}

	for rows.Next() {
		var (
			volumeID int64
			usedRaw  any
			usedType string
			nodeID   sql.NullInt64
			modeRaw  any
			modeType string
			sizeRaw  any
			sizeType string
		)
		if err := rows.Scan(
			&volumeID, &usedRaw, &usedType,
			&nodeID, &modeRaw, &modeType, &sizeRaw, &sizeType,
		); err != nil {
			return err
		}
		if !haveVolume || volumeID != currentVolume {
			finishVolume()
			haveVolume = true
			currentVolume = volumeID
			calculatedUsed = 0
			calculatedOverflow = false
			recordedUsed, recordedUsedValid = sqlvalue.StoredInteger(usedRaw, usedType)
			if !recordedUsedValid || recordedUsed < 0 {
				invalidUsed++
				recordedUsedValid = false
			}
		}
		if !nodeID.Valid {
			continue
		}
		mode, modeValid := sqlvalue.StoredInteger(modeRaw, modeType)
		size, sizeValid := sqlvalue.StoredInteger(sizeRaw, sizeType)
		if !modeValid || mode < 0 || mode > math.MaxUint32 || !sizeValid || size < 0 {
			invalidNodeValues++
			continue
		}
		if fs.FileMode(mode).Type() != 0 || calculatedOverflow {
			continue
		}
		if size > math.MaxInt64-calculatedUsed {
			calculatedOverflow = true
			continue
		}
		calculatedUsed += size
	}
	if err := rows.Err(); err != nil {
		return err
	}
	finishVolume()
	if invalidUsed != 0 || invalidNodeValues != 0 || overflowed != 0 || mismatched != 0 {
		return fmt.Errorf(
			"the database holds %d volumes with an invalid used counter, %d nodes with invalid accounting values, %d volumes whose file sizes overflow, and %d volumes whose used counter disagrees with their files: %w",
			invalidUsed, invalidNodeValues, overflowed, mismatched, syscall.EIO)
	}
	return nil
}
