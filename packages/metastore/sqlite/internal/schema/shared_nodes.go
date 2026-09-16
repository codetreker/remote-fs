package schema

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateSharedNodeValues(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = "volume = ? AND "
		args = []any{*volume}
	}
	var invalid int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM nodes WHERE `+where+`(
		kind NOT IN (1,2,3) OR size < 0 OR metadata_revision < 1 OR
		(kind = 2 AND directory_revision < 1) OR (kind != 2 AND directory_revision != 0) OR
		detached NOT IN (0,1) OR content_revision < 1 OR
		atime_nsec NOT BETWEEN 0 AND 999999999 OR mtime_nsec NOT BETWEEN 0 AND 999999999 OR
		(creation_sec IS NULL) != (creation_nsec IS NULL) OR
		(change_sec IS NULL) != (change_nsec IS NULL) OR
		creation_nsec NOT BETWEEN 0 AND 999999999 OR change_nsec NOT BETWEEN 0 AND 999999999 OR
		(kind = 2 AND (size != 0 OR content IS NOT NULL)) OR
		(kind != 3 AND length(link_target) != 0) OR
		(kind = 3 AND (content IS NOT NULL OR length(link_target) != size)) OR
		(content IS NOT NULL AND content = '')
	)`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("the database holds %d nodes with invalid common metadata values: %w", invalid, syscall.EIO)
	}
	return validateSharedNodePayloads(ctx, db, volume)
}

func validateSharedNodeRelationships(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	rootWhere, nodeWhere, entryWhere := "", "", ""
	var args, entryArgs []any
	if volume != nil {
		rootWhere, nodeWhere = "WHERE v.id = ?", "WHERE n.volume = ?"
		entryWhere = "WHERE e.volume = ? OR parent.volume = ? OR child.volume = ?"
		args, entryArgs = []any{*volume}, []any{*volume, *volume, *volume}
	}
	var roots, nodes, entries int64
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(CASE
		WHEN typeof(v.root) != 'integer' OR root.id IS NULL OR root.volume != v.id OR
			typeof(root.kind) != 'integer' OR root.kind != 2 OR root.detached != 0
		THEN 1 ELSE 0 END), 0)
		FROM volumes v LEFT JOIN nodes root ON root.id = v.root `+rootWhere, args...).Scan(&roots); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(invalid), 0) FROM (
		SELECT CASE
			WHEN v.id IS NULL THEN 1
			WHEN n.detached = 1 THEN CASE WHEN n.id = v.root OR count(e.node) != 0 THEN 1 ELSE 0 END
			WHEN n.id = v.root AND count(e.node) != 0 THEN 1
			WHEN n.id != v.root AND (count(e.node) != 1 OR
				count(CASE WHEN e.volume = n.volume THEN 1 END) != 1) THEN 1
			ELSE 0 END AS invalid
		FROM nodes n LEFT JOIN volumes v ON v.id = n.volume LEFT JOIN entries e ON e.node = n.id
		`+nodeWhere+` GROUP BY n.id, n.volume, n.detached, v.id, v.root
	)`, args...).Scan(&nodes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(CASE
		WHEN parent.id IS NULL OR child.id IS NULL OR e.volume != parent.volume OR e.volume != child.volume OR
			parent.kind != 2 OR parent.detached != 0 OR child.detached != 0 OR e.id <= 0 OR
			EXISTS (SELECT 1 FROM nodes identity_node WHERE identity_node.id = e.id) OR
			e.draining NOT IN (0,1) OR e.drain_generation < 0 OR e.drain_if_empty NOT IN (0,1) OR
			(e.draining = 0 AND (e.drain_if_empty != 0 OR e.drain_authority != '')) OR
			(e.draining = 1 AND (e.drain_generation < 1 OR e.drain_authority = '')) OR
			instr(e.drain_authority, char(0)) != 0
		THEN 1 ELSE 0 END), 0)
		FROM entries e LEFT JOIN nodes parent ON parent.id = e.parent LEFT JOIN nodes child ON child.id = e.node
		`+entryWhere, entryArgs...).Scan(&entries); err != nil {
		return err
	}
	if roots != 0 || nodes != 0 || entries != 0 {
		return fmt.Errorf("the database holds %d invalid volume roots, %d nodes with invalid entry cardinality, and %d entries with invalid endpoints: %w",
			roots, nodes, entries, syscall.EIO)
	}
	return validateNodeReachability(ctx, db, volume, firstSharedFileSchemaVersion)
}
