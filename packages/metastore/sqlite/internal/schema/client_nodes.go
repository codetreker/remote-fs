package schema

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateClientNodeValues(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = "volume=? AND "
		args = []any{*volume}
	}
	var invalid int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM nodes WHERE `+where+`(
		kind NOT IN (1,2,3) OR size<0 OR detached NOT IN (0,1) OR content_revision<1 OR
		atime_nsec NOT BETWEEN 0 AND 999999999 OR mtime_nsec NOT BETWEEN 0 AND 999999999 OR
		(birth_sec IS NULL)!=(birth_nsec IS NULL) OR (change_sec IS NULL)!=(change_nsec IS NULL) OR
		birth_nsec NOT BETWEEN 0 AND 999999999 OR change_nsec NOT BETWEEN 0 AND 999999999 OR
		(kind=2 AND (size!=0 OR content IS NOT NULL OR length(directory_revision)!=8 OR
			directory_revision<X'0000000000000001' OR directory_revision>X'7fffffffffffffff')) OR
		(kind!=2 AND length(directory_revision)!=0) OR length(directory_revision)>64 OR
		(kind=3 AND (size=0 OR content IS NOT NULL OR length(link_target)!=size)) OR
		(kind!=3 AND length(link_target)!=0) OR (content IS NOT NULL AND content='') OR
		pending_unlink NOT IN (0,1) OR pending_generation<0 OR (pending_unlink=1 AND pending_generation=0) OR
		(pending_unlink=1 AND id IN (SELECT root FROM volumes))
	)`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("%d nodes with invalid metadata values or pending state: %w", invalid, syscall.EIO)
	}
	return nil
}

func validateClientNodeRelationships(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	rootWhere, nodeWhere, entryWhere := "", "", ""
	var args, entryArgs []any
	if volume != nil {
		rootWhere, nodeWhere = "WHERE v.id=?", "WHERE n.volume=?"
		entryWhere = "WHERE e.volume=? OR p.volume=? OR c.volume=?"
		args = []any{*volume}
		entryArgs = []any{*volume, *volume, *volume}
	}
	var roots, nodes, entries int64
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(CASE WHEN r.id IS NULL OR r.volume!=v.id OR
		r.kind!=2 OR r.detached!=0 THEN 1 ELSE 0 END),0)
		FROM volumes v LEFT JOIN nodes r ON r.id=v.root `+rootWhere, args...).Scan(&roots); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(invalid),0) FROM (
		SELECT CASE WHEN v.id IS NULL THEN 1
		WHEN n.detached=1 THEN CASE WHEN n.id=v.root OR count(e.node)!=0 THEN 1 ELSE 0 END
		WHEN n.id=v.root AND count(e.node)!=0 THEN 1
		WHEN n.id!=v.root AND (count(e.node)!=1 OR count(CASE WHEN e.volume=n.volume THEN 1 END)!=1) THEN 1
		ELSE 0 END AS invalid
		FROM nodes n LEFT JOIN volumes v ON v.id=n.volume LEFT JOIN entries e ON e.node=n.id
		`+nodeWhere+` GROUP BY n.id,n.volume,n.detached,v.id,v.root)`, args...).Scan(&nodes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(CASE WHEN p.id IS NULL OR c.id IS NULL OR
		e.volume!=p.volume OR e.volume!=c.volume OR p.kind!=2 OR p.detached!=0 OR c.detached!=0
		THEN 1 ELSE 0 END),0)
		FROM entries e LEFT JOIN nodes p ON p.id=e.parent LEFT JOIN nodes c ON c.id=e.node `+entryWhere, entryArgs...).Scan(&entries); err != nil {
		return err
	}
	if roots != 0 || nodes != 0 || entries != 0 {
		return fmt.Errorf("the database holds %d invalid volume roots, %d nodes with invalid entry cardinality, and %d entries with invalid relationships: %w", roots, nodes, entries, syscall.EIO)
	}
	return validateNodeReachability(ctx, db, volume, firstClientCapabilitySchemaVersion)
}

func validateCloseIntents(ctx context.Context, db sqlvalue.Queryer, volume *int64) error {
	where := ""
	var args []any
	if volume != nil {
		where = "(i.volume=? OR n.volume=?) AND "
		args = []any{*volume, *volume}
	}
	var invalid int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM close_intents i
		LEFT JOIN nodes n ON n.id=i.node LEFT JOIN volumes v ON v.id=i.volume WHERE `+where+`(
		typeof(i.volume)!='integer' OR i.volume<=0 OR typeof(i.node)!='integer' OR i.node<=0 OR
		n.id IS NULL OR v.id IS NULL OR n.volume!=i.volume OR n.id=v.root OR
		typeof(i.incarnation)!='blob' OR length(i.incarnation)!=16 OR
		typeof(i.reference)!='blob' OR length(i.reference)!=16 OR
		typeof(i.if_empty)!='integer' OR i.if_empty NOT IN (0,1)
	)`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("%d close intentions have invalid identity, ownership or trigger state: %w", invalid, syscall.EIO)
	}
	return nil
}
