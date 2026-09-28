package schema

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateNeutralNodeValues(ctx context.Context, db sqlvalue.Queryer, volume *int64, version int, opaqueDirectoryRevisions bool) error {
	where := ""
	var args []any
	if volume != nil {
		where = "volume=? AND "
		args = []any{*volume}
	}
	durable := ""
	if version >= firstDurableIdentitySchemaVersion {
		durable = ` OR typeof(link_target)!='blob' OR length(link_target)>32768 OR pending_unlink NOT IN (0,1) OR pending_generation<0 OR
			(pending_unlink=1 AND pending_generation=0) OR
			(kind=3 AND (length(link_target)=0 OR size!=length(link_target))) OR (kind!=3 AND length(link_target)!=0)`
	}
	directoryRevision := ""
	if version >= firstDirectoryRevisionSchemaVersion {
		if opaqueDirectoryRevisions {
			directoryRevision = ` OR typeof(directory_revision)!='blob' OR length(directory_revision)>64 OR
				(kind=2 AND length(directory_revision)=0) OR (kind!=2 AND length(directory_revision)!=0)`
		} else {
			directoryRevision = ` OR typeof(directory_revision)!='blob' OR length(directory_revision)>64 OR
				(kind=2 AND (length(directory_revision)!=8 OR directory_revision<X'0000000000000001' OR directory_revision>X'7fffffffffffffff')) OR
				(kind!=2 AND length(directory_revision)!=0)`
		}
	}
	detachedKind := " OR (detached=1 AND kind!=1)"
	if version >= firstDurableIdentitySchemaVersion {
		detachedKind = ""
	}
	missingContent := " OR (kind=1 AND content IS NULL AND size!=0)"
	if opaqueDirectoryRevisions {
		missingContent = ""
	}
	var invalid int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM nodes WHERE `+where+`(
		kind NOT IN (1,2,3) OR size<0 OR detached NOT IN (0,1)`+detachedKind+` OR content_revision<1 OR
		atime_nsec NOT BETWEEN 0 AND 999999999 OR mtime_nsec NOT BETWEEN 0 AND 999999999 OR
		(birth_sec IS NULL)!=(birth_nsec IS NULL) OR (change_sec IS NULL)!=(change_nsec IS NULL) OR
		birth_nsec NOT BETWEEN 0 AND 999999999 OR change_nsec NOT BETWEEN 0 AND 999999999 OR
		(kind=2 AND (size!=0 OR content IS NOT NULL)) OR
		(kind=3 AND content IS NOT NULL) OR
		(content IS NOT NULL AND content='')`+missingContent+durable+directoryRevision+`
	)`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("%d nodes have invalid neutral metadata values: %w", invalid, syscall.EIO)
	}
	return nil
}

func validateNeutralNodeRelationships(ctx context.Context, db sqlvalue.Queryer, volume *int64, version int) error {
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
	return validateNodeReachability(ctx, db, volume, firstNeutralMetadataSchemaVersion)
}
