package schema

import (
	"context"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// The states an object passes through. Reserved is the state a key is in between the
// reservation and the commit. Unresolved retires a reservation whose Put did not prove
// ownership of whatever may be under the key. Garbage is deletion-authorized because a
// name stopped referencing the object or a caller with positive ownership proof abandoned
// it.
//
// The numbers are stored, so they are part of the schema.
const (
	StateReserved   = 0
	StateReferenced = 1
	StateGarbage    = 2
	StateUnresolved = 3
)

// validateLegacyObjectIntegrity refuses object records written before reservation size and Put
// ownership were recorded exactly. A non-referenced legacy row cannot prove that an object was
// created by this reservation, so carrying it forward into a deletion-authorized state would
// risk deleting unrelated bytes under the same key. Version 1 entry endpoints are checked in
// their native layout, and the normalized relationships are checked again before the migration
// transaction may commit.
func validateLegacyObjectIntegrity(
	ctx context.Context,
	db sqlvalue.Queryer,
	version int,
) error {
	var nonReferenced, invalidSizes int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			coalesce(sum(CASE WHEN typeof(state) != 'integer' OR state != ? THEN 1 ELSE 0 END), 0),
			coalesce(sum(CASE WHEN typeof(size) != 'integer' OR size < 0 THEN 1 ELSE 0 END), 0)
		FROM objects`, StateReferenced).Scan(&nonReferenced, &invalidSizes); err != nil {
		return err
	}
	if nonReferenced != 0 {
		return fmt.Errorf(
			"schema version %d holds %d non-referenced object records whose ownership and payload size cannot be proven: %w",
			version, nonReferenced, syscall.EIO)
	}
	if invalidSizes != 0 {
		return fmt.Errorf("schema version %d holds %d referenced objects with an invalid size: %w",
			version, invalidSizes, syscall.EIO)
	}
	if err := validateObjectRelationships(ctx, db, nil); err != nil {
		return err
	}
	return nil
}

// validateObjectRelationships checks both directions of the node/object relation. A nil
// namespace validates the whole database during a legacy migration; a non-nil namespace keeps
// ordinary reopen and status checks scoped to the Store being served.
func validateObjectRelationships(
	ctx context.Context,
	db sqlvalue.Queryer,
	namespace *int64,
) error {
	nodeWhere := ""
	objectWhere := ""
	var scopeArgs []any
	if namespace != nil {
		nodeWhere = "WHERE n.namespace = ?"
		objectWhere = "WHERE o.namespace = ?"
		scopeArgs = []any{*namespace}
	}
	var invalidNodes int64
	nodeArgs := append([]any{int64(fs.ModeType), StateReferenced}, scopeArgs...)
	if err := db.QueryRowContext(ctx, `
		SELECT coalesce(sum(CASE
			WHEN n.content IS NULL THEN
				CASE WHEN typeof(n.size) != 'integer' OR n.size != 0 THEN 1 ELSE 0 END
			WHEN typeof(n.content) != 'text'
				OR n.content = ''
				OR typeof(n.mode) != 'integer'
				OR typeof(n.size) != 'integer'
				OR n.size < 0
				OR (n.mode & ?) != 0
				OR o.key IS NULL
				OR o.namespace != n.namespace
				OR o.state != ?
				OR o.size != n.size
			THEN 1
			ELSE 0
		END), 0)
		FROM nodes n
		LEFT JOIN objects o ON o.key = n.content
		`+nodeWhere, nodeArgs...).Scan(&invalidNodes); err != nil {
		return err
	}

	var invalidReferenced, referencedPending int64
	objectArgs := append([]any{
		StateReferenced, StateReserved, StateGarbage, StateUnresolved,
	}, scopeArgs...)
	if err := db.QueryRowContext(ctx, `
		SELECT
			coalesce(sum(CASE
				WHEN state = ? AND (reference_count != 1 OR same_namespace_count != 1)
				THEN 1 ELSE 0 END), 0),
			coalesce(sum(CASE
				WHEN state IN (?, ?, ?) AND reference_count != 0
				THEN 1 ELSE 0 END), 0)
		FROM (
			SELECT
				o.state,
				count(n.id) AS reference_count,
				count(CASE WHEN n.namespace = o.namespace THEN 1 END) AS same_namespace_count
			FROM objects o
			LEFT JOIN nodes n ON n.content = o.key
			`+objectWhere+`
			GROUP BY o.key, o.namespace, o.state
		)`, objectArgs...).Scan(
		&invalidReferenced, &referencedPending,
	); err != nil {
		return err
	}
	if invalidNodes != 0 || invalidReferenced != 0 || referencedPending != 0 {
		return fmt.Errorf(
			"the database holds %d nodes with invalid object relationships, %d referenced objects without exactly one same-namespace file, and %d pending objects still referenced: %w",
			invalidNodes, invalidReferenced, referencedPending, syscall.EIO)
	}
	return nil
}
