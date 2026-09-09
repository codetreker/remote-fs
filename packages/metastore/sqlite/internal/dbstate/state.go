// Package dbstate reads and advances durable database identities and counters.
// SQL operations use caller-owned transactions; commit and witness publication remain with the store.
package dbstate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

type State struct {
	DatabaseID      string
	Generation      int64
	NodeHighWater   int64
	ChangeHighWater int64
}

func CheckState(s State) error {
	if len(s.DatabaseID) != 32 || strings.Trim(s.DatabaseID, "0123456789abcdef") != "" {
		return fmt.Errorf("database identity %q is not 16 lowercase hexadecimal bytes: %w", s.DatabaseID, syscall.EIO)
	}
	if s.Generation < 0 || s.NodeHighWater < 0 || s.ChangeHighWater < 0 {
		return fmt.Errorf("database state has negative counters %+v: %w", s, syscall.EIO)
	}
	return nil
}

func Read(ctx context.Context, db sqlvalue.Queryer) (State, error) {
	var (
		state State
		rows  int64
		valid int64
	)
	if err := db.QueryRowContext(ctx, `
		SELECT count(*), count(CASE
			WHEN typeof(singleton) = 'integer' AND singleton = 1 AND
				typeof(database_id) = 'text' AND length(CAST(database_id AS BLOB)) = 32 AND
				typeof(generation) = 'integer' AND
				typeof(node_high_water) = 'integer' AND
				typeof(change_high_water) = 'integer'
			THEN 1 END)
		FROM database_state`).Scan(&rows, &valid); err != nil {
		return State{}, err
	}
	if rows != 1 || valid != 1 {
		return State{}, fmt.Errorf("the database holds %d durable-state rows, of which %d have valid storage classes: %w",
			rows, valid, syscall.EIO)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT CASE
		           WHEN typeof(database_id) = 'text' AND length(CAST(database_id AS BLOB)) = 32
		           THEN database_id
		       END,
		       CASE WHEN typeof(generation) = 'integer' THEN generation END,
		       CASE WHEN typeof(node_high_water) = 'integer' THEN node_high_water END,
		       CASE WHEN typeof(change_high_water) = 'integer' THEN change_high_water END
		FROM database_state WHERE singleton = 1`).Scan(
		&state.DatabaseID, &state.Generation, &state.NodeHighWater, &state.ChangeHighWater,
	); err != nil {
		return State{}, err
	}
	if err := CheckState(state); err != nil {
		return State{}, err
	}
	return state, nil
}

const globalNodeIdentityBoundsQuery = `
	SELECT
		coalesce((SELECT 1
		          FROM namespaces INDEXED BY namespaces_by_root_identity
		          WHERE CASE WHEN typeof(root) = 'integer' THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
		coalesce((SELECT CASE WHEN typeof(root) = 'integer' THEN root ELSE 0 END
		          FROM namespaces INDEXED BY namespaces_by_root_identity
		          WHERE CASE WHEN typeof(root) = 'integer' THEN 0 ELSE 1 END = 0
		          ORDER BY root DESC LIMIT 1), 0),
		coalesce((SELECT 1
		          FROM entries INDEXED BY entries_by_node_identity
		          WHERE CASE
			WHEN typeof(parent) = 'integer' AND typeof(node) = 'integer' THEN 0 ELSE 1 END = 1
		          LIMIT 1), 0),
		coalesce((SELECT CASE
			WHEN typeof(parent) = 'integer' AND typeof(node) = 'integer'
			THEN max(parent, node) ELSE 0 END
		          FROM entries INDEXED BY entries_by_node_identity
		          WHERE CASE
			WHEN typeof(parent) = 'integer' AND typeof(node) = 'integer' THEN 0 ELSE 1 END = 0
		          ORDER BY max(parent, node) DESC LIMIT 1), 0),
		coalesce((SELECT 1
		          FROM changes INDEXED BY changes_by_node_identity
		          WHERE CASE
			WHEN typeof(parent) = 'integer'
				AND typeof(from_parent) IN ('integer', 'null')
				AND typeof(node) IN ('integer', 'null')
			THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
		coalesce((SELECT CASE
			WHEN typeof(parent) = 'integer'
				AND typeof(from_parent) IN ('integer', 'null')
				AND typeof(node) IN ('integer', 'null')
			THEN max(parent, coalesce(from_parent, 0), coalesce(node, 0)) ELSE 0 END
		          FROM changes INDEXED BY changes_by_node_identity
		          WHERE CASE
			WHEN typeof(parent) = 'integer'
				AND typeof(from_parent) IN ('integer', 'null')
				AND typeof(node) IN ('integer', 'null')
			THEN 0 ELSE 1 END = 0
		          ORDER BY max(parent, coalesce(from_parent, 0), coalesce(node, 0)) DESC LIMIT 1), 0)`

const maximumNodeIdentityQuery = `
	SELECT coalesce((SELECT CASE WHEN typeof(id) = 'integer' THEN id ELSE 0 END
	                 FROM nodes ORDER BY id DESC LIMIT 1), 0)`

const globalChangeIdentityBoundsQuery = `
	SELECT
		coalesce((SELECT 1
		          FROM changes INDEXED BY changes_by_position_identity
		          WHERE CASE
			WHEN typeof(position) = 'integer' AND typeof(previous_position) = 'integer'
			THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
		coalesce((SELECT CASE
			WHEN typeof(position) = 'integer' AND typeof(previous_position) = 'integer'
			THEN max(position, previous_position) ELSE 0 END
		          FROM changes INDEXED BY changes_by_position_identity
		          WHERE CASE
			WHEN typeof(position) = 'integer' AND typeof(previous_position) = 'integer'
			THEN 0 ELSE 1 END = 0
		          ORDER BY max(position, previous_position) DESC LIMIT 1), 0),
		coalesce((SELECT 1
		          FROM logs INDEXED BY logs_by_change_identity
		          WHERE CASE
			WHEN typeof(committed_position) = 'integer' AND typeof(trimmed_through) = 'integer'
			THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
		coalesce((SELECT CASE
			WHEN typeof(committed_position) = 'integer' AND typeof(trimmed_through) = 'integer'
			THEN max(committed_position, trimmed_through) ELSE 0 END
		          FROM logs INDEXED BY logs_by_change_identity
		          WHERE CASE
			WHEN typeof(committed_position) = 'integer' AND typeof(trimmed_through) = 'integer'
			THEN 0 ELSE 1 END = 0
		          ORDER BY max(committed_position, trimmed_through) DESC LIMIT 1), 0)`

func Validate(ctx context.Context, db sqlvalue.Queryer) (State, error) {
	state, err := Read(ctx, db)
	if err != nil {
		return State{}, err
	}
	nodeSequence, err := SequenceValue(ctx, db, "nodes")
	if err != nil {
		return State{}, err
	}
	changeSequence, err := SequenceValue(ctx, db, "changes")
	if err != nil {
		return State{}, err
	}
	if nodeSequence != state.NodeHighWater || changeSequence != state.ChangeHighWater {
		return State{}, fmt.Errorf(
			"SQLite sequences nodes=%d and changes=%d disagree with durable high-water marks nodes=%d and changes=%d: %w",
			nodeSequence, changeSequence, state.NodeHighWater, state.ChangeHighWater, syscall.EIO)
	}
	var (
		invalidNamespaceRoot, invalidEntryNode, invalidChangeNode int64
		maximumNamespaceRoot, maximumEntryNode, maximumChangeNode int64
	)
	if err := db.QueryRowContext(ctx, globalNodeIdentityBoundsQuery).Scan(
		&invalidNamespaceRoot, &maximumNamespaceRoot,
		&invalidEntryNode, &maximumEntryNode,
		&invalidChangeNode, &maximumChangeNode,
	); err != nil {
		return State{}, err
	}
	if invalidNamespaceRoot != 0 || invalidEntryNode != 0 || invalidChangeNode != 0 {
		return State{}, fmt.Errorf(
			"global node identities contain invalid storage classes in namespaces=%d entries=%d changes=%d: %w",
			invalidNamespaceRoot, invalidEntryNode, invalidChangeNode, syscall.EIO)
	}
	var maximumNodeID int64
	if err := db.QueryRowContext(ctx, maximumNodeIdentityQuery).Scan(&maximumNodeID); err != nil {
		return State{}, err
	}
	maximumNode := max(maximumNodeID, maximumNamespaceRoot, maximumEntryNode, maximumChangeNode)

	var invalidChangePosition, invalidLogPosition, maximumRetainedPosition, maximumLogPosition int64
	if err := db.QueryRowContext(ctx, globalChangeIdentityBoundsQuery).Scan(
		&invalidChangePosition, &maximumRetainedPosition,
		&invalidLogPosition, &maximumLogPosition,
	); err != nil {
		return State{}, err
	}
	if invalidChangePosition != 0 || invalidLogPosition != 0 {
		return State{}, fmt.Errorf(
			"global change identities contain invalid storage classes in changes=%d logs=%d: %w",
			invalidChangePosition, invalidLogPosition, syscall.EIO)
	}
	maximumChange := max(maximumRetainedPosition, maximumLogPosition)
	if maximumNode > state.NodeHighWater || maximumChange > state.ChangeHighWater {
		return State{}, fmt.Errorf(
			"durable high-water marks nodes=%d and changes=%d are below surviving identities nodes=%d and changes=%d: %w",
			state.NodeHighWater, state.ChangeHighWater, maximumNode, maximumChange, syscall.EIO)
	}

	return state, nil
}

func AdvanceGeneration(ctx context.Context, tx *sql.Tx) (State, error) {
	state, err := Read(ctx, tx)
	if err != nil {
		return State{}, err
	}
	if state.Generation == math.MaxInt64 {
		return State{}, fmt.Errorf("the database generation is exhausted: %w", syscall.ENOSPC)
	}
	state.Generation++
	result, err := tx.ExecContext(ctx, `UPDATE database_state SET generation = ? WHERE singleton = 1`, state.Generation)
	if err != nil {
		return State{}, err
	}
	if err := sqlvalue.ExactlyOne(result, "the singleton durable-state row, which is missing"); err != nil {
		return State{}, err
	}
	return state, nil
}
