package dbstate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func ValidateIdentityBounds(ctx context.Context, db sqlvalue.Queryer, volume int64) error {
	state, err := Read(ctx, db)
	if err != nil {
		return err
	}
	var invalidNodes, invalidChanges int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM volumes WHERE id = ? AND root > ?) +
			(SELECT count(*) FROM nodes WHERE volume = ? AND id > ?) +
			(SELECT count(*) FROM entries WHERE volume = ? AND (parent > ? OR node > ?)) +
			(SELECT count(*) FROM changes WHERE volume = ? AND (
				parent > ? OR coalesce(from_parent, 0) > ? OR coalesce(node, 0) > ?
			))`,
		volume, state.NodeHighWater,
		volume, state.NodeHighWater,
		volume, state.NodeHighWater, state.NodeHighWater,
		volume, state.NodeHighWater, state.NodeHighWater, state.NodeHighWater,
	).Scan(&invalidNodes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM logs WHERE volume = ? AND (
				committed_position > ? OR trimmed_through > ?
			)) +
			(SELECT count(*) FROM changes WHERE volume = ? AND (
				position > ? OR previous_position > ?
			))`,
		volume, state.ChangeHighWater, state.ChangeHighWater,
		volume, state.ChangeHighWater, state.ChangeHighWater,
	).Scan(&invalidChanges); err != nil {
		return err
	}
	if invalidNodes != 0 || invalidChanges != 0 {
		return fmt.Errorf("volume %d has %d node identities and %d change positions above durable high-water marks: %w",
			volume, invalidNodes, invalidChanges, syscall.EIO)
	}
	return nil
}

func SequenceValue(ctx context.Context, db sqlvalue.Queryer, name string) (int64, error) {
	var rows, valid, value int64
	if err := db.QueryRowContext(ctx, `
		SELECT count(*),
		       count(CASE WHEN typeof(seq) = 'integer' AND seq >= 0 THEN 1 END),
		       coalesce(max(CASE WHEN typeof(seq) = 'integer' THEN seq END), 0)
		FROM sqlite_sequence WHERE name = ?`, name).Scan(&rows, &valid, &value); err != nil {
		return 0, err
	}
	if rows > 1 || valid != rows {
		return 0, fmt.Errorf("SQLite sequence %q has %d rows, of which %d are valid: %w",
			name, rows, valid, syscall.EIO)
	}
	return value, nil
}

func AllocateNodeID(ctx context.Context, tx *sql.Tx) (int64, error) {
	state, err := Read(ctx, tx)
	if err != nil {
		return 0, err
	}
	sequence, err := SequenceValue(ctx, tx, "nodes")
	if err != nil {
		return 0, err
	}
	if sequence != state.NodeHighWater {
		return 0, fmt.Errorf("SQLite node sequence %d disagrees with durable high-water mark %d: %w",
			sequence, state.NodeHighWater, syscall.EIO)
	}
	if state.NodeHighWater == math.MaxInt64 {
		return 0, fmt.Errorf("the node identity space is exhausted: %w", syscall.ENOSPC)
	}
	next := state.NodeHighWater + 1
	if _, err := tx.ExecContext(ctx, `UPDATE database_state SET node_high_water = ? WHERE singleton = 1`, next); err != nil {
		return 0, err
	}
	return next, nil
}

func ObserveNewNodeID(ctx context.Context, tx *sql.Tx, id int64) error {
	if id <= 0 {
		return fmt.Errorf("node identity %d is not positive: %w", id, syscall.EIO)
	}
	state, err := Read(ctx, tx)
	if err != nil {
		return err
	}
	sequence, err := SequenceValue(ctx, tx, "nodes")
	if err != nil {
		return err
	}
	if sequence != state.NodeHighWater {
		return fmt.Errorf("SQLite node sequence %d disagrees with durable high-water mark %d: %w",
			sequence, state.NodeHighWater, syscall.EIO)
	}
	if id <= state.NodeHighWater {
		return fmt.Errorf("new node identity %d does not advance high-water mark %d: %w",
			id, state.NodeHighWater, syscall.EIO)
	}
	_, err = tx.ExecContext(ctx, `UPDATE database_state SET node_high_water = ? WHERE singleton = 1`, id)
	return err
}

func AllocateChangePosition(ctx context.Context, tx *sql.Tx) (int64, error) {
	state, err := Read(ctx, tx)
	if err != nil {
		return 0, err
	}
	sequence, err := SequenceValue(ctx, tx, "changes")
	if err != nil {
		return 0, err
	}
	if sequence != state.ChangeHighWater {
		return 0, fmt.Errorf("SQLite change sequence %d disagrees with durable high-water mark %d: %w",
			sequence, state.ChangeHighWater, syscall.EIO)
	}
	if state.ChangeHighWater == math.MaxInt64 {
		return 0, fmt.Errorf("the change position space is exhausted: %w", syscall.ENOSPC)
	}
	next := state.ChangeHighWater + 1
	if _, err := tx.ExecContext(ctx, `UPDATE database_state SET change_high_water = ? WHERE singleton = 1`, next); err != nil {
		return 0, err
	}
	return next, nil
}
