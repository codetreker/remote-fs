package dbstate

import (
	"context"
	"database/sql"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

type Startup struct {
	Accepted               State
	CheckpointedGeneration int64
	WALPresent             bool
	WALNonEmpty            bool
}

func CheckStartup(s Startup) error {
	accepted := s.Accepted
	if accepted.DatabaseID == "" {
		if accepted.Generation != 0 || accepted.NodeHighWater != 0 || accepted.ChangeHighWater != 0 ||
			s.CheckpointedGeneration != 0 {
			return fmt.Errorf("a startup without a database identity carries durable counters: %w", syscall.EINVAL)
		}
		return nil
	}
	if err := CheckState(accepted); err != nil {
		return fmt.Errorf("the accepted startup state is invalid: %w", err)
	}
	if s.CheckpointedGeneration < 0 || s.CheckpointedGeneration > accepted.Generation {
		return fmt.Errorf("checkpointed generation %d is outside accepted generation %d: %w",
			s.CheckpointedGeneration, accepted.Generation, syscall.EINVAL)
	}
	if s.WALNonEmpty && !s.WALPresent {
		return fmt.Errorf("a non-empty WAL cannot be reported as absent: %w", syscall.EINVAL)
	}
	return nil
}

func ReconcileStartup(ctx context.Context, tx *sql.Tx, startup Startup) error {
	accepted := startup.Accepted
	if accepted.DatabaseID == "" {
		return nil
	}
	if startup.CheckpointedGeneration < accepted.Generation && !startup.WALNonEmpty {
		return fmt.Errorf(
			"accepted generation %d is newer than checkpointed generation %d, but the startup WAL is not present with frames: %w",
			accepted.Generation, startup.CheckpointedGeneration, syscall.EIO)
	}
	visible, err := Validate(ctx, tx)
	if err != nil {
		return err
	}
	if visible.DatabaseID != accepted.DatabaseID {
		return fmt.Errorf("the visible database identity %q is not the accepted identity %q: %w",
			visible.DatabaseID, accepted.DatabaseID, syscall.EIO)
	}
	if visible.Generation < accepted.Generation {
		walDescription := "absent"
		if startup.WALNonEmpty {
			walDescription = "non-empty"
		} else if startup.WALPresent {
			walDescription = "empty"
		}
		return fmt.Errorf("the visible database generation %d is older than accepted generation %d with a %s startup WAL: %w",
			visible.Generation, accepted.Generation, walDescription, syscall.EIO)
	}
	if visible.Generation == accepted.Generation && visible != accepted {
		return fmt.Errorf("accepted and visible durable state disagree at generation %d: %w",
			visible.Generation, syscall.EIO)
	}
	if visible.Generation > accepted.Generation && !startup.WALNonEmpty {
		return fmt.Errorf(
			"visible generation %d is newer than accepted generation %d without a non-empty startup WAL: %w",
			visible.Generation, accepted.Generation, syscall.EIO)
	}
	if visible.NodeHighWater < accepted.NodeHighWater || visible.ChangeHighWater < accepted.ChangeHighWater {
		return fmt.Errorf("visible high-water marks %+v are below accepted state %+v: %w",
			visible, accepted, syscall.EIO)
	}
	return nil
}

func ValidateLegacySequences(ctx context.Context, db sqlvalue.Queryer, version int) error {
	nodeSequence, err := SequenceValue(ctx, db, "nodes")
	if err != nil {
		return err
	}
	var invalidNodeIdentities, maximumNode int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM nodes WHERE typeof(id) != 'integer') +
			(SELECT count(*) FROM volumes WHERE typeof(root) != 'integer') +
			(SELECT count(*) FROM entries
			 WHERE typeof(parent) != 'integer' OR typeof(node) != 'integer'),
			max(
				coalesce((SELECT max(CASE WHEN typeof(id) = 'integer' THEN id ELSE 0 END) FROM nodes), 0),
				coalesce((SELECT max(CASE WHEN typeof(root) = 'integer' THEN root ELSE 0 END) FROM volumes), 0),
				coalesce((SELECT max(CASE WHEN typeof(parent) = 'integer' THEN parent ELSE 0 END) FROM entries), 0),
				coalesce((SELECT max(CASE WHEN typeof(node) = 'integer' THEN node ELSE 0 END) FROM entries), 0)
			)
	`).Scan(&invalidNodeIdentities, &maximumNode); err != nil {
		return err
	}
	if invalidNodeIdentities != 0 {
		return fmt.Errorf("schema version %d holds %d node identities in invalid storage classes: %w",
			version, invalidNodeIdentities, syscall.EIO)
	}
	if maximumNode > 0 && nodeSequence == 0 || nodeSequence < maximumNode {
		return fmt.Errorf("schema version %d has node sequence %d below existing node %d: %w",
			version, nodeSequence, maximumNode, syscall.EIO)
	}
	if version < 2 {
		return nil
	}
	var invalidChangeNodes, changeNodeMaximum int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			count(CASE
				WHEN typeof(parent) != 'integer'
					OR typeof(from_parent) NOT IN ('integer', 'null')
					OR typeof(node) NOT IN ('integer', 'null')
				THEN 1 END),
			max(
				coalesce(max(CASE WHEN typeof(parent) = 'integer' THEN parent ELSE 0 END), 0),
				coalesce(max(CASE WHEN typeof(from_parent) = 'integer' THEN from_parent ELSE 0 END), 0),
				coalesce(max(CASE WHEN typeof(node) = 'integer' THEN node ELSE 0 END), 0)
			)
		FROM changes`).Scan(&invalidChangeNodes, &changeNodeMaximum); err != nil {
		return err
	}
	if invalidChangeNodes != 0 {
		return fmt.Errorf("schema version %d holds %d retained node identities in invalid storage classes: %w",
			version, invalidChangeNodes, syscall.EIO)
	}
	if changeNodeMaximum > nodeSequence {
		return fmt.Errorf("schema version %d has node sequence %d below retained log identity %d: %w",
			version, nodeSequence, changeNodeMaximum, syscall.EIO)
	}
	changeSequence, err := SequenceValue(ctx, db, "changes")
	if err != nil {
		return err
	}
	var invalidChangePositions, maximumChange int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM changes WHERE typeof(position) != 'integer') +
			(SELECT count(*) FROM logs
			 WHERE typeof(committed_position) != 'integer' OR typeof(trimmed_through) != 'integer'),
			max(
				coalesce((SELECT max(CASE WHEN typeof(position) = 'integer' THEN position ELSE 0 END)
				          FROM changes), 0),
				coalesce((SELECT max(CASE WHEN typeof(committed_position) = 'integer'
				                         THEN committed_position ELSE 0 END) FROM logs), 0),
				coalesce((SELECT max(CASE WHEN typeof(trimmed_through) = 'integer'
				                         THEN trimmed_through ELSE 0 END) FROM logs), 0)
			)
	`).Scan(&invalidChangePositions, &maximumChange); err != nil {
		return err
	}
	if invalidChangePositions != 0 {
		return fmt.Errorf("schema version %d holds %d change positions in invalid storage classes: %w",
			version, invalidChangePositions, syscall.EIO)
	}
	if changeSequence < maximumChange {
		return fmt.Errorf("schema version %d has change sequence %d below retained position %d: %w",
			version, changeSequence, maximumChange, syscall.EIO)
	}
	return nil
}
