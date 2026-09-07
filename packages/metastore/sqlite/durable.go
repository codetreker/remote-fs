package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// DurableState is the database identity and the monotonic facts that survive deletion of
// the rows which originally established them.
type DurableState struct {
	DatabaseID      string
	Generation      int64
	NodeHighWater   int64
	ChangeHighWater int64
}

// CommitWitness publishes database state outside SQLite's WAL. Accept is called only after
// SQLite has committed, and a mutating operation does not report success until Accept does.
// Checkpoint is called only after every WAL frame has reached the main database.
//
// Neither callback receives an operation context: cancellation cannot revoke the durability
// work required by a SQLite transaction which has already committed. An Accept failure
// poisons the Store because the committed state was not acknowledged. A Checkpoint failure
// leaves the accepted state valid and the WAL close barrier pending, so it may be retried.
type CommitWitness interface {
	Accept(DurableState) error
	Checkpoint(DurableState) error
}

// NamespaceOpenMode decides whether opening may create the named namespace.
type NamespaceOpenMode uint8

const (
	CreateNamespaceIfMissing NamespaceOpenMode = iota + 1
	RequireExistingNamespace
)

// DurableStartup is captured before SQLite opens the database and can create or recover WAL
// files. Accepted is the last state whose witness publication completed. A zero Accepted
// state is valid only while initializing a local store which has not published such a witness.
type DurableStartup struct {
	Accepted               DurableState
	CheckpointedGeneration int64
	WALPresent             bool
	WALNonEmpty            bool
}

// CheckpointMode selects SQLite's serialized checkpoint behavior.
type CheckpointMode uint8

const (
	PassiveCheckpoint CheckpointMode = iota + 1
	FullCheckpoint
)

// CheckpointResult reports SQLite's checkpoint counters. Complete is true only when every
// frame visible to the checkpoint was copied into the main database.
type CheckpointResult struct {
	State              DurableState
	Complete           bool
	LogFrames          int
	CheckpointedFrames int
}

type durableOpen struct {
	mode    NamespaceOpenMode
	startup DurableStartup
	witness CommitWitness
}

func (d durableOpen) check() error {
	if d.mode != CreateNamespaceIfMissing && d.mode != RequireExistingNamespace {
		return fmt.Errorf("namespace open mode %d is unknown: %w", d.mode, syscall.EINVAL)
	}
	if d.witness == nil {
		return fmt.Errorf("durable opening needs a commit witness: %w", syscall.EINVAL)
	}
	if err := d.startup.check(); err != nil {
		return err
	}
	return nil
}

func (s DurableStartup) check() error {
	accepted := s.Accepted
	if accepted.DatabaseID == "" {
		if accepted.Generation != 0 || accepted.NodeHighWater != 0 || accepted.ChangeHighWater != 0 ||
			s.CheckpointedGeneration != 0 {
			return fmt.Errorf("a startup without a database identity carries durable counters: %w", syscall.EINVAL)
		}
		return nil
	}
	if err := accepted.check(); err != nil {
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

func (s DurableState) check() error {
	if len(s.DatabaseID) != 32 || strings.Trim(s.DatabaseID, "0123456789abcdef") != "" {
		return fmt.Errorf("database identity %q is not 16 lowercase hexadecimal bytes: %w", s.DatabaseID, syscall.EIO)
	}
	if s.Generation < 0 || s.NodeHighWater < 0 || s.ChangeHighWater < 0 {
		return fmt.Errorf("database state has negative counters %+v: %w", s, syscall.EIO)
	}
	return nil
}

type databaseCoordinator struct {
	commit  commitGate
	health  sync.RWMutex
	poison  error
	refs    int
	key     string
	durable bool
	closing bool
}

type commitGate chan struct{}

func newCommitGate() commitGate {
	gate := make(commitGate, 1)
	gate <- struct{}{}
	return gate
}

func (g commitGate) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g:
	}
	if err := ctx.Err(); err != nil {
		g.release()
		return err
	}
	return nil
}

func (g commitGate) release() { g <- struct{}{} }

var databaseCoordinators = struct {
	sync.Mutex
	byPath map[string]*databaseCoordinator
}{byPath: make(map[string]*databaseCoordinator)}

func acquireCoordinator(database string, durable bool) (*databaseCoordinator, error) {
	key, err := filepath.Abs(filepath.Clean(database))
	if err != nil {
		return nil, fmt.Errorf("resolving the SQLite database path: %w", err)
	}
	databaseCoordinators.Lock()
	defer databaseCoordinators.Unlock()
	coordinator := databaseCoordinators.byPath[key]
	if coordinator == nil {
		coordinator = &databaseCoordinator{key: key, durable: durable, commit: newCommitGate()}
		databaseCoordinators.byPath[key] = coordinator
	} else if coordinator.durable || durable {
		return nil, fmt.Errorf("a durably witnessed SQLite database cannot share its process with another opener: %w",
			syscall.EBUSY)
	}
	coordinator.refs++
	return coordinator, nil
}

func releaseCoordinator(coordinator *databaseCoordinator) {
	databaseCoordinators.Lock()
	defer databaseCoordinators.Unlock()
	coordinator.refs--
	if coordinator.refs == 0 {
		delete(databaseCoordinators.byPath, coordinator.key)
	}
}

func (c *databaseCoordinator) healthy() error {
	c.health.RLock()
	defer c.health.RUnlock()
	return c.healthErrorLocked()
}

func (c *databaseCoordinator) beginHealthyRead() error {
	c.health.RLock()
	if err := c.healthErrorLocked(); err == nil {
		return nil
	} else {
		c.health.RUnlock()
		return err
	}
}

func (c *databaseCoordinator) endHealthyRead() { c.health.RUnlock() }

func (c *databaseCoordinator) poisonWith(err error) {
	c.health.Lock()
	defer c.health.Unlock()
	c.poisonLocked(err)
}

func (c *databaseCoordinator) poisonLocked(err error) {
	if c.poison == nil {
		c.poison = fmt.Errorf("%w: %w", syscall.EIO, err)
	}
}

func (c *databaseCoordinator) healthErrorLocked() error {
	if c.poison != nil {
		return fmt.Errorf("the SQLite database has an unresolved durability failure: %w", c.poison)
	}
	if c.closing {
		return fmt.Errorf("the SQLite database is closing: %w", syscall.EIO)
	}
	return nil
}

// InspectDurableState reads the database-wide state without changing schema, journal mode,
// or namespace contents. The visible WAL, when present, is part of the returned database.
func InspectDurableState(ctx context.Context, database string) (DurableState, error) {
	values := url.Values{}
	values.Set("mode", "ro")
	values.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	db, err := sql.Open("sqlite", "file:"+database+"?"+values.Encode())
	if err != nil {
		return DurableState{}, fmt.Errorf("opening SQLite state for inspection: %w", failure(err))
	}
	defer db.Close()
	state, err := validateDurableState(ctx, db)
	if err != nil {
		return DurableState{}, fmt.Errorf("inspecting SQLite durable state: %w", failure(err))
	}
	return state, nil
}

// DurableState returns the currently visible database-wide state. A poisoned Store refuses
// this operation because its witness no longer proves which committed state was accepted.
func (s *Store) DurableState(ctx context.Context) (DurableState, error) {
	var state DurableState
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = validateDurableState(ctx, tx)
		return err
	}); err != nil {
		return DurableState{}, fmt.Errorf("reading SQLite durable state: %w", failure(err))
	}
	return state, nil
}

func readDurableState(ctx context.Context, db integrityQueryer) (DurableState, error) {
	var (
		state DurableState
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
		return DurableState{}, err
	}
	if rows != 1 || valid != 1 {
		return DurableState{}, fmt.Errorf("the database holds %d durable-state rows, of which %d have valid storage classes: %w",
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
		return DurableState{}, err
	}
	if err := state.check(); err != nil {
		return DurableState{}, err
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

func validateDurableState(ctx context.Context, db integrityQueryer) (DurableState, error) {
	state, err := readDurableState(ctx, db)
	if err != nil {
		return DurableState{}, err
	}
	nodeSequence, err := sequenceValue(ctx, db, "nodes")
	if err != nil {
		return DurableState{}, err
	}
	changeSequence, err := sequenceValue(ctx, db, "changes")
	if err != nil {
		return DurableState{}, err
	}
	if nodeSequence != state.NodeHighWater || changeSequence != state.ChangeHighWater {
		return DurableState{}, fmt.Errorf(
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
		return DurableState{}, err
	}
	if invalidNamespaceRoot != 0 || invalidEntryNode != 0 || invalidChangeNode != 0 {
		return DurableState{}, fmt.Errorf(
			"global node identities contain invalid storage classes in namespaces=%d entries=%d changes=%d: %w",
			invalidNamespaceRoot, invalidEntryNode, invalidChangeNode, syscall.EIO)
	}
	var maximumNodeID int64
	if err := db.QueryRowContext(ctx, maximumNodeIdentityQuery).Scan(&maximumNodeID); err != nil {
		return DurableState{}, err
	}
	maximumNode := max(maximumNodeID, maximumNamespaceRoot, maximumEntryNode, maximumChangeNode)

	var invalidChangePosition, invalidLogPosition, maximumRetainedPosition, maximumLogPosition int64
	if err := db.QueryRowContext(ctx, globalChangeIdentityBoundsQuery).Scan(
		&invalidChangePosition, &maximumRetainedPosition,
		&invalidLogPosition, &maximumLogPosition,
	); err != nil {
		return DurableState{}, err
	}
	if invalidChangePosition != 0 || invalidLogPosition != 0 {
		return DurableState{}, fmt.Errorf(
			"global change identities contain invalid storage classes in changes=%d logs=%d: %w",
			invalidChangePosition, invalidLogPosition, syscall.EIO)
	}
	maximumChange := max(maximumRetainedPosition, maximumLogPosition)
	if maximumNode > state.NodeHighWater || maximumChange > state.ChangeHighWater {
		return DurableState{}, fmt.Errorf(
			"durable high-water marks nodes=%d and changes=%d are below surviving identities nodes=%d and changes=%d: %w",
			state.NodeHighWater, state.ChangeHighWater, maximumNode, maximumChange, syscall.EIO)
	}

	return state, nil
}

func validateIdentityBounds(ctx context.Context, db integrityQueryer, namespace int64) error {
	state, err := readDurableState(ctx, db)
	if err != nil {
		return err
	}
	var invalidNodes, invalidChanges int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM namespaces WHERE id = ? AND root > ?) +
			(SELECT count(*) FROM nodes WHERE namespace = ? AND id > ?) +
			(SELECT count(*) FROM entries WHERE namespace = ? AND (parent > ? OR node > ?)) +
			(SELECT count(*) FROM changes WHERE namespace = ? AND (
				parent > ? OR coalesce(from_parent, 0) > ? OR coalesce(node, 0) > ?
			))`,
		namespace, state.NodeHighWater,
		namespace, state.NodeHighWater,
		namespace, state.NodeHighWater, state.NodeHighWater,
		namespace, state.NodeHighWater, state.NodeHighWater, state.NodeHighWater,
	).Scan(&invalidNodes); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM logs WHERE namespace = ? AND (
				committed_position > ? OR trimmed_through > ?
			)) +
			(SELECT count(*) FROM changes WHERE namespace = ? AND (
				position > ? OR previous_position > ?
			))`,
		namespace, state.ChangeHighWater, state.ChangeHighWater,
		namespace, state.ChangeHighWater, state.ChangeHighWater,
	).Scan(&invalidChanges); err != nil {
		return err
	}
	if invalidNodes != 0 || invalidChanges != 0 {
		return fmt.Errorf("namespace %d has %d node identities and %d change positions above durable high-water marks: %w",
			namespace, invalidNodes, invalidChanges, syscall.EIO)
	}
	return nil
}

func sequenceValue(ctx context.Context, db integrityQueryer, name string) (int64, error) {
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

func validateLegacySequences(ctx context.Context, db integrityQueryer, version int) error {
	nodeSequence, err := sequenceValue(ctx, db, "nodes")
	if err != nil {
		return err
	}
	var invalidNodeIdentities, maximumNode int64
	if err := db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM nodes WHERE typeof(id) != 'integer') +
			(SELECT count(*) FROM namespaces WHERE typeof(root) != 'integer') +
			(SELECT count(*) FROM entries
			 WHERE typeof(parent) != 'integer' OR typeof(node) != 'integer'),
			max(
				coalesce((SELECT max(CASE WHEN typeof(id) = 'integer' THEN id ELSE 0 END) FROM nodes), 0),
				coalesce((SELECT max(CASE WHEN typeof(root) = 'integer' THEN root ELSE 0 END) FROM namespaces), 0),
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
	changeSequence, err := sequenceValue(ctx, db, "changes")
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

func reconcileStartup(ctx context.Context, tx *sql.Tx, startup DurableStartup) error {
	accepted := startup.Accepted
	if accepted.DatabaseID == "" {
		return nil
	}
	if startup.CheckpointedGeneration < accepted.Generation && !startup.WALNonEmpty {
		return fmt.Errorf(
			"accepted generation %d is newer than checkpointed generation %d, but the startup WAL is not present with frames: %w",
			accepted.Generation, startup.CheckpointedGeneration, syscall.EIO)
	}
	visible, err := validateDurableState(ctx, tx)
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

func advanceGeneration(ctx context.Context, tx *sql.Tx) (DurableState, error) {
	state, err := readDurableState(ctx, tx)
	if err != nil {
		return DurableState{}, err
	}
	if state.Generation == math.MaxInt64 {
		return DurableState{}, fmt.Errorf("the database generation is exhausted: %w", syscall.ENOSPC)
	}
	state.Generation++
	result, err := tx.ExecContext(ctx, `UPDATE database_state SET generation = ? WHERE singleton = 1`, state.Generation)
	if err != nil {
		return DurableState{}, err
	}
	if err := exactlyOne(result, "the singleton durable-state row, which is missing"); err != nil {
		return DurableState{}, err
	}
	return state, nil
}

func allocateNodeID(ctx context.Context, tx *sql.Tx) (int64, error) {
	state, err := readDurableState(ctx, tx)
	if err != nil {
		return 0, err
	}
	sequence, err := sequenceValue(ctx, tx, "nodes")
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

func observeNewNodeID(ctx context.Context, tx *sql.Tx, id int64) error {
	if id <= 0 {
		return fmt.Errorf("node identity %d is not positive: %w", id, syscall.EIO)
	}
	state, err := readDurableState(ctx, tx)
	if err != nil {
		return err
	}
	sequence, err := sequenceValue(ctx, tx, "nodes")
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

func allocateChangePosition(ctx context.Context, tx *sql.Tx) (int64, error) {
	state, err := readDurableState(ctx, tx)
	if err != nil {
		return 0, err
	}
	sequence, err := sequenceValue(ctx, tx, "changes")
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

func (s *Store) acceptLocked(state DurableState) error {
	if s.witness == nil {
		return nil
	}
	if err := s.witness.Accept(state); err != nil {
		s.coordinator.poisonLocked(fmt.Errorf("publishing accepted SQLite state at generation %d: %w", state.Generation, err))
		return s.coordinator.healthErrorLocked()
	}
	return nil
}

// Checkpoint copies WAL frames into the main database while serialized with package writes
// and every witness publication. An incomplete checkpoint is reported in-band because a live
// snapshot legitimately prevents SQLite from passing its read mark.
func (s *Store) Checkpoint(ctx context.Context, mode CheckpointMode) (CheckpointResult, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return CheckpointResult{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.coordinator.healthy(); err != nil {
		return CheckpointResult{}, err
	}
	return s.checkpointLocked(ctx, mode)
}

func (s *Store) checkpointLocked(ctx context.Context, mode CheckpointMode) (CheckpointResult, error) {
	pragma := ""
	switch mode {
	case PassiveCheckpoint:
		pragma = "PASSIVE"
	case FullCheckpoint:
		pragma = "FULL"
	default:
		return CheckpointResult{}, fmt.Errorf("checkpoint mode %d is unknown: %w", mode, syscall.EINVAL)
	}
	var busy, logFrames, checkpointedFrames int
	if err := s.write.QueryRowContext(ctx, `PRAGMA wal_checkpoint(`+pragma+`)`).Scan(
		&busy, &logFrames, &checkpointedFrames,
	); err != nil {
		return CheckpointResult{}, fmt.Errorf("checkpointing the SQLite WAL: %w", failure(err))
	}
	state, err := validateDurableState(ctx, s.write)
	if err != nil {
		return CheckpointResult{}, fmt.Errorf("validating SQLite state after checkpoint: %w", failure(err))
	}
	result := CheckpointResult{
		State: state, Complete: busy == 0 && logFrames == checkpointedFrames,
		LogFrames: logFrames, CheckpointedFrames: checkpointedFrames,
	}
	if !result.Complete || s.witness == nil {
		return result, nil
	}
	if err := s.witness.Checkpoint(state); err != nil {
		return CheckpointResult{}, fmt.Errorf("publishing checkpointed SQLite state at generation %d: %v: %w",
			state.Generation, err, syscall.EIO)
	}
	return result, nil
}

func isUncertainCommit(err error) bool {
	var uncertain *uncertainCommitError
	return errors.As(err, &uncertain)
}

type uncertainCommitError struct{ err error }

func (e *uncertainCommitError) Error() string { return e.err.Error() }
func (e *uncertainCommitError) Unwrap() error { return e.err }
