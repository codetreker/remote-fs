package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"syscall"

	moderncsqlite "modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
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

// VolumeOpenMode decides whether opening may create the named volume.
type VolumeOpenMode uint8

const (
	CreateVolumeIfMissing VolumeOpenMode = iota + 1
	RequireExistingVolume
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
	reapDetached bool
	mode         VolumeOpenMode
	startup      DurableStartup
	witness      CommitWitness
}

func (d durableOpen) check() error {
	if d.mode != CreateVolumeIfMissing && d.mode != RequireExistingVolume {
		return fmt.Errorf("volume open mode %d is unknown: %w", d.mode, syscall.EINVAL)
	}
	if d.witness == nil {
		return fmt.Errorf("durable opening needs a commit witness: %w", syscall.EINVAL)
	}
	if err := dbstate.CheckStartup(d.startup.databaseStartup()); err != nil {
		return err
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
	pins    map[retainedNode]int
	domains map[int64]*fileDomain
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
		coordinator = &databaseCoordinator{key: key, durable: durable, commit: newCommitGate(), pins: make(map[retainedNode]int), domains: make(map[int64]*fileDomain)}
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
		c.poison = sqlerr.NewDurabilityFailure(err)
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
// or volume contents. The visible WAL, when present, is part of the returned database.
func InspectDurableState(ctx context.Context, database string) (DurableState, error) {
	values := url.Values{}
	values.Set("mode", "ro")
	values.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	db, err := sql.Open("sqlite", "file:"+database+"?"+values.Encode())
	if err != nil {
		return DurableState{}, fmt.Errorf("opening SQLite state for inspection: %w", sqlerr.Failure(err))
	}
	defer db.Close()
	state, err := dbstate.Validate(ctx, db)
	if err != nil {
		return DurableState{}, fmt.Errorf("inspecting SQLite durable state: %w", sqlerr.Failure(err))
	}
	return DurableState(state), nil
}

// DurableState returns the currently visible database-wide state. A poisoned Store refuses
// this operation because its witness no longer proves which committed state was accepted.
func (s *Store) DurableState(ctx context.Context) (DurableState, error) {
	var state dbstate.State
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		state, err = dbstate.Validate(ctx, tx)
		return err
	}); err != nil {
		return DurableState{}, fmt.Errorf("reading SQLite durable state: %w", sqlerr.Failure(err))
	}
	return DurableState(state), nil
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
		return CheckpointResult{}, fmt.Errorf("checkpointing the SQLite WAL: %w", sqlerr.Failure(err))
	}
	state, err := dbstate.Validate(ctx, s.write)
	if err != nil {
		return CheckpointResult{}, fmt.Errorf("validating SQLite state after checkpoint: %w", sqlerr.Failure(err))
	}
	result := CheckpointResult{
		State: DurableState(state), Complete: busy == 0 && logFrames == checkpointedFrames,
		LogFrames: logFrames, CheckpointedFrames: checkpointedFrames,
	}
	if !result.Complete || s.witness == nil {
		return result, nil
	}
	if err := s.witness.Checkpoint(DurableState(state)); err != nil {
		return CheckpointResult{}, fmt.Errorf("publishing checkpointed SQLite state at generation %d: %w",
			state.Generation, errors.Join(err, syscall.EIO))
	}
	return result, nil
}

func (s DurableStartup) databaseStartup() dbstate.Startup {
	return dbstate.Startup{
		Accepted:               dbstate.State(s.Accepted),
		CheckpointedGeneration: s.CheckpointedGeneration,
		WALPresent:             s.WALPresent,
		WALNonEmpty:            s.WALNonEmpty,
	}
}

// persistentWALConnector keeps the WAL across abnormal pool closure so recovery retains the
// fact needed to reconcile a failed witness publication. A successful witnessed Close clears
// the flag only after every frame and the checkpoint witness are durable.
type persistentWALConnector struct{ driver.Connector }

func (c persistentWALConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connection, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	control, ok := connection.(moderncsqlite.FileControl)
	if !ok {
		return nil, errors.Join(
			openCloseFailure("SQLite persistent WAL connection", connection.Close()),
			fmt.Errorf("the SQLite driver does not expose persistent WAL control: %w", syscall.EIO),
		)
	}
	mode, err := control.FileControlPersistWAL("main", 1)
	if err != nil || mode != 1 {
		return nil, errors.Join(
			openCloseFailure("SQLite persistent WAL connection", connection.Close()),
			fmt.Errorf("enabling persistent SQLite WAL returned mode %d: %v: %w", mode, err, syscall.EIO),
		)
	}
	return connection, nil
}

func openPersistentWriterPool(
	ctx context.Context,
	database string,
	maxConnections int,
) (*sql.DB, error) {
	base, err := moderncsqlite.NewConnector(poolDataSource(database, true))
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(persistentWALConnector{Connector: base})
	db.SetMaxOpenConns(maxConnections)
	if err := requireFullSynchronous(ctx, db); err != nil {
		return nil, errors.Join(err, poolCloseFailure("writer pool", db.Close()))
	}
	return db, nil
}

func disablePersistentWAL(ctx context.Context, db *sql.DB) error {
	connection, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring the SQLite writer connection: %w", sqlerr.Failure(err))
	}
	rawErr := connection.Raw(func(driverConnection any) error {
		control, ok := driverConnection.(moderncsqlite.FileControl)
		if !ok {
			return fmt.Errorf("the SQLite driver does not expose persistent WAL control: %w", syscall.EIO)
		}
		mode, err := control.FileControlPersistWAL("main", 0)
		if err != nil || mode != 0 {
			return fmt.Errorf("disabling persistent SQLite WAL returned mode %d: %v: %w",
				mode, err, syscall.EIO)
		}
		return nil
	})
	return errors.Join(rawErr, connection.Close())
}
