// Package schema prepares and validates SQLite metadata in caller-selected volumes.
// Schema migration and startup recovery share one transaction.
package schema

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

// The migrations that build this package's schema. 0001_tree.sql is the tree as version 1 had
// it; 0002_replication.sql rekeys the entry table and adds the change log;
// 0003_durable_state.sql adds the backing-store binding and durable identity witnesses;
// 0004_lease_recovery.sql stores prepared and accepted lease-duration evidence;
// 0005_retained_files.sql records unnamed retained files and their content revisions;
// 0006_neutral_metadata.sql separates node kind and adds common times, canonical opaque
// metadata, and exact retained-metadata accounting; 0007_durable_identity.sql adds
// symbolic-link data and durable, restart-queryable deletion obligations;
// 0008_directory_revisions.sql adds persistent directory name-set revisions;
// 0009_delete_intent_owners.sql adds owner-scoped discovery with stable cursors.
//
// packages/sqliteschema documents what a numbered set of files buys and what rule they are kept
// under: a file that has landed is never edited, and a schema change is a new file.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

var schema = sqliteschema.MustLoad(migrationFiles, "migrations")

// Schema version 3 is the first version written together with exact reservation sizes and an
// unresolved state that does not authorize deletion when Put ownership is unknown.
const firstOwnershipAwareSchemaVersion = 3

const firstRetainedFileSchemaVersion = 5

const firstNeutralMetadataSchemaVersion = 6

const firstDurableIdentitySchemaVersion = 7

const firstDirectoryRevisionSchemaVersion = 8

const firstDeleteIntentOwnerSchemaVersion = 9

// VolumeOpenMode decides whether preparation may create the named volume.
type VolumeOpenMode uint8

const (
	CreateVolumeIfMissing VolumeOpenMode = iota + 1
	RequireExistingVolume
)

// DurableOpen contains startup evidence captured before the database was opened.
// Witnessed records witness presence; preparation never publishes that witness.
type DurableOpen struct {
	ReapDetached bool
	Mode         VolumeOpenMode
	Startup      dbstate.Startup
	Witnessed    bool
}

// Prepare brings the database to the layout this build writes, and returns the id of the
// named volume and of its root directory, creating both when the volume is new.
//
// All of it happens in one transaction, so two processes opening the same new database cannot
// both decide they are the one to create it, a migration that fails part way leaves the
// database exactly as it was, and a database whose version this build refuses is not touched
// at all.
func Prepare(
	ctx context.Context,
	db *sql.DB,
	volume, storeID string,
	window changes.Window,
	maxIntegrityRecords, maxIntegrityBytes int64,
) (id, root int64, err error) {
	return PrepareWithMetadataLimit(ctx, db, volume, storeID, window, maxIntegrityRecords, maxIntegrityBytes, 64<<20)

}

func PrepareWithMetadataLimit(
	ctx context.Context,
	db *sql.DB,
	volume, storeID string,
	window changes.Window,
	maxIntegrityRecords, maxIntegrityBytes, maxMetadataBytes int64,
) (id, root int64, err error) {
	id, root, _, err = PrepareConfiguredWithMetadataPolicy(
		ctx, db, volume, storeID, window, maxIntegrityRecords, maxIntegrityBytes, maxMetadataBytes, false, nil,
	)
	return id, root, err
}

// PrepareConfigured reconciles startup evidence and optionally reaps detached files
// in the preparation transaction. The caller owns witness publication after success;
// a commit failure is classified by sqlerr.IsUncertainCommit.
func PrepareConfigured(
	ctx context.Context,
	db *sql.DB,
	volume, storeID string,
	window changes.Window,
	maxIntegrityRecords, maxIntegrityBytes int64,
	durable *DurableOpen,
) (id, root int64, state dbstate.State, err error) {
	return PrepareConfiguredWithMetadataLimit(ctx, db, volume, storeID, window,
		maxIntegrityRecords, maxIntegrityBytes, 64<<20, durable)
}

func PrepareConfiguredWithMetadataLimit(
	ctx context.Context,
	db *sql.DB,
	volume, storeID string,
	window changes.Window,
	maxIntegrityRecords, maxIntegrityBytes, maxMetadataBytes int64,
	durable *DurableOpen,
) (id, root int64, state dbstate.State, err error) {
	return PrepareConfiguredWithMetadataPolicy(ctx, db, volume, storeID, window,
		maxIntegrityRecords, maxIntegrityBytes, maxMetadataBytes, false, durable)
}

func PrepareConfiguredWithMetadataPolicy(
	ctx context.Context,
	db *sql.DB,
	volume, storeID string,
	window changes.Window,
	maxIntegrityRecords, maxIntegrityBytes, maxMetadataBytes int64,
	opaqueMetadataVersions bool,
	durable *DurableOpen,
) (id, root int64, state dbstate.State, err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, 0, dbstate.State{}, err
	}
	var rollback preparationRollbacker
	defer func() { err = finishPreparation(rollback, conn.Close, err) }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, dbstate.State{}, err
	}
	rollback = tx

	version, recorded, err := recordedSchemaVersion(ctx, tx)
	if err != nil {
		return 0, 0, dbstate.State{}, err
	}
	if durable != nil && durable.Startup.Accepted.DatabaseID != "" {
		if !recorded || version < firstOwnershipAwareSchemaVersion || version > schema.Version() {
			return 0, 0, dbstate.State{}, fmt.Errorf(
				"accepted durable state requires a supported durable schema between %d and %d, found recorded version %d: %w",
				firstOwnershipAwareSchemaVersion, schema.Version(), version, syscall.EIO)
		}
		if err := dbstate.ReconcileStartup(ctx, tx, durable.Startup); err != nil {
			return 0, 0, dbstate.State{}, err
		}
	}
	if durable != nil && durable.Witnessed && durable.Startup.Accepted.DatabaseID == "" &&
		durable.Mode == CreateVolumeIfMissing && recorded && version > 0 {
		return 0, 0, dbstate.State{}, fmt.Errorf(
			"creating an unwitnessed volume requires a pristine database, found schema version %d: %w",
			version, syscall.EIO)
	}
	legacy := recorded && version > 0 && version < firstOwnershipAwareSchemaVersion
	if recorded && version >= firstOwnershipAwareSchemaVersion && version < schema.Version() {
		if err := validateIntegrityWithMetadataPolicy(ctx, tx, nil, maxIntegrityRecords, maxIntegrityBytes,
			version, maxMetadataBytes, opaqueMetadataVersions); err != nil {
			return 0, 0, dbstate.State{}, err
		}
	}
	if legacy {
		if err := validateIntegrityWork(ctx, tx, nil, maxIntegrityRecords); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if err := validateIntegrityBytes(ctx, tx, nil, maxIntegrityBytes, version); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if err := validateLegacyObjectIntegrity(ctx, tx, version); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if err := dbstate.ValidateLegacySequences(ctx, tx, version); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if version == 1 {
			if err := validateVersionOneNodeRelationships(ctx, tx); err != nil {
				return 0, 0, dbstate.State{}, err
			}
		} else if err := validateNodeRelationshipsVersion(ctx, tx, nil, version); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if err := validateUsedAccountingVersion(ctx, tx, nil, version); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		// Version 1 predates the log tables. Version 2's rows and recorded tail are
		// validated before migration; its missing predecessor chain is why migration
		// starts a new incarnation rather than carrying that history forward.
		if version >= 2 {
			if err := validateVersionTwoLogIntegrity(ctx, tx, nil); err != nil {
				return 0, 0, dbstate.State{}, err
			}
		}
	}
	if recorded && version >= 1 && version < firstNeutralMetadataSchemaVersion {
		if err := validateLegacyModeMapping(ctx, tx, version); err != nil {
			return 0, 0, dbstate.State{}, err
		}
	}
	if recorded && version == firstNeutralMetadataSchemaVersion {
		var missingTargets int64
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT count(*) FROM nodes WHERE kind=3) +
			(SELECT count(*) FROM changes WHERE node_kind=3)`).Scan(&missingTargets); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if missingTargets != 0 {
			return 0, 0, dbstate.State{}, fmt.Errorf(
				"schema version 6 contains %d symbolic-link facts without recoverable targets: %w",
				missingTargets, syscall.EIO)
		}
	}
	if err := schema.Reach(ctx, tx); err != nil {
		return 0, 0, dbstate.State{}, err
	}
	// Version 1 did not carry volume on entries. Validate the global rooted tree and used
	// accounting after the migrations normalize that table, while the same transaction can
	// still roll every schema change back on refusal.
	if !recorded || version < firstNeutralMetadataSchemaVersion {
		if err := validateIntegrityWithMetadataPolicy(ctx, tx, nil, maxIntegrityRecords, maxIntegrityBytes,
			schema.Version(), math.MaxInt64, opaqueMetadataVersions); err != nil {
			return 0, 0, dbstate.State{}, err
		}
	}
	if _, err := dbstate.Validate(ctx, tx); err != nil {
		return 0, 0, dbstate.State{}, err
	}
	if err := bindBackingStore(ctx, tx, storeID); err != nil {
		return 0, 0, dbstate.State{}, err
	}

	switch err := tx.QueryRowContext(ctx,
		`SELECT id, root FROM volumes WHERE name = ?`, volume).Scan(&id, &root); {
	case errors.Is(err, sql.ErrNoRows):
		if durable != nil && durable.Mode == RequireExistingVolume {
			return 0, 0, dbstate.State{}, fmt.Errorf("volume %q is missing from the bound database: %w",
				volume, syscall.EIO)
		}
		if id, root, err = createVolume(ctx, tx, volume); err != nil {
			return 0, 0, dbstate.State{}, err
		}
	case err != nil:
		return 0, 0, dbstate.State{}, err
	}
	if err := validateIntegrityWithMetadataPolicy(ctx, tx, &id, maxIntegrityRecords, maxIntegrityBytes,
		schema.Version(), maxMetadataBytes, opaqueMetadataVersions); err != nil {
		return 0, 0, dbstate.State{}, err
	}
	if durable != nil && durable.ReapDetached {
		if err := validateIntegrityWithMetadataPolicy(ctx, tx, nil, maxIntegrityRecords, maxIntegrityBytes,
			schema.Version(), math.MaxInt64, opaqueMetadataVersions); err != nil {
			return 0, 0, dbstate.State{}, err
		}
		if err := reapDetachedFiles(ctx, tx); err != nil {
			return 0, 0, dbstate.State{}, err
		}
	}

	// A volume that has been quiet since the last process was here gets the trim that no
	// append arrived to perform.
	if err := changes.Trim(ctx, tx, id, window); err != nil {
		return 0, 0, dbstate.State{}, err
	}
	state, err = dbstate.AdvanceGeneration(ctx, tx)
	if err != nil {
		return 0, 0, dbstate.State{}, err
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, dbstate.State{}, sqlerr.NewUncertainCommit(err)
	}
	return id, root, state, nil
}

type preparationRollbacker interface{ Rollback() error }

func finishPreparation(tx preparationRollbacker, closeConnection func() error, primary error) error {
	var rollbackErr error
	if tx != nil {
		rollbackErr = tx.Rollback()
	}
	closeErr := closeConnection()
	if rollbackErr == sql.ErrTxDone && closeErr == nil {
		rollbackErr = nil
	}
	if cleanupErr := errors.Join(rollbackErr, closeErr); cleanupErr != nil {
		return errors.Join(primary, fmt.Errorf("releasing the SQLite preparation transaction: %w", sqlerr.NewDurabilityFailure(cleanupErr)))
	}
	return primary
}

// recordedSchemaVersion reads enough migration state for package-specific preflight checks.
// sqliteschema.Reach remains authoritative for malformed, absent, and unsupported versions.
func recordedSchemaVersion(ctx context.Context, tx *sql.Tx) (version int, recorded bool, err error) {
	var present int
	switch err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_version'`).Scan(&present); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	var raw any
	var storageClass string
	if err := tx.QueryRowContext(ctx, `
		SELECT CASE WHEN typeof(version) = 'integer' THEN version END, typeof(version)
		FROM schema_version LIMIT 1`).Scan(&raw, &storageClass); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	stored, ok := sqlvalue.StoredInteger(raw, storageClass)
	if !ok || stored < 0 || stored > math.MaxInt {
		return 0, false, fmt.Errorf("the recorded schema version uses an invalid scalar value: %w", syscall.EIO)
	}
	return int(stored), true, nil
}

// bindBackingStore checks the database-level object-store binding requested by an opener.
// An empty storeID is the unbound Open API. A non-empty storeID either proves an existing
// binding or establishes one while the database holds no volume.
func bindBackingStore(ctx context.Context, tx *sql.Tx, storeID string) error {
	var rows, valid, matching int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*), count(CASE
			WHEN typeof(singleton) = 'integer' AND singleton = 1 AND
				typeof(store_id) = 'text' AND store_id != ''
			THEN 1
		END), count(CASE
			WHEN typeof(singleton) = 'integer' AND singleton = 1 AND
				typeof(store_id) = 'text' AND store_id != '' AND store_id = ?
			THEN 1
		END)
		FROM backing_store`, storeID).Scan(&rows, &valid, &matching); err != nil {
		return err
	}
	if rows > 1 || valid != rows {
		return fmt.Errorf("the database holds %d backing-store bindings, of which %d are valid: %w",
			rows, valid, syscall.EIO)
	}

	if rows == 1 {
		if storeID == "" {
			return fmt.Errorf("the database is bound to a backing store: %w", syscall.EINVAL)
		}
		if matching != 1 {
			return fmt.Errorf("the database is bound to another backing store, not %q: %w",
				storeID, syscall.EINVAL)
		}
		return nil
	}

	if storeID == "" {
		return nil
	}

	var populated int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM volumes)`).Scan(&populated); err != nil {
		return err
	}
	if populated != 0 {
		return fmt.Errorf("the database already holds unbound volumes and cannot be bound to backing store %q: %w",
			storeID, syscall.EINVAL)
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO backing_store (singleton, store_id) VALUES (1, ?)`, storeID)
	return err
}

// createVolume makes a volume, the root directory it starts life with, and the log it
// will record its changes in.
//
// The root is inserted first because volumes.root names it, and the placeholder that
// stands in for the id until it is known never outlives this transaction.
func createVolume(ctx context.Context, tx *sql.Tx, volume string) (id, root int64, err error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO volumes (name, root, used) VALUES (?, 0, 0)`, volume)
	if err != nil {
		return 0, 0, err
	}
	if id, err = result.LastInsertId(); err != nil {
		return 0, 0, err
	}

	// The root is a directory nobody made, so it gets the moment the volume came into being.
	now := time.Now()
	sec, nsec := sqlvalue.StoredTime(now)
	root, err = dbstate.AllocateNodeID(ctx, tx)
	if err != nil {
		return 0, 0, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO nodes (id, volume, kind, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec,
		                   content, birth_sec, birth_nsec, change_sec, change_nsec, directory_revision)
		VALUES (?, ?, 2, 0, ?, ?, ?, ?, NULL, ?, ?, ?, ?, X'0000000000000001')`,
		root, id, sec, nsec, sec, nsec, sec, nsec, sec, nsec)
	if err != nil {
		return 0, 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE volumes SET root = ? WHERE id = ?`, root, id); err != nil {
		return 0, 0, err
	}
	if err := changes.CreateLog(ctx, tx, id); err != nil {
		return 0, 0, err
	}
	return id, root, nil
}
