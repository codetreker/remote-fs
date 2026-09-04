package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

// The migrations that build this package's schema. 0001_tree.sql is the tree as version 1 had
// it; 0002_replication.sql rekeys the entry table and adds the change log;
// 0003_backing_store.sql records the object store the metadata belongs to.
//
// packages/sqliteschema documents what a numbered set of files buys and what rule they are kept
// under: a file that has landed is never edited, and a schema change is a new file.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

var schema = sqliteschema.MustLoad(migrationFiles, "migrations")

// The states an object passes through. Reserved is the state a key is in between the
// reservation and the commit. Unresolved retires a reservation whose Put did not prove
// ownership of whatever may be under the key. Garbage is deletion-authorized because a
// name stopped referencing the object or a caller with positive ownership proof abandoned
// it.
//
// The numbers are stored, so they are part of the schema.
const (
	stateReserved   = 0
	stateReferenced = 1
	stateGarbage    = 2
	stateUnresolved = 3
)

// Schema version 3 is the first version written together with exact reservation sizes and an
// unresolved state that does not authorize deletion when Put ownership is unknown.
const firstOwnershipAwareSchemaVersion = 3

// prepare brings the database to the layout this build writes, and returns the id of the
// named namespace and of its root directory, creating both when the namespace is new.
//
// All of it happens in one transaction, so two processes opening the same new database cannot
// both decide they are the one to create it, a migration that fails part way leaves the
// database exactly as it was, and a database whose version this build refuses is not touched
// at all.
func prepare(
	ctx context.Context,
	db *sql.DB,
	namespace, storeID string,
	window Window,
	maxIntegrityRecords int64,
) (id, root int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	version, recorded, err := recordedSchemaVersion(ctx, tx)
	if err != nil {
		return 0, 0, err
	}
	legacy := recorded && version > 0 && version < firstOwnershipAwareSchemaVersion
	if legacy {
		if err := validateIntegrityWork(ctx, tx, nil, maxIntegrityRecords); err != nil {
			return 0, 0, err
		}
		if err := validateLegacyObjectIntegrity(ctx, tx, version); err != nil {
			return 0, 0, err
		}
		if version == 1 {
			if err := validateVersionOneNodeRelationships(ctx, tx); err != nil {
				return 0, 0, err
			}
		} else if err := validateNodeRelationships(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
		if err := validateUsedAccounting(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
		// Version 1 predates the log tables. Version 2 already has the current log
		// layout, so its retained history can be proved before migration work starts.
		if version >= 2 {
			if err := validateLogIntegrity(ctx, tx, nil); err != nil {
				return 0, 0, err
			}
		}
	}
	if err := schema.Reach(ctx, tx); err != nil {
		return 0, 0, err
	}
	// Version 1 did not carry namespace on entries. Validate the global rooted tree and used
	// accounting after the migrations normalize that table, while the same transaction can
	// still roll every schema change back on refusal.
	if legacy {
		if err := validateStorageClasses(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
		if err := validateNodeValues(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
		if err := validateNodeRelationships(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
		if err := validateUsedAccounting(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
		if err := validateLogIntegrity(ctx, tx, nil); err != nil {
			return 0, 0, err
		}
	}
	if err := bindBackingStore(ctx, tx, storeID); err != nil {
		return 0, 0, err
	}

	switch err := tx.QueryRowContext(ctx,
		`SELECT id, root FROM namespaces WHERE name = ?`, namespace).Scan(&id, &root); {
	case errors.Is(err, sql.ErrNoRows):
		if id, root, err = createNamespace(ctx, tx, namespace); err != nil {
			return 0, 0, err
		}
	case err != nil:
		return 0, 0, err
	}
	if err := validateNamespaceIntegrity(ctx, tx, id, maxIntegrityRecords); err != nil {
		return 0, 0, err
	}

	// A namespace that has been quiet since the last process was here gets the trim that no
	// append arrived to perform.
	if err := trim(ctx, tx, id, window); err != nil {
		return 0, 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return id, root, nil
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
	if err := tx.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return version, true, nil
}

// bindBackingStore checks the database-level object-store binding requested by an opener.
// An empty storeID is the unbound Open API. A non-empty storeID either proves an existing
// binding or establishes one while the database holds no namespace.
func bindBackingStore(ctx context.Context, tx *sql.Tx, storeID string) error {
	var rows, valid int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*), count(CASE
			WHEN typeof(singleton) = 'integer' AND singleton = 1 AND
				typeof(store_id) = 'text' AND store_id != ''
			THEN 1
		END)
		FROM backing_store`).Scan(&rows, &valid); err != nil {
		return err
	}
	if rows > 1 || valid != rows {
		return fmt.Errorf("the database holds %d backing-store bindings, of which %d are valid: %w",
			rows, valid, syscall.EIO)
	}

	var bound string
	switch err := tx.QueryRowContext(ctx,
		`SELECT store_id FROM backing_store WHERE singleton = 1`).Scan(&bound); {
	case err == nil:
		switch {
		case storeID == "":
			return fmt.Errorf("the database is bound to backing store %q: %w", bound, syscall.EINVAL)
		case storeID != bound:
			return fmt.Errorf("the database is bound to backing store %q, not %q: %w",
				bound, storeID, syscall.EINVAL)
		default:
			return nil
		}
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}

	if storeID == "" {
		return nil
	}

	var populated int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM namespaces)`).Scan(&populated); err != nil {
		return err
	}
	if populated != 0 {
		return fmt.Errorf("the database already holds unbound namespaces and cannot be bound to backing store %q: %w",
			storeID, syscall.EINVAL)
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO backing_store (singleton, store_id) VALUES (1, ?)`, storeID)
	return err
}

// createNamespace makes a namespace, the root directory it starts life with, and the log it
// will record its changes in.
//
// The root is inserted first because namespaces.root names it, and the placeholder that
// stands in for the id until it is known never outlives this transaction.
func createNamespace(ctx context.Context, tx *sql.Tx, namespace string) (id, root int64, err error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO namespaces (name, root, used) VALUES (?, 0, 0)`, namespace)
	if err != nil {
		return 0, 0, err
	}
	if id, err = result.LastInsertId(); err != nil {
		return 0, 0, err
	}

	// The root is a directory nobody made, so it gets the mode a directory is made with and
	// the moment the namespace came into being.
	now := time.Now()
	sec, nsec := storedTime(now)
	result, err = tx.ExecContext(ctx, `
		INSERT INTO nodes (namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES (?, ?, 0, ?, ?, ?, ?, NULL)`,
		id, int64(fs.ModeDir|dirMode), sec, nsec, sec, nsec)
	if err != nil {
		return 0, 0, err
	}
	if root, err = result.LastInsertId(); err != nil {
		return 0, 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE namespaces SET root = ? WHERE id = ?`, root, id); err != nil {
		return 0, 0, err
	}
	if err := createLog(ctx, tx, id); err != nil {
		return 0, 0, err
	}
	return id, root, nil
}

// createLog gives a namespace an empty log: no entries, nothing committed, and an
// incarnation nothing has ever resumed against.
func createLog(ctx context.Context, tx *sql.Tx, namespace int64) error {
	incarnation, err := newIncarnation()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO logs (namespace, incarnation, committed_position, trimmed_through, trimmed_by_age)
		VALUES (?, ?, 0, 0, 0)`, namespace, string(incarnation))
	return err
}
