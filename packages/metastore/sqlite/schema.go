package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"io/fs"
	"time"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

// The migrations that build this package's schema. 0001_tree.sql is the tree as version 1 had
// it; 0002_replication.sql rekeys the entry table and adds the change log.
//
// packages/sqliteschema documents what a numbered set of files buys and what rule they are kept
// under: a file that has landed is never edited, and a schema change is a new file.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

var schema = sqliteschema.MustLoad(migrationFiles, "migrations")

// The states an object passes through. Reserved is the state a key is in between the
// reservation and the commit; garbage is where an object goes when the name that referenced
// it stops doing so, and where a reservation that was never committed is swept from.
//
// The numbers are stored, so they are part of the schema.
const (
	stateReserved   = 0
	stateReferenced = 1
	stateGarbage    = 2
)

// prepare brings the database to the layout this build writes, and returns the id of the
// named namespace and of its root directory, creating both when the namespace is new.
//
// All of it happens in one transaction, so two processes opening the same new database cannot
// both decide they are the one to create it, a migration that fails part way leaves the
// database exactly as it was, and a database whose version this build refuses is not touched
// at all.
func prepare(ctx context.Context, db *sql.DB, namespace string, window Window) (id, root int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	if err := schema.Reach(ctx, tx); err != nil {
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

	// Startup is where a log that lost entries is caught, and where a namespace that has been
	// quiet since the last process was here gets the trim that no append arrived to perform.
	if err := reconcile(ctx, tx, id); err != nil {
		return 0, 0, err
	}
	if err := trim(ctx, tx, id, window); err != nil {
		return 0, 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return id, root, nil
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
