package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// The migrations, in the order they run. Each file is one version: 0001_tree.sql carries a
// database to version 1, 0002_replication.sql to version 2, and the version a database
// records is the number of the last file applied to it.
//
// They are SQL files rather than Go because a landed migration is history, and history has
// to be out of reach of the build that reads it. The shape this replaced reached version 2
// by running the DDL the current build writes, which is the same string only until the next
// version edits it — after which "carry a database to version 2" would quietly have meant
// "give it whatever shape version 3 has", and the version 2 to 3 migration would then have
// run on top of that. A file cannot do this, because nothing edits it.
//
// The rule that follows is the whole discipline here: a file that has landed is never
// changed. Changing the schema means adding a file. Prose in a landed file may be corrected,
// because SQLite stores the text of a CREATE statement and nothing else — which is why the
// files keep their comments between statements rather than inside them, and why
// testdata/schema.sql catches the difference.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

var schema = loadSchema()

// migration is one file: the version it produces and the statements that get there.
type migration struct {
	version    int
	file       string
	statements string
}

// currentVersion is the version a database has once every migration has run, and the only
// one this build writes.
func currentVersion() int { return schema[len(schema)-1].version }

// loadSchema reads the embedded migrations and checks that they are a numbering rather than
// a pile: named NNNN_something.sql, and numbered 1, 2, 3 with no gaps, so that "the recorded
// version" and "how many files have been applied" are the same statement.
//
// A build whose migrations fail that is broken in a way no caller could handle and no
// deployment should reach, so it stops here rather than at the first database it opens.
func loadSchema() []migration {
	// fs.Glob returns its matches sorted, and the four digits make that order the numeric
	// one. The check below is what holds the two together.
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		panic(fmt.Sprintf("sqlite: reading the embedded migrations: %v", err))
	}
	if len(names) == 0 {
		panic("sqlite: this build embeds no migrations, so it has no schema to write")
	}

	loaded := make([]migration, 0, len(names))
	for _, name := range names {
		file := path.Base(name)
		digits, _, found := strings.Cut(strings.TrimSuffix(file, ".sql"), "_")
		if !found {
			panic(fmt.Sprintf("sqlite: migration %q is not named NNNN_something.sql", file))
		}
		version, err := strconv.Atoi(digits)
		if err != nil {
			panic(fmt.Sprintf("sqlite: migration %q does not begin with a version: %v", file, err))
		}
		if want := len(loaded) + 1; version != want {
			panic(fmt.Sprintf("sqlite: migration %q is version %d where version %d was expected: "+
				"the migrations are numbered from 1 without gaps", file, version, want))
		}
		statements, err := migrationFiles.ReadFile(name)
		if err != nil {
			panic(fmt.Sprintf("sqlite: reading migration %q: %v", file, err))
		}
		loaded = append(loaded, migration{version: version, file: file, statements: string(statements)})
	}
	return loaded
}

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
// All of it happens in one transaction, so two processes opening the same new database
// cannot both decide they are the one to create it, a migration that fails part way leaves
// the database exactly as it was, and a database whose version this build refuses is not
// touched at all.
func prepare(ctx context.Context, db *sql.DB, namespace string, window Window) (id, root int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	if err := reachVersion(ctx, tx); err != nil {
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

// reachVersion runs every migration the database has not had, or refuses one this build
// cannot carry forward.
//
// A database with nothing in it runs all of them, which is the point: there is no separate
// statement of what the current layout is, so a database built today and a database carried
// forward from version 1 are the same schema by construction rather than because two authors
// kept two descriptions in step. Replaying a rebuild on an empty table costs nothing, and the
// alternative — a shortcut straight to the current shape — is the thing that drifts.
//
// Refusing is the other half of the version's job. Every statement in this package addresses
// columns by the meaning this build gives them, so running them against another layout would
// not fail loudly — it would update the wrong things. A version above this build's is refused
// with no attempt to read it, because a newer layout may have dropped or repurposed any column
// here and nothing in an older binary can know which.
func reachVersion(ctx context.Context, tx *sql.Tx) error {
	version, recorded, err := versionOf(ctx, tx)
	if err != nil {
		return err
	}
	if recorded {
		switch {
		case version == currentVersion():
			return nil
		case version > currentVersion():
			return fmt.Errorf("the database holds schema version %d and this build understands version %d: %w",
				version, currentVersion(), syscall.EINVAL)
		case version < 1:
			// No build ever wrote this. Whatever put it there, the tables present are not
			// something the migrations below can be run against.
			return fmt.Errorf("the database records schema version %d, which no build has ever written: %w",
				version, syscall.EINVAL)
		}
	}

	for _, m := range schema {
		if m.version <= version {
			continue
		}
		if _, err := tx.ExecContext(ctx, m.statements); err != nil {
			return fmt.Errorf("applying %s: %w", m.file, err)
		}
	}
	return recordVersion(ctx, tx, currentVersion())
}

// recordVersion replaces the recorded version. The row is deleted rather than updated so that
// one pair of statements serves both callers: a database that has just had the table created
// under it by the first migration and holds no row at all, and one being carried forward from
// a version it already recorded.
func recordVersion(ctx context.Context, tx *sql.Tx, version int) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM schema_version`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, version)
	return err
}

// versionOf reads the recorded schema version, reporting whether there is one at all.
//
// The two are kept apart because a database with no schema and a database whose recorded
// version happens to be 0 call for opposite actions, and a single integer cannot say which is
// which. Building the schema over a populated database because its version read as zero is the
// kind of mistake that leaves no trace of having been made. That is also why the version is a
// table rather than PRAGMA user_version, which is 0 both for a database nothing has touched
// and for one somebody set to 0.
func versionOf(ctx context.Context, tx *sql.Tx) (version int, recorded bool, err error) {
	var present int
	switch err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_version'`).Scan(&present); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	switch err := tx.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, fmt.Errorf("the database has a schema but records no version for it: %w", syscall.EINVAL)
	case err != nil:
		return 0, false, err
	}
	return version, true, nil
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
