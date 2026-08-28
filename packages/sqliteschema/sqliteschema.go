// Package sqliteschema carries a SQLite database to the schema a build writes.
//
// A schema is a directory of numbered SQL files — 0001_something.sql, 0002_something.sql —
// replayed in order. The number is the version, and the version a database records is the
// number of the last file applied to it. Migrations go forward only: there is no down step,
// because the thing a down step is for, getting back to a build that has already been replaced,
// is a restore rather than a migration.
//
// # Why files
//
// A landed migration is history, and history has to be out of reach of the build that reads
// it. The shape this package exists to prevent is a migration written as a function that calls
// the DDL the current build writes: the two are the same string only until the next version
// edits it, after which "carry a database to version 2" quietly means "give it whatever shape
// version 3 has", and the version 2 to 3 migration then runs on top of that. A file cannot do
// this, because nothing edits it.
//
// The rule that follows is the whole discipline: a file that has landed is never changed, and
// changing the schema means adding a file. Prose in a landed file may be corrected, because
// SQLite stores the text of a CREATE statement and nothing else — which is why a file's
// comments belong between its statements rather than inside them.
//
// # Why a database with nothing in it replays everything
//
// Reach makes no distinction between an empty database and one being carried forward: both run
// every migration they have not had. There is deliberately no separate statement of the current
// layout to take a shortcut to, so a database built today and a database carried forward from
// version 1 are the same schema by construction rather than because two descriptions were kept
// in step by hand. Replaying a rebuild over an empty table costs nothing; the shortcut is the
// thing that drifts.
//
// What that gives up is the current shape being written down in one readable place. Dump is
// here for that: generate the schema a replay arrives at, record it beside the migrations, and
// compare. Comparing the two routes with Structure is the stronger check, because it fails even
// when somebody regenerates the record.
package sqliteschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"syscall"
)

// versionTable is where the version a database has reached is recorded.
//
// This package owns the table and creates it, so a caller's migrations describe only the
// caller's own schema. The name is fixed rather than configurable: every caller would pass the
// same value, and a database whose bookkeeping is somewhere unexpected is one no other tool can
// read.
const versionTable = "schema_version"

// Migrations is an ordered, forward-only numbering of the SQL files that build a schema.
type Migrations struct {
	steps []step
}

// step is one file: the version it produces and the statements that get there.
type step struct {
	version    int
	file       string
	statements string
}

// Load reads the numbered SQL files in dir and checks that they are a numbering rather than a
// pile: named NNNN_something.sql, and numbered 1, 2, 3 with no gaps, so that "the recorded
// version" and "how many files have been applied" are the same statement.
//
// Nothing is executed here and nothing is parsed beyond the names. A file's contents reach
// SQLite exactly as they were written.
func Load(fsys fs.FS, dir string) (Migrations, error) {
	// fs.Glob returns its matches sorted, and four digits make that order the numeric one. The
	// contiguity check below is what holds the two together.
	names, err := fs.Glob(fsys, path.Join(dir, "*.sql"))
	if err != nil {
		return Migrations{}, fmt.Errorf("reading the migrations in %s: %w", dir, err)
	}
	if len(names) == 0 {
		return Migrations{}, fmt.Errorf("there are no migrations in %s, so there is no schema to build: %w",
			dir, syscall.EINVAL)
	}

	steps := make([]step, 0, len(names))
	for _, name := range names {
		file := path.Base(name)
		digits, _, found := strings.Cut(strings.TrimSuffix(file, ".sql"), "_")
		if !found {
			return Migrations{}, fmt.Errorf("migration %q is not named NNNN_something.sql: %w", file, syscall.EINVAL)
		}
		version, err := strconv.Atoi(digits)
		if err != nil {
			return Migrations{}, fmt.Errorf("migration %q does not begin with a version: %w", file, syscall.EINVAL)
		}
		if want := len(steps) + 1; version != want {
			return Migrations{}, fmt.Errorf(
				"migration %q is version %d where version %d was expected, as they are numbered from 1 without gaps: %w",
				file, version, want, syscall.EINVAL)
		}
		statements, err := fs.ReadFile(fsys, name)
		if err != nil {
			return Migrations{}, fmt.Errorf("reading migration %q: %w", file, err)
		}
		steps = append(steps, step{version: version, file: file, statements: string(statements)})
	}
	return Migrations{steps: steps}, nil
}

// MustLoad is Load for the package-level variable a caller keeps its embedded migrations in.
//
// It panics rather than returning, because everything Load refuses is an authoring mistake in
// the build itself: a misnumbered file, a misnamed one, a glob that stopped matching. No caller
// could handle any of them, and a build carrying one is broken wherever it is deployed — so it
// stops here rather than at the first database somebody opens.
func MustLoad(fsys fs.FS, dir string) Migrations {
	migrations, err := Load(fsys, dir)
	if err != nil {
		panic("sqliteschema: " + err.Error())
	}
	return migrations
}

// Version is the version a database has once every migration has run.
func (m Migrations) Version() int { return m.steps[len(m.steps)-1].version }

// Reach runs every migration the database has not had, or refuses one this build cannot carry
// forward.
//
// It works inside the caller's transaction and does not commit: a migration that fails part way
// must leave the database exactly as it was, a database whose version is refused must not be
// touched at all, and the caller usually has more to do in the same atomic step — so the
// decision to commit is not this package's to take.
//
// Refusing is half of what a version is for. A caller's statements address columns by the
// meaning its own build gives them, so running them against another layout would not fail
// loudly, it would update the wrong things. A version above this build's is refused with no
// attempt to read it, because a newer layout may have dropped or repurposed any column and
// nothing in an older binary can know which. Every refusal wraps syscall.EINVAL.
func (m Migrations) Reach(ctx context.Context, tx *sql.Tx) error {
	version, recorded, err := versionOf(ctx, tx)
	if err != nil {
		return err
	}
	if recorded {
		switch {
		case version == m.Version():
			return nil
		case version > m.Version():
			return fmt.Errorf("the database holds schema version %d and this build understands version %d: %w",
				version, m.Version(), syscall.EINVAL)
		case version < 1:
			// No build ever wrote this. Whatever put it there, the tables present are not
			// something the migrations can be run against.
			return fmt.Errorf("the database records schema version %d, which no build has ever written: %w",
				version, syscall.EINVAL)
		}
	}

	for _, s := range m.steps {
		if s.version <= version {
			continue
		}
		if _, err := tx.ExecContext(ctx, s.statements); err != nil {
			return fmt.Errorf("applying %s: %w", s.file, err)
		}
	}
	if !recorded {
		if _, err := tx.ExecContext(ctx,
			`CREATE TABLE `+versionTable+` (version INTEGER NOT NULL)`); err != nil {
			return err
		}
	}
	// Replaced rather than updated, so that one pair of statements serves both a database that
	// has just had the table created above it and holds no row at all, and one being carried
	// forward from a version it already recorded.
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+versionTable); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO `+versionTable+` (version) VALUES (?)`, m.Version())
	return err
}

// versionOf reads the recorded schema version, reporting whether there is one at all.
//
// The two are kept apart because a database with no schema and a database whose recorded version
// happens to be 0 call for opposite actions, and a single integer cannot say which is which.
// Building a schema over a populated database because its version read as zero is the kind of
// mistake that leaves no trace of having been made. It is also why the version is a table rather
// than PRAGMA user_version, which reads 0 both for a database nothing has touched and for one
// somebody set to 0.
func versionOf(ctx context.Context, tx *sql.Tx) (version int, recorded bool, err error) {
	var present int
	switch err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?`, versionTable).Scan(&present); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	switch err := tx.QueryRowContext(ctx, `SELECT version FROM `+versionTable).Scan(&version); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, fmt.Errorf("the database has a schema but records no version for it: %w", syscall.EINVAL)
	case err != nil:
		return 0, false, err
	}
	return version, true, nil
}
