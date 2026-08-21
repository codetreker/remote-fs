package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"time"
)

// schemaVersion is the layout the statements below produce and the only one this package
// reads. A database carrying any other version is refused rather than adapted: the tree,
// the object states and the byte counter are all maintained by statements written against
// one shape, and a mismatch means those statements would be updating columns that mean
// something else.
//
// Migrations are hand-written, so a bump here comes with the statements that carry a
// database of the previous version forward.
const schemaVersion = 1

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

// schemaStatements builds the tables and the indexes. They run inside one transaction, so
// the database either has the whole layout or none of it.
//
// The tree is an inode table with an entry table keyed by (parent, name) beside it, which
// is what makes renaming a directory a change to one row rather than to every path beneath
// it. Keying a row by its whole path is the shape that fails that, and it fails a second
// obligation too: a path key has no way to tell "the parent directory is missing" from "the
// parent directory is not a directory", so it answers neither.
//
// Names are BLOB because a name on Linux is an arbitrary byte sequence. Declaring them TEXT
// would apply the default BINARY collation to values SQLite believes are UTF-8, and the
// ordering and the equality this schema depends on are the ones over bytes: `README` and
// `readme` are two names, and a name that is not valid UTF-8 is still a name.
//
// Times are two integer columns each rather than one count of nanoseconds. A nanosecond
// count in an int64 spans 1678 to 2262, and the instants a filesystem is asked to hold
// reach outside that in both directions; seconds and nanoseconds separately span every
// time.Time there is. The shape matches the one the wire format carries in
// packages/transport/httprest/message.go, so the two agree about what an instant is.
func schemaStatements() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS schema_version (
			version INTEGER NOT NULL
		)`,

		// A node knows nothing about the name it currently has; the entry table owns that.
		// That is what lets a rename move a subtree without touching the nodes in it.
		//
		// AUTOINCREMENT, so that an id is never handed out twice. Without it SQLite reuses
		// the largest id that was ever present once the row holding it is gone, and Node.ID
		// promises the opposite: something still holding an old id would find it pointing at
		// a node somebody else made.
		//
		// mode carries io/fs's bit layout rather than a kernel's st_mode, which is what
		// crosses every other boundary in this system.
		`CREATE TABLE IF NOT EXISTS nodes (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace  INTEGER NOT NULL REFERENCES namespaces(id),
			mode       INTEGER NOT NULL,
			size       INTEGER NOT NULL,
			atime_sec  INTEGER NOT NULL,
			atime_nsec INTEGER NOT NULL,
			mtime_sec  INTEGER NOT NULL,
			mtime_nsec INTEGER NOT NULL,
			content    TEXT REFERENCES objects(key)
		)`,

		// One row per name. The primary key is byte-exact over (parent, name), which is the
		// uniqueness a directory has.
		//
		// WITHOUT ROWID stores the rows in primary key order, so a directory's children are
		// contiguous and `ORDER BY name` is a scan of them in byte order rather than a sort.
		`CREATE TABLE IF NOT EXISTS entries (
			parent INTEGER NOT NULL REFERENCES nodes(id),
			name   BLOB    NOT NULL,
			node   INTEGER NOT NULL REFERENCES nodes(id),
			PRIMARY KEY (parent, name)
		) WITHOUT ROWID`,

		// Removing a node has SQLite check that no entry still points at it, which is a scan
		// of the whole table without this.
		`CREATE INDEX IF NOT EXISTS entries_by_node ON entries (node)`,

		// used is the exact number of bytes the namespace's files hold, maintained by the
		// same transactions that change a size. Space must not aggregate: a mount answers
		// Statfs under a two-second deadline because df touches every mountpoint on the
		// machine, so a SELECT SUM(size) over a large namespace would stall df everywhere on
		// the host.
		//
		// root has no REFERENCES clause, and it is the one column here that does not. The
		// reference it would carry runs the other way round the cycle nodes.namespace already
		// closes, and neither row can be inserted before the other. It is written once, in
		// the transaction that creates the root node it names.
		`CREATE TABLE IF NOT EXISTS namespaces (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT    NOT NULL UNIQUE,
			root INTEGER NOT NULL,
			used INTEGER NOT NULL
		)`,

		// An object's key is opaque and never reused, so it is its own identity. The state
		// is what a sweeper reads: an object nothing references is garbage whatever put it
		// there, and a reservation nobody committed becomes garbage once it is old enough
		// that no write could still be in flight for it.
		`CREATE TABLE IF NOT EXISTS objects (
			key          TEXT PRIMARY KEY,
			namespace    INTEGER NOT NULL REFERENCES namespaces(id),
			state        INTEGER NOT NULL,
			size         INTEGER NOT NULL,
			digest       BLOB,
			created_sec  INTEGER NOT NULL,
			created_nsec INTEGER NOT NULL
		)`,

		// The sweep reads by state and, for reservations, by age; and dropping an object's
		// row has SQLite check that no node still points at its key.
		`CREATE INDEX IF NOT EXISTS objects_by_state ON objects (namespace, state, created_sec)`,
		`CREATE INDEX IF NOT EXISTS nodes_by_content ON nodes (content)`,
	}
}

// prepare builds the schema if it is not there, checks the version if it is, and returns
// the id of the named namespace, creating it and its root directory when it is new.
//
// All of it happens in one transaction, so two processes opening the same new database
// cannot both decide they are the one to create it.
func prepare(ctx context.Context, db *sql.DB, namespace string) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	for _, statement := range schemaStatements() {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return 0, fmt.Errorf("building the schema: %w", err)
		}
	}
	if err := checkVersion(ctx, tx); err != nil {
		return 0, err
	}

	var id int64
	switch err := tx.QueryRowContext(ctx, `SELECT id FROM namespaces WHERE name = ?`, namespace).Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		if id, err = createNamespace(ctx, tx, namespace); err != nil {
			return 0, err
		}
	case err != nil:
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// checkVersion records the version this package writes, or refuses a database written by
// one it does not understand.
//
// Refusing is the whole point. Every statement in this package addresses columns by the
// meaning this version gives them, so running them against another layout would not fail
// loudly — it would update the wrong things.
func checkVersion(ctx context.Context, tx *sql.Tx) error {
	var version int
	switch err := tx.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version); {
	case errors.Is(err, sql.ErrNoRows):
		_, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, schemaVersion)
		return err
	case err != nil:
		return err
	case version != schemaVersion:
		return fmt.Errorf("the database holds schema version %d and this build understands version %d: %w",
			version, schemaVersion, syscall.EINVAL)
	}
	return nil
}

// createNamespace makes a namespace and the root directory it starts life with.
//
// The root is inserted first because namespaces.root names it, and the placeholder that
// stands in for the id until it is known never outlives this transaction.
func createNamespace(ctx context.Context, tx *sql.Tx, namespace string) (int64, error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO namespaces (name, root, used) VALUES (?, 0, 0)`, namespace)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
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
		return 0, err
	}
	root, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE namespaces SET root = ? WHERE id = ?`, root, id); err != nil {
		return 0, err
	}
	return id, nil
}
