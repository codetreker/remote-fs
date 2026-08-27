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
// writes. A database carrying a version this build does not know is refused rather than
// adapted: the tree, the object states, the byte counter and the change log are all
// maintained by statements written against one shape, and a mismatch means those statements
// would be updating columns that mean something else.
//
// Migrations are hand-written and go forward only, so a bump here comes with the statements
// that carry a database of the previous version forward. A binary older than the database
// still refuses to start: it cannot know which of the columns it addresses the newer layout
// kept, and guessing is how a metastore starts answering with somebody else's data.
const schemaVersion = 2

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

// treeStatements builds the tables and the indexes that hold the tree, its attributes and
// the objects its files point at. They run inside one transaction, so the database either
// has the whole layout or none of it.
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
func treeStatements() []string {
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

		// One row per name. Uniqueness is byte-exact over (parent, name), which is the
		// uniqueness a directory has; the namespace in front of it adds none, because a parent
		// is a node id and node ids are unique across the database.
		//
		// It leads the key for locality. A picture of the tree is a range over this key, and
		// with the namespace in front, one namespace's entries are one contiguous stretch of it
		// — so the scan reads that namespace and nothing else. Without it the filter has to
		// come from the node on the other side of the join, and every plan that produces is
		// either a scan of every namespace's entries with the foreign ones thrown away one at
		// a time, or a sort of the whole namespace repeated for every page. Measured against
		// modernc.org/sqlite v1.57.0 over a million entries: 10m34s as a per-page sort, 578ms
		// sieving with the join order pinned, 123ms as this range. The column is therefore
		// redundant with nodes.namespace and is kept equal to it by every statement that
		// writes an entry.
		//
		// WITHOUT ROWID stores the rows in primary key order, so a directory's children are
		// contiguous and `ORDER BY name` is a scan of them in byte order rather than a sort.
		`CREATE TABLE IF NOT EXISTS entries (
			namespace INTEGER NOT NULL REFERENCES namespaces(id),
			parent    INTEGER NOT NULL REFERENCES nodes(id),
			name      BLOB    NOT NULL,
			node      INTEGER NOT NULL REFERENCES nodes(id),
			PRIMARY KEY (namespace, parent, name)
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

// logStatements builds what schema version 2 added: the change log a mount replicates from,
// and the per-namespace bookkeeping that makes a position resumable.
//
// They are separate from treeStatements so that the migration below reaches version 2 by
// running exactly the statements a fresh database runs, rather than by a second copy of the
// same DDL that a later edit would have to remember to keep in step.
func logStatements() []string {
	return []string{
		// One row per recorded change, shared by every namespace in the database. The
		// namespace column is what separates them; a shared sequence only means a namespace's
		// positions have gaps, and nothing above compares positions for adjacency.
		//
		// AUTOINCREMENT, so that a position is never handed out twice. Without it SQLite picks
		// one above the largest rowid the table currently holds, which is a different promise:
		// empty the table and the next insert starts again at 1. Measured against
		// modernc.org/sqlite v1.57.0 — deleting a prefix leaves the sequence alone under either
		// declaration, and emptying the table restarts it at 1 without AUTOINCREMENT and
		// continues past the high-water mark with it.
		//
		// What that would cost is out of proportion to the saving. A replica applies an event
		// only when its position is strictly greater than the one it has applied, so a position
		// issued a second time is discarded in silence, and nothing afterwards corrects it. The
		// trim below keeps at least one entry per namespace, so it cannot empty this table
		// today — but the numbers it trims to are configurable and were chosen rather than
		// measured, and "a position is never reused" must be a property of the table rather than
		// a consequence of how the trim happens to be tuned this week. nodes carries the same
		// reasoning for its own ids.
		//
		// Neither parent, node nor content carries a REFERENCES clause, and that is
		// deliberate: the log outlives what it describes. A Removed entry names a node that
		// is gone by definition, the directory it was in may be removed later, and the object
		// a Modified entry mentions is forgotten as soon as its bytes are swept. A reference
		// here would refuse the very rows the log exists to keep.
		//
		// name is NULL for the root, which has no name and no parent, matching how
		// metastore.Row and metastore.Change name it. The node columns are NULL for Removed,
		// which is the one kind that says what a name no longer holds.
		`CREATE TABLE IF NOT EXISTS changes (
			position      INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace     INTEGER NOT NULL REFERENCES namespaces(id),
			kind          INTEGER NOT NULL,
			parent        INTEGER NOT NULL,
			name          BLOB,
			from_parent   INTEGER,
			from_name     BLOB,
			node          INTEGER,
			mode          INTEGER,
			size          INTEGER,
			atime_sec     INTEGER,
			atime_nsec    INTEGER,
			mtime_sec     INTEGER,
			mtime_nsec    INTEGER,
			content       TEXT,
			recorded_sec  INTEGER NOT NULL,
			recorded_nsec INTEGER NOT NULL
		)`,

		// Every read of the log is "this namespace, in position order, after some position",
		// and every trim is "this namespace, the oldest few". Both are this index.
		`CREATE INDEX IF NOT EXISTS changes_by_namespace ON changes (namespace, position)`,

		// What a namespace's log is, apart from its entries.
		//
		// committed_position is the newest position the tree was changed at, written in the
		// same transaction as the change it names. It is not derivable from the entries,
		// because the entries are trimmed and a log that has discarded everything must still
		// be able to tell "you are caught up" from "you missed everything".
		//
		// incarnation names a run of history. It is random rather than a counter: restoring
		// this database from a backup would make a counter go backwards, and two unrelated
		// logs both sitting at 1 is entirely possible. A random value makes "does not match,
		// so rebuild" the only branch there is.
		//
		// trimmed_through is the newest position the trim has discarded, and 0 while it has
		// discarded nothing. It is what decides whether a returning replica can carry on, and
		// the oldest surviving entry cannot decide it: the position sequence is shared with
		// every other namespace in this database, so a namespace's own positions are spread by
		// however much its neighbours were written to in between. Reading resumability off
		// that distance sends replicas that had missed nothing away to walk the whole tree
		// again — and a namespace whose first change is not position 1, which is every
		// namespace but the first one written here, would send away even a replica that had
		// applied nothing at all.
		//
		// trimmed_by_age records which dimension pushed the oldest entries out, because age
		// and volume say different things to whoever is holding the pager: age means that
		// caller was away too long, volume means the namespace changes faster than the log
		// was configured to hold.
		`CREATE TABLE IF NOT EXISTS logs (
			namespace          INTEGER PRIMARY KEY REFERENCES namespaces(id),
			incarnation        TEXT    NOT NULL,
			committed_position INTEGER NOT NULL,
			trimmed_through    INTEGER NOT NULL,
			trimmed_by_age     INTEGER NOT NULL
		)`,
	}
}

// prepare builds the schema if it is not there, carries an older one forward if it can, and
// returns the id of the named namespace and of its root directory, creating both when the
// namespace is new.
//
// All of it happens in one transaction, so two processes opening the same new database
// cannot both decide they are the one to create it, and a database whose version this build
// refuses is left exactly as it was found.
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

// reachVersion brings the database to the layout this build writes, or refuses one written
// by a version it does not understand.
//
// Refusing is the whole point of the version. Every statement in this package addresses
// columns by the meaning this version gives them, so running them against another layout
// would not fail loudly — it would update the wrong things. That applies in both directions,
// which is why there is no attempt to read a version above this one: a newer layout may have
// dropped or repurposed any column here, and nothing in an older binary can know which.
func reachVersion(ctx context.Context, tx *sql.Tx) error {
	version, recorded, err := versionOf(ctx, tx)
	if err != nil {
		return err
	}
	switch {
	case !recorded:
		for _, statement := range append(treeStatements(), logStatements()...) {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("building the schema: %w", err)
			}
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, schemaVersion)
		return err
	case version == schemaVersion:
		return nil
	case version == 1:
		return migrateToTwo(ctx, tx)
	default:
		return fmt.Errorf("the database holds schema version %d and this build understands version %d: %w",
			version, schemaVersion, syscall.EINVAL)
	}
}

// versionOf reads the recorded schema version, reporting whether there is one at all.
//
// The two are kept apart because a database with no schema and a database whose recorded
// version happens to be 0 call for opposite actions, and a single integer cannot say which
// is which. Building the schema over a populated database because its version read as zero
// is the kind of mistake that leaves no trace of having been made.
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

// migrateToTwo carries a database written against version 1 forward to version 2, which
// added the change log and the per-namespace bookkeeping beside it.
//
// This is one hand-written path rather than the first entry in a migration framework. A
// framework has one user today, and the questions it would have to answer — a Go function or
// embedded SQL, a shared package or this one, how two processes starting at once interlock —
// have nothing behind them yet but a guess at the second migration's shape. It earns itself
// when there is a second one to generalise from.
//
// Every namespace comes out of this with a log that holds nothing and an incarnation it has
// never had before, which is the truthful statement: no replica can resume against a log
// whose history was never written down, and a new incarnation is how that is said.
func migrateToTwo(ctx context.Context, tx *sql.Tx) error {
	if err := rebuildEntries(ctx, tx); err != nil {
		return err
	}
	for _, statement := range logStatements() {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("adding the change log: %w", err)
		}
	}

	rows, err := tx.QueryContext(ctx, `SELECT id FROM namespaces`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var namespaces []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		namespaces = append(namespaces, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	for _, id := range namespaces {
		if err := createLog(ctx, tx, id); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE schema_version SET version = ?`, schemaVersion); err != nil {
		return err
	}
	return nil
}

// rebuildEntries carries the entry table from the shape version 1 gave it — keyed by
// (parent, name) — to the one version 2 needs, with the namespace in front of the key.
//
// A key cannot be changed in place, so the table is rebuilt: SQLite's ALTER TABLE will add a
// column but not move it into the primary key, and the primary key is the whole point of the
// change. The namespace each row belongs to is the one its node belongs to, which is where
// version 1 kept that fact and is the invariant every writer maintains from here on.
//
// Nothing references entries, so dropping it does not disturb a foreign key anywhere else,
// and the new table's own references to nodes and namespaces are satisfied by rows that are
// already there. The index over the node column goes with the old table and is rebuilt after.
func rebuildEntries(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		`CREATE TABLE entries_rebuilt (
			namespace INTEGER NOT NULL REFERENCES namespaces(id),
			parent    INTEGER NOT NULL REFERENCES nodes(id),
			name      BLOB    NOT NULL,
			node      INTEGER NOT NULL REFERENCES nodes(id),
			PRIMARY KEY (namespace, parent, name)
		) WITHOUT ROWID`,
		`INSERT INTO entries_rebuilt (namespace, parent, name, node)
		 SELECT n.namespace, e.parent, e.name, e.node FROM entries e JOIN nodes n ON n.id = e.node`,
		`DROP TABLE entries`,
		`ALTER TABLE entries_rebuilt RENAME TO entries`,
		`CREATE INDEX IF NOT EXISTS entries_by_node ON entries (node)`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("moving the entry table to its version 2 key: %w", err)
		}
	}
	return nil
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
