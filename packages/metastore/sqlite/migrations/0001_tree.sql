-- The tree, its attributes, and the objects its files point at.
--
-- This file has landed. Its statements are history and are never edited: every database
-- that has ever recorded version 1 was built by exactly these, and a later build that
-- changed them would be describing a database that never existed. A change to the schema
-- is a new file, not an edit to this one.
--
-- Prose goes between statements rather than inside them. SQLite stores the text of a
-- CREATE statement verbatim, comments and all, so a comment inside one ends up in
-- sqlite_schema and in testdata/schema.sql beside it.

CREATE TABLE schema_version (
	version INTEGER NOT NULL
);

-- The tree is an inode table with an entry table beside it, which is what makes renaming a
-- directory a change to one row rather than to every path beneath it. Keying a row by its
-- whole path is the shape that fails that, and it fails a second obligation too: a path key
-- has no way to tell "the parent directory is missing" from "the parent directory is not a
-- directory", so it answers neither.
--
-- A node knows nothing about the name it currently has; the entry table owns that. That is
-- what lets a rename move a subtree without touching the nodes in it.
--
-- AUTOINCREMENT, so that an id is never handed out twice. Without it SQLite reuses the
-- largest id that was ever present once the row holding it is gone, and Node.ID promises the
-- opposite: something still holding an old id would find it pointing at a node somebody else
-- made.
--
-- mode carries io/fs's bit layout rather than a kernel's st_mode, which is what crosses every
-- other boundary in this system.
--
-- Times are two integer columns each rather than one count of nanoseconds. A nanosecond count
-- in an int64 spans 1678 to 2262, and the instants a filesystem is asked to hold reach outside
-- that in both directions; seconds and nanoseconds separately span every time.Time there is.
-- The shape matches the one the wire format carries in
-- packages/transport/httprest/message.go, so the two agree about what an instant is.
CREATE TABLE nodes (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	namespace  INTEGER NOT NULL REFERENCES namespaces(id),
	mode       INTEGER NOT NULL,
	size       INTEGER NOT NULL,
	atime_sec  INTEGER NOT NULL,
	atime_nsec INTEGER NOT NULL,
	mtime_sec  INTEGER NOT NULL,
	mtime_nsec INTEGER NOT NULL,
	content    TEXT REFERENCES objects(key)
);

-- One row per name, keyed byte-exactly by the directory it is in and the name itself.
--
-- Names are BLOB because a name on Linux is an arbitrary byte sequence. Declaring them TEXT
-- would apply the default BINARY collation to values SQLite believes are UTF-8, and the
-- ordering and the equality this schema depends on are the ones over bytes: `README` and
-- `readme` are two names, and a name that is not valid UTF-8 is still a name.
--
-- WITHOUT ROWID stores the rows in primary key order, so a directory's children are
-- contiguous and `ORDER BY name` is a scan of them in byte order rather than a sort.
CREATE TABLE entries (
	parent INTEGER NOT NULL REFERENCES nodes(id),
	name   BLOB    NOT NULL,
	node   INTEGER NOT NULL REFERENCES nodes(id),
	PRIMARY KEY (parent, name)
) WITHOUT ROWID;

-- Removing a node has SQLite check that no entry still points at it, which is a scan of the
-- whole table without this.
CREATE INDEX entries_by_node ON entries (node);

-- used is the exact number of bytes the namespace's files hold, maintained by the same
-- transactions that change a size. Space must not aggregate: a mount answers Statfs under a
-- two-second deadline because df touches every mountpoint on the machine, so a
-- SELECT SUM(size) over a large namespace would stall df everywhere on the host.
--
-- root has no REFERENCES clause, and it is the one column here that does not. The reference
-- it would carry runs the other way round the cycle nodes.namespace already closes, and
-- neither row can be inserted before the other. It is written once, in the transaction that
-- creates the root node it names.
CREATE TABLE namespaces (
	id   INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT    NOT NULL UNIQUE,
	root INTEGER NOT NULL,
	used INTEGER NOT NULL
);

-- An object's key is opaque and never reused, so it is its own identity. The state is what a
-- sweeper reads: an object nothing references is garbage whatever put it there, and a
-- reservation nobody committed becomes garbage once it is old enough that no write could
-- still be in flight for it. The numbers a state takes are stored, so they are part of this
-- schema; they are named in schema.go, beside the statements that read them.
CREATE TABLE objects (
	key          TEXT PRIMARY KEY,
	namespace    INTEGER NOT NULL REFERENCES namespaces(id),
	state        INTEGER NOT NULL,
	size         INTEGER NOT NULL,
	digest       BLOB,
	created_sec  INTEGER NOT NULL,
	created_nsec INTEGER NOT NULL
);

-- The sweep reads by state and, for reservations, by age; and dropping an object's row has
-- SQLite check that no node still points at its key.
CREATE INDEX objects_by_state ON objects (namespace, state, created_sec);
CREATE INDEX nodes_by_content ON nodes (content);
