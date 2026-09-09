-- Frozen schema version 3 with two populated namespaces.
-- DDL source: https://github.com/codetreker/remote-fs/blob/134e936ce3bde78a9db7becdfbccd72d6efe7536/packages/metastore/sqlite/testdata/schema.sql
-- Tables precede indexes for execution; SQLite creates sqlite_sequence itself.
-- Fixed rows exercise identity, content metadata, retained history, and durability migration.

CREATE TABLE backing_store (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	store_id  TEXT    NOT NULL CHECK (store_id <> '')
) WITHOUT ROWID;

CREATE TABLE changes (
	position          INTEGER PRIMARY KEY AUTOINCREMENT,
	previous_position INTEGER NOT NULL,
	namespace         INTEGER NOT NULL REFERENCES namespaces(id),
	kind              INTEGER NOT NULL,
	parent            INTEGER NOT NULL,
	name              BLOB,
	from_parent       INTEGER,
	from_name         BLOB,
	node              INTEGER,
	mode              INTEGER,
	size              INTEGER,
	atime_sec         INTEGER,
	atime_nsec        INTEGER,
	mtime_sec         INTEGER,
	mtime_nsec        INTEGER,
	content           TEXT,
	recorded_sec      INTEGER NOT NULL,
	recorded_nsec     INTEGER NOT NULL
);

CREATE TABLE database_state (
	singleton         INTEGER PRIMARY KEY CHECK (singleton = 1),
	database_id       TEXT    NOT NULL CHECK (length(database_id) = 32),
	generation        INTEGER NOT NULL CHECK (generation >= 0),
	node_high_water   INTEGER NOT NULL CHECK (node_high_water >= 0),
	change_high_water INTEGER NOT NULL CHECK (change_high_water >= 0)
) WITHOUT ROWID;

CREATE TABLE entries (
	namespace INTEGER NOT NULL REFERENCES namespaces(id),
	parent    INTEGER NOT NULL REFERENCES nodes(id),
	name      BLOB    NOT NULL,
	node      INTEGER NOT NULL REFERENCES nodes(id),
	PRIMARY KEY (namespace, parent, name)
) WITHOUT ROWID;

CREATE TABLE logs (
	namespace          INTEGER PRIMARY KEY REFERENCES namespaces(id),
	incarnation        TEXT    NOT NULL,
	committed_position INTEGER NOT NULL,
	trimmed_through    INTEGER NOT NULL,
	trimmed_by_age     INTEGER NOT NULL
);

CREATE TABLE namespaces (
	id   INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT    NOT NULL UNIQUE,
	root INTEGER NOT NULL,
	used INTEGER NOT NULL
);

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

CREATE TABLE objects (
	key          TEXT PRIMARY KEY,
	namespace    INTEGER NOT NULL REFERENCES namespaces(id),
	state        INTEGER NOT NULL,
	size         INTEGER NOT NULL,
	digest       BLOB,
	created_sec  INTEGER NOT NULL,
	created_nsec INTEGER NOT NULL
);

CREATE TABLE schema_version (version INTEGER NOT NULL);

CREATE INDEX changes_by_namespace ON changes (namespace, position);

CREATE INDEX changes_by_node_identity ON changes (
	CASE
	WHEN typeof(parent) = 'integer'
	AND typeof(from_parent) IN ('integer', 'null')
	AND typeof(node) IN ('integer', 'null')
	THEN 0 ELSE 1
	END,
	max(parent, coalesce(from_parent, 0), coalesce(node, 0))
);

CREATE INDEX changes_by_position_identity ON changes (
	CASE
	WHEN typeof(position) = 'integer' AND typeof(previous_position) = 'integer'
	THEN 0 ELSE 1
	END,
	max(position, previous_position)
);

CREATE INDEX entries_by_node ON entries (node);

CREATE INDEX entries_by_node_identity ON entries (
	CASE WHEN typeof(parent) = 'integer' AND typeof(node) = 'integer' THEN 0 ELSE 1 END,
	max(parent, node)
);

CREATE INDEX logs_by_change_identity ON logs (
	CASE
	WHEN typeof(committed_position) = 'integer' AND typeof(trimmed_through) = 'integer'
	THEN 0 ELSE 1
	END,
	max(committed_position, trimmed_through)
);

CREATE INDEX namespaces_by_root_identity ON namespaces (
	CASE WHEN typeof(root) = 'integer' THEN 0 ELSE 1 END,
	root
);

CREATE INDEX nodes_by_content ON nodes (content);

CREATE INDEX objects_by_state ON objects (namespace, state, created_sec);

INSERT INTO schema_version (version) VALUES (3);

INSERT INTO namespaces (id, name, root, used) VALUES
    (1, 'A', 1, 5),
    (2, 'B', 3, 7);

INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content) VALUES
    (1, 1, 2147484141, 0, 1700000000, 101, 1700000000, 102, NULL),
    (2, 1, 416, 5, 1700000100, 201, 1700000100, 202, 'historical-alpha-object'),
    (3, 2, 2147484141, 0, 1700000200, 301, 1700000200, 302, NULL),
    (4, 2, 384, 7, 1700000300, 401, 1700000300, 402, 'historical-bravo-object');

INSERT INTO entries (namespace, parent, name, node) VALUES
    (1, 1, X'616C7068612E747874', 2),
    (2, 3, X'627261766F2E747874', 4);

INSERT INTO objects (key, namespace, state, size, digest, created_sec, created_nsec) VALUES
    ('historical-alpha-object', 1, 1, 5, X'8ed3f6ad685b959ead7022518e1af76cd816f8e8ec7ccdda1ed4018e8f2223f8', 1700000100, 200),
    ('historical-bravo-object', 2, 1, 7, X'6cea34dead2db6cb9d17944e3d65f359f4091c2c1c68c3a0fbaaace005cd1ad3', 1700000300, 400);

INSERT INTO logs (namespace, incarnation, committed_position, trimmed_through, trimmed_by_age) VALUES
    (1, '11111111111111111111111111111111', 3, 0, 0),
    (2, '22222222222222222222222222222222', 4, 0, 0);

INSERT INTO changes (
    position, previous_position, namespace, kind, parent, name, from_parent, from_name,
    node, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content, recorded_sec, recorded_nsec
) VALUES
    (1, 0, 1, 0, 1, X'616C7068612E747874', NULL, NULL,
     2, 416, 5, 1700000100, 201, 1700000100, 202, 'historical-alpha-object', 1700000100, 203),
    (2, 0, 2, 0, 3, X'627261766F2E747874', NULL, NULL,
     4, 384, 7, 1700000300, 401, 1700000300, 402, 'historical-bravo-object', 1700000300, 403),
    (3, 1, 1, 2, 1, X'616C7068612E747874', NULL, NULL,
     2, 416, 5, 1700000100, 201, 1700000100, 202, 'historical-alpha-object', 1700000400, 501),
    (4, 2, 2, 2, 3, X'627261766F2E747874', NULL, NULL,
     4, 384, 7, 1700000300, 401, 1700000300, 402, 'historical-bravo-object', 1700000400, 502);

INSERT INTO database_state (singleton, database_id, generation, node_high_water, change_high_water)
VALUES (1, '33333333333333333333333333333333', 9, 4, 4);
