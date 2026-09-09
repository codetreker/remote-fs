-- An independent witness of the schema version 2 databases already hold.
-- It is executable test data, not a migration and not a description of today's schema.

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

CREATE TABLE entries (
	namespace INTEGER NOT NULL REFERENCES namespaces(id),
	parent    INTEGER NOT NULL REFERENCES nodes(id),
	name      BLOB    NOT NULL,
	node      INTEGER NOT NULL REFERENCES nodes(id),
	PRIMARY KEY (namespace, parent, name)
) WITHOUT ROWID;

CREATE INDEX entries_by_node ON entries (node);

CREATE TABLE namespaces (
	id   INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT    NOT NULL UNIQUE,
	root INTEGER NOT NULL,
	used INTEGER NOT NULL
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

CREATE INDEX objects_by_state ON objects (namespace, state, created_sec);
CREATE INDEX nodes_by_content ON nodes (content);

CREATE TABLE changes (
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
);

CREATE INDEX changes_by_namespace ON changes (namespace, position);

CREATE TABLE logs (
	namespace          INTEGER PRIMARY KEY REFERENCES namespaces(id),
	incarnation        TEXT    NOT NULL,
	committed_position INTEGER NOT NULL,
	trimmed_through    INTEGER NOT NULL,
	trimmed_by_age     INTEGER NOT NULL
);

CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version (version) VALUES (2);
