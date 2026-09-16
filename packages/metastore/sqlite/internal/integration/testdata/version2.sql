-- An independent witness of the schema version 2 databases already hold.
-- It is executable test data, not a migration and not a description of today's schema.

CREATE TABLE nodes (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	volume  INTEGER NOT NULL REFERENCES volumes(id),
	mode       INTEGER NOT NULL,
	size       INTEGER NOT NULL,
	atime_sec  INTEGER NOT NULL,
	atime_nsec INTEGER NOT NULL,
	mtime_sec  INTEGER NOT NULL,
	mtime_nsec INTEGER NOT NULL,
	content    TEXT REFERENCES objects(key)
);

CREATE TABLE entries (
	volume INTEGER NOT NULL REFERENCES volumes(id),
	parent    INTEGER NOT NULL REFERENCES nodes(id),
	name      BLOB    NOT NULL,
	node      INTEGER NOT NULL REFERENCES nodes(id),
	PRIMARY KEY (volume, parent, name)
) WITHOUT ROWID;

CREATE INDEX entries_by_node ON entries (node);

CREATE TABLE volumes (
	id   INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT    NOT NULL UNIQUE,
	root INTEGER NOT NULL,
	used INTEGER NOT NULL
);

CREATE TABLE objects (
	key          TEXT PRIMARY KEY,
	volume    INTEGER NOT NULL REFERENCES volumes(id),
	state        INTEGER NOT NULL,
	size         INTEGER NOT NULL,
	digest       BLOB,
	created_sec  INTEGER NOT NULL,
	created_nsec INTEGER NOT NULL
);

CREATE INDEX objects_by_state ON objects (volume, state, created_sec);
CREATE INDEX nodes_by_content ON nodes (content);

CREATE TABLE changes (
	position      INTEGER PRIMARY KEY AUTOINCREMENT,
	volume     INTEGER NOT NULL REFERENCES volumes(id),
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

CREATE INDEX changes_by_volume ON changes (volume, position);

CREATE TABLE logs (
	volume          INTEGER PRIMARY KEY REFERENCES volumes(id),
	incarnation        TEXT    NOT NULL,
	committed_position INTEGER NOT NULL,
	trimmed_through    INTEGER NOT NULL,
	trimmed_by_age     INTEGER NOT NULL
);

CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version (version) VALUES (2);
