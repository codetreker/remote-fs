-- The backing-store binding, database-wide durability state, and verifiable retained log.
--
-- The binding and the durability witnesses form one format boundary: every object reference,
-- identity allocation, retained-log link, and external commit witness describes the same
-- database lineage.

-- A database is either unbound, represented by no row, or bound permanently to one backing
-- store. The fixed primary key makes that cardinality part of the schema.
CREATE TABLE backing_store (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	store_id  TEXT    NOT NULL CHECK (store_id <> '')
) WITHOUT ROWID;

-- This row is independent evidence for identities whose source rows may have been deleted.
-- SQLite's AUTOINCREMENT sequence remains enabled as redundant engine-level enforcement;
-- opening and allocation require both records to agree exactly.
CREATE TABLE database_state (
	singleton         INTEGER PRIMARY KEY CHECK (singleton = 1),
	database_id       TEXT    NOT NULL CHECK (length(database_id) = 32),
	generation        INTEGER NOT NULL CHECK (generation >= 0),
	node_high_water   INTEGER NOT NULL CHECK (node_high_water >= 0),
	change_high_water INTEGER NOT NULL CHECK (change_high_water >= 0)
) WITHOUT ROWID;

-- The leading expression exposes invalid storage classes without returning their values;
-- the second expression provides the largest valid identity as a bounded index lookup.
CREATE INDEX namespaces_by_root_identity ON namespaces (
	CASE WHEN typeof(root) = 'integer' THEN 0 ELSE 1 END,
	root
);
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

INSERT INTO database_state (
	singleton, database_id, generation, node_high_water, change_high_water
)
SELECT
	1,
	lower(hex(randomblob(16))),
	0,
	max(
		coalesce((SELECT max(seq) FROM sqlite_sequence WHERE name = 'nodes'), 0),
		coalesce((SELECT max(id) FROM nodes), 0),
		coalesce((SELECT max(root) FROM namespaces), 0),
		coalesce((SELECT max(parent) FROM entries), 0),
		coalesce((SELECT max(node) FROM entries), 0),
		coalesce((SELECT max(parent) FROM changes), 0),
		coalesce((SELECT max(from_parent) FROM changes), 0),
		coalesce((SELECT max(node) FROM changes), 0)
	),
	max(
		coalesce((SELECT max(seq) FROM sqlite_sequence WHERE name = 'changes'), 0),
		coalesce((SELECT max(position) FROM changes), 0),
		coalesce((SELECT max(committed_position) FROM logs), 0),
		coalesce((SELECT max(trimmed_through) FROM logs), 0)
	);

-- Version 2 did not record a predecessor for each retained change. Its history cannot be
-- upgraded into a continuity proof, so version 3 starts a new incarnation with an empty
-- retained log while preserving the global position high-water mark above.
ALTER TABLE changes RENAME TO changes_v2;

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

DROP TABLE changes_v2;

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

DELETE FROM sqlite_sequence WHERE name = 'changes';
INSERT INTO sqlite_sequence (name, seq)
SELECT 'changes', change_high_water FROM database_state;

UPDATE logs
SET incarnation = lower(hex(randomblob(16))),
	committed_position = 0,
	trimmed_through = 0,
	trimmed_by_age = 0;
