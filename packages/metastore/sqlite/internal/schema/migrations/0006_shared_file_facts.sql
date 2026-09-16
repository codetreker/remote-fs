-- Common node facts, stable entry identities, and durable removal intentions.
-- Historical POSIX permissions become a versioned opaque value. The serving
-- schema does not interpret that value or preserve a platform mode column.

ALTER TABLE nodes ADD COLUMN kind INTEGER NOT NULL DEFAULT 1;
ALTER TABLE nodes ADD COLUMN metadata_revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE nodes ADD COLUMN directory_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN creation_sec INTEGER;
ALTER TABLE nodes ADD COLUMN creation_nsec INTEGER;
ALTER TABLE nodes ADD COLUMN change_sec INTEGER;
ALTER TABLE nodes ADD COLUMN change_nsec INTEGER;
ALTER TABLE nodes ADD COLUMN metadata BLOB NOT NULL DEFAULT X'52464d010000';
ALTER TABLE nodes ADD COLUMN link_target BLOB NOT NULL DEFAULT X'';

-- RFM1 contains one key, "posix", at version 1 with a 16-byte value:
-- little-endian flags=1, POSIX mode, UID=0, GID=0. UID/GID are absent;
-- historical data records neither ownership nor creation/change instants.
WITH permissions AS (
    SELECT id, (mode & 511) |
        CASE WHEN (mode & 1048576) != 0 THEN 512 ELSE 0 END |
        CASE WHEN (mode & 4194304) != 0 THEN 1024 ELSE 0 END |
        CASE WHEN (mode & 8388608) != 0 THEN 2048 ELSE 0 END AS posix_mode
    FROM nodes
)
UPDATE nodes
SET kind = CASE WHEN (mode & 2147483648) != 0 THEN 2 ELSE 1 END,
    directory_revision = CASE WHEN (mode & 2147483648) != 0 THEN 1 ELSE 0 END,
    metadata = (
        SELECT unhex('52464d01010005000100000010000000706f73697801000000' ||
            printf('%02x%02x', posix_mode & 255, (posix_mode >> 8) & 255) ||
            '00000000000000000000')
        FROM permissions WHERE permissions.id = nodes.id
    );

ALTER TABLE nodes DROP COLUMN mode;

-- Entry IDs reserve the same witnessed identity space as nodes. The temporary
-- CHECK refuses arithmetic overflow even when this migration is run directly.
CREATE TEMP TABLE shared_identity_migration (
    high_water INTEGER NOT NULL CHECK (typeof(high_water) = 'integer' AND high_water >= 0)
);
INSERT INTO shared_identity_migration
SELECT node_high_water + (SELECT count(*) FROM entries) FROM database_state;

ALTER TABLE entries ADD COLUMN id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE entries ADD COLUMN draining INTEGER NOT NULL DEFAULT 0;
ALTER TABLE entries ADD COLUMN drain_generation INTEGER NOT NULL DEFAULT 0;
ALTER TABLE entries ADD COLUMN drain_if_empty INTEGER NOT NULL DEFAULT 0;
ALTER TABLE entries ADD COLUMN drain_authority TEXT NOT NULL DEFAULT '';

WITH identities AS (
    SELECT volume, parent, name,
        (SELECT node_high_water FROM database_state) +
        row_number() OVER (ORDER BY volume, parent, name) AS identity
    FROM entries
)
UPDATE entries SET id = (
    SELECT identity FROM identities
    WHERE identities.volume = entries.volume AND identities.parent = entries.parent
        AND identities.name = entries.name
);
CREATE UNIQUE INDEX entries_by_identity ON entries (id);
CREATE INDEX entries_by_draining ON entries (draining);
CREATE INDEX entries_by_identity_bounds ON entries (
    CASE WHEN typeof(id) = 'integer' THEN 0 ELSE 1 END,
    id
);

UPDATE database_state SET node_high_water = (SELECT high_water FROM shared_identity_migration);
DELETE FROM sqlite_sequence WHERE name = 'nodes';
INSERT INTO sqlite_sequence (name, seq) SELECT 'nodes', node_high_water FROM database_state;
DROP TABLE shared_identity_migration;

-- Prepared intentions may outlive their entry. Retirement then consumes the
-- intention without granting permission to remove a replacement entry.
CREATE TABLE removal_intents (
    volume INTEGER NOT NULL,
    reference TEXT PRIMARY KEY,
    token TEXT NOT NULL UNIQUE,
    entry INTEGER NOT NULL,
    if_empty INTEGER NOT NULL,
    authority TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX removal_intents_by_volume ON removal_intents (volume, entry);

CREATE INDEX removal_intents_by_identity ON removal_intents (
    CASE WHEN typeof(entry) = 'integer' THEN 0 ELSE 1 END,
    entry
);

-- Old events do not contain complete immutable locations and metadata images.
-- A new incarnation explicitly invalidates old cursors; identity allocation
-- continues above the previous high-water marks even when every event is gone.
ALTER TABLE changes RENAME TO changes_v5;
CREATE TABLE changes (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    previous_position INTEGER NOT NULL,
    volume INTEGER NOT NULL REFERENCES volumes(id),
    kind INTEGER NOT NULL,
    parent INTEGER NOT NULL,
    name BLOB,
    from_parent INTEGER,
    from_name BLOB,
    node INTEGER,
    node_kind INTEGER,
    size INTEGER,
    atime_sec INTEGER,
    atime_nsec INTEGER,
    mtime_sec INTEGER,
    mtime_nsec INTEGER,
    creation_sec INTEGER,
    creation_nsec INTEGER,
    change_sec INTEGER,
    change_nsec INTEGER,
    metadata_revision INTEGER,
    directory_revision INTEGER,
    content TEXT,
    metadata BLOB,
    link_target BLOB,
    recorded_sec INTEGER NOT NULL,
    recorded_nsec INTEGER NOT NULL,
    identity_high_water INTEGER NOT NULL,
    notification BLOB NOT NULL
);
DROP TABLE changes_v5;

CREATE INDEX changes_by_volume ON changes (volume, position);
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
CREATE INDEX changes_by_fact_identity ON changes (
    CASE WHEN typeof(identity_high_water) = 'integer' THEN 0 ELSE 1 END,
    identity_high_water
);

DELETE FROM sqlite_sequence WHERE name = 'changes';
INSERT INTO sqlite_sequence (name, seq) SELECT 'changes', change_high_water FROM database_state;
UPDATE logs
SET incarnation = lower(hex(randomblob(16))),
    committed_position = 0,
    trimmed_through = 0,
    trimmed_by_age = 0;

CREATE TABLE file_lease_recovery (
    singleton           INTEGER PRIMARY KEY CHECK (singleton = 1),
    database_id         TEXT NOT NULL,
    state_id            TEXT NOT NULL,
    accepted_generation INTEGER NOT NULL,
    accepted_nanos      INTEGER NOT NULL,
    accepted_quiescent  INTEGER NOT NULL,
    prepared_generation INTEGER,
    prepared_nanos      INTEGER,
    prepared_quiescent  INTEGER
) WITHOUT ROWID;

-- Pending permits interrupted first-time anchor publication to resume. Once
-- Ready, a missing independent file-lease binding cannot initialize again.
CREATE TABLE file_lease_initialization (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state INTEGER NOT NULL CHECK (state IN (0, 1))
) WITHOUT ROWID;
INSERT INTO file_lease_initialization (singleton, state) VALUES (1, 0);
