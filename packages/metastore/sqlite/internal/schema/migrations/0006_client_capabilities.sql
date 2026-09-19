-- Common facts and opaque client metadata share the existing node identities.
-- Historical permissions use the client-owned posix.permissions.v1 namespace:
-- one little-endian uint32 containing POSIX permission and special bits.

ALTER TABLE nodes ADD COLUMN kind INTEGER NOT NULL DEFAULT 1;
ALTER TABLE nodes ADD COLUMN birth_sec INTEGER;
ALTER TABLE nodes ADD COLUMN birth_nsec INTEGER;
ALTER TABLE nodes ADD COLUMN change_sec INTEGER;
ALTER TABLE nodes ADD COLUMN change_nsec INTEGER;
ALTER TABLE nodes ADD COLUMN metadata BLOB NOT NULL DEFAULT X'52464d010000';
ALTER TABLE nodes ADD COLUMN link_target BLOB NOT NULL DEFAULT X'';
ALTER TABLE nodes ADD COLUMN directory_revision BLOB NOT NULL DEFAULT X'';
ALTER TABLE nodes ADD COLUMN pending_unlink INTEGER NOT NULL DEFAULT 0 CHECK (pending_unlink IN (0, 1));
ALTER TABLE nodes ADD COLUMN pending_generation INTEGER NOT NULL DEFAULT 0 CHECK (pending_generation >= 0);

WITH permissions AS (
    SELECT id, (mode & 511) |
        CASE WHEN (mode & 1048576) != 0 THEN 512 ELSE 0 END |
        CASE WHEN (mode & 4194304) != 0 THEN 1024 ELSE 0 END |
        CASE WHEN (mode & 8388608) != 0 THEN 2048 ELSE 0 END AS posix_mode
    FROM nodes
)
UPDATE nodes
SET kind = CASE WHEN (mode & 2147483648) != 0 THEN 2 ELSE 1 END,
    directory_revision = CASE WHEN (mode & 2147483648) != 0 THEN X'0000000000000001' ELSE X'' END,
    metadata = (
        SELECT unhex('52464d0101001400080004000000706f7369782e7065726d697373696f6e732e76310000000000000001' ||
            printf('%02x%02x', posix_mode & 255, (posix_mode >> 8) & 255) || '0000')
        FROM permissions WHERE permissions.id = nodes.id
    );
ALTER TABLE nodes DROP COLUMN mode;
CREATE INDEX nodes_by_volume ON nodes (volume, id);
CREATE INDEX nodes_pending_unlink ON nodes (volume, id) WHERE pending_unlink = 1;

-- Retained events keep their own historical attributes and positions. Missing
-- creation/change instants remain NULL; no current node supplies past facts.
ALTER TABLE changes ADD COLUMN node_kind INTEGER;
ALTER TABLE changes ADD COLUMN birth_sec INTEGER;
ALTER TABLE changes ADD COLUMN birth_nsec INTEGER;
ALTER TABLE changes ADD COLUMN change_sec INTEGER;
ALTER TABLE changes ADD COLUMN change_nsec INTEGER;
ALTER TABLE changes ADD COLUMN metadata BLOB;
ALTER TABLE changes ADD COLUMN link_target BLOB;
ALTER TABLE changes ADD COLUMN directory_revision BLOB;

WITH permissions AS (
    SELECT position, (mode & 511) |
        CASE WHEN (mode & 1048576) != 0 THEN 512 ELSE 0 END |
        CASE WHEN (mode & 4194304) != 0 THEN 1024 ELSE 0 END |
        CASE WHEN (mode & 8388608) != 0 THEN 2048 ELSE 0 END AS posix_mode
    FROM changes WHERE node IS NOT NULL
)
UPDATE changes
SET node_kind = CASE WHEN (mode & 2147483648) != 0 THEN 2 ELSE 1 END,
    metadata = (
        SELECT unhex('52464d0101001400080004000000706f7369782e7065726d697373696f6e732e76310000000000000001' ||
            printf('%02x%02x', posix_mode & 255, (posix_mode >> 8) & 255) || '0000')
        FROM permissions WHERE permissions.position = changes.position
    ),
    link_target = X''
WHERE node IS NOT NULL;
ALTER TABLE changes DROP COLUMN mode;

ALTER TABLE volumes ADD COLUMN metadata_used INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(metadata_used) = 'integer' AND metadata_used >= 0);
UPDATE volumes SET metadata_used =
    coalesce((SELECT sum(length(metadata) + length(link_target)) FROM nodes WHERE nodes.volume = volumes.id), 0) +
    coalesce((SELECT sum(coalesce(length(metadata), 0) + coalesce(length(link_target), 0)) FROM changes WHERE changes.volume = volumes.id), 0);

CREATE TABLE close_intents (
    volume INTEGER NOT NULL,
    node INTEGER NOT NULL,
    incarnation BLOB NOT NULL,
    reference BLOB PRIMARY KEY,
    if_empty INTEGER NOT NULL CHECK (if_empty IN (0, 1))
) WITHOUT ROWID;
CREATE INDEX close_intents_by_node ON close_intents (volume, node, incarnation, reference);

-- The aggregate includes detached nodes and retained history copies. SQL writers,
-- replica ingestion, trimming and physical deletion use the same accounting.
CREATE TRIGGER nodes_metadata_insert AFTER INSERT ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used + length(NEW.metadata) + length(NEW.link_target)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER nodes_metadata_update AFTER UPDATE OF metadata, link_target, volume ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - length(OLD.metadata) - length(OLD.link_target)
    WHERE id = OLD.volume;
    UPDATE volumes SET metadata_used = metadata_used + length(NEW.metadata) + length(NEW.link_target)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER nodes_metadata_delete AFTER DELETE ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - length(OLD.metadata) - length(OLD.link_target)
    WHERE id = OLD.volume;
END;

CREATE TRIGGER changes_metadata_insert AFTER INSERT ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used + coalesce(length(NEW.metadata), 0) + coalesce(length(NEW.link_target), 0)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER changes_metadata_update AFTER UPDATE OF metadata, link_target, volume ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - coalesce(length(OLD.metadata), 0) - coalesce(length(OLD.link_target), 0)
    WHERE id = OLD.volume;
    UPDATE volumes SET metadata_used = metadata_used + coalesce(length(NEW.metadata), 0) + coalesce(length(NEW.link_target), 0)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER changes_metadata_delete AFTER DELETE ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - coalesce(length(OLD.metadata), 0) - coalesce(length(OLD.link_target), 0)
    WHERE id = OLD.volume;
END;
