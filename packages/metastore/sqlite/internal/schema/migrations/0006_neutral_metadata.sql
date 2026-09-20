-- Node kind and client-owned metadata are authority facts independent of any
-- platform projection. Historical POSIX permission and special bits move into
-- posix.permissions.v1 as one little-endian uint32 with authority version 1.

ALTER TABLE nodes ADD COLUMN kind INTEGER NOT NULL DEFAULT 1;
ALTER TABLE nodes ADD COLUMN birth_sec INTEGER;
ALTER TABLE nodes ADD COLUMN birth_nsec INTEGER;
ALTER TABLE nodes ADD COLUMN change_sec INTEGER;
ALTER TABLE nodes ADD COLUMN change_nsec INTEGER;
ALTER TABLE nodes ADD COLUMN metadata BLOB NOT NULL DEFAULT X'52464d010000';

WITH permissions AS (
    SELECT id, (mode & 511) |
        CASE WHEN (mode & 1048576) != 0 THEN 512 ELSE 0 END |
        CASE WHEN (mode & 4194304) != 0 THEN 1024 ELSE 0 END |
        CASE WHEN (mode & 8388608) != 0 THEN 2048 ELSE 0 END AS posix_mode
    FROM nodes
)
UPDATE nodes
SET kind = CASE WHEN (mode & 2147483648) != 0 THEN 2 ELSE 1 END,
    metadata = (
        SELECT unhex('52464d0101001400080004000000706f7369782e7065726d697373696f6e732e76310000000000000001' ||
            printf('%02x%02x', posix_mode & 255, (posix_mode >> 8) & 255) || '0000')
        FROM permissions WHERE permissions.id = nodes.id
    );
ALTER TABLE nodes DROP COLUMN mode;
CREATE INDEX nodes_by_volume ON nodes (volume, id);

ALTER TABLE changes ADD COLUMN node_kind INTEGER;
ALTER TABLE changes ADD COLUMN birth_sec INTEGER;
ALTER TABLE changes ADD COLUMN birth_nsec INTEGER;
ALTER TABLE changes ADD COLUMN change_sec INTEGER;
ALTER TABLE changes ADD COLUMN change_nsec INTEGER;
ALTER TABLE changes ADD COLUMN metadata BLOB;

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
    )
WHERE node IS NOT NULL;
ALTER TABLE changes DROP COLUMN mode;

ALTER TABLE volumes ADD COLUMN metadata_used INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(metadata_used) = 'integer' AND metadata_used >= 0);
UPDATE volumes SET metadata_used =
    coalesce((SELECT sum(length(metadata)) FROM nodes WHERE nodes.volume = volumes.id), 0) +
    coalesce((SELECT sum(coalesce(length(metadata), 0)) FROM changes WHERE changes.volume = volumes.id), 0);

CREATE TRIGGER nodes_metadata_insert AFTER INSERT ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used + length(NEW.metadata)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER nodes_metadata_update AFTER UPDATE OF metadata, volume ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - length(OLD.metadata)
    WHERE id = OLD.volume;
    UPDATE volumes SET metadata_used = metadata_used + length(NEW.metadata)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER nodes_metadata_delete AFTER DELETE ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - length(OLD.metadata)
    WHERE id = OLD.volume;
END;

CREATE TRIGGER changes_metadata_insert AFTER INSERT ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used + coalesce(length(NEW.metadata), 0)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER changes_metadata_update AFTER UPDATE OF metadata, volume ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - coalesce(length(OLD.metadata), 0)
    WHERE id = OLD.volume;
    UPDATE volumes SET metadata_used = metadata_used + coalesce(length(NEW.metadata), 0)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER changes_metadata_delete AFTER DELETE ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - coalesce(length(OLD.metadata), 0)
    WHERE id = OLD.volume;
END;
