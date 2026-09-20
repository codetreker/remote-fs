-- Identity-retained nodes preserve symbolic-link data and accepted deletion
-- obligations independently of the process that created their references.

ALTER TABLE nodes ADD COLUMN link_target BLOB NOT NULL DEFAULT X'';
ALTER TABLE nodes ADD COLUMN pending_unlink INTEGER NOT NULL DEFAULT 0
    CHECK (pending_unlink IN (0, 1));
ALTER TABLE nodes ADD COLUMN pending_generation INTEGER NOT NULL DEFAULT 0
    CHECK (pending_generation >= 0);

ALTER TABLE changes ADD COLUMN link_target BLOB;
UPDATE changes SET link_target=X'' WHERE node IS NOT NULL;

CREATE INDEX nodes_pending_unlink ON nodes (volume, id) WHERE pending_unlink = 1;

CREATE TABLE delete_intents (
    intent TEXT PRIMARY KEY
        CHECK (length(CAST(intent AS BLOB)) = 32 AND intent NOT GLOB '*[^0-9a-f]*'),
    volume INTEGER NOT NULL,
    node INTEGER NOT NULL,
    parent INTEGER,
    name BLOB,
    reference BLOB NOT NULL CHECK (length(reference) = 16),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    if_empty INTEGER NOT NULL CHECK (if_empty IN (0, 1)),
    outcome INTEGER NOT NULL CHECK (outcome BETWEEN 1 AND 5),
    failure INTEGER,
    updated_sec INTEGER NOT NULL,
    updated_nsec INTEGER NOT NULL,
    CHECK ((parent IS NULL) = (name IS NULL)),
    CHECK (name IS NULL OR (
        typeof(name) = 'blob' AND length(name) > 0 AND
        name NOT IN (X'2e', X'2e2e') AND
        instr(name, X'2f') = 0 AND instr(name, X'00') = 0
    )),
    CHECK ((outcome = 5) = (failure IS NOT NULL))
) WITHOUT ROWID;

CREATE INDEX delete_intents_by_node ON delete_intents (volume, node, outcome, intent);

DROP TRIGGER nodes_metadata_insert;
DROP TRIGGER nodes_metadata_update;
DROP TRIGGER nodes_metadata_delete;
DROP TRIGGER changes_metadata_insert;
DROP TRIGGER changes_metadata_update;
DROP TRIGGER changes_metadata_delete;

UPDATE volumes SET metadata_used = metadata_used +
    coalesce((SELECT sum(length(link_target)) FROM nodes WHERE nodes.volume=volumes.id),0) +
    coalesce((SELECT sum(coalesce(length(link_target),0)) FROM changes WHERE changes.volume=volumes.id),0);

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
