-- Every live directory carries an opaque, nonzero revision of its complete
-- child-name set. Existing directories begin at one; later namespace changes
-- advance the revision in the same transaction as the entry update.

ALTER TABLE nodes ADD COLUMN directory_revision BLOB NOT NULL DEFAULT X'';
UPDATE nodes SET directory_revision = CASE
    WHEN kind = 2 THEN X'0000000000000001'
    ELSE X''
END;

-- Retained history predating this schema has no trustworthy directory
-- revision. Rotate the incarnation and discard that history atomically so an
-- existing replica must reseed from a snapshot that carries current revisions.
ALTER TABLE changes ADD COLUMN directory_revision BLOB;
DELETE FROM changes;
UPDATE logs SET
    incarnation = lower(hex(randomblob(16))),
    committed_position = 0,
    trimmed_through = 0,
    trimmed_by_age = 0;

DROP TRIGGER nodes_metadata_insert;
DROP TRIGGER nodes_metadata_update;
DROP TRIGGER nodes_metadata_delete;
DROP TRIGGER changes_metadata_insert;
DROP TRIGGER changes_metadata_update;
DROP TRIGGER changes_metadata_delete;

UPDATE volumes SET metadata_used = metadata_used +
    coalesce((SELECT sum(length(directory_revision)) FROM nodes WHERE nodes.volume=volumes.id),0);

CREATE TRIGGER nodes_metadata_insert AFTER INSERT ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used + length(NEW.metadata) + length(NEW.link_target) + length(NEW.directory_revision)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER nodes_metadata_update AFTER UPDATE OF metadata, link_target, directory_revision, volume ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - length(OLD.metadata) - length(OLD.link_target) - length(OLD.directory_revision)
    WHERE id = OLD.volume;
    UPDATE volumes SET metadata_used = metadata_used + length(NEW.metadata) + length(NEW.link_target) + length(NEW.directory_revision)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER nodes_metadata_delete AFTER DELETE ON nodes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - length(OLD.metadata) - length(OLD.link_target) - length(OLD.directory_revision)
    WHERE id = OLD.volume;
END;

CREATE TRIGGER changes_metadata_insert AFTER INSERT ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used + coalesce(length(NEW.metadata),0) + coalesce(length(NEW.link_target),0) + coalesce(length(NEW.directory_revision),0)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER changes_metadata_update AFTER UPDATE OF metadata, link_target, directory_revision, volume ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - coalesce(length(OLD.metadata),0) - coalesce(length(OLD.link_target),0) - coalesce(length(OLD.directory_revision),0)
    WHERE id = OLD.volume;
    UPDATE volumes SET metadata_used = metadata_used + coalesce(length(NEW.metadata),0) + coalesce(length(NEW.link_target),0) + coalesce(length(NEW.directory_revision),0)
    WHERE id = NEW.volume;
END;

CREATE TRIGGER changes_metadata_delete AFTER DELETE ON changes BEGIN
    UPDATE volumes SET metadata_used = metadata_used - coalesce(length(OLD.metadata),0) - coalesce(length(OLD.link_target),0) - coalesce(length(OLD.directory_revision),0)
    WHERE id = OLD.volume;
END;
