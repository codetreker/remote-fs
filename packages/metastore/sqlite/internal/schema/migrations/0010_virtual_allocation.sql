-- Virtual allocation is a durable node fact. A nonempty regular file occupies
-- whole 4096-byte units; directories, symlinks, and empty files occupy none.
-- Preparation populates historical allocation only after it identifies whether
-- the database is an authority or a replica of an arbitrary source.
ALTER TABLE nodes ADD COLUMN allocation_size INTEGER
    CHECK (allocation_size IS NULL OR (typeof(allocation_size) = 'integer' AND allocation_size >= 0));

ALTER TABLE changes ADD COLUMN allocation_size INTEGER;

ALTER TABLE volumes ADD COLUMN allocated_used INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(allocated_used) = 'integer' AND allocated_used >= 0);

CREATE TRIGGER changes_allocation_insert AFTER INSERT ON changes BEGIN
    UPDATE changes SET allocation_size = CASE
        WHEN NEW.node IS NULL THEN NULL
        WHEN NEW.node_kind = 1 AND NEW.size > 0 THEN ((NEW.size + 4095) / 4096) * 4096
        ELSE 0
    END WHERE position = NEW.position;
END;
