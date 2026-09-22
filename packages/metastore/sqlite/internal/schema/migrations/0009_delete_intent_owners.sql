-- Delete-intent owners let a recovering producer discover only the durable
-- obligations it created. A per-volume high-water mark gives every intent a
-- stable paging position that acknowledgements never reuse.

ALTER TABLE volumes ADD COLUMN delete_intent_high_water INTEGER NOT NULL DEFAULT 0
    CHECK (delete_intent_high_water >= 0);

ALTER TABLE delete_intents RENAME TO delete_intents_v8;

CREATE TABLE delete_intents (
    intent TEXT PRIMARY KEY
        CHECK (length(CAST(intent AS BLOB)) = 32 AND intent NOT GLOB '*[^0-9a-f]*'),
    volume INTEGER NOT NULL,
    owner TEXT NOT NULL CHECK (
        length(CAST(owner AS BLOB)) BETWEEN 1 AND 128 AND
        instr(CAST(owner AS BLOB), X'00') = 0
    ),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
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

INSERT INTO delete_intents (
    intent, volume, owner, sequence, node, parent, name, reference,
    request_hash, if_empty, outcome, failure, updated_sec, updated_nsec
)
SELECT
    intent,
    volume,
    intent,
    row_number() OVER (PARTITION BY volume ORDER BY intent),
    node,
    parent,
    name,
    reference,
    request_hash,
    if_empty,
    outcome,
    failure,
    updated_sec,
    updated_nsec
FROM delete_intents_v8;

UPDATE volumes SET delete_intent_high_water = coalesce((
    SELECT max(sequence) FROM delete_intents WHERE delete_intents.volume = volumes.id
), 0);

DROP TABLE delete_intents_v8;

CREATE UNIQUE INDEX delete_intents_by_sequence ON delete_intents (volume, sequence);
CREATE INDEX delete_intents_by_owner ON delete_intents (volume, owner, sequence);
CREATE INDEX delete_intents_by_node ON delete_intents (volume, node, outcome, intent);
