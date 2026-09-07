CREATE TABLE lease_recovery (
    singleton           INTEGER PRIMARY KEY CHECK (singleton = 1),
    database_id         TEXT NOT NULL,
    state_id            TEXT NOT NULL,
    accepted_generation INTEGER NOT NULL,
    accepted_nanos      INTEGER NOT NULL,
    prepared_generation INTEGER,
    prepared_nanos      INTEGER
) WITHOUT ROWID;
