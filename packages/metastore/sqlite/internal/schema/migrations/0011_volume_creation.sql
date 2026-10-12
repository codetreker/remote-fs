-- Historical volume creation is unknown; the mutable root birth time cannot
-- establish it. New volumes record their creation instant independently.
ALTER TABLE volumes ADD COLUMN created_sec INTEGER
    CHECK (created_sec IS NULL OR typeof(created_sec) = 'integer');

ALTER TABLE volumes ADD COLUMN created_nsec INTEGER
    CHECK ((created_sec IS NULL AND created_nsec IS NULL) OR
        (typeof(created_sec) = 'integer' AND typeof(created_nsec) = 'integer'
            AND created_nsec >= 0 AND created_nsec < 1000000000));
