-- The backing store whose objects the metadata in this database names.
--
-- This file has landed; see the note at the top of 0001_tree.sql.

-- A database is either unbound, represented by no row, or bound permanently to one backing
-- store. The fixed primary key makes that cardinality part of the schema, and the non-empty
-- check keeps the stored form aligned with the API's definition of an identity.
CREATE TABLE backing_store (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	store_id  TEXT    NOT NULL CHECK (store_id <> '')
) WITHOUT ROWID;
