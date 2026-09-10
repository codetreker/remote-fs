-- The change log a mount replicates from, the per-volume bookkeeping that makes a position
-- resumable, and the entry key that lets one volume be paged out of a shared database.
--
-- This file has landed; see the note at the top of 0001_tree.sql.

-- Version 1 keyed an entry by (parent, name), and a picture of one volume could not be
-- taken as a range over that key. The volume goes in front of it.
--
-- A key cannot be changed in place — SQLite's ALTER TABLE will add a column but not move one
-- into the primary key — so the table is rebuilt. The old table is renamed out of the way
-- first so that the new one is created under the name it keeps: a table reached by renaming
-- something else into place carries the quoted form of its original CREATE statement in
-- sqlite_schema forever after, which is a difference between two databases holding the same
-- schema.
--
-- The volume a row belongs to is the one its node belongs to, which is where version 1
-- kept that fact and is the invariant every writer maintains from here on.
--
-- Nothing references entries, so renaming and dropping it disturbs no foreign key elsewhere,
-- and the new table's own references to nodes and volumes are satisfied by rows already
-- there. entries_by_node follows the old table through the rename and goes with it when it is
-- dropped, so the index is created again at the end rather than in the middle.
ALTER TABLE entries RENAME TO entries_v1;

-- The volume leads the key for locality. A picture of the tree is a range over this key,
-- and with the volume in front, one volume's entries are one contiguous stretch of it —
-- so a page seeks straight to its cursor and reads this volume and nothing else.
--
-- This key leaves the planner no choice to make. The volume equality and the cursor range
-- address one index, and that index supplies the ordering as well, so there is no join order
-- to weigh and no sort to consider — the plan is a property of the key rather than a verdict
-- that could be revisited. It held byte for byte across every combination tried: one volume
-- and twenty, tiny and four hundred thousand entries, with and without table statistics, and
-- with and without an index on nodes(volume).
-- TestAPictureIsPagedByRangeRatherThanByScanningAndSorting holds it there in the two worlds a
-- test can build.
--
-- Without the volume in the key it can only be filtered from the node on the far side of
-- the join, and SQLite then has a choice between two plans that both cost the whole table
-- rather than the page: read every volume's entries and discard the foreign ones one at a
-- time, or drive from the node table and sort the volume's rows again for every page. Which
-- one it took varied, in the shapes measured, with the table statistics, with how many
-- volumes shared the database, and with what other indexes existed — so the cost is not
-- merely high, it changes shape as a database fills and as it is maintained.
--
-- An index on nodes(volume) settles that choice on the sorting plan, and settling a choice
-- is not the same as making it a good one. A per-page sort costs what the pictured volume
-- costs, so with that index a picture of a small volume came out several times faster than
-- without it and a picture of a large one several times slower — measured both ways with the
-- same million rows in the database, so what decides it is the size of the volume being
-- pictured rather than how many share the file. Nobody knows that size when the schema is
-- written, which makes the index a bet on the shape of somebody's volume rather than an
-- answer to the question.
--
-- Those two paragraphs are what was measured over the shapes above, not statements about how
-- SQLite chooses, and a later measurement may move them. The sentence this key rests on is the
-- first one, and it does not depend on them.
--
-- The key is not free, and the shape where it costs is the one where sieving has nothing to
-- sieve. A database holding a single volume gains nothing from the volume column and
-- still pays for it — not in the plan, which is the same range either way, but in what that
-- plan reads: the column widens every row of the entry b-tree, so a picture touches a few
-- percent more pages to cover the same entries. That is the trade, and it is pages rather than
-- planning — a few percent where one volume is alone, against several times over where many
-- share a database, which is the right way round for a system whose premise is that many
-- volumes exist and few are mounted at once. Over a million entries the cost showed in
-- every run of an alternating comparison and tracked the extra pages closely, so it is
-- mechanical rather than noise. It is written down rather than left out because the first
-- person to measure a single-volume database will find it, and a comment claiming this key
-- wins everywhere is what would read as false then.
--
-- The plan is where the rest of this is checkable — what it costs depends on the machine, the
-- cache and the shape of the tree, and none of those belong in a comment.
--
-- The volume column is therefore redundant with nodes.volume, and every statement that
-- writes an entry keeps the two equal. The volume adds nothing to the uniqueness, which a
-- directory already has over (parent, name), because a parent is a node id and node ids are
-- unique across the database.
CREATE TABLE entries (
	volume INTEGER NOT NULL REFERENCES volumes(id),
	parent    INTEGER NOT NULL REFERENCES nodes(id),
	name      BLOB    NOT NULL,
	node      INTEGER NOT NULL REFERENCES nodes(id),
	PRIMARY KEY (volume, parent, name)
) WITHOUT ROWID;

INSERT INTO entries (volume, parent, name, node)
SELECT n.volume, e.parent, e.name, e.node FROM entries_v1 e JOIN nodes n ON n.id = e.node;

DROP TABLE entries_v1;

CREATE INDEX entries_by_node ON entries (node);

-- One row per recorded change, shared by every volume in the database. The volume column
-- is what separates them; a shared sequence only means a volume's positions have gaps, and
-- nothing above compares positions for adjacency.
--
-- AUTOINCREMENT, so that a position is never handed out twice. Without it SQLite picks one
-- above the largest rowid the table currently holds, which is a different promise: empty the
-- table and the next insert starts again at 1. Measured against modernc.org/sqlite v1.57.0 —
-- deleting a prefix leaves the sequence alone under either declaration, and emptying the table
-- restarts it at 1 without AUTOINCREMENT and continues past the high-water mark with it.
--
-- What that would cost is out of proportion to the saving. A replica applies an event only when
-- its position is strictly greater than the one it has applied, so a position issued a second
-- time is discarded in silence, and nothing afterwards corrects it. The trim keeps at least one
-- entry per volume, so it cannot empty this table today — but the numbers it trims to are
-- configurable and were chosen rather than measured, and "a position is never reused" must be a
-- property of the table rather than a consequence of how the trim happens to be tuned this
-- week. nodes carries the same reasoning for its own ids.
--
-- Neither parent, node nor content carries a REFERENCES clause, and that is deliberate: the log
-- outlives what it describes. A Removed entry names a node that is gone by definition, the
-- directory it was in may be removed later, and the object a Modified entry mentions is
-- forgotten as soon as its bytes are swept. A reference here would refuse the very rows the log
-- exists to keep.
--
-- name is NULL for the root, which has no name and no parent, matching how metastore.Row and
-- metastore.Change name it. The node columns are NULL for Removed, which is the one kind that
-- says what a name no longer holds.
CREATE TABLE changes (
	position      INTEGER PRIMARY KEY AUTOINCREMENT,
	volume     INTEGER NOT NULL REFERENCES volumes(id),
	kind          INTEGER NOT NULL,
	parent        INTEGER NOT NULL,
	name          BLOB,
	from_parent   INTEGER,
	from_name     BLOB,
	node          INTEGER,
	mode          INTEGER,
	size          INTEGER,
	atime_sec     INTEGER,
	atime_nsec    INTEGER,
	mtime_sec     INTEGER,
	mtime_nsec    INTEGER,
	content       TEXT,
	recorded_sec  INTEGER NOT NULL,
	recorded_nsec INTEGER NOT NULL
);

-- Every read of the log is "this volume, in position order, after some position", and every
-- trim is "this volume, the oldest few". Both are this index.
CREATE INDEX changes_by_volume ON changes (volume, position);

-- What a volume's log is, apart from its entries.
--
-- committed_position is the newest position the tree was changed at, written in the same
-- transaction as the change it names. It is not derivable from the entries, because the entries
-- are trimmed and a log that has discarded everything must still be able to tell "you are
-- caught up" from "you missed everything".
--
-- incarnation names a run of history. It is random rather than a counter: restoring this
-- database from a backup would make a counter go backwards, and two unrelated logs both sitting
-- at 1 is entirely possible. A random value makes "does not match, so rebuild" the only branch
-- there is.
--
-- trimmed_through is the newest position the trim has discarded, and 0 while it has discarded
-- nothing. It is what decides whether a returning replica can carry on, and the oldest surviving
-- entry cannot decide it: the position sequence is shared with every other volume in this
-- database, so a volume's own positions are spread by however much its neighbours were
-- written to in between. Reading resumability off that distance sends replicas that had missed
-- nothing away to walk the whole tree again — and a volume whose first change is not position
-- 1, which is every volume but the first one written here, would send away even a replica
-- that had applied nothing at all.
--
-- trimmed_by_age records which dimension pushed the oldest entries out, because age and volume
-- say different things to whoever is holding the pager: age means that caller was away too long,
-- volume means the volume changes faster than the log was configured to hold.
CREATE TABLE logs (
	volume          INTEGER PRIMARY KEY REFERENCES volumes(id),
	incarnation        TEXT    NOT NULL,
	committed_position INTEGER NOT NULL,
	trimmed_through    INTEGER NOT NULL,
	trimmed_by_age     INTEGER NOT NULL
);

-- Every volume that was already here gets a log, and it gets an empty one. That is the
-- truthful statement: nothing recorded the history this database accumulated before it had a
-- log, so no replica may resume against it, and an incarnation nothing has ever seen is how
-- that is said. On a database being built from nothing this selects no rows, because a
-- volume is created after the schema is in place.
--
-- The incarnation comes from SQLite's own generator rather than from crypto/rand, which is
-- what mints one at runtime. The requirement is the same either way and it is the only one an
-- incarnation has — to differ from every other value ever minted, by this process or any
-- other — and 16 bytes from a generator the OS seeds meets it. It names a run of history; it
-- authenticates nothing.
INSERT INTO logs (volume, incarnation, committed_position, trimmed_through, trimmed_by_age)
SELECT id, lower(hex(randomblob(16))), 0, 0, 0 FROM volumes;
