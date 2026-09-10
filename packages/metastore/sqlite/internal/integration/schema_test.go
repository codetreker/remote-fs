package integration_test

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	_ "modernc.org/sqlite"
)

func TestLegacyMigrationRejectsLargeBlobIdentityScalars(t *testing.T) {
	tests := []struct {
		name   string
		write  func(*testing.T, string)
		damage string
	}{
		{
			"version one root",
			writeVersionOne,
			`UPDATE volumes SET root = zeroblob(4 * 1024 * 1024)`,
		},
		{
			"version two committed position",
			writeVersionTwo,
			`UPDATE logs SET committed_position = zeroblob(4 * 1024 * 1024)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			test.write(t, path)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
			if err == nil {
				store.Close()
				t.Fatal("migrating a large BLOB identity scalar succeeded")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("migrating a large BLOB identity scalar returned %v, want EIO", err)
			}
		})
	}
}

func TestOpenRejectsLargeBlobSchemaVersionBeforeMigration(t *testing.T) {
	path := database(t)
	writeVersionOne(t, path)
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE schema_version SET version = zeroblob(4 * 1024 * 1024)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening a large BLOB schema version succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening a large BLOB schema version returned %v, want EIO", err)
	}
}

func writePreRetainedDatabase(t *testing.T, version int) string {
	t.Helper()
	path := writeHistoricalLeaseDatabase(t, true)
	if version == 4 {
		migration, err := os.ReadFile("../schema/migrations/0004_lease_recovery.sql")
		if err != nil {
			t.Fatal(err)
		}
		damageDatabase(t, path, string(migration))
		damageDatabase(t, path, `UPDATE schema_version SET version = 4`)
	}
	return path
}

func TestWitnessedRetainedMigrationPreservesNodesAndAcceptedState(t *testing.T) {
	path := writePreRetainedDatabase(t, 4)
	before := historicalLeaseRows(t, path)
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(t.Context(), path, "A", durableStoreID, 1024,
		sqlite.DefaultOptions(), sqlite.RequireExistingVolume, sqlite.DurableStartup{
			Accepted: historicalLeaseDurableState, CheckpointedGeneration: historicalLeaseDurableState.Generation,
		}, witness)
	if err != nil {
		t.Fatalf("migrate witnessed version 4: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	assertHistoricalLeaseVolume(t, store, "A")
	assertHistoricalLeaseRows(t, path, before)
	assertHistoricalLeaseSchemaVersion(t, path, 5)
	db := raw(t, path)
	defer db.Close()
	var total, initialized int
	if err := db.QueryRow(`SELECT count(*), count(CASE WHEN typeof(detached) = 'integer' AND detached = 0
		AND typeof(content_revision) = 'integer' AND content_revision = 1 THEN 1 END) FROM nodes`).Scan(&total, &initialized); err != nil {
		t.Fatal(err)
	}
	if total != 4 || initialized != total {
		t.Fatalf("migrated nodes = %d, valid linked revisions = %d", total, initialized)
	}
	accepted, visible := witness.accepts()
	want := historicalLeaseDurableState
	want.Generation++
	if len(accepted) != 1 || len(visible) != 1 || accepted[0] != want || visible[0] != want {
		t.Fatalf("migration acceptance = %+v, visible = %+v; want %+v", accepted, visible, want)
	}
}

func TestWitnessedRetainedMigrationRefusesOldSchemaCorruptionWithoutChanges(t *testing.T) {
	for _, version := range []int{3, 4} {
		for _, damage := range []struct {
			name string
			sql  string
		}{
			{"unmarked orphan", `DELETE FROM entries WHERE node = 2`},
			{"foreign volume orphan", `DELETE FROM entries WHERE node = 4`},
			{"foreign volume undercharge", `UPDATE volumes SET used = 0 WHERE id = 2`},
			{"invalid node scalar", `UPDATE nodes SET atime_nsec = 'damaged' WHERE id = 2`},
			{"missing object", `DELETE FROM objects WHERE key = 'historical-alpha-object'`},
			{"invalid log predecessor", `UPDATE changes SET previous_position = 2 WHERE position = 3`},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, damage.name), func(t *testing.T) {
				path := writePreRetainedDatabase(t, version)
				damageDatabase(t, path, damage.sql)
				assertRetainedMigrationRefused(t, path, version, sqlite.DefaultOptions(), syscall.EIO)
			})
		}
	}
}

func TestWitnessedRetainedMigrationBoundsOldSchemaValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		options sqlite.Options
	}{
		{"records", integrityOptions(3)},
		{"name bytes", integrityByteOptions(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writePreRetainedDatabase(t, 4)
			assertRetainedMigrationRefused(t, path, 4, test.options, syscall.EFBIG)
		})
	}
}

func assertRetainedMigrationRefused(t *testing.T, path string, version int, options sqlite.Options, want error) {
	t.Helper()
	beforeSchema := schemaOf(t, path)
	before := historicalLeaseRows(t, path)
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(t.Context(), path, "A", durableStoreID, 1024,
		options, sqlite.RequireExistingVolume, sqlite.DurableStartup{
			Accepted: historicalLeaseDurableState, CheckpointedGeneration: historicalLeaseDurableState.Generation,
		}, witness)
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, want) {
		t.Fatalf("migrate invalid version %d = %v; want %v", version, err, want)
	}
	assertHistoricalLeaseSchemaVersion(t, path, version)
	if after := schemaOf(t, path); after != beforeSchema {
		t.Fatal("refused migration changed the schema")
	}
	assertHistoricalLeaseRows(t, path, before)
	state, err := sqlite.InspectDurableState(t.Context(), path)
	if err != nil || state != historicalLeaseDurableState {
		t.Fatalf("refused migration changed durable state: %+v, %v", state, err)
	}
	if accepted, _ := witness.accepts(); len(accepted) != 0 {
		t.Fatalf("refused migration accepted state: %+v", accepted)
	}
}

var update = flag.Bool("update", false, "rewrite testdata/schema.sql from the schema the migrations produce")

// The golden schema is the current layout written down in one place, which the migrations no
// longer state anywhere: after a few of them, "what does entries look like" is a question
// spread across files. This answers it, and it is also the alarm on the rule those files are
// kept under. A landed migration is never edited, and an edit that changes what it produces
// moves this file — so the diff is in the review whether or not anyone thought to mention it.
const goldenSchema = "testdata/schema.sql"

// goldenPreamble says what the file is to whoever opens it without having read this test.
const goldenPreamble = `-- The schema the migrations in ../schema/migrations/ arrive at, as SQLite reports it, generated by
-- TestEveryRouteToTheCurrentSchemaArrivesAtTheSameOne. Nothing reads this file at runtime.
--
-- Edit the migrations, not this: run ` + "`go test ./packages/metastore/sqlite/internal/integration -update`" + ` to
-- record what they now produce. A diff here without a new migration beside it means a landed
-- migration was edited, which is the one thing the numbering forbids.

`

// A fresh database and a database carried forward from version 1 must be the same schema, and
// must be the one recorded in testdata/schema.sql.
//
// A fresh database is not built from a description of the current layout; it replays every
// migration, so there is only one path and it cannot drift from itself. What this case checks
// is that replaying the migrations and carrying a real version 1 forward reach the same place.
//
// It says nothing about whether a landed migration still describes the version it is named for.
// Both routes end at the current schema, so an edit to 0001_tree.sql touching anything
// 0002_replication.sql rebuilds — the entry table and its index, which is most of what
// 0001_tree.sql says — is replaced before either route finishes and passes here.
// TestTheFirstMigrationDescribesTheVersionOneDatabasesThatExist is what covers that.
func TestEveryRouteToTheCurrentSchemaArrivesAtTheSameOne(t *testing.T) {
	fresh := database(t)
	open(t, fresh, "workspace", 0)

	migrated := database(t)
	writeVersionOne(t, migrated)
	open(t, migrated, "workspace", 0)

	built, carried := schemaOf(t, fresh), schemaOf(t, migrated)
	if sqliteschema.Structure(built) != sqliteschema.Structure(carried) {
		t.Fatalf("a database built from nothing and one carried forward from version 1 hold different schemas.\n"+
			"built from nothing:\n%s\ncarried forward:\n%s", built, carried)
	}

	recorded := goldenPreamble + built
	if *update {
		if err := os.WriteFile(goldenSchema, []byte(recorded), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(goldenSchema)
	if err != nil {
		t.Fatalf("reading the recorded schema: %v (run the tests with -update to write it)", err)
	}
	if recorded != string(want) {
		t.Fatalf("the migrations no longer produce the schema in %s.\nproduced:\n%s\nrecorded:\n%s\n"+
			"If this change was intended it belongs in a new migration, not in an edited one; "+
			"run the tests with -update to record it.", goldenSchema, built, want)
	}
}

func schemaOf(t *testing.T, path string) string {
	t.Helper()
	db := raw(t, path)
	defer db.Close()
	dump, err := sqliteschema.Dump(t.Context(), db)
	if err != nil {
		t.Fatalf("reading the schema of %s back: %v", path, err)
	}
	return dump
}

// The version a database ends up at is the number of migrations there are. That is what makes
// the recorded version readable — version 2 means 0002_replication.sql was the last file
// applied — and it is the thing an added migration is easiest to get wrong, by landing a file
// that nothing runs.
//
// The count comes from the directory rather than from what the package embedded, so a go:embed
// pattern that stopped agreeing with the directory MustLoad is given is a failure here rather
// than a schema quietly missing a table.
func TestTheRecordedVersionIsTheNumberOfMigrations(t *testing.T) {
	files, err := filepath.Glob("../schema/migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("there are no migrations in ../schema/migrations/, so nothing builds the schema")
	}

	path := database(t)
	open(t, path, "workspace", 0)

	db := raw(t, path)
	defer db.Close()
	var version int
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != len(files) {
		t.Fatalf("a fresh database records schema version %d and there are %d migrations (%v): "+
			"a migration that does not move the version is one nothing will ever run",
			version, len(files), files)
	}
}

// 0001_tree.sql must describe the version 1 databases that exist, and the comparison above
// cannot check that. That one compares where the two routes end, and 0002_replication.sql
// rebuilds the entry table — so whatever 0001_tree.sql says about `entries` is replaced before
// either route finishes, and both arrive at the same place regardless. An edit claiming version
// 1 stored names as TEXT rather than BLOB passes it, which was measured rather than reasoned
// about.
//
// This is the check that does not depend on where the routes end. writeVersionOne is version 1
// written out by hand; running 0001_tree.sql by itself must arrive at the same layout, so an
// edit to it is an edit to a claim about databases nobody can go back and change.
func TestTheFirstMigrationDescribesTheVersionOneDatabasesThatExist(t *testing.T) {
	stated := database(t)
	statements, err := os.ReadFile(filepath.Join("..", "schema", "migrations", "0001_tree.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db := raw(t, stated)
	if _, err := db.Exec(string(statements)); err != nil {
		t.Fatalf("running 0001_tree.sql on its own: %v", err)
	}
	// The version table belongs to the migration runner rather than to any migration, and a
	// version 1 database has it from its own DDL. Adding it is what makes the two comparable.
	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	witness := database(t)
	writeVersionOne(t, witness)

	said, was := schemaOf(t, stated), schemaOf(t, witness)
	if sqliteschema.Structure(said) != sqliteschema.Structure(was) {
		t.Fatalf("0001_tree.sql no longer describes the version 1 databases that exist.\n"+
			"what it states:\n%s\nwhat version 1 was:\n%s\n"+
			"A landed migration is a claim about databases already written; changing the schema "+
			"means adding a file.", said, was)
	}
}

// Adding version 3 made version 2 part of the database history this build must continue to
// describe exactly. The witness is independent of the migrations: replaying 0001 and 0002 is
// compared with the layout a version 2 build actually wrote down, so an edit to either landed
// file cannot make both sides move together.
func TestTheSecondMigrationDescribesTheVersionTwoDatabasesThatExist(t *testing.T) {
	stated := database(t)
	db := raw(t, stated)
	for _, name := range []string{"0001_tree.sql", "0002_replication.sql"} {
		statements, err := os.ReadFile(filepath.Join("..", "schema", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(statements)); err != nil {
			t.Fatalf("running %s: %v", name, err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	witness := database(t)
	writeVersionTwo(t, witness)

	said, was := schemaOf(t, stated), schemaOf(t, witness)
	if sqliteschema.Structure(said) != sqliteschema.Structure(was) {
		t.Fatalf("0002_replication.sql no longer describes the version 2 databases that exist.\n"+
			"what it states:\n%s\nwhat version 2 was:\n%s\n"+
			"A landed migration is a claim about databases already written; changing the schema "+
			"means adding a file.", said, was)
	}
}

// writeVersionTwo builds the independent historical schema witness with the same referenced
// file used by the version 1 fixture and the log row version 2 required for that volume.
func writeVersionTwo(t *testing.T, path string) {
	t.Helper()
	statements, err := os.ReadFile(filepath.Join("testdata", "version2.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(string(statements)); err != nil {
		db.Close()
		t.Fatalf("building the version 2 witness: %v", err)
	}
	const (
		directory = 1<<31 | 0o755
		file      = 0o640
	)
	at := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).Unix()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO volumes (id, name, root, used) VALUES (1, 'workspace', 1, 700)`, nil},
		{`INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		  VALUES (1, 1, ?, 0, ?, 0, ?, 0, NULL)`, []any{directory, at, at}},
		{`INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		  VALUES (2, 1, ?, 0, ?, 0, ?, 0, NULL)`, []any{directory, at, at}},
		{`INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
		  VALUES ('carried', 1, 1, 700, NULL, ?, 0)`, []any{at}},
		{`INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		  VALUES (3, 1, ?, 700, ?, 0, ?, 0, 'carried')`, []any{file, at, at}},
		{`INSERT INTO entries (volume, parent, name, node) VALUES (1, 1, ?, 2)`, []any{[]byte("d")}},
		{`INSERT INTO entries (volume, parent, name, node) VALUES (1, 2, ?, 3)`, []any{[]byte("f")}},
		{`INSERT INTO logs (volume, incarnation, committed_position, trimmed_through, trimmed_by_age)
		  VALUES (1, 'version-two-incarnation', 0, 0, 0)`, nil},
	} {
		if _, err := db.Exec(statement.sql, statement.args...); err != nil {
			db.Close()
			t.Fatalf("filling the version 2 database: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// Versions 1 and 2 stored zero as a reservation's size and had no state for a Put whose
// ownership outcome was unknown. Neither a reserved nor a garbage row can therefore authorize
// deletion after an upgrade, even when its stored size happens to be non-zero.
func TestLegacyNonReferencedObjectsAreRefusedWithoutMigrating(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	states := []struct {
		name  string
		state int
	}{
		{"reserved", 0},
		{"garbage", 2},
	}

	for _, version := range versions {
		for _, state := range states {
			t.Run(version.name+" "+state.name, func(t *testing.T) {
				path := database(t)
				version.write(t, path)
				db := raw(t, path)
				if _, err := db.Exec(`
					INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
					VALUES ('ambiguous', 1, ?, 0, NULL, 0, 0)`, state.state); err != nil {
					db.Close()
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				before := schemaOf(t, path)

				assertLegacyMigrationRefused(t, path, version.version, before)

				db = raw(t, path)
				var gotState, gotSize int
				if err := db.QueryRow(`SELECT state, size FROM objects WHERE key = 'ambiguous'`).Scan(
					&gotState, &gotSize,
				); err != nil {
					db.Close()
					t.Fatal(err)
				}
				if gotState != state.state || gotSize != 0 {
					db.Close()
					t.Fatalf("the refused migration rewrote the ambiguous object to state %d at size %d", gotState, gotSize)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// Referenced rows are the only legacy state that carries enough ownership information to
// preserve. Migration still requires the node and object halves to agree before changing the
// database schema.
func TestLegacyReferencedObjectsMustBeConsistentBeforeMigrating(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	damage := []struct {
		name      string
		statement string
	}{
		{"missing object", `DELETE FROM objects WHERE key = 'carried'`},
		{"invalid node mode", `UPDATE nodes SET mode = 'regular' WHERE content = 'carried'`},
		{"size mismatch", `UPDATE nodes SET size = 701 WHERE content = 'carried'`},
		{"unreferenced object", `UPDATE nodes SET content = NULL, size = 0 WHERE content = 'carried'`},
	}

	for _, version := range versions {
		for _, corruption := range damage {
			t.Run(version.name+" "+corruption.name, func(t *testing.T) {
				path := database(t)
				version.write(t, path)
				db := raw(t, path)
				if _, err := db.Exec(corruption.statement); err != nil {
					db.Close()
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				before := schemaOf(t, path)
				assertLegacyMigrationRefused(t, path, version.version, before)
			})
		}
	}
}

func TestLegacyNodeEntryRelationshipsMustBeConsistentBeforeMigrating(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	relations := []struct {
		name string
		v1   string
		v2   string
	}{
		{
			name: "duplicate node entry",
			v1:   `INSERT INTO entries (parent, name, node) VALUES (1, CAST('alias' AS BLOB), 3)`,
			v2:   `INSERT INTO entries (volume, parent, name, node) VALUES (1, 1, CAST('alias' AS BLOB), 3)`,
		},
		{
			name: "orphan node",
			v1:   `DELETE FROM entries WHERE node = 3`,
			v2:   `DELETE FROM entries WHERE node = 3`,
		},
		{
			name: "root entry",
			v1:   `INSERT INTO entries (parent, name, node) VALUES (1, CAST('root-alias' AS BLOB), 1)`,
			v2:   `INSERT INTO entries (volume, parent, name, node) VALUES (1, 1, CAST('root-alias' AS BLOB), 1)`,
		},
		{
			name: "missing child endpoint",
			v1:   `UPDATE entries SET node = 999 WHERE node = 3`,
			v2:   `UPDATE entries SET node = 999 WHERE node = 3`,
		},
		{
			name: "missing parent endpoint",
			v1:   `UPDATE entries SET parent = 999 WHERE node = 3`,
			v2:   `UPDATE entries SET parent = 999 WHERE node = 3`,
		},
	}

	for _, version := range versions {
		for _, relation := range relations {
			t.Run(version.name+" "+relation.name, func(t *testing.T) {
				path := database(t)
				version.write(t, path)
				statement := relation.v1
				if version.version == 2 {
					statement = relation.v2
				}
				db := raw(t, path)
				if _, err := db.Exec(statement); err != nil {
					db.Close()
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				before := schemaOf(t, path)
				assertLegacyMigrationRefused(t, path, version.version, before)
			})
		}
	}
}

func TestLegacyEntryNamesMustRemainAddressableBytes(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	corruptions := []struct {
		name  string
		value string
	}{
		{"text storage", `'text-name'`},
		{"empty component", `X''`},
		{"dot component", `X'2e'`},
		{"slash component", `CAST('bad/name' AS BLOB)`},
		{"nul component", `X'626164006e616d65'`},
	}
	for _, version := range versions {
		for _, corruption := range corruptions {
			t.Run(version.name+" "+corruption.name, func(t *testing.T) {
				path := database(t)
				version.write(t, path)
				db := raw(t, path)
				if _, err := db.Exec(`UPDATE entries SET name = ` + corruption.value + ` WHERE node = 3`); err != nil {
					db.Close()
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				before := schemaOf(t, path)
				assertLegacyMigrationRefused(t, path, version.version, before)
			})
		}
	}
}

func TestVersionTwoLogIntegrityMustHoldBeforeMigration(t *testing.T) {
	tests := []struct {
		name   string
		damage string
	}{
		{"missing log", `DELETE FROM logs WHERE volume = 1`},
		{"empty incarnation", `UPDATE logs SET incarnation = '' WHERE volume = 1`},
		{"missing committed tail", `UPDATE logs SET committed_position = 1 WHERE volume = 1`},
		{"text change name", `
			INSERT INTO changes (
				position, volume, kind, parent, name, node, mode, size,
				atime_sec, atime_nsec, mtime_sec, mtime_nsec, content, recorded_sec, recorded_nsec
			)
			SELECT 1, 1, 0, 2, 'text-name', id, mode, size,
				atime_sec, atime_nsec, mtime_sec, mtime_nsec, content, 0, 0
			FROM nodes WHERE id = 3;
			UPDATE logs SET committed_position = 1 WHERE volume = 1`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			writeVersionTwo(t, path)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := schemaOf(t, path)
			assertLegacyMigrationRefused(t, path, 2, before)
		})
	}
}

func addLegacyDisconnectedDirectories(t *testing.T, path string, version int, cycle bool) {
	t.Helper()
	db := raw(t, path)
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	insertNode := func() int64 {
		result, err := tx.Exec(`
			INSERT INTO nodes (volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
			SELECT ns.id, root.mode, 0, 0, 0, 0, 0, NULL
			FROM volumes ns JOIN nodes root ON root.id = ns.root
			WHERE ns.id = 1`)
		if err != nil {
			t.Fatal(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first, second := insertNode(), insertNode()
	insertEntry := func(parent int64, name string, node int64) {
		statement := `INSERT INTO entries (parent, name, node) VALUES (?, CAST(? AS BLOB), ?)`
		args := []any{parent, name, node}
		if version == 2 {
			statement = `INSERT INTO entries (volume, parent, name, node) VALUES (1, ?, CAST(? AS BLOB), ?)`
		}
		if _, err := tx.Exec(statement, args...); err != nil {
			t.Fatal(err)
		}
	}
	insertEntry(first, "child", second)
	if cycle {
		insertEntry(second, "parent", first)
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyNodesMustBeReachableFromTheirVolumeRoot(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	shapes := []struct {
		name  string
		cycle bool
	}{
		{"disconnected cycle", true},
		{"disconnected subtree", false},
	}
	for _, version := range versions {
		for _, shape := range shapes {
			t.Run(version.name+" "+shape.name, func(t *testing.T) {
				path := database(t)
				version.write(t, path)
				addLegacyDisconnectedDirectories(t, path, version.version, shape.cycle)
				before := schemaOf(t, path)
				assertLegacyMigrationRefused(t, path, version.version, before)
			})
		}
	}
}

func makeLegacyFileSizesOverflow(t *testing.T, path string, version int) {
	t.Helper()
	db := raw(t, path)
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`UPDATE objects SET size = ? WHERE key = 'carried'`, []any{int64(math.MaxInt64)}},
		{`UPDATE nodes SET size = ? WHERE content = 'carried'`, []any{int64(math.MaxInt64)}},
		{`INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
		  VALUES ('overflow-byte', 1, 1, 1, NULL, 0, 0)`, nil},
		{`UPDATE volumes SET used = ? WHERE id = 1`, []any{int64(math.MaxInt64)}},
	} {
		if _, err := tx.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	result, err := tx.Exec(`
		INSERT INTO nodes (volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		SELECT volume, mode, 1, 0, 0, 0, 0, 'overflow-byte' FROM nodes WHERE id = 3`)
	if err != nil {
		t.Fatal(err)
	}
	node, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	statement := `INSERT INTO entries (parent, name, node) VALUES (1, CAST('overflow' AS BLOB), ?)`
	if version == 2 {
		statement = `INSERT INTO entries (volume, parent, name, node) VALUES (1, 1, CAST('overflow' AS BLOB), ?)`
	}
	if _, err := tx.Exec(statement, node); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyUsedAccountingMustBeExactBeforeMigrating(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	damage := []struct {
		name      string
		statement string
		overflow  bool
	}{
		{name: "undercount", statement: `UPDATE volumes SET used = 699 WHERE id = 1`},
		{name: "overcount", statement: `UPDATE volumes SET used = 701 WHERE id = 1`},
		{name: "non-integer", statement: `UPDATE volumes SET used = 'seven hundred' WHERE id = 1`},
		{name: "overflow", overflow: true},
	}
	for _, version := range versions {
		for _, corruption := range damage {
			t.Run(version.name+" "+corruption.name, func(t *testing.T) {
				path := database(t)
				version.write(t, path)
				if corruption.overflow {
					makeLegacyFileSizesOverflow(t, path, version.version)
				} else {
					db := raw(t, path)
					if _, err := db.Exec(corruption.statement); err != nil {
						db.Close()
						t.Fatal(err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
				}
				before := schemaOf(t, path)
				assertLegacyMigrationRefused(t, path, version.version, before)
			})
		}
	}
}

func TestLegacyMigrationValidatesEveryVolumeUsedCounter(t *testing.T) {
	versions := []struct {
		name    string
		version int
		write   func(*testing.T, string)
	}{
		{"version 1", 1, writeVersionOne},
		{"version 2", 2, writeVersionTwo},
	}
	for _, version := range versions {
		t.Run(version.name, func(t *testing.T) {
			path := database(t)
			version.write(t, path)
			db := raw(t, path)
			tx, err := db.Begin()
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			result, err := tx.Exec(`INSERT INTO volumes (name, root, used) VALUES ('neighbour', 0, 1)`)
			if err != nil {
				tx.Rollback()
				db.Close()
				t.Fatal(err)
			}
			volume, err := result.LastInsertId()
			if err != nil {
				tx.Rollback()
				db.Close()
				t.Fatal(err)
			}
			result, err = tx.Exec(`
				INSERT INTO nodes (volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
				SELECT ?, mode, 0, 0, 0, 0, 0, NULL FROM nodes WHERE id = 1`, volume)
			if err != nil {
				tx.Rollback()
				db.Close()
				t.Fatal(err)
			}
			root, err := result.LastInsertId()
			if err != nil {
				tx.Rollback()
				db.Close()
				t.Fatal(err)
			}
			if _, err := tx.Exec(`UPDATE volumes SET root = ? WHERE id = ?`, root, volume); err != nil {
				tx.Rollback()
				db.Close()
				t.Fatal(err)
			}
			if version.version == 2 {
				if _, err := tx.Exec(`
					INSERT INTO logs (volume, incarnation, committed_position, trimmed_through, trimmed_by_age)
					VALUES (?, 'neighbour-incarnation', 0, 0, 0)`, volume); err != nil {
					tx.Rollback()
					db.Close()
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := schemaOf(t, path)
			assertLegacyMigrationRefused(t, path, version.version, before)
		})
	}
}

func assertLegacyMigrationRefused(t *testing.T, path string, version int, before string) {
	t.Helper()
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatalf("opening inconsistent schema version %d succeeded, want EIO", version)
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening inconsistent schema version %d: %v, want EIO", version, err)
	}
	if after := schemaOf(t, path); after != before {
		t.Fatalf("refusing schema version %d changed its schema\nbefore:\n%s\nafter:\n%s", version, before, after)
	}
	db := raw(t, path)
	defer db.Close()
	var recorded int
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != version {
		t.Fatalf("refusing schema version %d recorded version %d", version, recorded)
	}
}

func TestAReferencedOnlyVersionTwoDatabaseIsCarriedForward(t *testing.T) {
	path := database(t)
	writeVersionTwo(t, path)
	store := open(t, path, "workspace", 4096)
	node, err := store.Stat(t.Context(), "d/f")
	if err != nil {
		t.Fatal(err)
	}
	if node.Content != "carried" || node.Size != 700 {
		t.Fatalf("the migrated version 2 file is %+v, want 700 bytes under object carried", node)
	}
}

func TestVersionTwoMigrationStartsAVerifiableLogAboveItsOldHighWater(t *testing.T) {
	path := database(t)
	writeVersionTwo(t, path)
	db := raw(t, path)
	var before string
	if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume = 1`).Scan(&before); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO changes (
			position, volume, kind, parent, name, node, mode, size,
			atime_sec, atime_nsec, mtime_sec, mtime_nsec, content, recorded_sec, recorded_nsec
		)
		SELECT 50, 1, 0, 2, CAST('old' AS BLOB), id, mode, size,
			atime_sec, atime_nsec, mtime_sec, mtime_nsec, content, 0, 0
		FROM nodes WHERE id = 3;
		UPDATE logs SET committed_position = 50 WHERE volume = 1;
		UPDATE sqlite_sequence SET seq = 80 WHERE name = 'changes'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store := open(t, path, "workspace", 4096)
	after, err := store.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) == before {
		t.Fatal("migrating an unverifiable version 2 log preserved its incarnation")
	}
	changes, retention, err := readChanges(t.Context(), store, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || retention.Tail != 0 || retention.TrimmedThrough != 0 {
		t.Fatalf("the migrated log holds %d changes and %+v, want a new empty history", len(changes), retention)
	}
	state, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.ChangeHighWater != 80 {
		t.Fatalf("migration preserved change high-water %d, want 80", state.ChangeHighWater)
	}
	if err := store.Create(t.Context(), "after"); err != nil {
		t.Fatal(err)
	}
	committed, err := store.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if committed <= 80 {
		t.Fatalf("the first new history ended at position %d, want above old high-water 80", committed)
	}
}

func TestVersionTwoMigrationAcceptsAFullyTrimmedLog(t *testing.T) {
	path := database(t)
	writeVersionTwo(t, path)
	db := raw(t, path)
	if _, err := db.Exec(`
		UPDATE logs
		SET committed_position = 50, trimmed_through = 50
		WHERE volume = 1;
		DELETE FROM sqlite_sequence WHERE name = 'changes';
		INSERT INTO sqlite_sequence (name, seq) VALUES ('changes', 50)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store := open(t, path, "workspace", 4096)
	state, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.ChangeHighWater != 50 {
		t.Fatalf("fully trimmed migration preserved change high-water %d, want 50", state.ChangeHighWater)
	}
	changes, retention, err := readChanges(t.Context(), store, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || retention.Tail != 0 || retention.TrimmedThrough != 0 {
		t.Fatalf("fully trimmed migration holds %d changes and %+v, want a new empty history", len(changes), retention)
	}
}

func TestLegacySequenceDamageIsRefusedWithoutMigrating(t *testing.T) {
	tests := []struct {
		name    string
		version int
		write   func(*testing.T, string)
		damage  string
	}{
		{"version 1 node sequence", 1, writeVersionOne, `UPDATE sqlite_sequence SET seq = 2 WHERE name = 'nodes'`},
		{"version 2 node sequence", 2, writeVersionTwo, `UPDATE sqlite_sequence SET seq = 2 WHERE name = 'nodes'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			test.write(t, path)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := schemaOf(t, path)
			assertLegacyMigrationRefused(t, path, test.version, before)
		})
	}
}

func TestLegacyNodeSequenceHighWaterSurvivesMigration(t *testing.T) {
	path := database(t)
	writeVersionOne(t, path)
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE sqlite_sequence SET seq = 40 WHERE name = 'nodes'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store := open(t, path, "workspace", 4096)
	state, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.NodeHighWater != 40 {
		t.Fatalf("migration preserved node high-water %d, want 40", state.NodeHighWater)
	}
	if err := store.Create(t.Context(), "after-high-water"); err != nil {
		t.Fatal(err)
	}
	node, err := store.Stat(t.Context(), "after-high-water")
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != 41 {
		t.Fatalf("the first migrated allocation used node %d, want 41", node.ID)
	}
}

// A database written by a version we do not understand is refused rather than adapted. Every
// statement in this package addresses columns by the meaning its own version gives them, so
// running them against another layout would not fail loudly — it would update the wrong
// things.
//
// The direction that matters most is the one this simulates: a database written by a later
// build, opened by an earlier one. There is nothing an earlier build can do but stop, because
// it cannot know which of the columns it addresses the later layout dropped or repurposed.
// Version 0 recorded in the row is here too, because a store that read it as "no schema yet"
// would build a fresh layout over a populated database.
func TestADatabaseFromAnotherSchemaVersionIsRefused(t *testing.T) {
	migrations, err := filepath.Glob("../schema/migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{0, len(migrations) + 1, 999} {
		path := database(t)
		store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
		if err != nil {
			t.Fatalf("opening a fresh database: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("closing: %v", err)
		}

		db := raw(t, path)
		if _, err := db.Exec(`UPDATE schema_version SET version = ?`, version); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
		if err == nil {
			reopened.Close()
			t.Fatalf("a database recording schema version %d opened, want a refusal", version)
		}
		if !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("opening a database recording schema version %d: %v, want EINVAL", version, err)
		}
	}
}

// A database with tables but no recorded version is not a database with no schema, and the
// difference decides between building a layout over data that is already there and refusing to
// touch it. There is nothing to guess from, so it is refused.
func TestADatabaseWithASchemaAndNoVersionIsRefused(t *testing.T) {
	path := database(t)
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening a fresh database: %v", err)
	}
	if err := store.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	if _, err := db.Exec(`DELETE FROM schema_version`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("a database whose schema records no version opened, want a refusal")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a database whose schema records no version: %v, want EINVAL", err)
	}
}

// Version 1 is the layout that had no change log. Carrying it forward adds the log and the
// bookkeeping beside it, and leaves everything the tree already held exactly where it was.
//
// The layout below is written out rather than derived from today's statements, which is the
// only way this asks the real question: a migration tested against a schema the current build
// produced is a migration tested against nothing.
func TestAVersionOneDatabaseIsCarriedForwardIntact(t *testing.T) {
	path := database(t)
	writeVersionOne(t, path)

	store := open(t, path, "workspace", 4096)

	// Everything the tree held is still there, with the same node ids: a replica keyed by node
	// id is the reason those may not be reassigned by a migration.
	dir, err := store.Stat(t.Context(), "d")
	if err != nil {
		t.Fatalf("the directory did not survive the migration: %v", err)
	}
	if !dir.IsDir() || dir.ID != 2 {
		t.Fatalf("the directory came back as node %d with mode %v, want node 2 and a directory", dir.ID, dir.Mode)
	}
	file, err := store.Stat(t.Context(), "d/f")
	if err != nil {
		t.Fatalf("the file did not survive the migration: %v", err)
	}
	if file.ID != 3 || file.Size != 700 || file.Content != "carried" {
		t.Fatalf("the file came back as %+v, want node 3 of 700 bytes referencing \"carried\"", file)
	}
	if file.Mode.Perm() != 0o640 {
		t.Fatalf("the file came back with mode %v, want 0640", file.Mode)
	}
	space, err := store.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if space.Used != 700 {
		t.Fatalf("the migrated volume reports %d bytes used, want the 700 it held", space.Used)
	}

	// The log is there, and it is empty. That is the truthful state: nothing recorded the
	// history this database accumulated before it had a log, so no replica may resume against
	// it — which is exactly what an incarnation nothing has ever seen says.
	changes, retention, err := readChanges(t.Context(), store, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || retention.Tail != 0 || retention.Oldest != 0 {
		t.Fatalf("the migrated log holds %d changes and %+v, want an empty log", len(changes), retention)
	}
	incarnation, err := store.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if incarnation == "" {
		t.Fatal("the migrated log names its history with the empty string, which every other log would match")
	}

	// And it records from here on.
	if err := store.Create(t.Context(), "d/after"); err != nil {
		t.Fatal(err)
	}
	changes, _, err = readChanges(t.Context(), store, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) == 0 {
		t.Fatal("a write after the migration recorded nothing")
	}
	if changes[0].Kind != metastore.Created || changes[0].Parent != dir.ID {
		t.Fatalf("the first change after the migration is a %v under %d, want a creation under the directory %d",
			changes[0].Kind, changes[0].Parent, dir.ID)
	}

	// Reopening does not migrate again; an empty log at committed position 0 remains consistent.
	settled, err := store.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	again := open(t, path, "workspace", 4096)
	stable, err := again.Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if stable != settled {
		t.Fatalf("reopening the migrated database moved the incarnation from %q to %q", settled, stable)
	}
}

// writeVersionOne builds a database in the layout schema version 1 produced, holding one
// volume with a directory, a file of 700 bytes and the object those bytes are under.
func writeVersionOne(t *testing.T, path string) {
	t.Helper()
	db := raw(t, path)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("closing the version 1 database: %v", err)
		}
	}()

	for _, statement := range []string{
		`CREATE TABLE schema_version (version INTEGER NOT NULL)`,
		`CREATE TABLE nodes (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			volume  INTEGER NOT NULL REFERENCES volumes(id),
			mode       INTEGER NOT NULL,
			size       INTEGER NOT NULL,
			atime_sec  INTEGER NOT NULL,
			atime_nsec INTEGER NOT NULL,
			mtime_sec  INTEGER NOT NULL,
			mtime_nsec INTEGER NOT NULL,
			content    TEXT REFERENCES objects(key)
		)`,
		`CREATE TABLE entries (
			parent INTEGER NOT NULL REFERENCES nodes(id),
			name   BLOB    NOT NULL,
			node   INTEGER NOT NULL REFERENCES nodes(id),
			PRIMARY KEY (parent, name)
		) WITHOUT ROWID`,
		`CREATE INDEX entries_by_node ON entries (node)`,
		`CREATE TABLE volumes (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT    NOT NULL UNIQUE,
			root INTEGER NOT NULL,
			used INTEGER NOT NULL
		)`,
		`CREATE TABLE objects (
			key          TEXT PRIMARY KEY,
			volume    INTEGER NOT NULL REFERENCES volumes(id),
			state        INTEGER NOT NULL,
			size         INTEGER NOT NULL,
			digest       BLOB,
			created_sec  INTEGER NOT NULL,
			created_nsec INTEGER NOT NULL
		)`,
		`CREATE INDEX objects_by_state ON objects (volume, state, created_sec)`,
		`CREATE INDEX nodes_by_content ON nodes (content)`,
		`INSERT INTO schema_version (version) VALUES (1)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("building the version 1 layout: %v", err)
		}
	}

	// io/fs's bits: a directory of 0755 for the root and for d, a file of 0640 for d/f.
	const (
		directory = 1<<31 | 0o755
		file      = 0o640
	)
	at := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).Unix()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO volumes (id, name, root, used) VALUES (1, 'workspace', 1, 700)`, nil},
		{`INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		  VALUES (1, 1, ?, 0, ?, 0, ?, 0, NULL)`, []any{directory, at, at}},
		{`INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		  VALUES (2, 1, ?, 0, ?, 0, ?, 0, NULL)`, []any{directory, at, at}},
		{`INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
		  VALUES ('carried', 1, 1, 700, NULL, ?, 0)`, []any{at}},
		{`INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		  VALUES (3, 1, ?, 700, ?, 0, ?, 0, 'carried')`, []any{file, at, at}},
		// The names go in as bytes, which is what the BLOB column holds and what version 1
		// wrote: a string literal here would be stored as TEXT and would never compare equal to
		// the name a lookup asks with.
		{`INSERT INTO entries (parent, name, node) VALUES (1, ?, 2)`, []any{[]byte("d")}},
		{`INSERT INTO entries (parent, name, node) VALUES (2, ?, 3)`, []any{[]byte("f")}},
	} {
		if _, err := db.Exec(statement.sql, statement.args...); err != nil {
			t.Fatalf("filling the version 1 database: %v", err)
		}
	}
}
