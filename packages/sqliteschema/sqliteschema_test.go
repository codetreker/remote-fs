package sqliteschema_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"

	_ "modernc.org/sqlite"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

// The migrations these cases run. They are an fstest.MapFS rather than an embed.FS, which is
// the arrangement every caller will not have — and that is the point: the package takes an
// fs.FS, so nothing here depends on the files being compiled in.
func migrations(files map[string]string) fstest.MapFS {
	mapped := fstest.MapFS{}
	for name, contents := range files {
		mapped["migrations/"+name] = &fstest.MapFile{Data: []byte(contents)}
	}
	return mapped
}

// two is a schema in two steps, where the second changes what the first built. A migration set
// whose steps only add would not show the difference between replaying history and taking a
// shortcut to the end of it.
func two() fstest.MapFS {
	return migrations(map[string]string{
		"0001_first.sql": `
			CREATE TABLE animals (name TEXT PRIMARY KEY, legs INTEGER NOT NULL);
			INSERT INTO animals (name, legs) VALUES ('cat', 4);
		`,
		"0002_second.sql": `
			ALTER TABLE animals ADD COLUMN tail INTEGER;
			CREATE INDEX animals_by_legs ON animals (legs);
			UPDATE animals SET tail = 1;
		`,
	})
}

func database(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "schema.db"))
	if err != nil {
		t.Fatalf("opening a database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing the database: %v", err)
		}
	})
	return db
}

// reach runs the migrations in one transaction and commits, which is what a caller does around
// whatever else belongs in the same atomic step.
func reach(t *testing.T, db *sql.DB, m sqliteschema.Migrations) error {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := m.Reach(t.Context(), tx); err != nil {
		return err
	}
	return tx.Commit()
}

func versionOf(t *testing.T, db *sql.DB) int {
	t.Helper()
	var version int
	if err := db.QueryRowContext(t.Context(), `SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("reading the recorded version: %v", err)
	}
	return version
}

// A database with nothing in it gets every migration, in order, and records where it arrived.
func TestADatabaseWithNothingInItRunsEveryMigration(t *testing.T) {
	db := database(t)
	if err := reach(t, db, sqliteschema.MustLoad(two(), "migrations")); err != nil {
		t.Fatalf("reaching the schema: %v", err)
	}

	if version := versionOf(t, db); version != 2 {
		t.Fatalf("a database that ran both migrations records version %d, want 2", version)
	}
	// Both ran, and in order: the second alters what the first made and updates the row it
	// inserted, so a set run backwards or partly would not produce this.
	var name string
	var tail int
	if err := db.QueryRowContext(t.Context(), `SELECT name, tail FROM animals`).Scan(&name, &tail); err != nil {
		t.Fatalf("reading what the migrations built: %v", err)
	}
	if name != "cat" || tail != 1 {
		t.Fatalf("the migrations left %q with tail %d, want cat with tail 1", name, tail)
	}
}

// A database that has had some of them gets the rest, and nothing it already had runs again.
// Running one twice is not a subtle failure — the second CREATE would refuse — so this is
// checked by arriving at version 2 from version 1 rather than by inspection.
func TestOnlyTheMigrationsADatabaseHasNotHadAreRun(t *testing.T) {
	db := database(t)
	first := migrations(map[string]string{"0001_first.sql": string(two()["migrations/0001_first.sql"].Data)})
	if err := reach(t, db, sqliteschema.MustLoad(first, "migrations")); err != nil {
		t.Fatalf("reaching version 1: %v", err)
	}
	if version := versionOf(t, db); version != 1 {
		t.Fatalf("a database that ran one migration records version %d, want 1", version)
	}

	if err := reach(t, db, sqliteschema.MustLoad(two(), "migrations")); err != nil {
		t.Fatalf("carrying it forward to version 2: %v", err)
	}
	if version := versionOf(t, db); version != 2 {
		t.Fatalf("after carrying it forward the database records version %d, want 2", version)
	}
	var tail int
	if err := db.QueryRowContext(t.Context(), `SELECT tail FROM animals`).Scan(&tail); err != nil {
		t.Fatalf("reading what the second migration added: %v", err)
	}
	if tail != 1 {
		t.Fatalf("the row the first migration inserted has tail %d, want the 1 the second set", tail)
	}
}

// Reaching a schema a database already has does nothing at all, which is what makes Reach safe
// to call on every open rather than only when something is known to have changed.
func TestReachingASchemaADatabaseAlreadyHasDoesNothing(t *testing.T) {
	db := database(t)
	schema := sqliteschema.MustLoad(two(), "migrations")
	if err := reach(t, db, schema); err != nil {
		t.Fatal(err)
	}
	before, err := sqliteschema.Dump(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}

	if err := reach(t, db, schema); err != nil {
		t.Fatalf("reaching a schema the database already has: %v", err)
	}
	after, err := sqliteschema.Dump(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("opening a database at its own version changed the schema.\nbefore:\n%s\nafter:\n%s", before, after)
	}
	var rows int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM animals`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("the animals table holds %d rows, want the 1 the first migration inserted", rows)
	}
}

// Reach does not commit, and a caller that rolls back gets a database that was never touched.
// This is the property a caller relies on to put a migration and its own work in one step: if
// Reach committed, the failure of anything after it would leave a half-built database recorded
// as whole.
func TestReachLeavesTheCommitToTheCaller(t *testing.T) {
	db := database(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sqliteschema.MustLoad(two(), "migrations").Reach(t.Context(), tx); err != nil {
		t.Fatalf("reaching the schema: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var objects int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if objects != 0 {
		t.Fatalf("a rolled-back migration left %d objects in the database, want none", objects)
	}
}

// A database from a build we do not have is refused rather than adapted, and so is one
// recording a version no build ever wrote. There is nothing an older binary can do but stop: it
// cannot know which of the columns it addresses the newer layout dropped or repurposed.
func TestAVersionThisBuildCannotCarryForwardIsRefused(t *testing.T) {
	for _, version := range []int{-1, 0, 3, 999} {
		db := database(t)
		if err := reach(t, db, sqliteschema.MustLoad(two(), "migrations")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `UPDATE schema_version SET version = ?`, version); err != nil {
			t.Fatal(err)
		}

		err := reach(t, db, sqliteschema.MustLoad(two(), "migrations"))
		if !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("a database recording version %d was answered %v, want EINVAL", version, err)
		}
	}
}

// A database with tables but no recorded version is not a database with no schema, and the
// difference decides between building a layout over data that is already there and refusing to
// touch it. There is nothing to guess from, so it is refused.
func TestADatabaseWithASchemaAndNoVersionIsRefused(t *testing.T) {
	db := database(t)
	if err := reach(t, db, sqliteschema.MustLoad(two(), "migrations")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `DELETE FROM schema_version`); err != nil {
		t.Fatal(err)
	}

	err := reach(t, db, sqliteschema.MustLoad(two(), "migrations"))
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("a database whose schema records no version was answered %v, want EINVAL", err)
	}
	// The refusal did not touch what was there.
	var rows int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM animals`).Scan(&rows); err != nil {
		t.Fatalf("the refused database lost the table it had: %v", err)
	}
	if rows != 1 {
		t.Fatalf("the refused database holds %d rows, want the 1 it had", rows)
	}
}

// A migration that fails takes the whole attempt with it, including the migrations before it in
// the same call. Half a schema recorded as whole is the state nothing downstream could detect.
func TestAMigrationThatFailsLeavesTheDatabaseAsItWas(t *testing.T) {
	db := database(t)
	broken := migrations(map[string]string{
		"0001_first.sql":  `CREATE TABLE animals (name TEXT PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE animals (name TEXT PRIMARY KEY);`,
	})

	err := reach(t, db, sqliteschema.MustLoad(broken, "migrations"))
	if err == nil {
		t.Fatal("a migration that cannot run reported success")
	}
	if !strings.Contains(err.Error(), "0002_second.sql") {
		t.Fatalf("the failure is reported as %v, which does not name the migration that failed", err)
	}

	var objects int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema`).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if objects != 0 {
		t.Fatalf("a failed migration left %d objects behind, want a database exactly as it was found", objects)
	}
}

// The numbering is the whole contract between a file's name and what it means, so everything
// that breaks it is refused at load rather than at the first database somebody opens.
func TestMigrationsThatAreNotANumberingAreRefused(t *testing.T) {
	for _, badly := range []struct {
		named string
		files map[string]string
	}{
		{"a set with nothing in it", map[string]string{}},
		{"a name with no version in front", map[string]string{"tree.sql": `CREATE TABLE t (x INTEGER);`}},
		{"a version that is not a number", map[string]string{"first_tree.sql": `CREATE TABLE t (x INTEGER);`}},
		{"a set that does not start at 1", map[string]string{"0002_tree.sql": `CREATE TABLE t (x INTEGER);`}},
		{"a gap in the middle", map[string]string{
			"0001_tree.sql": `CREATE TABLE t (x INTEGER);`,
			"0003_more.sql": `CREATE TABLE u (y INTEGER);`,
		}},
		{"the same version twice", map[string]string{
			"0001_tree.sql": `CREATE TABLE t (x INTEGER);`,
			"0001_more.sql": `CREATE TABLE u (y INTEGER);`,
		}},
	} {
		_, err := sqliteschema.Load(migrations(badly.files), "migrations")
		if !errors.Is(err, syscall.EINVAL) {
			t.Errorf("%s was answered %v, want EINVAL", badly.named, err)
		}
	}
}

// MustLoad is what a package-level variable uses, so what it does with a set Load refuses is
// part of the contract: it stops the build that carries it rather than the first database that
// build opens.
func TestMustLoadStopsABuildWhoseMigrationsAreNotANumbering(t *testing.T) {
	defer func() {
		said, ok := recover().(string)
		if !ok {
			t.Fatal("a misnumbered set loaded without a panic")
		}
		if !strings.Contains(said, "0003_more.sql") {
			t.Fatalf("the panic said %q, which does not name the file that broke the numbering", said)
		}
	}()
	sqliteschema.MustLoad(migrations(map[string]string{
		"0001_tree.sql": `CREATE TABLE t (x INTEGER);`,
		"0003_more.sql": `CREATE TABLE u (y INTEGER);`,
	}), "migrations")
}

// Two databases holding the same layout compare equal through Structure however the statements
// that built them were laid out, and two holding different layouts do not.
//
// This is what lets a caller check a database built from nothing against one carried forward,
// where one side's tables came from .sql files and the other's from Go string literals.
func TestStructureIgnoresLayoutAndNothingElse(t *testing.T) {
	// The same two tables laid out two ways. AUTOINCREMENT and WITHOUT ROWID need a table each,
	// because SQLite refuses them together.
	const packedSQL = `CREATE TABLE t (x INTEGER PRIMARY KEY AUTOINCREMENT, y BLOB);` +
		`CREATE TABLE u (a INTEGER, b BLOB, PRIMARY KEY (a, b)) WITHOUT ROWID;`
	const spreadSQL = "CREATE TABLE t (\n\t\t\tx INTEGER PRIMARY KEY AUTOINCREMENT,\n\t\t\ty BLOB\n\t\t);\n" +
		"CREATE TABLE u (\n\t\t\ta INTEGER,\n\t\t\tb BLOB,\n\t\t\tPRIMARY KEY (a, b)\n\t\t) WITHOUT ROWID;"

	spread, packed := database(t), database(t)
	if _, err := spread.ExecContext(t.Context(), spreadSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := packed.ExecContext(t.Context(), packedSQL); err != nil {
		t.Fatal(err)
	}

	spreadOut, err := sqliteschema.Dump(t.Context(), spread)
	if err != nil {
		t.Fatal(err)
	}
	packedOut, err := sqliteschema.Dump(t.Context(), packed)
	if err != nil {
		t.Fatal(err)
	}
	if spreadOut == packedOut {
		t.Fatal("the two databases dumped identically, so this case is not asking anything")
	}
	if sqliteschema.Structure(spreadOut) != sqliteschema.Structure(packedOut) {
		t.Fatalf("the same tables laid out two ways did not compare equal.\n%q\n%q",
			sqliteschema.Structure(spreadOut), sqliteschema.Structure(packedOut))
	}
	// The tokens that survive are the ones only this text carries: a reading through
	// PRAGMA table_info would report these tables and the ones below identically.
	if structure := sqliteschema.Structure(packedOut); !strings.Contains(structure, "AUTOINCREMENT") ||
		!strings.Contains(structure, "WITHOUT ROWID") {
		t.Fatalf("the structure is %q, which has lost what only the statement says", structure)
	}

	different := database(t)
	if _, err := different.ExecContext(t.Context(),
		`CREATE TABLE t (x INTEGER PRIMARY KEY, y BLOB);`+
			`CREATE TABLE u (a INTEGER, b BLOB, PRIMARY KEY (a, b));`); err != nil {
		t.Fatal(err)
	}
	differentOut, err := sqliteschema.Dump(t.Context(), different)
	if err != nil {
		t.Fatal(err)
	}
	if sqliteschema.Structure(differentOut) == sqliteschema.Structure(packedOut) {
		t.Fatal("tables without AUTOINCREMENT or WITHOUT ROWID compared equal to ones with both")
	}
}

// Dump names what SQLite maintains itself rather than skipping it, because whether an implicit
// index exists is part of the layout even though nothing wrote it.
func TestDumpNamesWhatNothingWrote(t *testing.T) {
	db := database(t)
	if _, err := db.ExecContext(t.Context(),
		`CREATE TABLE t (x TEXT UNIQUE, y INTEGER)`); err != nil {
		t.Fatal(err)
	}
	dump, err := sqliteschema.Dump(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dump, "sqlite_autoindex_t_1") {
		t.Fatalf("the dump is %q, and does not mention the index SQLite built for the UNIQUE column", dump)
	}
}

// Dump reads through whatever the caller has open, which for a database part way through being
// built is the transaction building it.
func TestDumpReadsThroughATransaction(t *testing.T) {
	db := database(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := sqliteschema.MustLoad(two(), "migrations").Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}

	dump, err := sqliteschema.Dump(t.Context(), tx)
	if err != nil {
		t.Fatalf("dumping an uncommitted schema: %v", err)
	}
	if !strings.Contains(dump, "animals") || !strings.Contains(dump, "animals_by_legs") {
		t.Fatalf("the dump of an uncommitted schema is %q, and does not hold what the migrations just built", dump)
	}
}

// Load takes an fs.FS, so a caller may keep its migrations wherever it keeps anything else.
// This is the same set read from a subdirectory of a different name.
func TestMigrationsAreReadFromWhereverTheCallerKeepsThem(t *testing.T) {
	elsewhere := fstest.MapFS{
		"db/steps/0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE animals (name TEXT);`)},
	}
	schema, err := sqliteschema.Load(elsewhere, "db/steps")
	if err != nil {
		t.Fatalf("loading migrations from db/steps: %v", err)
	}
	if schema.Version() != 1 {
		t.Fatalf("the set reaches version %d, want 1", schema.Version())
	}

	db := database(t)
	if err := reach(t, db, schema); err != nil {
		t.Fatal(err)
	}
	if version := versionOf(t, db); version != 1 {
		t.Fatalf("the database records version %d, want 1", version)
	}
}
