package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func TestMiddleRetainedChangeDeletionIsRefusedByOpenAndSince(t *testing.T) {
	deleteMiddle := func(t *testing.T, path string) {
		t.Helper()
		db := raw(t, path)
		defer db.Close()
		result, err := db.Exec(`
			DELETE FROM changes
			WHERE position = (
				SELECT position FROM changes
				WHERE volume = (SELECT id FROM volumes WHERE name = 'workspace')
				ORDER BY position
				LIMIT 1 OFFSET 1
			)`)
		if err != nil {
			t.Fatal(err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if deleted != 1 {
			t.Fatalf("deleted %d middle changes, want 1", deleted)
		}
	}
	populate := func(t *testing.T, path string) *sqlite.Store {
		t.Helper()
		store := open(t, path, "workspace", 0)
		for _, name := range []string{"first", "middle", "last"} {
			if err := store.Create(t.Context(), name); err != nil {
				t.Fatal(err)
			}
		}
		return store
	}

	t.Run("open", func(t *testing.T) {
		path := database(t)
		store := populate(t, path)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		deleteMiddle(t, path)

		reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
		if err == nil {
			reopened.Close()
			t.Fatal("opening a log missing a middle retained change succeeded")
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("opening a log missing a middle retained change: %v, want EIO", err)
		}
	})

	t.Run("since", func(t *testing.T) {
		path := database(t)
		store := populate(t, path)
		deleteMiddle(t, path)

		changes, _, err := readChanges(t.Context(), store, 0, 100)
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("reading a log missing a middle retained change returned %v, want EIO", err)
		}
		if changes != nil {
			t.Fatalf("failed Since exposed changes: %+v", changes)
		}
	})

	t.Run("snapshot and status", func(t *testing.T) {
		path := database(t)
		store := populate(t, path)
		deleteMiddle(t, path)

		if snapshot, _, err := store.Snapshot(t.Context()); !errors.Is(err, syscall.EIO) {
			if snapshot != nil {
				snapshot.Close()
			}
			t.Fatalf("snapshot over a missing middle change returned %v, want EIO", err)
		}
		if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
			t.Fatalf("object status over a missing middle change returned %v, want EIO", err)
		}
	})
}

func TestFullIntegrityRejectsPredecessorAndTrimAnchorCorruption(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, string) *sqlite.Store
		damage  string
	}{
		{
			name: "previous position",
			prepare: func(t *testing.T, path string) *sqlite.Store {
				store := open(t, path, "workspace", 0)
				for _, name := range []string{"first", "second"} {
					if err := store.Create(t.Context(), name); err != nil {
						t.Fatal(err)
					}
				}
				return store
			},
			damage: `
				UPDATE changes SET previous_position = 0
				WHERE volume = (SELECT id FROM volumes WHERE name = 'workspace')
				  AND position = (
					SELECT position FROM changes
					WHERE volume = (SELECT id FROM volumes WHERE name = 'workspace')
					ORDER BY position LIMIT 1 OFFSET 1
				  )`,
		},
		{
			name: "trim anchor",
			prepare: func(t *testing.T, path string) *sqlite.Store {
				volume := open(t, path, "workspace", 0)
				neighbour := open(t, path, "neighbour", 0)
				if err := neighbour.Create(t.Context(), "gap"); err != nil {
					t.Fatal(err)
				}
				if err := volume.Create(t.Context(), "file"); err != nil {
					t.Fatal(err)
				}
				return volume
			},
			damage: `
				UPDATE logs SET trimmed_through = 1
				WHERE volume = (SELECT id FROM volumes WHERE name = 'workspace')`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			store := test.prepare(t, path)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if snapshot, _, err := store.Snapshot(t.Context()); !errors.Is(err, syscall.EIO) {
				if snapshot != nil {
					snapshot.Close()
				}
				t.Fatalf("snapshot over corrupt continuity returned %v, want EIO", err)
			}
			if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("status over corrupt continuity returned %v, want EIO", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
			if err == nil {
				reopened.Close()
				t.Fatal("opening corrupt continuity succeeded")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("opening corrupt continuity returned %v, want EIO", err)
			}
		})
	}
}

func TestLiveReadersRejectLargeBlobScalarsBeforeMaterializingThem(t *testing.T) {
	tests := []struct {
		name   string
		damage string
		read   func(*sqlite.Store) error
	}{
		{
			"database identity",
			`PRAGMA ignore_check_constraints = ON;
			 UPDATE database_state SET database_id = CAST(zeroblob(4 * 1024 * 1024) AS TEXT)`,
			func(store *sqlite.Store) error {
				_, err := store.DurableState(t.Context())
				return err
			},
		},
		{
			"global root identity",
			`UPDATE volumes SET root = zeroblob(4 * 1024 * 1024) WHERE name = 'workspace'`,
			func(store *sqlite.Store) error {
				_, err := store.DurableState(t.Context())
				return err
			},
		},
		{
			"change mode",
			`UPDATE changes SET mode = zeroblob(4 * 1024 * 1024)
			 WHERE position = (SELECT min(position) FROM changes)`,
			func(store *sqlite.Store) error {
				result, err := metastore.NewChangeResult(1<<20, 0,
					func(_ int, _ metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) {
						return 1, nil
					})
				if err != nil {
					return err
				}
				_, err = store.Since(t.Context(), 0, 100, result)
				if changes, resultErr := result.Changes(); changes != nil || !errors.Is(resultErr, syscall.EIO) {
					return fmt.Errorf("failed Since exposed %+v, %v", changes, resultErr)
				}
				return err
			},
		},
		{
			"log age flag",
			`UPDATE logs SET trimmed_by_age = zeroblob(4 * 1024 * 1024)`,
			func(store *sqlite.Store) error {
				_, err := store.Barrier(t.Context(), 1024)
				return err
			},
		},
		{
			"snapshot committed position",
			`UPDATE logs SET committed_position = zeroblob(4 * 1024 * 1024)`,
			func(store *sqlite.Store) error {
				snapshot, _, err := store.Snapshot(t.Context())
				if snapshot != nil {
					snapshot.Close()
				}
				return err
			},
		},
		{
			"append committed position",
			`UPDATE logs SET committed_position = zeroblob(4 * 1024 * 1024)`,
			func(store *sqlite.Store) error {
				return store.Create(t.Context(), "after-corruption")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			store := open(t, path, "workspace", 0)
			if err := store.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := test.read(store); !errors.Is(err, syscall.EIO) {
				t.Fatalf("reading a large corrupt scalar returned %v, want EIO", err)
			}
		})
	}
}

func integrityOptions(limit int64) sqlite.Options {
	return sqlite.Options{Window: sqlite.DefaultWindow(), MaxIntegrityRecords: limit}
}

func integrityByteOptions(limit int64) sqlite.Options {
	return sqlite.Options{Window: sqlite.DefaultWindow(), MaxIntegrityBytes: limit}
}

func TestIntegrityWorkLimitAcceptsItsBoundaryAndRefusesTheNextRecord(t *testing.T) {
	path := database(t)
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "two"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// One volume, three nodes, two entries, one log row, and four retained changes.
	atBoundary, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityOptions(11))
	if err != nil {
		t.Fatalf("opening at the exact integrity work limit: %v", err)
	}
	if err := atBoundary.Close(); err != nil {
		t.Fatal(err)
	}

	overLimit, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityOptions(10))
	if err == nil {
		overLimit.Close()
		t.Fatal("opening one integrity record above the limit succeeded")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("opening one integrity record above the limit: %v, want EFBIG", err)
	}
}

func TestObjectStatusRefusesIntegrityWorkAboveItsConfiguredLimit(t *testing.T) {
	store, err := sqlite.OpenWithOptions(
		t.Context(), database(t), "workspace", 0, integrityOptions(5),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Create(t.Context(), "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "two"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("status one integrity record above its limit: %v, want EFBIG", err)
	}
	if _, err := store.Stat(t.Context(), "one"); err != nil {
		t.Fatalf("a refused status changed the volume: %v", err)
	}
}

func TestIntegrityWorkCountIncludesForeignLabelsTouchingTheVolume(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenWithOptions(
		t.Context(), path, "workspace", 0, integrityOptions(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	neighbour, err := sqlite.Open(t.Context(), path, "neighbour", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := neighbour.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		INSERT INTO entries (volume, parent, name, node)
		SELECT foreign_ns.id, local_ns.root, names.name, foreign_ns.root
		FROM volumes local_ns, volumes foreign_ns,
			(SELECT CAST('hidden-one' AS BLOB) AS name UNION ALL SELECT CAST('hidden-two' AS BLOB)) names
		WHERE local_ns.name = 'workspace' AND foreign_ns.name = 'neighbour'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("foreign-labeled edges above the integrity limit returned %v, want EFBIG", err)
	}
}

func TestCanceledObjectStatusDoesNotPoisonTheStore(t *testing.T) {
	store, err := sqlite.Open(t.Context(), database(t), "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.ObjectStatus(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled integrity status returned %v, want context.Canceled", err)
	}
	if _, err := store.ObjectStatus(t.Context()); err != nil {
		t.Fatalf("status after cancellation: %v", err)
	}
}

func TestLegacyIntegrityWorkLimitRefusesBeforeMigration(t *testing.T) {
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
			before := schemaOf(t, path)
			store, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityOptions(5))
			if err == nil {
				store.Close()
				t.Fatal("migrating one integrity record above the limit succeeded")
			}
			if !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("migrating one integrity record above the limit: %v, want EFBIG", err)
			}
			if after := schemaOf(t, path); after != before {
				t.Fatalf("the refused integrity limit changed schema version %d", version.version)
			}
		})
	}
}

func TestCanceledLegacyOpenDoesNotMigrate(t *testing.T) {
	path := database(t)
	writeVersionTwo(t, path)
	before := schemaOf(t, path)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	store, err := sqlite.OpenWithOptions(ctx, path, "workspace", 0, integrityOptions(6))
	if err == nil {
		store.Close()
		t.Fatal("a canceled legacy open succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled legacy open returned %v, want context.Canceled", err)
	}
	if after := schemaOf(t, path); after != before {
		t.Fatal("a canceled legacy open changed the schema")
	}
}

func TestIntegrityByteLimitAcceptsItsExactBoundary(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	for _, name := range []string{"a", "bc"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Entry names and their Created log records each retain three bytes.
	atBoundary, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityByteOptions(6))
	if err != nil {
		t.Fatalf("opening at the exact integrity byte limit: %v", err)
	}
	if err := atBoundary.Close(); err != nil {
		t.Fatal(err)
	}
	over, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityByteOptions(5))
	if err == nil {
		over.Close()
		t.Fatal("opening one byte above the integrity limit succeeded")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("opening one byte above the integrity limit: %v, want EFBIG", err)
	}
}

func TestOversizedCorruptNameIsRejectedByLengthAdmission(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		UPDATE entries
		SET name = CAST(zeroblob(8388608) AS BLOB)
		WHERE volume = (SELECT id FROM volumes WHERE name = 'workspace')
		  AND name = CAST('file' AS BLOB)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, integrityByteOptions(1024))
	if err == nil {
		opened.Close()
		t.Fatal("opening an oversized corrupt name succeeded")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("opening an oversized corrupt name: %v, want EFBIG", err)
	}
}

type objectIntegrityFixture struct {
	path       string
	live       metastore.Key
	foreign    metastore.Key
	reserved   metastore.Key
	unresolved metastore.Key
	garbage    metastore.Key
	volume     int64
}

func newObjectIntegrityFixture(t *testing.T) objectIntegrityFixture {
	t.Helper()
	path := database(t)
	volume, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	neighbour, err := sqlite.Open(t.Context(), path, "neighbour", 0, sqlite.DefaultWindow())
	if err != nil {
		volume.Close()
		t.Fatal(err)
	}

	live := commit(t, volume, "live", 10)
	if err := volume.Create(t.Context(), "copy"); err != nil {
		t.Fatal(err)
	}
	if err := volume.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	reserved, err := volume.Reserve(t.Context(), "reserved", 20)
	if err != nil {
		t.Fatal(err)
	}
	unresolved, err := volume.Reserve(t.Context(), "unresolved", 25)
	if err != nil {
		t.Fatal(err)
	}
	if err := volume.Quarantine(t.Context(), unresolved); err != nil {
		t.Fatal(err)
	}
	garbage, err := volume.Reserve(t.Context(), "garbage", 30)
	if err != nil {
		t.Fatal(err)
	}
	if err := volume.Abandon(t.Context(), garbage); err != nil {
		t.Fatal(err)
	}
	foreign := commit(t, neighbour, "foreign", 40)

	if err := volume.Close(); err != nil {
		t.Fatal(err)
	}
	if err := neighbour.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	defer db.Close()
	var fixture objectIntegrityFixture
	fixture.path = path
	fixture.live = live
	fixture.foreign = foreign
	fixture.reserved = reserved
	fixture.unresolved = unresolved
	fixture.garbage = garbage
	if err := db.QueryRow(`SELECT id FROM volumes WHERE name = 'workspace'`).Scan(&fixture.volume); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return fixture
}

type objectIntegrityImage struct {
	fixture objectIntegrityFixture
	data    []byte
}

func (image objectIntegrityImage) clone(t *testing.T) objectIntegrityFixture {
	t.Helper()
	fixture := image.fixture
	fixture.path = database(t)
	if err := os.WriteFile(fixture.path, image.data, 0o600); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// Corruption cases need independent copies of the same valid state. Checkpointing
// after every seed handle closes makes the database file the complete fixture.
func newObjectIntegrityImage(t *testing.T) objectIntegrityImage {
	t.Helper()
	fixture := newObjectIntegrityFixture(t)
	db := raw(t, fixture.path)
	defer db.Close()
	db.SetMaxOpenConns(1)
	var journal string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("fixture journal mode=%q, error=%v; want wal", journal, err)
	}
	var busy, frames, checkpointed int
	if err := db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || frames != 0 || checkpointed != 0 {
		t.Fatalf("fixture checkpoint incomplete: busy=%d frames=%d checkpointed=%d", busy, frames, checkpointed)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	image := objectIntegrityImage{fixture: fixture, data: data}
	t.Cleanup(func() {
		current, err := os.ReadFile(fixture.path)
		if err != nil || !bytes.Equal(current, image.data) {
			t.Errorf("integrity seed changed after cloning: %v", err)
		}
	})
	image.checkClones(t)
	return image
}

func (image objectIntegrityImage) checkClones(t *testing.T) {
	t.Helper()
	first := image.clone(t)
	changed := open(t, first.path, "workspace", 0)
	const probe = "clone-isolation-probe"
	if err := changed.Create(t.Context(), probe); err != nil {
		t.Fatal(err)
	}
	if _, err := changed.Stat(t.Context(), probe); err != nil {
		t.Fatalf("clone mutation was not observable: %v", err)
	}
	if err := changed.Close(); err != nil {
		t.Fatal(err)
	}
	second := image.clone(t)
	unchanged := open(t, second.path, "workspace", 0)
	if _, err := unchanged.ObjectStatus(t.Context()); err != nil {
		t.Fatalf("healthy integrity clone failed validation: %v", err)
	}
	if _, err := unchanged.Stat(t.Context(), probe); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("another clone observed an isolated mutation: %v", err)
	}
	if err := unchanged.Close(); err != nil {
		t.Fatal(err)
	}
}

func damageDatabase(t *testing.T, path, statement string, args ...any) {
	t.Helper()
	db := raw(t, path)
	defer db.Close()
	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatalf("damaging the database: %v", err)
	}
}

func addDisconnectedDirectories(t *testing.T, fixture objectIntegrityFixture, cycle bool) {
	t.Helper()
	db := raw(t, fixture.path)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	insertNode := func() int64 {
		result, err := tx.Exec(`
			INSERT INTO nodes (volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
			SELECT ns.id, root.mode, 0, 0, 0, 0, 0, NULL
			FROM volumes ns JOIN nodes root ON root.id = ns.root
			WHERE ns.id = ?`, fixture.volume)
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
	if _, err := tx.Exec(`
		INSERT INTO entries (volume, parent, name, node)
		VALUES (?, ?, CAST('child' AS BLOB), ?)`, fixture.volume, first, second); err != nil {
		t.Fatal(err)
	}
	if cycle {
		if _, err := tx.Exec(`
			INSERT INTO entries (volume, parent, name, node)
			VALUES (?, ?, CAST('parent' AS BLOB), ?)`, fixture.volume, second, first); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func makeFileSizesOverflow(t *testing.T, fixture objectIntegrityFixture) {
	t.Helper()
	db := raw(t, fixture.path)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`UPDATE objects SET size = ? WHERE key = ?`, []any{int64(math.MaxInt64), fixture.live}},
		{`UPDATE nodes SET size = ? WHERE content = ?`, []any{int64(math.MaxInt64), fixture.live}},
		{`INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
		  VALUES ('overflow-byte', ?, 1, 1, NULL, 0, 0)`, []any{fixture.volume}},
		{`UPDATE nodes SET size = 1, content = 'overflow-byte' WHERE ` + nodeNamed,
			[]any{fixture.volume, "copy"}},
		{`UPDATE volumes SET used = ? WHERE id = ?`, []any{int64(math.MaxInt64), fixture.volume}},
	} {
		if _, err := tx.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

const nodeNamed = `id = (SELECT node FROM entries WHERE volume = ? AND name = CAST(? AS BLOB))`

func TestOpenRefusesInconsistentVolumeIntegrity(t *testing.T) {
	image := newObjectIntegrityImage(t)
	tests := []struct {
		name   string
		damage func(*testing.T, objectIntegrityFixture)
	}{
		{"a node names a missing object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `DELETE FROM objects WHERE key = ?`, f.live)
		}},
		{"a node names another volume's object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 40 WHERE content = ?`, f.foreign, f.live)
		}},
		{"a node names a reserved object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 20 WHERE content = ?`, f.reserved, f.live)
		}},
		{"a node names an unresolved object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 25 WHERE content = ?`, f.unresolved, f.live)
		}},
		{"a node names a garbage object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 30 WHERE content = ?`, f.garbage, f.live)
		}},
		{"a referenced object has no node", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = NULL, size = 0 WHERE content = ?`, f.live)
		}},
		{"a referenced object has two nodes in its volume", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 10 WHERE `+nodeNamed,
				f.live, f.volume, "copy")
		}},
		{"a referenced object also has a node in another volume", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 10 WHERE content = ?`, f.live, f.foreign)
		}},
		{"a node and its object disagree about size", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET size = 11 WHERE content = ?`, f.live)
		}},
		{"a node holding content has a non-integer mode", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET mode = 'regular' WHERE content = ?`, f.live)
		}},
		{"a node claims bytes without an object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = NULL, size = 10 WHERE content = ?`, f.live)
		}},
		{"a directory names an object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 10 WHERE `+nodeNamed,
				f.live, f.volume, "directory")
		}},
		{"a non-root node has two entries", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				INSERT INTO entries (volume, parent, name, node)
				SELECT ?, root, CAST('alias' AS BLOB),
					(SELECT node FROM entries WHERE volume = ? AND name = CAST('live' AS BLOB))
				FROM volumes WHERE id = ?`, f.volume, f.volume, f.volume)
		}},
		{"a non-root node has no entry", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path,
				`DELETE FROM entries WHERE volume = ? AND name = CAST('live' AS BLOB)`, f.volume)
		}},
		{"the root has an entry", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				INSERT INTO entries (volume, parent, name, node)
				SELECT id, root, CAST('root-alias' AS BLOB), root FROM volumes WHERE id = ?`, f.volume)
		}},
		{"the volume root belongs to another volume", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE volumes SET root = (SELECT root FROM volumes WHERE name = 'neighbour')
				WHERE id = ?`, f.volume)
		}},
		{"an entry claims another volume", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE entries SET volume = (SELECT id FROM volumes WHERE name = 'neighbour')
				WHERE volume = ? AND name = CAST('live' AS BLOB)`, f.volume)
		}},
		{"a foreign entry uses this volume as its parent", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				INSERT INTO entries (volume, parent, name, node)
				SELECT foreign_ns.id, local_ns.root, CAST('hidden-parent' AS BLOB), foreign_ns.root
				FROM volumes local_ns, volumes foreign_ns
				WHERE local_ns.id = ? AND foreign_ns.name = 'neighbour'`, f.volume)
		}},
		{"a foreign entry claims this volume's child", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE entries
				SET volume = (SELECT id FROM volumes WHERE name = 'neighbour'),
					parent = (SELECT root FROM volumes WHERE name = 'neighbour')
				WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
		}},
		{"an entry has a regular-file parent", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE entries
				SET parent = (SELECT node FROM entries WHERE volume = ? AND name = CAST('live' AS BLOB))
				WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume, f.volume)
		}},
		{"two directories form a disconnected cycle", func(t *testing.T, f objectIntegrityFixture) {
			addDisconnectedDirectories(t, f, true)
		}},
		{"an orphan directory heads a disconnected subtree", func(t *testing.T, f objectIntegrityFixture) {
			addDisconnectedDirectories(t, f, false)
		}},
		{"the volume used counter undercounts files", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE volumes SET used = 9 WHERE id = ?`, f.volume)
		}},
		{"the volume used counter overcounts files", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE volumes SET used = 11 WHERE id = ?`, f.volume)
		}},
		{"the volume used counter is not an integer", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE volumes SET used = 'ten' WHERE id = ?`, f.volume)
		}},
		{"an empty leaf has an invalid stored mode", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET mode = -1 WHERE `+nodeNamed, f.volume, "copy")
		}},
		{"a node has an unsupported type", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET mode = ? WHERE `+nodeNamed,
				int64(fs.ModeSymlink|0o777), f.volume, "copy")
		}},
		{"a node has invalid access nanoseconds", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET atime_nsec = 1000000000 WHERE `+nodeNamed,
				f.volume, "copy")
		}},
		{"a node has a negative id", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE nodes SET id = -1 WHERE `+nodeNamed+`;
				UPDATE entries SET node = -1 WHERE volume = ? AND name = CAST('copy' AS BLOB)`,
				f.volume, "copy", f.volume)
		}},
		{"the volume has a zero root id", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE volumes SET root = 0 WHERE id = ?`, f.volume)
		}},
		{"an entry name is text", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = 'text-name' WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
		}},
		{"an entry name is empty", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = X'' WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
		}},
		{"an entry name is dot", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = X'2e' WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
		}},
		{"an entry name contains a slash", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = CAST('bad/name' AS BLOB) WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
		}},
		{"an entry name contains a nul", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = X'626164006e616d65' WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
		}},
		{"a log has an empty incarnation", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE logs SET incarnation = '' WHERE volume = ?`, f.volume)
		}},
		{"a committed log tail is behind its newest change", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE logs SET committed_position = 0 WHERE volume = ?`, f.volume)
		}},
		{"a change kind is text", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET kind = 'created' WHERE volume = ? AND position = (SELECT min(position) FROM changes WHERE volume = ?)`, f.volume, f.volume)
		}},
		{"a change carries an unsupported node type", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET mode = ? WHERE position = (
				SELECT min(position) FROM changes WHERE volume = ? AND node IS NOT NULL)`,
				int64(fs.ModeSymlink|0o777), f.volume)
		}},
		{"a change carries file bytes without a content key", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET content = NULL WHERE position = (
				SELECT min(position) FROM changes WHERE volume = ? AND (mode & ?) = 0 AND size > 0)`,
				f.volume, int64(fs.ModeType))
		}},
		{"a created change has no name", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET name = NULL WHERE volume = ? AND kind = 0`, f.volume)
		}},
		{"the volume file sizes overflow its counter", func(t *testing.T, f objectIntegrityFixture) {
			makeFileSizesOverflow(t, f)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := image.clone(t)
			test.damage(t, fixture)

			store, err := sqlite.Open(t.Context(), fixture.path, "workspace", 0, sqlite.DefaultWindow())
			if err == nil {
				store.Close()
				t.Fatal("opening inconsistent object records succeeded, want EIO")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("opening inconsistent object records: %v, want EIO", err)
			}
		})
	}
}

func TestObjectStatusRefusesInconsistentObjectRelationships(t *testing.T) {
	fixture := newObjectIntegrityFixture(t)
	store, err := sqlite.Open(t.Context(), fixture.path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	damageDatabase(t, fixture.path, `UPDATE nodes SET content = NULL, size = 0 WHERE content = ?`, fixture.live)
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading status with an unreferenced live object: %v, want EIO", err)
	}
}

func TestObjectStatusRefusesInconsistentNodeRelationships(t *testing.T) {
	fixture := newObjectIntegrityFixture(t)
	store, err := sqlite.Open(t.Context(), fixture.path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	damageDatabase(t, fixture.path, `
		INSERT INTO entries (volume, parent, name, node)
		SELECT ?, root, CAST('alias' AS BLOB),
			(SELECT node FROM entries WHERE volume = ? AND name = CAST('live' AS BLOB))
		FROM volumes WHERE id = ?`, fixture.volume, fixture.volume, fixture.volume)
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading status with a multiply-linked node: %v, want EIO", err)
	}
}

func TestObjectStatusRefusesADisconnectedCycle(t *testing.T) {
	fixture := newObjectIntegrityFixture(t)
	store, err := sqlite.Open(t.Context(), fixture.path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	addDisconnectedDirectories(t, fixture, true)
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading status with a disconnected cycle: %v, want EIO", err)
	}
}

func TestObjectStatusRefusesAUsedCounterMismatch(t *testing.T) {
	fixture := newObjectIntegrityFixture(t)
	store, err := sqlite.Open(t.Context(), fixture.path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	damageDatabase(t, fixture.path, `UPDATE volumes SET used = 9 WHERE id = ?`, fixture.volume)
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading status with a used-counter mismatch: %v, want EIO", err)
	}
}

func detachIntegrityFile(t *testing.T, f objectIntegrityFixture) {
	t.Helper()
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE content = ?`, f.live)
	damageDatabase(t, f.path, `DELETE FROM entries WHERE node = (SELECT id FROM nodes WHERE content = ?)`, f.live)
}

func TestSharedOpenPreservesDetachedObjectsAndQuotaOutsideSnapshots(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachIntegrityFile(t, f)
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE `+nodeNamed, f.volume, "copy")
	damageDatabase(t, f.path, `DELETE FROM entries WHERE volume = ? AND name = CAST('copy' AS BLOB)`, f.volume)
	store, err := sqlite.Open(t.Context(), f.path, "workspace", 100, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := store.ObjectStatus(t.Context()); err != nil {
		t.Fatalf("valid detached graph: %v", err)
	}
	space, err := store.Space(t.Context())
	if err != nil || space.Used != 10 {
		t.Fatalf("retained quota = %+v, %v; want 10 used bytes", space, err)
	}
	snap, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := snap.Close(); err != nil {
			t.Error(err)
		}
	}()
	var count int
	for {
		rows, done, err := readRows(t.Context(), snap, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if string(row.Name) == "live" || string(row.Name) == "copy" || row.Node.Content == f.live {
				t.Fatalf("snapshot exposed a detached file: %+v", row)
			}
			count++
		}
		if done {
			break
		}
	}
	if count != 2 {
		t.Fatalf("snapshot nodes = %d; want root and directory", count)
	}
	db := raw(t, f.path)
	defer db.Close()
	var retained int
	if err := db.QueryRow(`SELECT count(*) FROM nodes n JOIN objects o ON o.key = n.content
		WHERE n.detached = 1 AND n.content = ? AND o.state = 1`, f.live).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("shared open retained %d original objects; want 1", retained)
	}
}

func TestRetainedNodesRemainInsideIntegrityWorkLimit(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachIntegrityFile(t, f)
	db := raw(t, f.path)
	var records int64
	if err := db.QueryRow(`SELECT
		(SELECT count(*) FROM volumes WHERE id = ?) +
		(SELECT count(*) FROM nodes WHERE volume = ?) +
		(SELECT count(*) FROM objects WHERE volume = ?) +
		(SELECT count(*) FROM entries WHERE volume = ?) +
		(SELECT count(*) FROM logs WHERE volume = ?) +
		(SELECT count(*) FROM changes WHERE volume = ?)`,
		f.volume, f.volume, f.volume, f.volume, f.volume, f.volume).Scan(&records); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenWithOptions(t.Context(), f.path, "workspace", 100, integrityOptions(records))
	if err != nil {
		t.Fatalf("exact retained record limit: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenWithOptions(t.Context(), f.path, "workspace", 100, integrityOptions(records-1))
	if store != nil {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("retained node above work limit: %v; want EFBIG", err)
	}
}

func TestRetainedIntegrityRefusesInvalidDetachedState(t *testing.T) {
	image := newObjectIntegrityImage(t)
	for _, damage := range []struct {
		name string
		sql  string
	}{
		{"detached text", `UPDATE nodes SET detached = 'detached' WHERE content = ?`},
		{"detached blob", `UPDATE nodes SET detached = X'01' WHERE content = ?`},
		{"detached fractional", `UPDATE nodes SET detached = 0.5 WHERE content = ?`},
		{"detached nonboolean", `UPDATE nodes SET detached = 2 WHERE content = ?`},
		{"revision text", `UPDATE nodes SET content_revision = 'revision' WHERE content = ?`},
		{"revision blob", `UPDATE nodes SET content_revision = X'01' WHERE content = ?`},
		{"revision fractional", `UPDATE nodes SET content_revision = 1.5 WHERE content = ?`},
		{"revision zero", `UPDATE nodes SET content_revision = 0 WHERE content = ?`},
		{"revision negative", `UPDATE nodes SET content_revision = -1 WHERE content = ?`},
		{"unmarked orphan", `UPDATE nodes SET detached = 0 WHERE content = ?`},
		{"incoming entry", `INSERT INTO entries (volume, parent, name, node)
			SELECT n.volume, ns.root, CAST('restored' AS BLOB), n.id FROM nodes n
			JOIN volumes ns ON ns.id = n.volume WHERE n.content = ?`},
		{"outgoing entry", `UPDATE entries SET parent = (SELECT id FROM nodes WHERE content = ?)
			WHERE name = CAST('copy' AS BLOB)`},
		{"missing object", `DELETE FROM objects WHERE key = ?`},
		{"unreferenced object", `UPDATE objects SET state = 2 WHERE key = ?`},
		{"object size mismatch", `UPDATE objects SET size = 11 WHERE key = ?`},
		{"foreign object", `UPDATE objects SET volume = (SELECT id FROM volumes WHERE name = 'neighbour') WHERE key = ?`},
		{"quota undercharge", `UPDATE volumes SET used = 0 WHERE id = (SELECT volume FROM nodes WHERE content = ?)`},
		{"quota overcharge", `UPDATE volumes SET used = 11 WHERE id = (SELECT volume FROM nodes WHERE content = ?)`},
	} {
		for _, entry := range []string{"open", "status"} {
			t.Run(fmt.Sprintf("%s/%s", damage.name, entry), func(t *testing.T) {
				f := image.clone(t)
				detachIntegrityFile(t, f)
				var store *sqlite.Store
				if entry == "status" {
					store = open(t, f.path, "workspace", 100)
				}
				damageDatabase(t, f.path, damage.sql, f.live)
				assertRetainedIntegrityFailure(t, f.path, store)
			})
		}
	}
}

func TestRetainedIntegrityRefusesDetachedDirectoriesAndRoots(t *testing.T) {
	for _, node := range []string{"directory", "root"} {
		for _, entry := range []string{"open", "status"} {
			t.Run(node+"/"+entry, func(t *testing.T) {
				f := newObjectIntegrityFixture(t)
				var store *sqlite.Store
				if entry == "status" {
					store = open(t, f.path, "workspace", 100)
				}
				if node == "root" {
					damageDatabase(t, f.path, `UPDATE nodes SET detached = 1
						WHERE id = (SELECT root FROM volumes WHERE id = ?)`, f.volume)
				} else {
					damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE `+nodeNamed,
						f.volume, "directory")
					damageDatabase(t, f.path, `DELETE FROM entries WHERE volume = ? AND name = CAST('directory' AS BLOB)`, f.volume)
				}
				assertRetainedIntegrityFailure(t, f.path, store)
			})
		}
	}
}

func assertRetainedIntegrityFailure(t *testing.T, path string, store *sqlite.Store) {
	t.Helper()
	var err error
	if store == nil {
		store, err = sqlite.Open(t.Context(), path, "workspace", 100, sqlite.DefaultWindow())
		if store != nil {
			if closeErr := store.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
	} else {
		_, err = store.ObjectStatus(t.Context())
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("invalid retained graph = %v; want EIO", err)
	}
}
