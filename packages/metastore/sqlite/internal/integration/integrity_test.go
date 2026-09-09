package integration_test

import (
	"errors"
	"io/fs"
	"math"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

type objectIntegrityFixture struct {
	path       string
	live       metastore.Key
	foreign    metastore.Key
	reserved   metastore.Key
	unresolved metastore.Key
	garbage    metastore.Key
	workspace  int64
}

func newObjectIntegrityFixture(t *testing.T) objectIntegrityFixture {
	t.Helper()
	path := database(t)
	workspace, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	neighbour, err := sqlite.Open(t.Context(), path, "neighbour", 0, sqlite.DefaultWindow())
	if err != nil {
		workspace.Close()
		t.Fatal(err)
	}

	live := commit(t, workspace, "live", 10)
	if err := workspace.Create(t.Context(), "copy"); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	reserved, err := workspace.Reserve(t.Context(), "reserved", 20)
	if err != nil {
		t.Fatal(err)
	}
	unresolved, err := workspace.Reserve(t.Context(), "unresolved", 25)
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.Quarantine(t.Context(), unresolved); err != nil {
		t.Fatal(err)
	}
	garbage, err := workspace.Reserve(t.Context(), "garbage", 30)
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.Abandon(t.Context(), garbage); err != nil {
		t.Fatal(err)
	}
	foreign := commit(t, neighbour, "foreign", 40)

	if err := workspace.Close(); err != nil {
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
	if err := db.QueryRow(`SELECT id FROM namespaces WHERE name = 'workspace'`).Scan(&fixture.workspace); err != nil {
		t.Fatal(err)
	}
	return fixture
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
			INSERT INTO nodes (namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
			SELECT ns.id, root.mode, 0, 0, 0, 0, 0, NULL
			FROM namespaces ns JOIN nodes root ON root.id = ns.root
			WHERE ns.id = ?`, fixture.workspace)
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
		INSERT INTO entries (namespace, parent, name, node)
		VALUES (?, ?, CAST('child' AS BLOB), ?)`, fixture.workspace, first, second); err != nil {
		t.Fatal(err)
	}
	if cycle {
		if _, err := tx.Exec(`
			INSERT INTO entries (namespace, parent, name, node)
			VALUES (?, ?, CAST('parent' AS BLOB), ?)`, fixture.workspace, second, first); err != nil {
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
		{`INSERT INTO objects (key, namespace, state, size, digest, created_sec, created_nsec)
		  VALUES ('overflow-byte', ?, 1, 1, NULL, 0, 0)`, []any{fixture.workspace}},
		{`UPDATE nodes SET size = 1, content = 'overflow-byte' WHERE ` + nodeNamed,
			[]any{fixture.workspace, "copy"}},
		{`UPDATE namespaces SET used = ? WHERE id = ?`, []any{int64(math.MaxInt64), fixture.workspace}},
	} {
		if _, err := tx.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

const nodeNamed = `id = (SELECT node FROM entries WHERE namespace = ? AND name = CAST(? AS BLOB))`

func TestOpenRefusesInconsistentNamespaceIntegrity(t *testing.T) {
	tests := []struct {
		name   string
		damage func(*testing.T, objectIntegrityFixture)
	}{
		{"a node names a missing object", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `DELETE FROM objects WHERE key = ?`, f.live)
		}},
		{"a node names another namespace's object", func(t *testing.T, f objectIntegrityFixture) {
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
		{"a referenced object has two nodes in its namespace", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET content = ?, size = 10 WHERE `+nodeNamed,
				f.live, f.workspace, "copy")
		}},
		{"a referenced object also has a node in another namespace", func(t *testing.T, f objectIntegrityFixture) {
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
				f.live, f.workspace, "directory")
		}},
		{"a non-root node has two entries", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				INSERT INTO entries (namespace, parent, name, node)
				SELECT ?, root, CAST('alias' AS BLOB),
					(SELECT node FROM entries WHERE namespace = ? AND name = CAST('live' AS BLOB))
				FROM namespaces WHERE id = ?`, f.workspace, f.workspace, f.workspace)
		}},
		{"a non-root node has no entry", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path,
				`DELETE FROM entries WHERE namespace = ? AND name = CAST('live' AS BLOB)`, f.workspace)
		}},
		{"the root has an entry", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				INSERT INTO entries (namespace, parent, name, node)
				SELECT id, root, CAST('root-alias' AS BLOB), root FROM namespaces WHERE id = ?`, f.workspace)
		}},
		{"the namespace root belongs to another namespace", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE namespaces SET root = (SELECT root FROM namespaces WHERE name = 'neighbour')
				WHERE id = ?`, f.workspace)
		}},
		{"an entry claims another namespace", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE entries SET namespace = (SELECT id FROM namespaces WHERE name = 'neighbour')
				WHERE namespace = ? AND name = CAST('live' AS BLOB)`, f.workspace)
		}},
		{"a foreign entry uses this namespace as its parent", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				INSERT INTO entries (namespace, parent, name, node)
				SELECT foreign_ns.id, local_ns.root, CAST('hidden-parent' AS BLOB), foreign_ns.root
				FROM namespaces local_ns, namespaces foreign_ns
				WHERE local_ns.id = ? AND foreign_ns.name = 'neighbour'`, f.workspace)
		}},
		{"a foreign entry claims this namespace's child", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE entries
				SET namespace = (SELECT id FROM namespaces WHERE name = 'neighbour'),
					parent = (SELECT root FROM namespaces WHERE name = 'neighbour')
				WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
		}},
		{"an entry has a regular-file parent", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE entries
				SET parent = (SELECT node FROM entries WHERE namespace = ? AND name = CAST('live' AS BLOB))
				WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace, f.workspace)
		}},
		{"two directories form a disconnected cycle", func(t *testing.T, f objectIntegrityFixture) {
			addDisconnectedDirectories(t, f, true)
		}},
		{"an orphan directory heads a disconnected subtree", func(t *testing.T, f objectIntegrityFixture) {
			addDisconnectedDirectories(t, f, false)
		}},
		{"the namespace used counter undercounts files", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE namespaces SET used = 9 WHERE id = ?`, f.workspace)
		}},
		{"the namespace used counter overcounts files", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE namespaces SET used = 11 WHERE id = ?`, f.workspace)
		}},
		{"the namespace used counter is not an integer", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE namespaces SET used = 'ten' WHERE id = ?`, f.workspace)
		}},
		{"an empty leaf has an invalid stored mode", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET mode = -1 WHERE `+nodeNamed, f.workspace, "copy")
		}},
		{"a node has an unsupported type", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET mode = ? WHERE `+nodeNamed,
				int64(fs.ModeSymlink|0o777), f.workspace, "copy")
		}},
		{"a node has invalid access nanoseconds", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE nodes SET atime_nsec = 1000000000 WHERE `+nodeNamed,
				f.workspace, "copy")
		}},
		{"a node has a negative id", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `
				UPDATE nodes SET id = -1 WHERE `+nodeNamed+`;
				UPDATE entries SET node = -1 WHERE namespace = ? AND name = CAST('copy' AS BLOB)`,
				f.workspace, "copy", f.workspace)
		}},
		{"the namespace has a zero root id", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE namespaces SET root = 0 WHERE id = ?`, f.workspace)
		}},
		{"an entry name is text", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = 'text-name' WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
		}},
		{"an entry name is empty", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = X'' WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
		}},
		{"an entry name is dot", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = X'2e' WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
		}},
		{"an entry name contains a slash", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = CAST('bad/name' AS BLOB) WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
		}},
		{"an entry name contains a nul", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE entries SET name = X'626164006e616d65' WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
		}},
		{"a log has an empty incarnation", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE logs SET incarnation = '' WHERE namespace = ?`, f.workspace)
		}},
		{"a committed log tail is behind its newest change", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE logs SET committed_position = 0 WHERE namespace = ?`, f.workspace)
		}},
		{"a change kind is text", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET kind = 'created' WHERE namespace = ? AND position = (SELECT min(position) FROM changes WHERE namespace = ?)`, f.workspace, f.workspace)
		}},
		{"a change carries an unsupported node type", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET mode = ? WHERE position = (
				SELECT min(position) FROM changes WHERE namespace = ? AND node IS NOT NULL)`,
				int64(fs.ModeSymlink|0o777), f.workspace)
		}},
		{"a change carries file bytes without a content key", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET content = NULL WHERE position = (
				SELECT min(position) FROM changes WHERE namespace = ? AND (mode & ?) = 0 AND size > 0)`,
				f.workspace, int64(fs.ModeType))
		}},
		{"a created change has no name", func(t *testing.T, f objectIntegrityFixture) {
			damageDatabase(t, f.path, `UPDATE changes SET name = NULL WHERE namespace = ? AND kind = 0`, f.workspace)
		}},
		{"the namespace file sizes overflow its counter", func(t *testing.T, f objectIntegrityFixture) {
			makeFileSizesOverflow(t, f)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newObjectIntegrityFixture(t)
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
		INSERT INTO entries (namespace, parent, name, node)
		SELECT ?, root, CAST('alias' AS BLOB),
			(SELECT node FROM entries WHERE namespace = ? AND name = CAST('live' AS BLOB))
		FROM namespaces WHERE id = ?`, fixture.workspace, fixture.workspace, fixture.workspace)
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

	damageDatabase(t, fixture.path, `UPDATE namespaces SET used = 9 WHERE id = ?`, fixture.workspace)
	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading status with a used-counter mismatch: %v, want EIO", err)
	}
}
