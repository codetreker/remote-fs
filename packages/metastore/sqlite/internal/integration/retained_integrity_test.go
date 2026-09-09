package integration_test

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func detachIntegrityFile(t *testing.T, f objectIntegrityFixture) {
	t.Helper()
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE content = ?`, f.live)
	damageDatabase(t, f.path, `DELETE FROM entries WHERE node = (SELECT id FROM nodes WHERE content = ?)`, f.live)
}

func TestSharedOpenPreservesDetachedObjectsAndQuotaOutsideSnapshots(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachIntegrityFile(t, f)
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE `+nodeNamed, f.workspace, "copy")
	damageDatabase(t, f.path, `DELETE FROM entries WHERE namespace = ? AND name = CAST('copy' AS BLOB)`, f.workspace)
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
		(SELECT count(*) FROM namespaces WHERE id = ?) +
		(SELECT count(*) FROM nodes WHERE namespace = ?) +
		(SELECT count(*) FROM objects WHERE namespace = ?) +
		(SELECT count(*) FROM entries WHERE namespace = ?) +
		(SELECT count(*) FROM logs WHERE namespace = ?) +
		(SELECT count(*) FROM changes WHERE namespace = ?)`,
		f.workspace, f.workspace, f.workspace, f.workspace, f.workspace, f.workspace).Scan(&records); err != nil {
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
		{"incoming entry", `INSERT INTO entries (namespace, parent, name, node)
			SELECT n.namespace, ns.root, CAST('restored' AS BLOB), n.id FROM nodes n
			JOIN namespaces ns ON ns.id = n.namespace WHERE n.content = ?`},
		{"outgoing entry", `UPDATE entries SET parent = (SELECT id FROM nodes WHERE content = ?)
			WHERE name = CAST('copy' AS BLOB)`},
		{"missing object", `DELETE FROM objects WHERE key = ?`},
		{"unreferenced object", `UPDATE objects SET state = 2 WHERE key = ?`},
		{"object size mismatch", `UPDATE objects SET size = 11 WHERE key = ?`},
		{"foreign object", `UPDATE objects SET namespace = (SELECT id FROM namespaces WHERE name = 'neighbour') WHERE key = ?`},
		{"quota undercharge", `UPDATE namespaces SET used = 0 WHERE id = (SELECT namespace FROM nodes WHERE content = ?)`},
		{"quota overcharge", `UPDATE namespaces SET used = 11 WHERE id = (SELECT namespace FROM nodes WHERE content = ?)`},
	} {
		for _, entry := range []string{"open", "status"} {
			t.Run(fmt.Sprintf("%s/%s", damage.name, entry), func(t *testing.T) {
				f := newObjectIntegrityFixture(t)
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
						WHERE id = (SELECT root FROM namespaces WHERE id = ?)`, f.workspace)
				} else {
					damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE `+nodeNamed,
						f.workspace, "directory")
					damageDatabase(t, f.path, `DELETE FROM entries WHERE namespace = ? AND name = CAST('directory' AS BLOB)`, f.workspace)
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
