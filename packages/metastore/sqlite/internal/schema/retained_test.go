package schema

import (
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
)

func TestPreparationReclaimsOnlyDetachedFiles(t *testing.T) {
	db := testDatabase(t, 0)
	id, root := testNamespace(t, db, "workspace")
	live, liveKey := testFile(t, db, id, root, "live", 3, false)
	detached, detachedKey := testFile(t, db, id, root, "unlinked", 7, true)
	other, otherRoot := testNamespace(t, db, "other")
	otherDetached, _ := testFile(t, db, other, otherRoot, "orphan", 11, true)
	_, _, _, err := PrepareConfigured(t.Context(), db, "workspace", "", changes.DefaultWindow(), 1000, 1<<20,
		&DurableOpen{Mode: RequireExistingNamespace, ReapDetached: true})
	if err != nil {
		t.Fatal(err)
	}
	var retained, used, state, liveState int64
	if err := db.QueryRow(`SELECT count(*) FROM nodes WHERE id IN (?,?)`, detached, otherDetached).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT used FROM namespaces WHERE id=?`, id).Scan(&used); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT state FROM objects WHERE key=?`, detachedKey).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT o.state FROM nodes n JOIN objects o ON o.key=n.content WHERE n.id=? AND o.key=?`, live, liveKey).Scan(&liveState); err != nil {
		t.Fatal(err)
	}
	if retained != 0 || used != 3 || state != StateGarbage || liveState != StateReferenced {
		t.Fatalf("wrong recovery: retained=%d used=%d garbage-state=%d live-state=%d", retained, used, state, liveState)
	}
	if err := db.QueryRow(`SELECT used FROM namespaces WHERE id=?`, other).Scan(&used); err != nil || used != 0 {
		t.Fatalf("other namespace's orphan remains charged: used=%d err=%v", used, err)
	}
}

func TestReclamationFailureRollsBackAllThreeSteps(t *testing.T) {
	for _, target := range []struct{ name, event string }{
		{"object retirement", "UPDATE ON objects"},
		{"quota release", "UPDATE ON namespaces"},
		{"node deletion", "DELETE ON nodes"},
	} {
		t.Run(target.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testNamespace(t, db, "workspace")
			node, key := testFile(t, db, id, root, "unlinked", 7, true)
			execute(t, db, `CREATE TRIGGER refuse_reap BEFORE `+target.event+` BEGIN SELECT RAISE(ABORT,'reclamation refused'); END`)
			tx := testTransaction(t, db)
			err := reapDetachedFiles(t.Context(), tx)
			if err == nil || !strings.Contains(err.Error(), "reclamation refused") {
				t.Fatalf("reclamation failure disappeared: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var state, size, used int64
			if err := db.QueryRow(`SELECT o.state,n.size,ns.used FROM nodes n JOIN objects o ON o.key=n.content
				JOIN namespaces ns ON ns.id=n.namespace WHERE n.id=? AND o.key=?`, node, key).Scan(&state, &size, &used); err != nil {
				t.Fatal(err)
			}
			if state != StateReferenced || size != 7 || used != 7 {
				t.Fatalf("failed reclamation lost ownership or charge: state=%d size=%d used=%d", state, size, used)
			}
		})
	}
}
