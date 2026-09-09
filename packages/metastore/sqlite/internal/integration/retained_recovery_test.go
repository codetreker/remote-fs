package integration_test

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func detachRecoveryFiles(t *testing.T, f objectIntegrityFixture) {
	t.Helper()
	detachIntegrityFile(t, f)
	damageDatabase(t, f.path, `UPDATE nodes SET detached = 1 WHERE content = ?`, f.foreign)
	damageDatabase(t, f.path, `DELETE FROM entries WHERE node = (SELECT id FROM nodes WHERE content = ?)`, f.foreign)
}

func TestExclusiveOwnerReapsEveryNamespaceAtFullPendingAdmission(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	options := sqlite.DefaultOptions()
	options.ObjectLimits.MaxPendingObjects = 1
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Namespace: "workspace", Allowance: 100,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatalf("exclusive retained-file recovery: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	status, err := store.ObjectStatus(t.Context())
	if err != nil || !status.OverLimit || status.GarbageCount != 2 || status.GarbageBytes != 40 {
		t.Fatalf("recovered pending objects = %+v, %v", status, err)
	}
	db := raw(t, f.path)
	defer db.Close()
	var detached, used, retired int64
	if err := db.QueryRow(`SELECT
		(SELECT count(*) FROM nodes WHERE detached = 1),
		(SELECT sum(used) FROM namespaces),
		(SELECT count(*) FROM objects WHERE key IN (?, ?) AND state = 2)`, f.live, f.foreign).Scan(
		&detached, &used, &retired); err != nil {
		t.Fatal(err)
	}
	if detached != 0 || used != 0 || retired != 2 {
		t.Fatalf("recovery retained=%d used=%d retired=%d; want 0, 0, 2", detached, used, retired)
	}
}

func TestExclusiveOwnerRefusesCorruptRetainedGraphBeforeReaping(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	damageDatabase(t, f.path, `UPDATE namespaces SET used = 0 WHERE name = 'neighbour'`)
	before := historicalLeaseRows(t, f.path)
	beforeSchema := schemaOf(t, f.path)
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Namespace: "workspace", Allowance: 100,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("exclusive recovery with corrupt foreign retained quota: %v; want EIO", err)
	}
	assertHistoricalLeaseRows(t, f.path, before)
	if after := schemaOf(t, f.path); after != beforeSchema {
		t.Fatal("refused recovery changed schema")
	}
}

func TestExclusiveOwnerReapRollsBackObjectAndQuotaChanges(t *testing.T) {
	f := newObjectIntegrityFixture(t)
	detachRecoveryFiles(t, f)
	damageDatabase(t, f.path, `CREATE TRIGGER refuse_retained_delete BEFORE DELETE ON nodes
		WHEN OLD.detached = 1 BEGIN SELECT RAISE(ABORT, 'retained cleanup blocked'); END`)
	before := historicalLeaseRows(t, f.path)
	state, err := sqlite.InspectDurableState(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: f.path, Namespace: "workspace", Allowance: 100,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("recovery with failed node cleanup: %v; want EIO", err)
	}
	assertHistoricalLeaseRows(t, f.path, before)
	after, err := sqlite.InspectDurableState(t.Context(), f.path)
	if err != nil || after != state {
		t.Fatalf("failed reap changed durable generation: %+v, %v; want %+v", after, err, state)
	}
}
