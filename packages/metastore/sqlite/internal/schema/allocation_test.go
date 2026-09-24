package schema

import (
	"errors"
	"math"
	"syscall"
	"testing"
)

func TestVirtualAllocationMigrationKeepsLogicalUsageAndCountsDetachedNodes(t *testing.T) {
	db := testDatabase(t, firstDeleteIntentOwnerSchemaVersion)
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'old',1,4098)`)
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,detached)
		VALUES(1,1,2,0,0,0,0,0,0),(2,1,1,1,0,0,0,0,0),(3,1,1,4097,0,0,0,0,1)`)
	tx := testTransaction(t, db)
	if err := schema.Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := backfillAuthorityAllocation(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var used, allocated int64
	if err := db.QueryRow(`SELECT used,allocated_used FROM volumes WHERE id=1`).Scan(&used, &allocated); err != nil {
		t.Fatal(err)
	}
	if used != 4098 || allocated != 12288 {
		t.Fatalf("migrated counters = %d logical, %d allocated; want 4098 and 12288", used, allocated)
	}
	id := int64(1)
	if err := validateAllocation(t.Context(), db, &id, false); err != nil {
		t.Fatal(err)
	}
	execute(t, db, `UPDATE nodes SET allocation_size=0 WHERE id=3`)
	if err := validateAllocation(t.Context(), db, &id, false); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt allocation = %v, want EIO", err)
	}
}

func TestVirtualAllocationMigrationRejectsUnrepresentableExtentAtomically(t *testing.T) {
	db := testDatabase(t, firstDeleteIntentOwnerSchemaVersion)
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'old',1,?)`, int64(math.MaxInt64))
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,detached)
		VALUES(1,1,2,0,0,0,0,0,0),(2,1,1,?,0,0,0,0,0)`, int64(math.MaxInt64))
	tx := testTransaction(t, db)
	if err := schema.Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := backfillAuthorityAllocation(t.Context(), tx); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("unrepresentable allocation backfill = %v, want EOVERFLOW", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != firstDeleteIntentOwnerSchemaVersion {
		t.Fatalf("failed migration advanced version to %d", version)
	}
}

func TestReplicaIntegrityRejectsObjectOwnershipInItsMetadataCopy(t *testing.T) {
	db := testDatabase(t, 0)
	replicaID, _ := testVolume(t, db, "replica")
	otherID, _ := testVolume(t, db, "other")
	if err := validateReplicaObjectAbsence(t.Context(), db, &replicaID); err != nil {
		t.Fatalf("empty replica object ledger: %v", err)
	}
	execute(t, db, `INSERT INTO objects(key,volume,state,size,created_sec,created_nsec)
		VALUES('unowned-payload',?,?,1,0,0)`, otherID, StateReferenced)
	if err := validateReplicaObjectAbsence(t.Context(), db, &replicaID); err != nil {
		t.Fatalf("another volume's object changed the scoped replica: %v", err)
	}
	if err := validateReplicaObjectAbsence(t.Context(), db, nil); !errors.Is(err, syscall.EIO) {
		t.Fatalf("global replica integrity accepted an object record: %v", err)
	}
	if err := validateReplicaObjectAbsence(t.Context(), db, &otherID); !errors.Is(err, syscall.EIO) {
		t.Fatalf("scoped replica integrity accepted an object record: %v", err)
	}
}
