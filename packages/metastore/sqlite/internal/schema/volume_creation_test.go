package schema

import (
	"database/sql"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
)

func TestVolumeCreationSharesRootCreationInstant(t *testing.T) {
	db := testDatabase(t, 0)
	before := time.Now()
	id, root := testVolume(t, db, "workspace")
	after := time.Now()
	var seconds, nanoseconds int64
	var sameTimes bool
	if err := db.QueryRow(`SELECT v.created_sec, v.created_nsec,
		v.created_sec=n.birth_sec AND v.created_nsec=n.birth_nsec AND
		v.created_sec=n.change_sec AND v.created_nsec=n.change_nsec AND
		v.created_sec=n.atime_sec AND v.created_nsec=n.atime_nsec AND
		v.created_sec=n.mtime_sec AND v.created_nsec=n.mtime_nsec
		FROM volumes v JOIN nodes n ON n.id=v.root WHERE v.id=?`, id).
		Scan(&seconds, &nanoseconds, &sameTimes); err != nil {
		t.Fatal(err)
	}
	created := time.Unix(seconds, nanoseconds)
	if !sameTimes || created.Before(before) || created.After(after) {
		t.Fatalf("volume/root creation = %v, common times match = %v, interval = [%v, %v]",
			created, sameTimes, before, after)
	}
	execute(t, db, `UPDATE nodes SET birth_sec=-1, birth_nsec=2 WHERE id=?`, root)
	var file string
	if err := db.QueryRow(`SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&file); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	reopened.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	gotID, gotRoot := testVolume(t, reopened, "workspace")
	var gotSeconds, gotNanoseconds, birthSeconds, birthNanoseconds int64
	if err := reopened.QueryRow(`SELECT v.created_sec,v.created_nsec,n.birth_sec,n.birth_nsec
		FROM volumes v JOIN nodes n ON n.id=v.root WHERE v.id=?`, id).
		Scan(&gotSeconds, &gotNanoseconds, &birthSeconds, &birthNanoseconds); err != nil {
		t.Fatal(err)
	}
	if gotID != id || gotRoot != root || gotSeconds != seconds || gotNanoseconds != nanoseconds ||
		birthSeconds != -1 || birthNanoseconds != 2 {
		t.Fatalf("reopen changed volume identity/creation or mutable root birth: %d %d %d %d %d %d",
			gotID, gotRoot, gotSeconds, gotNanoseconds, birthSeconds, birthNanoseconds)
	}
}

func versionTenVolume(t *testing.T, db *sql.DB) (int64, int64) {
	t.Helper()
	tx := testTransaction(t, db)
	execute(t, tx, `INSERT INTO volumes(id,name,root,used) VALUES(1,'historical',0,0)`)
	root, err := dbstate.AllocateNodeID(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, tx, `INSERT INTO nodes(id,volume,kind,size,allocation_size,
		atime_sec,atime_nsec,mtime_sec,mtime_nsec,birth_sec,birth_nsec,directory_revision)
		VALUES(?,1,2,0,0,1,2,1,2,3,4,X'0000000000000001')`, root)
	execute(t, tx, `UPDATE volumes SET root=? WHERE id=1`, root)
	if err := changes.CreateLog(t.Context(), tx, 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return 1, root
}

func TestVolumeCreationMigrationPreservesUnknownHistory(t *testing.T) {
	db := testDatabase(t, firstVirtualAllocationSchemaVersion)
	id, root := versionTenVolume(t, db)
	gotID, gotRoot := testVolume(t, db, "historical")
	if gotID != id || gotRoot != root {
		t.Fatalf("migration changed historical identity: %d %d", gotID, gotRoot)
	}
	var seconds, nanoseconds any
	if err := db.QueryRow(`SELECT created_sec,created_nsec FROM volumes WHERE id=?`, id).
		Scan(&seconds, &nanoseconds); err != nil {
		t.Fatal(err)
	}
	if seconds != nil || nanoseconds != nil {
		t.Fatalf("migration invented historical creation: %v %v", seconds, nanoseconds)
	}
	before := time.Now()
	newID, _ := testVolume(t, db, "new")
	after := time.Now()
	var newSeconds, newNanoseconds int64
	if err := db.QueryRow(`SELECT created_sec,created_nsec FROM volumes WHERE id=?`, newID).
		Scan(&newSeconds, &newNanoseconds); err != nil {
		t.Fatal(err)
	}
	created := time.Unix(newSeconds, newNanoseconds)
	if created.Before(before) || created.After(after) {
		t.Fatalf("new volume creation %v outside [%v, %v]", created, before, after)
	}
	if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); err != nil {
		t.Fatalf("historical unknown creation rejected: %v", err)
	}
}

func TestVolumeCreationRejectsCorruptPairs(t *testing.T) {
	for _, test := range []struct {
		name           string
		seconds, nanos any
	}{
		{"missing seconds", nil, int64(1)},
		{"missing nanoseconds", int64(1), nil},
		{"text seconds", "invalid", int64(1)},
		{"text nanoseconds", int64(1), "invalid"},
		{"real seconds", 1.5, int64(1)},
		{"real nanoseconds", int64(1), 1.5},
		{"blob seconds", []byte{1}, int64(1)},
		{"blob nanoseconds", int64(1), []byte{1}},
		{"negative nanoseconds", int64(1), int64(-1)},
		{"overflow nanoseconds", int64(1), int64(1_000_000_000)},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, _ := testVolume(t, db, "workspace")
			if _, err := db.Exec(`UPDATE volumes SET created_sec=?,created_nsec=? WHERE id=?`, test.seconds, test.nanos, id); err == nil {
				t.Fatal("schema constraints accepted invalid creation")
			}
			execute(t, db, `PRAGMA ignore_check_constraints=ON`)
			execute(t, db, `UPDATE volumes SET created_sec=?,created_nsec=? WHERE id=?`, test.seconds, test.nanos, id)
			execute(t, db, `PRAGMA ignore_check_constraints=OFF`)
			if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("live integrity accepted corrupt pair: %v", err)
			}
			before, err := dbstate.Read(t.Context(), db)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := Prepare(t.Context(), db, "workspace", "", changes.DefaultWindow(), 1000, 1<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("startup accepted corrupt pair: %v", err)
			}
			after, err := dbstate.Read(t.Context(), db)
			if err != nil || before != after {
				t.Fatalf("startup refusal changed state: %+v -> %+v, %v", before, after, err)
			}
			var seconds, nanos any
			if err := db.QueryRow(`SELECT created_sec,created_nsec FROM volumes WHERE id=?`, id).
				Scan(&seconds, &nanos); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(seconds, test.seconds) || !reflect.DeepEqual(nanos, test.nanos) {
				t.Fatalf("refusal changed corrupt facts: %v %v", seconds, nanos)
			}
		})
	}
}

func TestVolumeCreationMigrationRefusalRollsBack(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "post-migration refusal"
		if corrupt {
			name = "version-ten preflight corruption"
		}
		t.Run(name, func(t *testing.T) {
			db := testDatabase(t, firstVirtualAllocationSchemaVersion)
			versionTenVolume(t, db)
			if corrupt {
				execute(t, db, `UPDATE volumes SET used='invalid'`)
			}
			before, err := dbstate.Read(t.Context(), db)
			if err != nil {
				t.Fatal(err)
			}
			_, _, _, err = PrepareConfigured(t.Context(), db, "missing", "", changes.DefaultWindow(), 1000, 1<<20,
				&DurableOpen{Mode: RequireExistingVolume})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("migration refusal = %v", err)
			}
			after, err := dbstate.Read(t.Context(), db)
			if err != nil || before != after {
				t.Fatalf("migration refusal changed state: %+v -> %+v, %v", before, after, err)
			}
			var version, newColumns, volumes int
			if err := db.QueryRow(`SELECT (SELECT version FROM schema_version),
				(SELECT count(*) FROM pragma_table_info('volumes') WHERE name IN ('created_sec','created_nsec')),
				(SELECT count(*) FROM volumes)`).Scan(&version, &newColumns, &volumes); err != nil {
				t.Fatal(err)
			}
			if version != firstVirtualAllocationSchemaVersion || newColumns != 0 || volumes != 1 {
				t.Fatalf("refusal advanced schema/volume data: version=%d columns=%d volumes=%d", version, newColumns, volumes)
			}
		})
	}
}
