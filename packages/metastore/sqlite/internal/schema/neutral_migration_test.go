package schema

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"io/fs"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/storage"
)

func historicalMetadataDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db := testDatabase(t, 5)
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'legacy',1,3)`)
	execute(t, db, `INSERT INTO objects(key,volume,state,size,created_sec,created_nsec) VALUES('data',1,1,3,0,0)`)
	execute(t, db, `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content)
		VALUES(1,1,?,0,10,1,20,2,NULL),(2,1,?,3,30,1,40,2,'data')`,
		int64(fs.ModeDir|0o755), int64(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky|0o640))
	execute(t, db, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,X'66ff',2)`)
	execute(t, db, `INSERT INTO logs VALUES(1,'0123456789abcdef0123456789abcdef',7,0,0)`)
	execute(t, db, `INSERT INTO changes(position,previous_position,volume,kind,parent,name,node,mode,size,
		atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,recorded_sec,recorded_nsec)
		VALUES(7,0,1,0,1,X'66ff',2,?,3,50,3,60,4,'data',?,0)`, int64(fs.ModeSetgid|0o600), time.Now().Unix())
	execute(t, db, `UPDATE database_state SET generation=5,node_high_water=2,change_high_water=7`)
	return db
}

func TestNeutralMetadataMigrationPreservesCurrentAttributesAndLineage(t *testing.T) {
	db := historicalMetadataDatabase(t)
	_, _, state, err := PrepareConfigured(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 6 || state.NodeHighWater != 2 || state.ChangeHighWater != 7 {
		t.Fatalf("migration changed identity lineage: %+v", state)
	}
	var used int64
	if err := db.QueryRow(`SELECT metadata_used FROM volumes WHERE id=1`).Scan(&used); err != nil || used != 100 {
		t.Fatalf("migrated metadata charge = %d, %v; want 100", used, err)
	}
	var kind, access, modified int64
	var birth, changed sql.NullInt64
	var metadata []byte
	if err := db.QueryRow(`SELECT kind,atime_sec,mtime_sec,birth_sec,change_sec,metadata FROM nodes WHERE id=2`).
		Scan(&kind, &access, &modified, &birth, &changed, &metadata); err != nil {
		t.Fatal(err)
	}
	values, err := storage.DecodeMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	permissions, found := values["posix.permissions.v1"]
	if kind != int64(storage.NodeRegular) || access != 30 || modified != 40 || birth.Valid || changed.Valid ||
		!found || len(values) != 1 || len(permissions.Data) != 4 || binary.LittleEndian.Uint32(permissions.Data) != 0o7640 ||
		len(permissions.Version) != 8 || binary.BigEndian.Uint64(permissions.Version) != 2 {
		t.Fatalf("current attributes changed: kind=%d times=%d/%d known=%t/%t metadata=%+v", kind, access, modified, birth.Valid, changed.Valid, values)
	}
	var retained int
	if err := db.QueryRow(`SELECT count(*) FROM changes`).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("pre-revision history remains replayable: count=%d error=%v", retained, err)
	}
	if _, _, _, err := PrepareConfigured(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 1<<20, nil); err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
}

func TestNeutralMetadataMigrationFailureRollsBackSchemaAndRows(t *testing.T) {
	for _, test := range []struct {
		name, damage string
		limit        int64
		want         error
	}{
		{"postflight metadata limit", "", 99, syscall.EFBIG},
		{"mid-migration write", `CREATE TRIGGER reject_migration BEFORE UPDATE ON nodes BEGIN SELECT RAISE(ABORT,'migration write failed'); END`, 100, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := historicalMetadataDatabase(t)
			if test.damage != "" {
				execute(t, db, test.damage)
			}
			_, _, err := PrepareWithMetadataLimit(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 1<<20, test.limit)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("migration failure = %v, want %v", err, test.want)
			}
			if test.want == nil && !strings.Contains(err.Error(), "migration write failed") {
				t.Fatalf("migration did not reach injected write failure: %v", err)
			}
			var version, metadataColumns int
			var mode int64
			if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('nodes') WHERE name='metadata'`).Scan(&metadataColumns); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT mode FROM nodes WHERE id=2`).Scan(&mode); err != nil {
				t.Fatal(err)
			}
			if version != 5 || metadataColumns != 0 || mode != int64(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky|0o640) {
				t.Fatalf("failed migration changed source: version=%d metadata-columns=%d mode=%d", version, metadataColumns, mode)
			}
		})
	}
}
