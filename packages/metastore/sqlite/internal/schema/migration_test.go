package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"testing"

	"database/sql"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	"github.com/codetreker/remote-fs/packages/storage"
)

func populatedMigrationSource(t *testing.T, version int) *sql.DB {
	t.Helper()
	db := testDatabase(t, version)
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'A',1,3),(2,'B',3,5)`)
	execute(t, db, `INSERT INTO objects(key,volume,state,size,created_sec,created_nsec)
		VALUES('one',1,1,3,0,0),('two',2,1,5,0,0)`)
	execute(t, db, `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content)
		VALUES(1,1,?,0,10,1,20,2,NULL),(2,1,?,3,30,3,40,4,'one'),
		(3,2,?,0,50,5,60,6,NULL),(4,2,384,5,70,7,80,8,'two')`,
		int64(fs.ModeDir|0755), int64(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky|0640), int64(fs.ModeDir|0700))
	if version == 1 {
		execute(t, db, `INSERT INTO entries(parent,name,node) VALUES(1,X'61',2),(3,X'62',4)`)
	} else {
		execute(t, db, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,X'61',2),(2,3,X'62',4)`)
		execute(t, db, `INSERT INTO logs(volume,incarnation,committed_position,trimmed_through,trimmed_by_age)
			VALUES(1,'11111111111111111111111111111111',7,0,0),(2,'22222222222222222222222222222222',9,0,0)`)
		previousColumn, previousValue := "", ""
		if version >= 3 {
			previousColumn, previousValue = "previous_position,", "0,"
		}
		execute(t, db, `INSERT INTO changes(position,`+previousColumn+`volume,kind,parent,name,node,mode,size,
			atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,recorded_sec,recorded_nsec)
			SELECT 7,`+previousValue+`1,0,1,X'61',id,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,90,9 FROM nodes WHERE id=2`)
		execute(t, db, `INSERT INTO changes(position,`+previousColumn+`volume,kind,parent,name,node,mode,size,
			atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,recorded_sec,recorded_nsec)
			SELECT 9,`+previousValue+`2,0,3,X'62',id,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,90,9 FROM nodes WHERE id=4`)
		execute(t, db, `UPDATE sqlite_sequence SET seq=12 WHERE name='changes'`)
	}
	execute(t, db, `UPDATE sqlite_sequence SET seq=9 WHERE name='nodes'`)
	if version >= 3 {
		execute(t, db, `UPDATE database_state SET database_id='33333333333333333333333333333333',
			generation=11,node_high_water=9,change_high_water=12`)
	}
	return db
}

func TestSharedFactsMigrationPreservesEachHistoricalSourceAndItsIdentities(t *testing.T) {
	fresh := testDatabase(t, 0)
	testVolume(t, fresh, "A")
	freshSchema, err := sqliteschema.Dump(t.Context(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			db := populatedMigrationSource(t, version)
			_, _, state, err := PrepareConfigured(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			wantChange := int64(12)
			if version == 1 {
				wantChange = 0
			}
			if state.NodeHighWater != 11 || state.ChangeHighWater != wantChange {
				t.Fatalf("migration identity witnesses = %+v", state)
			}
			if version >= 3 && (state.DatabaseID != "33333333333333333333333333333333" || state.Generation != 12) {
				t.Fatalf("existing durable lineage changed: %+v", state)
			}
			var kind, metadataRevision, directoryRevision, size, access, modified int64
			var creation, changed sql.NullInt64
			var metadata []byte
			var content string
			if err := db.QueryRow(`SELECT kind,metadata_revision,directory_revision,size,atime_sec,mtime_sec,
				creation_sec,change_sec,metadata,content FROM nodes WHERE id=2`).Scan(
				&kind, &metadataRevision, &directoryRevision, &size, &access, &modified, &creation, &changed, &metadata, &content); err != nil {
				t.Fatal(err)
			}
			if kind != 1 || metadataRevision != 1 || directoryRevision != 0 || size != 3 || access != 30 || modified != 40 || creation.Valid || changed.Valid || content != "one" {
				t.Fatalf("historical node facts changed: kind=%d revision=%d/%d size=%d times=%d/%d creation=%+v change=%+v content=%q", kind, metadataRevision, directoryRevision, size, access, modified, creation, changed, content)
			}
			decoded, err := storage.DecodeMetadata(metadata)
			if err != nil || len(decoded) != 1 || decoded[0].Key != "posix" || decoded[0].Version != 1 || len(decoded[0].Data) != 16 {
				t.Fatalf("historical permissions envelope: %+v, %v", decoded, err)
			}
			data := decoded[0].Data
			if binary.LittleEndian.Uint32(data) != 1 || binary.LittleEndian.Uint32(data[4:]) != 07640 || !bytes.Equal(data[8:], make([]byte, 8)) {
				t.Fatalf("historical mode or invented ownership: %x", data)
			}
			var entry, counter, records, totalUsed int64
			if err := db.QueryRow(`SELECT (SELECT id FROM entries WHERE node=2),
				(SELECT seq FROM sqlite_sequence WHERE name='nodes'),(SELECT count(*) FROM changes),
				(SELECT sum(used) FROM volumes)`).Scan(&entry, &counter, &records, &totalUsed); err != nil {
				t.Fatal(err)
			}
			if entry != 10 || counter != 11 || records != 0 || totalUsed != 8 {
				t.Fatalf("entry=%d sequence=%d history=%d quota=%d", entry, counter, records, totalUsed)
			}
			var incarnation string
			if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume=1`).Scan(&incarnation); err != nil {
				t.Fatal(err)
			}
			if len(incarnation) != 32 || incarnation == "11111111111111111111111111111111" {
				t.Fatalf("old history was not explicitly retired: %q", incarnation)
			}
			actualSchema, err := sqliteschema.Dump(t.Context(), db)
			if err != nil || sqliteschema.Structure(actualSchema) != sqliteschema.Structure(freshSchema) {
				t.Fatalf("fresh and upgraded schemas differ: %v", err)
			}
			_, _, again, err := PrepareConfigured(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20, nil)
			if err != nil || again.NodeHighWater != state.NodeHighWater || again.ChangeHighWater != state.ChangeHighWater || again.Generation != state.Generation+1 {
				t.Fatalf("v6 reopen changed identity state: %+v, %v", again, err)
			}
			var reopened string
			if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume=1`).Scan(&reopened); err != nil || reopened != incarnation {
				t.Fatalf("v6 reopen rotated history: %q, %v", reopened, err)
			}
			tx := testTransaction(t, db)
			position, err := dbstate.AllocateChangePosition(t.Context(), tx)
			if err != nil || position <= wantChange {
				t.Fatalf("new history reused its previous position: %d, %v", position, err)
			}
		})
	}
}

func migrationImage(t *testing.T, db *sql.DB) string {
	t.Helper()
	ddl, err := sqliteschema.Dump(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}
	contents := make(map[string][][]any)
	for _, table := range tables {
		name := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		rows, err := db.Query(`SELECT * FROM ` + name)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, targets := make([]any, len(columns)), make([]any, len(columns))
			for index := range values {
				targets[index] = &values[index]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Fatal(err)
			}
			for index, value := range values {
				if data, ok := value.([]byte); ok {
					values[index] = bytes.Clone(data)
				}
			}
			contents[table] = append(contents[table], values)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(contents)
	if err != nil {
		t.Fatal(err)
	}
	return ddl + string(encoded)
}

func TestSharedFactsMigrationRefusesBadSourcesWithoutLosingHistory(t *testing.T) {
	for _, version := range []int{1, 2, 3, 4, 5} {
		for _, damage := range []struct{ name, sql string }{
			{"quota", `UPDATE volumes SET used=99 WHERE id=2`},
			{"mode storage class", `UPDATE nodes SET mode='invalid' WHERE id=2`},
			{"unsupported node kind", fmt.Sprintf(`UPDATE nodes SET mode=%d WHERE id=2`, fs.ModeSymlink)},
			{"unmapped mode flags", fmt.Sprintf(`UPDATE nodes SET mode=mode|%d WHERE id=2`, fs.ModeAppend)},
		} {
			t.Run(fmt.Sprintf("%d/%s", version, damage.name), func(t *testing.T) {
				db := populatedMigrationSource(t, version)
				execute(t, db, damage.sql)
				before := migrationImage(t, db)
				_, _, err := Prepare(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20)
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("damaged source %d accepted: %v", version, err)
				}
				if after := migrationImage(t, db); after != before {
					t.Fatal("refused source changed schema, history, identities or data")
				}
			})
		}
	}
}

func TestSharedFactsMigrationRollbackRestoresSourceAfterDDLStarted(t *testing.T) {
	db := populatedMigrationSource(t, 5)
	execute(t, db, `CREATE TRIGGER refuse_metadata BEFORE UPDATE ON nodes
		WHEN OLD.id=2 BEGIN SELECT RAISE(ABORT, 'migration metadata refused'); END`)
	before := migrationImage(t, db)
	_, _, err := Prepare(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20)
	if err == nil || !strings.Contains(err.Error(), "migration metadata refused") {
		t.Fatalf("migration failure lost its cause: %v", err)
	}
	if after := migrationImage(t, db); after != before {
		t.Fatal("failed migration retained partial DDL or lost source rows")
	}
}

func TestSharedFactsMigrationRejectsExpandedVersionFiveAndIdentityExhaustion(t *testing.T) {
	for _, test := range []struct {
		name, mutation string
		want           error
	}{
		{"expanded layout", `ALTER TABLE nodes ADD COLUMN unknown_platform_field INTEGER NOT NULL DEFAULT 0`, syscall.EIO},
		{"missing column", `ALTER TABLE nodes RENAME COLUMN mode TO historical_mode`, syscall.EIO},
		{"identity exhaustion", `UPDATE database_state SET node_high_water=9223372036854775807;
			UPDATE sqlite_sequence SET seq=9223372036854775807 WHERE name='nodes'`, syscall.ENOSPC},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := populatedMigrationSource(t, 5)
			execute(t, db, test.mutation)
			before := migrationImage(t, db)
			_, _, err := Prepare(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20)
			if !errors.Is(err, test.want) {
				t.Fatalf("unsupported source: %v, want %v", err, test.want)
			}
			if migrationImage(t, db) != before {
				t.Fatal("unsupported source was repaired or partly migrated")
			}
		})
	}
}
