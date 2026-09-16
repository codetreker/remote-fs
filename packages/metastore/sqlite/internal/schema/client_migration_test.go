package schema

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
	"github.com/codetreker/remote-fs/packages/storage"
)

func historicalMetadataDatabase(t *testing.T, version int) *sql.DB {
	t.Helper()
	db := testDatabase(t, version)
	execute(t, db, `INSERT INTO volumes(id,name,root,used) VALUES(1,'legacy',1,3)`)
	execute(t, db, `INSERT INTO objects(key,volume,state,size,created_sec,created_nsec) VALUES('data',1,1,3,0,0)`)
	execute(t, db, `INSERT INTO nodes(id,volume,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content)
		VALUES(1,1,?,0,10,1,20,2,NULL),(2,1,?,3,30,1,40,2,'data')`,
		int64(fs.ModeDir|0755), int64(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky|0640))
	execute(t, db, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,X'66ff',2)`)
	execute(t, db, `INSERT INTO logs VALUES(1,'0123456789abcdef0123456789abcdef',7,0,0)`)
	execute(t, db, `INSERT INTO changes(position,previous_position,volume,kind,parent,name,node,mode,size,
		atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,recorded_sec,recorded_nsec)
		VALUES(7,0,1,0,1,X'66ff',2,?,3,50,3,60,4,'data',?,0)`, int64(fs.ModeSetgid|0600), time.Now().Unix())
	execute(t, db, `UPDATE database_state SET generation=5,node_high_water=2,change_high_water=7`)
	return db
}

func TestClientMigrationPreservesHistoricalAttributesAndLogLineage(t *testing.T) {
	for _, version := range []int{3, 4, 5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			db := historicalMetadataDatabase(t, version)
			_, _, state, err := PrepareConfigured(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 4, 138, nil)
			if err != nil {
				t.Fatal(err)
			}
			if state.Generation != 6 || state.NodeHighWater != 2 || state.ChangeHighWater != 7 {
				t.Fatalf("migration changed identity lineage: %+v", state)
			}
			var incarnation string
			var committed, trimmed, position, previous, used int64
			if err := db.QueryRow(`SELECT incarnation,committed_position,trimmed_through,
				(SELECT position FROM changes),(SELECT previous_position FROM changes),
				(SELECT metadata_used FROM volumes) FROM logs`).Scan(&incarnation, &committed, &trimmed, &position, &previous, &used); err != nil {
				t.Fatal(err)
			}
			if incarnation != "0123456789abcdef0123456789abcdef" || committed != 7 || trimmed != 0 || position != 7 || previous != 0 || used != 138 {
				t.Fatalf("history or payload charge changed: %q tail=%d trim=%d row=%d/%d used=%d", incarnation, committed, trimmed, position, previous, used)
			}
			for _, row := range []struct {
				table, kindColumn, where string
				mode                     uint32
				access, modified         int64
			}{
				{"nodes", "kind", "id=2", 07640, 30, 40},
				{"changes", "node_kind", "position=7", 02600, 50, 60},
			} {
				var kind, access, modified int64
				var birth, changed sql.NullInt64
				var metadata []byte
				if err := db.QueryRow(`SELECT `+row.kindColumn+`,atime_sec,mtime_sec,birth_sec,change_sec,metadata FROM `+row.table+` WHERE `+row.where).Scan(&kind, &access, &modified, &birth, &changed, &metadata); err != nil {
					t.Fatal(err)
				}
				values, err := storage.DecodeMetadata(metadata)
				if err != nil {
					t.Fatal(err)
				}
				permissions, found := values["posix.permissions.v1"]
				if kind != 1 || access != row.access || modified != row.modified || birth.Valid || changed.Valid || !found || len(values) != 1 || len(permissions.Data) != 4 || binary.LittleEndian.Uint32(permissions.Data) != row.mode || len(permissions.Version) != 8 || binary.BigEndian.Uint64(permissions.Version) != 1 {
					t.Fatalf("%s attributes were fabricated or taken from another state: kind=%d times=%d/%d known=%t/%t metadata=%+v", row.table, kind, access, modified, birth.Valid, changed.Valid, values)
				}
			}
			var rawName []byte
			if err := db.QueryRow(`SELECT name FROM changes`).Scan(&rawName); err != nil {
				t.Fatal(err)
			}
			if string(rawName) != "f\xff" {
				t.Fatalf("raw historical name changed: %x", rawName)
			}
			if _, _, _, err := PrepareConfigured(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 4, 138, nil); err != nil {
				t.Fatal(err)
			}
			var after string
			if err := db.QueryRow(`SELECT incarnation FROM logs`).Scan(&after); err != nil || after != incarnation {
				t.Fatalf("reopen reran data migration: %q, %v", after, err)
			}
		})
	}
}

func metadataDatabaseImage(t *testing.T, db *sql.DB) string {
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	contents := make(map[string][][]any)
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values, targets := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Fatal(err)
			}
			contents[table] = append(contents[table], values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(contents)
	if err != nil {
		t.Fatal(err)
	}
	return ddl + string(encoded)
}

func TestClientMigrationRejectsSourceDamageAndRollsBackEveryField(t *testing.T) {
	for _, test := range []struct {
		name, damage string
		limit        int64
		want         error
	}{
		{"node storage class", `UPDATE nodes SET mode=zeroblob(1048576) WHERE id=2`, 138, syscall.EIO},
		{"unmapped flags", fmt.Sprintf(`UPDATE nodes SET mode=%d WHERE id=2`, fs.ModeAppend|0644), 138, syscall.EIO},
		{"historical storage class", `UPDATE changes SET mode='regular'`, 138, syscall.EIO},
		{"historical invalid type", fmt.Sprintf(`UPDATE changes SET mode=%d`, fs.ModeSymlink|0644), 138, syscall.EIO},
		{"historical unmapped flags", fmt.Sprintf(`UPDATE changes SET mode=%d`, fs.ModeAppend|0644), 138, syscall.EIO},
		{"postflight metadata limit", "", 137, syscall.EFBIG},
		{"mid migration write", `CREATE TRIGGER reject_migration BEFORE UPDATE ON nodes BEGIN SELECT RAISE(ABORT,'migration write failed'); END`, 138, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := historicalMetadataDatabase(t, 5)
			if test.damage != "" {
				execute(t, db, test.damage)
			}
			before := metadataDatabaseImage(t, db)
			_, _, err := Prepare(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 4, test.limit)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("damaged source migrated: %v", err)
			}
			if test.want == nil && !strings.Contains(err.Error(), "migration write failed") {
				t.Fatalf("fault did not reach migration: %v", err)
			}
			if after := metadataDatabaseImage(t, db); after != before {
				t.Fatal("refusal changed source schema, rows, history or counters")
			}
		})
	}
}
