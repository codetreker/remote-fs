package schema

import (
	"bytes"
	"database/sql"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
)

func versionSevenDirectoryDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db := testDatabase(t, 7)
	execute(t, db, `INSERT INTO volumes(id,name,root,used,metadata_used) VALUES(1,'workspace',1,0,0)`)
	execute(t, db, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content)
		VALUES(1,1,2,0,0,0,0,0,NULL),(2,1,2,0,0,0,0,0,NULL)`)
	execute(t, db, `INSERT INTO entries(volume,parent,name,node) VALUES(1,1,X'646972',2)`)
	execute(t, db, `INSERT INTO logs(volume,incarnation,committed_position,trimmed_through,trimmed_by_age)
		VALUES(1,'0123456789abcdef0123456789abcdef',1,0,0)`)
	execute(t, db, `INSERT INTO changes(position,previous_position,volume,kind,parent,name,node,node_kind,size,
		atime_sec,atime_nsec,mtime_sec,mtime_nsec,content,recorded_sec,recorded_nsec,metadata,link_target)
		VALUES(1,0,1,0,1,X'646972',2,2,0,0,0,0,0,NULL,0,0,X'52464d010000',X'')`)
	execute(t, db, `UPDATE database_state SET node_high_water=2,change_high_water=1`)
	return db
}

func TestDirectoryRevisionMigrationInitializesLiveDirectoriesAndForcesReplicaReseed(t *testing.T) {
	db := versionSevenDirectoryDatabase(t)
	var oldIncarnation string
	if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume=1`).Scan(&oldIncarnation); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := PrepareConfigured(t.Context(), db, "workspace", "", changes.DefaultWindow(), 1000, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	rows, err := db.Query(`SELECT directory_revision FROM nodes ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var got []byte
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("migrated directory revision=%x", got)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 2 {
		t.Fatalf("migrated directories=%d error=%v", count, err)
	}
	var retainedChanges, tail, trimmed int64
	var newIncarnation string
	if err := db.QueryRow(`SELECT count(*) FROM changes`).Scan(&retainedChanges); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT incarnation,committed_position,trimmed_through FROM logs WHERE volume=1`).Scan(&newIncarnation, &tail, &trimmed); err != nil {
		t.Fatal(err)
	}
	if retainedChanges != 0 || tail != 0 || trimmed != 0 || newIncarnation == oldIncarnation {
		t.Fatalf("migration did not establish a rebuild boundary: changes=%d log=%q/%d/%d old=%q", retainedChanges, newIncarnation, tail, trimmed, oldIncarnation)
	}
	var used int64
	if err := db.QueryRow(`SELECT metadata_used FROM volumes WHERE id=1`).Scan(&used); err != nil || used != 28 {
		t.Fatalf("migrated metadata accounting=%d error=%v", used, err)
	}
	if _, _, _, err := PrepareConfigured(t.Context(), db, "workspace", "", changes.DefaultWindow(), 1000, 1<<20, nil); err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
}

func TestDirectoryRevisionIntegrityRejectsInvalidCurrentState(t *testing.T) {
	for _, damage := range []string{
		`UPDATE nodes SET directory_revision=X'' WHERE kind=2`,
		`UPDATE nodes SET directory_revision=X'0000000000000000' WHERE kind=2`,
		`UPDATE nodes SET directory_revision=X'8000000000000000' WHERE kind=2`,
		`UPDATE nodes SET directory_revision='revision' WHERE kind=2`,
		`UPDATE nodes SET directory_revision=X'01' WHERE kind=1`,
	} {
		t.Run(damage, func(t *testing.T) {
			db := testDatabase(t, 0)
			volume, root := testVolume(t, db, "workspace")
			if bytes.Contains([]byte(damage), []byte("kind=1")) {
				testFile(t, db, volume, root, "file", 1, false)
			}
			execute(t, db, damage)
			if err := validateIntegrityWithMetadataPolicy(t.Context(), db, &volume, 1000, 1<<20, schema.Version(), 1<<20, false); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid directory revision accepted: %v", err)
			}
		})
	}
}

func TestDirectoryRevisionMigrationRefusesCorruptHistoryBeforeRebuild(t *testing.T) {
	db := versionSevenDirectoryDatabase(t)
	var incarnation string
	if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume=1`).Scan(&incarnation); err != nil {
		t.Fatal(err)
	}
	execute(t, db, `DELETE FROM changes WHERE position=1`)
	_, _, _, err := PrepareConfigured(t.Context(), db, "workspace", "", changes.DefaultWindow(), 1000, 1<<20, nil)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt pre-v8 history migration=%v", err)
	}
	var version, revisionColumns, nodes int
	var afterIncarnation string
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('nodes') WHERE name='directory_revision'`).Scan(&revisionColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM nodes`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume=1`).Scan(&afterIncarnation); err != nil {
		t.Fatal(err)
	}
	if version != 7 || revisionColumns != 0 || nodes != 2 || afterIncarnation != incarnation {
		t.Fatalf("refused migration changed durable state: version=%d revision-columns=%d nodes=%d incarnation=%q", version, revisionColumns, nodes, afterIncarnation)
	}
}

func TestDirectoryRevisionMigrationRefusesMalformedIncarnationBeforeRebuild(t *testing.T) {
	db := versionSevenDirectoryDatabase(t)
	execute(t, db, `UPDATE logs SET incarnation='nonempty-but-malformed' WHERE volume=1`)
	_, _, _, err := PrepareConfigured(t.Context(), db, "workspace", "", changes.DefaultWindow(), 1000, 1<<20, nil)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("malformed incumbent incarnation migration=%v", err)
	}
	var version, revisionColumns, retainedChanges, nodes int
	var incarnation string
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('nodes') WHERE name='directory_revision'`).Scan(&revisionColumns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM changes`).Scan(&retainedChanges); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM nodes`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT incarnation FROM logs WHERE volume=1`).Scan(&incarnation); err != nil {
		t.Fatal(err)
	}
	if version != 7 || revisionColumns != 0 || retainedChanges != 1 || nodes != 2 || incarnation != "nonempty-but-malformed" {
		t.Fatalf("refused incarnation migration changed state: version=%d revision-columns=%d changes=%d nodes=%d incarnation=%q",
			version, revisionColumns, retainedChanges, nodes, incarnation)
	}
}
