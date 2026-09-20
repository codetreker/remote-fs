package schema

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMetadataAccountingReleasesTrimmedHistory(t *testing.T) {
	db := testDatabase(t, 0)
	volume, root := testVolume(t, db, "workspace")
	nodeID, key := testFile(t, db, volume, root, "file", 3, false)
	metadata := map[string]storage.OpaquePayload{
		"client.attribute": {Version: binary.BigEndian.AppendUint64(nil, 1), Data: []byte("value")},
	}
	encoded, err := storage.EncodeMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, db, `UPDATE nodes SET metadata=? WHERE id=?`, encoded, nodeID)
	for range 2 {
		tx := testTransaction(t, db)
		node := metastore.Node{
			ID: nodeID, Kind: storage.NodeRegular, Size: 3, Content: metastore.Key(key),
			AccessTime: time.Unix(1, 0), ModTime: time.Unix(2, 0), Metadata: metadata,
		}
		if err := changes.Record(t.Context(), tx, volume, metastore.Change{
			Kind: metastore.Modified, Parent: root, Name: []byte("file"), Node: &node,
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	assertMetadataUsed(t, db, volume, int64(14+3*len(encoded)))

	tx := testTransaction(t, db)
	if err := changes.Trim(t.Context(), tx, volume, changes.Window{Cap: 1, Floor: 1, Age: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertMetadataUsed(t, db, volume, int64(14+2*len(encoded)))
}

func TestMetadataIntegrityRejectsCorruptAccountingAndPayload(t *testing.T) {
	badVersion, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{
		"client.attribute": {Version: []byte{1}, Data: nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, damage string
	}{
		{"undercharge", `UPDATE volumes SET metadata_used=metadata_used+1`},
		{"wrong counter type", `PRAGMA ignore_check_constraints=ON; UPDATE volumes SET metadata_used='bad'`},
		{"missing trigger", `DROP TRIGGER nodes_metadata_update`},
		{"changed trigger", `DROP TRIGGER nodes_metadata_update; CREATE TRIGGER nodes_metadata_update AFTER UPDATE OF metadata ON nodes BEGIN SELECT 1; END`},
		{"text metadata", `UPDATE nodes SET metadata='bad'`},
		{"unknown envelope", `UPDATE nodes SET metadata=X'52464d020000'`},
		{"trailing envelope bytes", `UPDATE nodes SET metadata=X'52464d01000000'`},
		{"invalid native version", fmt.Sprintf(`UPDATE nodes SET metadata=X'%x'`, badVersion)},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			volume, _ := testVolume(t, db, "workspace")
			execute(t, db, test.damage)
			if err := validateMetadataIntegrity(t.Context(), db, &volume, math.MaxInt64, false, schema.Version()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("corrupt metadata accepted: %v", err)
			}
		})
	}
}

func assertMetadataUsed(t *testing.T, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, volume, want int64) {
	t.Helper()
	var recorded, actual int64
	if err := db.QueryRowContext(t.Context(), `SELECT metadata_used,
		coalesce((SELECT sum(length(metadata)+length(link_target)+length(directory_revision)) FROM nodes WHERE volume=?),0)+
		coalesce((SELECT sum(coalesce(length(metadata),0)+coalesce(length(link_target),0)+coalesce(length(directory_revision),0)) FROM changes WHERE volume=?),0)
		FROM volumes WHERE id=?`, volume, volume, volume).Scan(&recorded, &actual); err != nil {
		t.Fatal(err)
	}
	if recorded != want || actual != want {
		t.Fatalf("metadata accounting recorded=%d actual=%d, want %d", recorded, actual, want)
	}
}

func TestMetadataValidationRejectsNonBlobBeforeLoadingPayload(t *testing.T) {
	db := testDatabase(t, 0)
	id, _ := testVolume(t, db, "workspace")
	const payloadBytes = 32 << 20
	execute(t, db, `UPDATE nodes SET metadata=CAST(zeroblob(?) AS TEXT) WHERE volume=?`, payloadBytes, id)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := validateMetadataPayloads(t.Context(), db, &id, false)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("non-BLOB metadata = %v", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= payloadBytes/4 {
		t.Fatalf("metadata validation allocated %d bytes before rejecting a %d-byte TEXT value", allocated, payloadBytes)
	}
}
