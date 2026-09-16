package schema

import (
	"bytes"
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestCommonMetadataRejectsCorruptTimestampsAndPayloads(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE nodes SET creation_nsec=-1`,
		`UPDATE nodes SET change_nsec=1000000000`,
		`UPDATE nodes SET metadata_revision=0`,
		`UPDATE nodes SET creation_sec='invalid'`,
		`UPDATE nodes SET link_target='text'`,
		`UPDATE nodes SET link_target=X'61'`,
		`UPDATE nodes SET metadata=X'00'`,
		`UPDATE nodes SET metadata=X'52464dff0000'`,
		`UPDATE nodes SET creation_sec=NULL`,
	} {
		t.Run(mutation, func(t *testing.T) {
			db := testDatabase(t, 0)
			id, _ := testVolume(t, db, "common")
			if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); err != nil {
				t.Fatal(err)
			}
			execute(t, db, mutation)
			if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("validation=%v", err)
			}
		})
	}
}

func TestGenericSymlinkPayloadConsumesIntegrityBudget(t *testing.T) {
	db := testDatabase(t, 0)
	id, root := testVolume(t, db, "links")
	node, key := testFile(t, db, id, root, "link", 0, false)
	execute(t, db, `UPDATE nodes SET kind=3,size=4096,content=NULL,link_target=? WHERE id=?`, bytes.Repeat([]byte("x"), 4096), node)
	execute(t, db, `DELETE FROM objects WHERE key=?`, key)
	execute(t, db, `UPDATE volumes SET used=4096 WHERE id=?`, id)
	if err := validateSharedNodeValues(t.Context(), db, &id); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []*int64{nil, &id} {
		if err := validateIntegrityBytes(t.Context(), db, scope, 4095, schema.Version()); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("payload budget=%v", err)
		}
	}
}

func TestGenericLinkTargetsAreByteDataWithoutPlatformSyntax(t *testing.T) {
	for _, target := range [][]byte{nil, {0xff}, []byte("bad\x00target"), []byte(`C:\host`), []byte("//host/share")} {
		t.Run(fmt.Sprintf("%x", target), func(t *testing.T) {
			db := testDatabase(t, 0)
			id, root := testVolume(t, db, "links")
			node, key := testFile(t, db, id, root, "link", 0, false)
			execute(t, db, `UPDATE nodes SET kind=3,size=?,content=NULL,link_target=CAST(? AS BLOB) WHERE id=?`, len(target), append([]byte{}, target...), node)
			execute(t, db, `DELETE FROM objects WHERE key=?`, key)
			execute(t, db, `UPDATE volumes SET used=? WHERE id=?`, len(target), id)
			if err := ValidateVolumeIntegrity(t.Context(), db, id, 1000, 1<<20); err != nil {
				t.Fatalf("generic target %x was interpreted as platform syntax: %v", target, err)
			}
			var persisted []byte
			if err := db.QueryRow(`SELECT link_target FROM nodes WHERE id=?`, node).Scan(&persisted); err != nil || !bytes.Equal(persisted, target) {
				t.Fatalf("generic target changed: %x, %v", persisted, err)
			}
		})
	}
}
