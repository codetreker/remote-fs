package schema

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestMetadataBudgetCountsNodeAndRetainedCopiesIndependentlyOfNames(t *testing.T) {
	db := testDatabase(t, 0)
	volume, root := testVolume(t, db, "metadata")
	id, key := testFile(t, db, volume, root, "file", 3, false)
	metadata := map[string]storage.OpaquePayload{"example.flags.v1": {Version: binary.BigEndian.AppendUint64(nil, 1), Data: []byte{0, 255, 4}}}
	encoded, err := storage.EncodeMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, db, `UPDATE nodes SET metadata=? WHERE id=?`, encoded, id)
	appendChange := func() {
		tx := testTransaction(t, db)
		node := metastore.Node{ID: id, Kind: storage.NodeRegular, Size: 3, Content: metastore.Key(key), AccessTime: time.Unix(0, 0), ModTime: time.Unix(0, 0), Metadata: metadata}
		if err := changes.Record(t.Context(), tx, volume, metastore.Change{Kind: metastore.Modified, Parent: root, Name: []byte("file"), Node: &node}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	appendChange()
	want := int64(6 + 2*len(encoded))
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 8, want); err != nil {
		t.Fatalf("exact independent bounds: %v", err)
	}
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 8, want-1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("metadata escaped exact bound: %v", err)
	}
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 7, want); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("name escaped independent bound: %v", err)
	}
	appendChange()
	tx := testTransaction(t, db)
	if err := changes.Trim(t.Context(), tx, volume, changes.Window{Cap: 1, Floor: 1, Age: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var used int64
	if err := db.QueryRow(`SELECT metadata_used FROM volumes WHERE id=?`, volume).Scan(&used); err != nil {
		t.Fatal(err)
	}
	if used != want {
		t.Fatalf("trim retained metadata charge %d, want %d", used, want)
	}
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 8, want); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataIntegrityRejectsMissingOrCorruptAccountingAndPayload(t *testing.T) {
	badVersion, err := storage.EncodeMetadata(map[string]storage.OpaquePayload{"example.v1": {Version: []byte{1}, Data: nil}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, damage string
		want         error
	}{
		{"undercharge", `UPDATE volumes SET metadata_used=0`, syscall.EIO},
		{"wrong counter type", `PRAGMA ignore_check_constraints=ON; UPDATE volumes SET metadata_used='bad'`, syscall.EIO},
		{"missing trigger", `DROP TRIGGER nodes_metadata_update`, syscall.EIO},
		{"changed trigger", `DROP TRIGGER nodes_metadata_update; CREATE TRIGGER nodes_metadata_update AFTER UPDATE OF metadata ON nodes BEGIN SELECT 1; END`, syscall.EIO},
		{"text metadata", `UPDATE nodes SET metadata='bad'`, syscall.EIO},
		{"unknown encoding", `UPDATE nodes SET metadata=X'52464d020000'`, syscall.EIO},
		{"trailing bytes", `UPDATE nodes SET metadata=X'52464d01000000'`, syscall.EIO},
		{"invalid native version", fmt.Sprintf(`UPDATE nodes SET metadata=X'%x'`, badVersion), syscall.EIO},
		{"oversized metadata", `UPDATE nodes SET metadata=zeroblob(65537)`, syscall.EFBIG},
		{"oversized target", `UPDATE nodes SET link_target=zeroblob(32769)`, syscall.EFBIG},
		{"text target", `UPDATE nodes SET link_target='bad'`, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			volume, _ := testVolume(t, db, "metadata")
			execute(t, db, test.damage)
			if err := validateMetadataIntegrity(t.Context(), db, &volume, 64<<20); !errors.Is(err, test.want) {
				t.Fatalf("corrupt metadata accepted: %v", err)
			}
		})
	}
}

func TestClientNodeFactsRejectMalformedNewColumns(t *testing.T) {
	for _, damage := range []string{
		`UPDATE nodes SET kind=257`,
		`UPDATE nodes SET birth_sec=0,birth_nsec=NULL`,
		`UPDATE nodes SET change_sec=0,change_nsec=1000000000`,
		`UPDATE nodes SET directory_revision=X''`,
		`UPDATE nodes SET directory_revision=X'01'`,
		`UPDATE nodes SET directory_revision=X'0000000000000000'`,
		`UPDATE nodes SET directory_revision=X'ffffffffffffffff'`,
		`UPDATE nodes SET link_target=X'61'`,
		`UPDATE nodes SET pending_unlink=1,pending_generation=1`,
	} {
		t.Run(damage, func(t *testing.T) {
			db := testDatabase(t, 0)
			volume, _ := testVolume(t, db, "facts")
			execute(t, db, damage)
			if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 1<<20, 64<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid common fact accepted: %v", err)
			}
		})
	}
}

func TestMetadataValidationPreservesCancellation(t *testing.T) {
	db := testDatabase(t, 0)
	volume, _ := testVolume(t, db, "cancel")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := validateMetadataIntegrity(ctx, db, &volume, 64<<20); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
