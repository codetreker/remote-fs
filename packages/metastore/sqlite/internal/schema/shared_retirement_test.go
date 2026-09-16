package schema

import (
	"errors"
	"syscall"
	"testing"
)

func TestPreparedRemovalMayOutliveItsEntryAndConsumesIntegrityBudget(t *testing.T) {
	db := testDatabase(t, 0)
	volume, root := testVolume(t, db, "retirement")
	node, _ := testFile(t, db, volume, root, "file", 0, false)
	var entry int64
	if err := db.QueryRow(`SELECT id FROM entries WHERE node=?`, node).Scan(&entry); err != nil {
		t.Fatal(err)
	}
	execute(t, db, `INSERT INTO removal_intents(volume,reference,token,entry,if_empty,authority)
		VALUES(?,'reference','token',?,1,'authority')`, volume, entry)
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 7, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 6, 1<<20); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("prepared intent escaped record budget: %v", err)
	}
	execute(t, db, `UPDATE nodes SET detached=1 WHERE id=?`, node)
	execute(t, db, `DELETE FROM entries WHERE id=?`, entry)
	if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 1<<20); err != nil {
		t.Fatalf("intention referring to a removed entry was rejected: %v", err)
	}
}

func TestRemovalScopeAndDrainStateCannotHideCorruption(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE removal_intents SET volume=999`,
		`UPDATE removal_intents SET entry='invalid'`,
		`UPDATE removal_intents SET if_empty=2`,
		`UPDATE removal_intents SET token=''`,
		`UPDATE entries SET draining=1`,
		`UPDATE entries SET drain_if_empty=1`,
		`UPDATE entries SET drain_authority='unexpected'`,
		`UPDATE entries SET drain_generation=-1`,
	} {
		t.Run(mutation, func(t *testing.T) {
			db := testDatabase(t, 0)
			volume, root := testVolume(t, db, "retirement")
			node, _ := testFile(t, db, volume, root, "file", 0, false)
			execute(t, db, `INSERT INTO removal_intents(volume,reference,token,entry,if_empty,authority)
				SELECT volume,'reference','token',id,1,'authority' FROM entries WHERE node=?`, node)
			execute(t, db, mutation)
			if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 1<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid removal state accepted: %v", err)
			}
		})
	}
}
