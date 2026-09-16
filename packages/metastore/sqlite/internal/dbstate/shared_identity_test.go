package dbstate

import (
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"
)

func TestNodeAndEntryAllocationsShareOneDurableSequence(t *testing.T) {
	db, before := stateFixture(t)
	tx := stateTransaction(t, db)
	entry, err := AllocateEntryID(t.Context(), tx)
	if err != nil || entry != before.NodeHighWater+1 {
		t.Fatalf("entry allocation = %d, %v", entry, err)
	}
	node, err := AllocateNodeID(t.Context(), tx)
	if err != nil || node != entry+1 {
		t.Fatalf("node allocation after entry = %d, %v", node, err)
	}
	sequence, err := SequenceValue(t.Context(), tx, "nodes")
	if err != nil || sequence != node {
		t.Fatalf("reserved identities did not advance the canonical sequence: %d, %v", sequence, err)
	}
	execState(t, tx, `INSERT INTO nodes(id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec)
		VALUES(?,1,1,0,0,0,0,0)`, node)
	execState(t, tx, `INSERT INTO entries(id,volume,parent,name,node) VALUES(?,1,1,X'67',?)`, entry, node)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	current, err := Validate(t.Context(), db)
	if err != nil || current.NodeHighWater != node {
		t.Fatalf("shared identity witness = %+v, %v", current, err)
	}
	tx = stateTransaction(t, db)
	execState(t, tx, `DELETE FROM entries WHERE id=?`, entry)
	execState(t, tx, `DELETE FROM nodes WHERE id=?`, node)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx = stateTransaction(t, db)
	next, err := AllocateEntryID(t.Context(), tx)
	if err != nil || next <= node || next == entry {
		t.Fatalf("deletion permitted identity reuse: entry=%d node=%d next=%d error=%v", entry, node, next, err)
	}
}

func TestSharedIdentityAllocationRollsBackBothWitnessAndSequence(t *testing.T) {
	db, before := stateFixture(t)
	tx := stateTransaction(t, db)
	if _, err := AllocateEntryID(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	got, err := Validate(t.Context(), db)
	sequence, sequenceErr := SequenceValue(t.Context(), db, "nodes")
	if err != nil || sequenceErr != nil || got != before || sequence != before.NodeHighWater {
		t.Fatalf("rollback left a partial identity reservation: %+v sequence=%d errors=%v/%v", got, sequence, err, sequenceErr)
	}
	before.NodeHighWater = math.MaxInt64
	setFixtureState(t, db, before)
	tx = stateTransaction(t, db)
	if _, err := AllocateEntryID(t.Context(), tx); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("exhausted entry identity allocation: %v", err)
	}
}

func TestSharedIdentityWitnessCoversEntriesRetirementAndHistory(t *testing.T) {
	for _, test := range []struct{ name, setup string }{
		{"live entry", `UPDATE entries SET id=7`},
		{"removed entry intention", `INSERT INTO removal_intents(volume,reference,token,entry,if_empty,authority) VALUES(1,'ref','token',7,0,'authority')`},
		{"retained image", `UPDATE changes SET identity_high_water=7`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, state := stateFixture(t)
			state.NodeHighWater = 7
			setFixtureState(t, db, state)
			execState(t, db, test.setup)
			if _, err := Validate(t.Context(), db); err != nil {
				t.Fatalf("valid identity bound rejected: %v", err)
			}
			state.NodeHighWater = 6
			setFixtureState(t, db, state)
			if _, err := Validate(t.Context(), db); !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "high-water") {
				t.Fatalf("coordinated counter rollback accepted: %v", err)
			}
			if err := ValidateIdentityBounds(t.Context(), db, 1); !errors.Is(err, syscall.EIO) {
				t.Fatalf("volume identity rollback accepted: %v", err)
			}
		})
	}
}

func TestSharedIdentityIndexRejectsInvalidScalarClasses(t *testing.T) {
	for _, setup := range []string{
		`UPDATE entries SET id='invalid'`,
		`INSERT INTO removal_intents(volume,reference,token,entry,if_empty,authority) VALUES(1,'ref','token','invalid',0,'authority')`,
		`UPDATE changes SET identity_high_water='invalid'`,
	} {
		t.Run(setup, func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, setup)
			if _, err := Validate(t.Context(), db); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid identity scalar accepted: %v", err)
			}
		})
	}
}
