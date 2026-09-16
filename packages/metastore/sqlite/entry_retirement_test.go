package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"syscall"
	"testing"
)

func retirementDatabase(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "retirement.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	// This projection isolates durable transitions from session admission and
	// object publication, whose transactions own the complete schema.
	for _, statement := range []string{
		`CREATE TABLE entries(volume INTEGER NOT NULL,id INTEGER PRIMARY KEY,parent INTEGER NOT NULL,name BLOB NOT NULL,node INTEGER NOT NULL,draining INTEGER NOT NULL DEFAULT 0,drain_generation INTEGER NOT NULL DEFAULT 0,drain_if_empty INTEGER NOT NULL DEFAULT 0,drain_authority TEXT NOT NULL DEFAULT '',UNIQUE(volume,parent,name),UNIQUE(volume,node))`,
		`CREATE TABLE removal_intents(volume INTEGER NOT NULL,reference TEXT PRIMARY KEY,token TEXT UNIQUE NOT NULL,entry INTEGER NOT NULL,if_empty INTEGER NOT NULL,authority TEXT NOT NULL)`,
		`INSERT INTO entries(volume,id,parent,name,node) VALUES(1,10,1,x'61',20),(1,11,1,x'62',21)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return &Store{volume: 1, root: 1}, db
}

func retirementTransaction(t *testing.T, db *sql.DB, call func(*sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := call(tx); err != nil {
		if rollback := tx.Rollback(); rollback != nil {
			t.Fatal(rollback)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return nil
}

func requireRetirementSuccess(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestEntryRetirementPreparedAndDrainRemainIndependent(t *testing.T) {
	s, db := retirementDatabase(t)
	ctx := t.Context()
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		if err := s.prepareEntryRemoval(ctx, tx, "a1", "b1", 10, false, "c1"); err != nil {
			return err
		}
		return s.prepareEntryRemoval(ctx, tx, "a2", "b2", 10, true, "c2")
	}))
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		if err := s.checkEntryActive(ctx, tx, 10); err != nil {
			return err
		}
		state, err := s.drainEntry(ctx, tx, 10, false, "c3")
		if err != nil {
			return err
		}
		if err := s.checkEntryActive(ctx, tx, 10); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("draining entry admits retain: %v", err)
		}
		if err := s.checkParentEntriesActive(ctx, tx, 20); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("draining parent admits child: %v", err)
		}
		if err := s.cancelEntryDrain(ctx, tx, 10, state.Generation); err != nil {
			return err
		}
		state, err = s.activatePreparedRemoval(ctx, tx, "a2")
		if err != nil {
			return err
		}
		if state.Generation != 2 || !state.IfEmpty || state.Authority != "c2" {
			t.Fatalf("reactivation lost accepted facts: %+v", state)
		}
		if err := s.cancelEntryDrain(ctx, tx, 10, 1); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("old generation cleared reactivation: %v", err)
		}
		if err := s.cancelPreparedRemoval(ctx, tx, "a1", "b2"); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("foreign token canceled Prepared: %v", err)
		}
		state, err = s.activatePreparedRemoval(ctx, tx, "a1")
		if err != nil {
			return err
		}
		if state.Generation != 3 || !state.IfEmpty || state.Authority != "c1" {
			t.Fatalf("later activation lost stronger condition: %+v", state)
		}
		return nil
	}))
	var pending int
	requireRetirementSuccess(t, db.QueryRow(`SELECT count(*) FROM removal_intents`).Scan(&pending))
	if pending != 0 {
		t.Fatalf("consumed intents retained: %d", pending)
	}
}

func TestEntryRetirementFollowsEntryIdentityThroughRenameAndReplacement(t *testing.T) {
	for _, detached := range []bool{false, true} {
		t.Run(map[bool]string{false: "renamed", true: "detached"}[detached], func(t *testing.T) {
			s, db := retirementDatabase(t)
			ctx := t.Context()
			requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
				return s.prepareEntryRemoval(ctx, tx, "a1", "b1", 10, false, "c1")
			}))
			statement := `UPDATE entries SET name=x'63',parent=21 WHERE id=10`
			if detached {
				statement = `DELETE FROM entries WHERE id=10`
			}
			_, err := db.Exec(statement)
			requireRetirementSuccess(t, err)
			_, err = db.Exec(`INSERT INTO entries(volume,id,parent,name,node) VALUES(1,12,1,x'61',22)`)
			requireRetirementSuccess(t, err)
			requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
				state, err := s.activatePreparedRemoval(ctx, tx, "a1")
				if err != nil {
					return err
				}
				if detached && state != (entryRetirement{}) || !detached && (state.EntryID != 10 || state.NodeID != 20 || !state.Draining) {
					t.Fatalf("activation follows wrong identity: %+v", state)
				}
				return s.checkEntryActive(ctx, tx, 12)
			}))
		})
	}
}

func TestEntryRetirementChecksEmptyAtActivationAndFinalDetach(t *testing.T) {
	s, db := retirementDatabase(t)
	ctx := t.Context()
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		if err := s.prepareEntryRemoval(ctx, tx, "a1", "b1", 10, true, "c1"); err != nil {
			return err
		}
		if err := s.checkParentEntriesActive(ctx, tx, 20); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO entries(volume,id,parent,name,node) VALUES(1,12,20,x'63',22)`)
		return err
	}))
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		if _, err := s.drainEntry(ctx, tx, 10, true, "c2"); !errors.Is(err, syscall.ENOTEMPTY) {
			t.Fatalf("explicit nonempty drain: %v", err)
		}
		state, err := s.drainEntry(ctx, tx, 10, false, "c2")
		if err != nil {
			return err
		}
		activated, err := s.activatePreparedRemoval(ctx, tx, "a1")
		if err != nil {
			return err
		}
		if activated != (entryRetirement{}) {
			t.Fatalf("nonempty Prepared activated: %+v", activated)
		}
		after, err := s.entryRetirementState(ctx, tx, 10)
		if err != nil {
			return err
		}
		if after != state {
			t.Fatalf("nonempty Prepared changed other drain: %+v -> %+v", state, after)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE id=12`); err != nil {
			return err
		}
		state, err = s.drainEntry(ctx, tx, 10, true, "c3")
		if err != nil {
			return err
		}
		if _, err := s.entryReadyToDetach(ctx, tx, 10, state.Generation); err != nil {
			return err
		}
		// Bypass admission to prove the final transaction independently checks
		// the stored condition, even if an earlier observation was empty.
		if _, err := tx.ExecContext(ctx, `INSERT INTO entries(volume,id,parent,name,node) VALUES(1,12,20,x'63',22)`); err != nil {
			return err
		}
		if _, err := s.entryReadyToDetach(ctx, tx, 10, state.Generation); !errors.Is(err, syscall.ENOTEMPTY) {
			t.Fatalf("final detach ignored newly occupied directory: %v", err)
		}
		return nil
	}))
}

func TestEntryRetirementFailureRollsBackConsumedIntent(t *testing.T) {
	s, db := retirementDatabase(t)
	ctx := t.Context()
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		return s.prepareEntryRemoval(ctx, tx, "a1", "b1", 10, false, "c1")
	}))
	_, err := db.Exec(`CREATE TRIGGER reject_drain BEFORE UPDATE OF draining ON entries BEGIN SELECT RAISE(FAIL,'injected drain failure'); END`)
	requireRetirementSuccess(t, err)
	err = retirementTransaction(t, db, func(tx *sql.Tx) error {
		_, err := s.activatePreparedRemoval(ctx, tx, "a1")
		return err
	})
	if err == nil {
		t.Fatal("injected failure succeeded")
	}
	var pending int
	requireRetirementSuccess(t, db.QueryRow(`SELECT count(*) FROM removal_intents WHERE reference='a1'`).Scan(&pending))
	if pending != 1 {
		t.Fatal("failed activation lost cleanup ownership")
	}
	_, err = db.Exec(`DROP TRIGGER reject_drain`)
	requireRetirementSuccess(t, err)
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		state, err := s.activatePreparedRemoval(ctx, tx, "a1")
		if err == nil && (state.Generation != 1 || !state.Draining) {
			t.Fatalf("retry did not activate exactly once: %+v", state)
		}
		return err
	}))
}

func TestEntryRetirementRejectsStaleInvalidAndExhaustedState(t *testing.T) {
	s, db := retirementDatabase(t)
	ctx := t.Context()
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		if err := s.checkParentEntriesActive(ctx, tx, 1); err != nil {
			return err
		}
		if err := s.checkParentEntriesActive(ctx, tx, 99); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("detached parent admitted insertion: %v", err)
		}
		if err := s.checkEntryActive(ctx, tx, 0); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("zero identity: %v", err)
		}
		if err := s.checkEntryActive(ctx, tx, 99); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("missing identity: %v", err)
		}
		if err := s.prepareEntryRemoval(ctx, tx, "", "b1", 10, false, "c1"); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid reference: %v", err)
		}
		if err := s.prepareEntryRemoval(ctx, tx, "a1", "b1", 10, false, "c1"); err != nil {
			return err
		}
		if err := s.prepareEntryRemoval(ctx, tx, "a1", "b2", 11, false, "c2"); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("duplicate Prepared replaced original: %v", err)
		}
		if err := s.cancelEntryDrain(ctx, tx, 10, 0); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("zero generation: %v", err)
		}
		if err := s.cancelEntryDrain(ctx, tx, 10, 1); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("active entry canceled: %v", err)
		}
		if _, err := s.entryReadyToDetach(ctx, tx, 10, 0); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("active entry detached: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE entries SET drain_generation=? WHERE id=10`, int64(math.MaxInt64)); err != nil {
			return err
		}
		if _, err := s.drainEntry(ctx, tx, 10, false, "c1"); !errors.Is(err, syscall.EOVERFLOW) {
			t.Fatalf("generation overflow: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE entries SET drain_if_empty=1 WHERE id=11`); err != nil {
			return err
		}
		if _, err := s.entryRetirementState(ctx, tx, 11); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid active state: %v", err)
		}
		return nil
	}))
}

func TestEntryRetirementCanceledObservationDoesNotInventAbsence(t *testing.T) {
	s, db := retirementDatabase(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	requireRetirementSuccess(t, retirementTransaction(t, db, func(tx *sql.Tx) error {
		_, err := s.activatePreparedRemoval(ctx, tx, "a1")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled observation returned %v", err)
		}
		return nil
	}))
}
