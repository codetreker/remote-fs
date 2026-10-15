package dbstate

import (
	"context"
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"
)

func TestCheckStateRejectsInvalidIdentityAndNegativeCounters(t *testing.T) {
	valid := State{DatabaseID: "0123456789abcdef0123456789abcdef"}
	for _, test := range []struct {
		name   string
		change func(*State)
	}{
		{"absent identity", func(s *State) { s.DatabaseID = "" }},
		{"short identity", func(s *State) { s.DatabaseID = "abcd" }},
		{"uppercase identity", func(s *State) { s.DatabaseID = strings.Repeat("A", 32) }},
		{"nonhex identity", func(s *State) { s.DatabaseID = strings.Repeat("z", 32) }},
		{"negative generation", func(s *State) { s.Generation = -1 }},
		{"negative node counter", func(s *State) { s.NodeHighWater = -1 }},
		{"negative change counter", func(s *State) { s.ChangeHighWater = -1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := valid
			test.change(&state)
			if err := CheckState(state); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid state %+v accepted: %v", state, err)
			}
		})
	}
	for _, counter := range []int64{0, math.MaxInt64} {
		state := valid
		state.Generation, state.NodeHighWater, state.ChangeHighWater = counter, counter, counter
		if err := CheckState(state); err != nil {
			t.Fatalf("valid counter boundary %d rejected: %v", counter, err)
		}
	}
}

func TestReadPreservesStateAndRefusesCorruptRows(t *testing.T) {
	f := preparedState(t)
	for _, test := range []struct{ name, mutation string }{
		{"intact", ""},
		{"missing singleton", `DELETE FROM database_state`},
		{"extra singleton", `INSERT INTO database_state SELECT 2, database_id, generation, node_high_water, change_high_water FROM database_state`},
		{"wrong singleton", `UPDATE database_state SET singleton=2`},
		{"text counter", `UPDATE database_state SET generation='bad'`},
		{"short identity", `UPDATE database_state SET database_id='bad'`},
		{"invalid identity alphabet", `UPDATE database_state SET database_id='zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'`},
		{"negative counter", `UPDATE database_state SET change_high_water=-1`},
	} {
		if !t.Run(test.name, func(t *testing.T) {
			tx := f.transaction(t)
			want := f.before
			execState(t, tx, `PRAGMA ignore_check_constraints=ON`)
			if test.mutation != "" {
				execState(t, tx, test.mutation)
			}
			got, err := Read(t.Context(), tx)
			if test.mutation == "" {
				if err != nil || got != want {
					t.Fatalf("read state=%+v, %v; want %+v", got, err, want)
				}
			} else if !errors.Is(err, syscall.EIO) || got != (State{}) {
				t.Fatalf("corrupt row produced %+v, %v", got, err)
			}
		}) {
			return
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := Read(ctx, f.db); !errors.Is(err, context.Canceled) || got != (State{}) {
		t.Fatalf("canceled read returned %+v, %v", got, err)
	}
}

func TestValidateRejectsSequenceAndSurvivingIdentityCorruption(t *testing.T) {
	f := preparedState(t)
	for _, test := range []struct{ name, mutation string }{
		{"coherent", ""},
		{"node sequence mismatch", `UPDATE sqlite_sequence SET seq=3 WHERE name='nodes'`},
		{"change sequence mismatch", `UPDATE sqlite_sequence SET seq=5 WHERE name='changes'`},
		{"volume scalar", `UPDATE volumes SET root='bad'`},
		{"entry scalar", `UPDATE entries SET parent='bad'`},
		{"change node scalar", `UPDATE changes SET node='bad'`},
		{"change predecessor scalar", `UPDATE changes SET previous_position='bad'`},
		{"log scalar", `UPDATE logs SET trimmed_through='bad'`},
		{"volume identity above high-water", `UPDATE volumes SET root=5`},
		{"entry identity above high-water", `UPDATE entries SET node=5`},
		{"retained node above high-water", `UPDATE changes SET from_parent=5`},
		{"retained predecessor above high-water", `UPDATE changes SET previous_position=7`},
		{"log position above high-water", `UPDATE logs SET committed_position=7`},
		{"missing state", `DELETE FROM database_state`},
		{"corrupt node sequence", `UPDATE sqlite_sequence SET seq='bad' WHERE name='nodes'`},
		{"corrupt change sequence", `UPDATE sqlite_sequence SET seq='bad' WHERE name='changes'`},
	} {
		if !t.Run(test.name, func(t *testing.T) {
			tx := f.transaction(t)
			want := f.before
			if test.mutation != "" {
				execState(t, tx, test.mutation)
			}
			got, err := Validate(t.Context(), tx)
			if test.mutation == "" {
				if err != nil || got != want {
					t.Fatalf("valid state=%+v, %v; want %+v", got, err, want)
				}
			} else if !errors.Is(err, syscall.EIO) || got != (State{}) {
				t.Fatalf("corruption produced %+v, %v", got, err)
			}
		}) {
			return
		}
	}
}

func TestValidatePreservesDatabaseQueryFailures(t *testing.T) {
	for _, table := range []string{"volumes", "nodes", "logs"} {
		t.Run(table, func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, "DROP TABLE "+table)
			if table == "nodes" {
				execState(t, db, `INSERT INTO sqlite_sequence VALUES ('nodes', 4)`)
			}
			got, err := Validate(t.Context(), db)
			if got != (State{}) || err == nil || !strings.Contains(err.Error(), "no such table: "+table) {
				t.Fatalf("missing %s returned %+v, %v", table, got, err)
			}
		})
	}
}

func TestAdvanceGenerationBelongsToTheCallingTransaction(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
			db, before := stateFixture(t)
			tx := stateTransaction(t, db)
			got, err := AdvanceGeneration(t.Context(), tx)
			want := before
			want.Generation++
			if err != nil || got != want {
				t.Fatalf("advanced=%+v, %v; want %+v", got, err, want)
			}
			if commit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
				want = before
			}
			if err != nil {
				t.Fatal(err)
			}
			var generation int64
			if err := db.QueryRow(`SELECT generation FROM database_state`).Scan(&generation); err != nil {
				t.Fatal(err)
			}
			if generation != want.Generation {
				t.Fatalf("stored generation=%d, want %d", generation, want.Generation)
			}
		})
	}
}

func TestAdvanceGenerationRefusesExhaustionAndFailedWrites(t *testing.T) {
	for _, test := range []struct {
		name, preparation, detail string
		want                      error
	}{
		{"exhausted", `UPDATE database_state SET generation=9223372036854775807`, "", syscall.ENOSPC},
		{"missing state", `DELETE FROM database_state`, "", syscall.EIO},
		{"rejected write", `CREATE TRIGGER reject_generation BEFORE UPDATE ON database_state BEGIN SELECT RAISE(ABORT, 'generation write refused'); END`, "generation write refused", nil},
		{"missing update", `CREATE TRIGGER ignore_generation BEFORE UPDATE ON database_state BEGIN SELECT RAISE(IGNORE); END`, "", syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, test.preparation)
			tx := stateTransaction(t, db)
			got, err := AdvanceGeneration(t.Context(), tx)
			if got != (State{}) || err == nil || test.want != nil && !errors.Is(err, test.want) ||
				test.detail != "" && !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("failed generation update returned %+v, %v", got, err)
			}
		})
	}
}
