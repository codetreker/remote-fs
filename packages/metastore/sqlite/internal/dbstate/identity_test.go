package dbstate

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/sqliteschema"
	_ "modernc.org/sqlite"
)

func TestGlobalIdentityBoundsUseExpressionIndexSearches(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/metastore.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	migrations := sqliteschema.MustLoad(os.DirFS("../schema"), "migrations")
	if err := migrations.Reach(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	plan := func(query string) string {
		rows, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details = append(details, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(details, "\n")
	}

	combined := plan(globalNodeIdentityBoundsQuery) + "\n" + plan(globalChangeIdentityBoundsQuery)
	if strings.Contains(combined, "USE TEMP B-TREE") {
		t.Fatalf("global identity bounds build a temporary ordering:\n%s", combined)
	}
	for _, index := range []string{
		"volumes_by_root_identity",
		"entries_by_node_identity",
		"changes_by_node_identity",
		"changes_by_position_identity",
		"logs_by_change_identity",
	} {
		if count := strings.Count(combined, index); count != 2 {
			t.Fatalf("global identity bounds use %s %d times, want invalid and maximum searches:\n%s",
				index, count, combined)
		}
	}
	for _, table := range []string{"volumes", "entries", "changes", "logs"} {
		if strings.Contains(combined, "SCAN "+table) {
			t.Fatalf("global identity bounds scan %s:\n%s", table, combined)
		}
	}
}

func TestSequenceValueAcceptsAbsentAndBoundedCounters(t *testing.T) {
	for _, test := range []struct {
		name, preparation string
		want              int64
		invalid           bool
	}{
		{"absent", `DELETE FROM sqlite_sequence WHERE name='nodes'`, 0, false},
		{"zero", `UPDATE sqlite_sequence SET seq=0 WHERE name='nodes'`, 0, false},
		{"maximum", `UPDATE sqlite_sequence SET seq=9223372036854775807 WHERE name='nodes'`, math.MaxInt64, false},
		{"duplicate", `INSERT INTO sqlite_sequence VALUES ('nodes', 4)`, 0, true},
		{"negative", `UPDATE sqlite_sequence SET seq=-1 WHERE name='nodes'`, 0, true},
		{"text", `UPDATE sqlite_sequence SET seq='four' WHERE name='nodes'`, 0, true},
		{"fractional", `UPDATE sqlite_sequence SET seq=4.5 WHERE name='nodes'`, 0, true},
		{"null", `UPDATE sqlite_sequence SET seq=NULL WHERE name='nodes'`, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, test.preparation)
			got, err := SequenceValue(t.Context(), db, "nodes")
			if test.invalid {
				if got != 0 || !errors.Is(err, syscall.EIO) {
					t.Fatalf("corrupt sequence returned %d, %v", got, err)
				}
			} else if err != nil || got != test.want {
				t.Fatalf("sequence=%d, %v; want %d", got, err, test.want)
			}
		})
	}
	db, _ := stateFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := SequenceValue(ctx, db, "nodes"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled sequence read: %v", err)
	}
}

func TestAllocatorsPublishOnlyThroughTheCallingTransaction(t *testing.T) {
	for _, kind := range []string{"node", "change"} {
		for _, commit := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: " rollback", true: " commit"}[commit], func(t *testing.T) {
				db, before := stateFixture(t)
				tx := stateTransaction(t, db)
				var next int64
				var err error
				want := before
				if kind == "node" {
					next, err = AllocateNodeID(t.Context(), tx)
					want.NodeHighWater++
					if next != 5 || err != nil {
						t.Fatalf("node allocation=%d, %v", next, err)
					}
					execState(t, tx, `INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec)
						SELECT ?, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec FROM nodes WHERE id=1`, next)
				} else {
					next, err = AllocateChangePosition(t.Context(), tx)
					want.ChangeHighWater++
					if next != 7 || err != nil {
						t.Fatalf("change allocation=%d, %v", next, err)
					}
					execState(t, tx, `INSERT INTO changes (position, previous_position, volume, kind, parent, recorded_sec, recorded_nsec)
						VALUES (?, 2, 1, 0, 1, 0, 0)`, next)
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
				got, err := Validate(t.Context(), db)
				if err != nil || got != want {
					t.Fatalf("published allocator state=%+v, %v; want %+v", got, err, want)
				}
				var count int
				query := `SELECT count(*) FROM nodes WHERE id=?`
				if kind == "change" {
					query = `SELECT count(*) FROM changes WHERE position=?`
				}
				if err := db.QueryRow(query, next).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != map[bool]int{false: 0, true: 1}[commit] {
					t.Fatalf("transaction retained %d allocated rows", count)
				}
			})
		}
	}
}

func TestIdentityAllocationRejectsCorruptionAndExhaustion(t *testing.T) {
	for _, kind := range []string{"node", "change", "observed node"} {
		for _, fault := range []string{"missing state", "sequence scalar", "sequence mismatch", "write failure", "exhausted"} {
			t.Run(kind+" "+fault, func(t *testing.T) {
				db, before := stateFixture(t)
				sequence := "nodes"
				if kind == "change" {
					sequence = "changes"
				}
				want := error(syscall.EIO)
				switch fault {
				case "missing state":
					execState(t, db, `DELETE FROM database_state`)
				case "sequence scalar":
					execState(t, db, `UPDATE sqlite_sequence SET seq='broken' WHERE name=?`, sequence)
				case "sequence mismatch":
					execState(t, db, `UPDATE sqlite_sequence SET seq=0 WHERE name=?`, sequence)
				case "write failure":
					execState(t, db, `CREATE TRIGGER reject_identity BEFORE UPDATE ON database_state BEGIN SELECT RAISE(ABORT, 'identity write refused'); END`)
					want = nil
				case "exhausted":
					if kind == "change" {
						before.ChangeHighWater = math.MaxInt64
					} else {
						before.NodeHighWater = math.MaxInt64
					}
					setFixtureState(t, db, before)
					if kind != "observed node" {
						want = syscall.ENOSPC
					}
				}
				tx := stateTransaction(t, db)
				var err error
				var id int64
				switch kind {
				case "node":
					id, err = AllocateNodeID(t.Context(), tx)
				case "change":
					id, err = AllocateChangePosition(t.Context(), tx)
				case "observed node":
					err = ObserveNewNodeID(t.Context(), tx, 8)
				}
				if id != 0 || err == nil || want != nil && !errors.Is(err, want) ||
					fault == "write failure" && !strings.Contains(err.Error(), "identity write refused") {
					t.Fatalf("failed identity allocation=%d, %v; want %v", id, err, want)
				}
				if fault != "missing state" {
					got, readErr := Read(t.Context(), tx)
					if readErr != nil || got != before {
						t.Fatalf("rejected allocation changed %+v to %+v: %v", before, got, readErr)
					}
				}
			})
		}
	}
}

func TestObserveNewNodeIDRetainsHistoricalHighWater(t *testing.T) {
	db, before := stateFixture(t)
	tx := stateTransaction(t, db)
	for _, id := range []int64{-1, 0, 1, before.NodeHighWater} {
		if err := ObserveNewNodeID(t.Context(), tx, id); !errors.Is(err, syscall.EIO) {
			t.Fatalf("reused or invalid identity %d was accepted: %v", id, err)
		}
	}
	if err := ObserveNewNodeID(t.Context(), tx, 8); err != nil {
		t.Fatal(err)
	}
	execState(t, tx, `INSERT INTO nodes (id, volume, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec)
		VALUES (8, 1, 0, 0, 0, 0, 0, 0)`)
	execState(t, tx, `DELETE FROM nodes WHERE id=8`)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := Validate(t.Context(), db)
	before.NodeHighWater = 8
	if err != nil || got != before {
		t.Fatalf("deleted identity lost its high-water: %+v, %v", got, err)
	}
	tx = stateTransaction(t, db)
	if id, err := AllocateNodeID(t.Context(), tx); err != nil || id != 9 {
		t.Fatalf("next identity after deleting8=%d, %v", id, err)
	}
}

func TestValidateIdentityBoundsChecksEachVolumeReference(t *testing.T) {
	for _, mutation := range []string{
		"",
		`UPDATE volumes SET root=5`,
		`UPDATE entries SET parent=5`,
		`UPDATE changes SET from_parent=5`,
		`UPDATE logs SET trimmed_through=7`,
		`UPDATE changes SET previous_position=7`,
		`UPDATE database_state SET node_high_water=0`,
		`DELETE FROM database_state`,
	} {
		t.Run(mutation, func(t *testing.T) {
			db, _ := stateFixture(t)
			if mutation != "" {
				execState(t, db, mutation)
			}
			err := ValidateIdentityBounds(t.Context(), db, 1)
			if mutation == "" {
				if err != nil {
					t.Fatalf("valid volume bounds: %v", err)
				}
			} else if !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid volume bounds accepted: %v", err)
			}
			if mutation != `DELETE FROM database_state` {
				if err := ValidateIdentityBounds(t.Context(), db, 2); err != nil {
					t.Fatalf("another volume inherited foreign corruption: %v", err)
				}
			}
		})
	}
	for _, table := range []string{"nodes", "logs"} {
		t.Run("missing "+table, func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, "DROP TABLE "+table)
			if err := ValidateIdentityBounds(t.Context(), db, 1); err == nil || !strings.Contains(err.Error(), "no such table: "+table) {
				t.Fatalf("missing %s error was lost: %v", table, err)
			}
		})
	}
}
