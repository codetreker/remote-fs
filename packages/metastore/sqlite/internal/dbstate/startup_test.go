package dbstate

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestCheckStartupRejectsContradictoryDurabilityEvidence(t *testing.T) {
	accepted := State{DatabaseID: "0123456789abcdef0123456789abcdef", Generation: 7, NodeHighWater: 4, ChangeHighWater: 6}
	for _, test := range []struct {
		name    string
		startup Startup
		want    error
	}{
		{"new database", Startup{}, nil},
		{"checkpointed", Startup{Accepted: accepted, CheckpointedGeneration: 7}, nil},
		{"wal recovery", Startup{Accepted: accepted, CheckpointedGeneration: 6, WALPresent: true, WALNonEmpty: true}, nil},
		{"identity absent with generation", Startup{Accepted: State{Generation: 1}}, syscall.EINVAL},
		{"identity absent with node history", Startup{Accepted: State{NodeHighWater: 1}}, syscall.EINVAL},
		{"identity absent with change history", Startup{Accepted: State{ChangeHighWater: 1}}, syscall.EINVAL},
		{"identity absent with checkpoint", Startup{CheckpointedGeneration: 1}, syscall.EINVAL},
		{"invalid identity", Startup{Accepted: State{DatabaseID: "bad"}}, syscall.EIO},
		{"negative checkpoint", Startup{Accepted: accepted, CheckpointedGeneration: -1}, syscall.EINVAL},
		{"checkpoint ahead of accepted", Startup{Accepted: accepted, CheckpointedGeneration: 8}, syscall.EINVAL},
		{"frames without wal", Startup{Accepted: accepted, CheckpointedGeneration: 7, WALNonEmpty: true}, syscall.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := CheckStartup(test.startup); !errors.Is(err, test.want) {
				t.Fatalf("startup=%+v returned %v, want %v", test.startup, err, test.want)
			}
		})
	}
}

func TestReconcileStartupRequiresTheAcceptedLineageAndWAL(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Startup)
		want   string
	}{
		{"matching checkpoint", func(s *Startup) {}, ""},
		{"initialization", func(s *Startup) { *s = Startup{} }, ""},
		{"uncheckpointed frames", func(s *Startup) { s.CheckpointedGeneration = 6; s.WALPresent = true; s.WALNonEmpty = true }, ""},
		{"missing required frames", func(s *Startup) { s.CheckpointedGeneration = 6; s.WALPresent = true }, "startup WAL is not present with frames"},
		{"foreign database", func(s *Startup) { s.Accepted.DatabaseID = strings.Repeat("f", 32) }, "not the accepted identity"},
		{"rollback with absent wal", func(s *Startup) { s.Accepted.Generation = 8; s.CheckpointedGeneration = 8 }, "absent startup WAL"},
		{"rollback with empty wal", func(s *Startup) { s.Accepted.Generation = 8; s.CheckpointedGeneration = 8; s.WALPresent = true }, "empty startup WAL"},
		{"rollback with surviving wal", func(s *Startup) {
			s.Accepted.Generation = 8
			s.CheckpointedGeneration = 8
			s.WALPresent = true
			s.WALNonEmpty = true
		}, "non-empty startup WAL"},
		{"same generation different counters", func(s *Startup) { s.Accepted.NodeHighWater = 3 }, "disagree at generation"},
		{"unwitnessed generation without wal", func(s *Startup) { s.Accepted.Generation = 6; s.CheckpointedGeneration = 6 }, "without a non-empty startup WAL"},
		{"unwitnessed generation with wal", func(s *Startup) {
			s.Accepted.Generation = 6
			s.CheckpointedGeneration = 6
			s.WALPresent = true
			s.WALNonEmpty = true
		}, ""},
		{"lost node history", func(s *Startup) {
			s.Accepted.Generation = 6
			s.CheckpointedGeneration = 6
			s.WALPresent = true
			s.WALNonEmpty = true
			s.Accepted.NodeHighWater = 5
		}, "below accepted state"},
		{"lost change history", func(s *Startup) {
			s.Accepted.Generation = 6
			s.CheckpointedGeneration = 6
			s.WALPresent = true
			s.WALNonEmpty = true
			s.Accepted.ChangeHighWater = 7
		}, "below accepted state"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, before := stateFixture(t)
			startup := Startup{Accepted: before, CheckpointedGeneration: before.Generation}
			test.change(&startup)
			tx := stateTransaction(t, db)
			err := ReconcileStartup(t.Context(), tx, startup)
			if test.want == "" {
				if err != nil {
					t.Fatalf("valid startup rejected: %v", err)
				}
			} else if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid startup returned %v, want EIO with %q", err, test.want)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			got, err := Read(t.Context(), db)
			if err != nil || got != before {
				t.Fatalf("startup validation changed stored state to %+v: %v", got, err)
			}
		})
	}
	db, accepted := stateFixture(t)
	execState(t, db, `DELETE FROM database_state`)
	if err := ReconcileStartup(t.Context(), stateTransaction(t, db), Startup{Accepted: accepted, CheckpointedGeneration: accepted.Generation}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing visible state accepted: %v", err)
	}
}

func TestValidateLegacySequencesRejectsLostAndMalformedHistory(t *testing.T) {
	for _, test := range []struct{ name, mutation, detail string }{
		{"intact", "", ""},
		{"malformed node sequence", `UPDATE sqlite_sequence SET seq='bad' WHERE name='nodes'`, "SQLite sequence"},
		{"missing node sequence", `DELETE FROM sqlite_sequence WHERE name='nodes'`, "below existing node"},
		{"malformed root", `UPDATE namespaces SET root='bad'`, "node identities in invalid storage classes"},
		{"entry above sequence", `UPDATE entries SET node=5`, "below existing node"},
		{"malformed retained node", `UPDATE changes SET from_parent='bad'`, "retained node identities in invalid storage classes"},
		{"retained node above sequence", `UPDATE changes SET node=5`, "below retained log identity"},
		{"malformed change sequence", `UPDATE sqlite_sequence SET seq='bad' WHERE name='changes'`, "SQLite sequence"},
		{"malformed log position", `UPDATE logs SET committed_position='bad'`, "change positions in invalid storage classes"},
		{"missing change sequence", `DELETE FROM sqlite_sequence WHERE name='changes'`, "below retained position"},
		{"retained log tail above sequence", `UPDATE logs SET committed_position=7`, "below retained position"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := stateFixture(t)
			if test.mutation != "" {
				execState(t, db, test.mutation)
			}
			err := ValidateLegacySequences(t.Context(), db, 2)
			if test.detail == "" {
				if err != nil {
					t.Fatalf("valid legacy sequences: %v", err)
				}
			} else if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("corrupt legacy sequence returned %v, want %q", err, test.detail)
			}
		})
	}
	t.Run("version one has no history", func(t *testing.T) {
		db, _ := stateFixture(t)
		execState(t, db, `DROP TABLE changes`)
		execState(t, db, `DROP TABLE logs`)
		if err := ValidateLegacySequences(t.Context(), db, 1); err != nil {
			t.Fatalf("version one required log tables: %v", err)
		}
	})
	for _, table := range []string{"entries", "changes", "logs"} {
		t.Run("missing "+table, func(t *testing.T) {
			db, _ := stateFixture(t)
			execState(t, db, "DROP TABLE "+table)
			if err := ValidateLegacySequences(t.Context(), db, 2); err == nil || !strings.Contains(err.Error(), "no such table: "+table) {
				t.Fatalf("missing legacy table %s returned %v", table, err)
			}
		})
	}
}
