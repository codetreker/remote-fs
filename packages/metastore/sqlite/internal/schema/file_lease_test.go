package schema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
)

const initialFileLeaseRecord = `INSERT INTO file_lease_recovery
	SELECT 1,database_id,'11111111111111111111111111111111',0,0,1,NULL,NULL,NULL FROM database_state`

func TestFileLeaseInitializationValidatesItsCrashStates(t *testing.T) {
	for _, test := range []struct{ name, setup string }{
		{"pending before anchor", ""},
		{"pending after initial record", initialFileLeaseRecord},
		{"ready initial", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1`},
		{"ready accepted", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1;
			UPDATE file_lease_recovery SET accepted_generation=7,accepted_nanos=99`},
		{"ready active", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1;
			UPDATE file_lease_recovery SET accepted_generation=7,accepted_nanos=99,accepted_quiescent=0`},
		{"prepared quiescence", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1;
			UPDATE file_lease_recovery SET accepted_generation=7,accepted_nanos=99,accepted_quiescent=0,
			prepared_generation=8,prepared_nanos=99,prepared_quiescent=1`},
		{"ready prepared", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1;
			UPDATE file_lease_recovery SET accepted_generation=7,accepted_nanos=99,prepared_generation=8,prepared_nanos=100,prepared_quiescent=0`},
		{"ready equal duration", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1;
			UPDATE file_lease_recovery SET accepted_generation=7,accepted_nanos=99,prepared_generation=8,prepared_nanos=99,prepared_quiescent=0`},
		{"ready final generation", initialFileLeaseRecord + `; UPDATE file_lease_initialization SET state=1;
			UPDATE file_lease_recovery SET accepted_generation=9223372036854775807,accepted_nanos=9223372036854775807`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			testVolume(t, db, "A")
			if test.setup != "" {
				execute(t, db, test.setup)
			}
			if err := validateFileLeaseInitialization(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			before := migrationImage(t, db)
			if err := validateIntegrity(t.Context(), db, nil, 1000, 1<<20, schema.Version()); err != nil {
				t.Fatal(err)
			}
			if after := migrationImage(t, db); after != before {
				t.Fatal("validation changed lease evidence")
			}
		})
	}
}

func TestFileLeaseInitializationRefusesDamagedEvidenceBeforeOpening(t *testing.T) {
	for _, test := range []struct{ name, damage string }{
		{"missing marker", `DELETE FROM file_lease_initialization`},
		{"duplicate marker", `INSERT INTO file_lease_initialization VALUES(2,1)`},
		{"wrong marker singleton", `UPDATE file_lease_initialization SET singleton=2`},
		{"marker storage class", `UPDATE file_lease_initialization SET state='bad'`},
		{"unknown marker", `UPDATE file_lease_initialization SET state=2`},
		{"ready without record", `DELETE FROM file_lease_recovery`},
		{"duplicate record", `INSERT INTO file_lease_recovery SELECT 2,database_id,state_id,0,0,1,NULL,NULL,NULL FROM file_lease_recovery`},
		{"wrong record singleton", `UPDATE file_lease_recovery SET singleton=2`},
		{"wrong database", `UPDATE file_lease_recovery SET database_id='22222222222222222222222222222222'`},
		{"oversized database", `UPDATE file_lease_recovery SET database_id=zeroblob(1048576)`},
		{"oversized state", `UPDATE file_lease_recovery SET state_id=zeroblob(1048576)`},
		{"invalid state alphabet", `UPDATE file_lease_recovery SET state_id='zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'`},
		{"negative generation", `UPDATE file_lease_recovery SET accepted_generation=-1`},
		{"negative duration", `UPDATE file_lease_recovery SET accepted_nanos=-1`},
		{"generation storage class", `UPDATE file_lease_recovery SET accepted_generation=zeroblob(1048576)`},
		{"duration storage class", `UPDATE file_lease_recovery SET accepted_nanos='bad'`},
		{"accepted quiescence storage class", `UPDATE file_lease_recovery SET accepted_quiescent=zeroblob(1048576)`},
		{"accepted quiescence negative", `UPDATE file_lease_recovery SET accepted_quiescent=-1`},
		{"accepted quiescence unknown", `UPDATE file_lease_recovery SET accepted_quiescent=2`},
		{"active initial generation", `UPDATE file_lease_recovery SET accepted_quiescent=0`},
		{"nonzero initial lease", `UPDATE file_lease_recovery SET accepted_nanos=1`},
		{"accepted active without lease", `UPDATE file_lease_recovery SET accepted_generation=1,accepted_quiescent=0`},
		{"prepared active without lease", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=0,prepared_quiescent=0`},
		{"partial prepared generation", `UPDATE file_lease_recovery SET prepared_generation=1`},
		{"partial prepared duration", `UPDATE file_lease_recovery SET prepared_nanos=1`},
		{"partial prepared quiescence", `UPDATE file_lease_recovery SET prepared_quiescent=1`},
		{"missing prepared quiescence", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=1`},
		{"missing prepared duration", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_quiescent=1`},
		{"missing prepared generation", `UPDATE file_lease_recovery SET prepared_nanos=1,prepared_quiescent=1`},
		{"prepared quiescence storage class", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=1,prepared_quiescent='bad'`},
		{"prepared quiescence negative", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=1,prepared_quiescent=-1`},
		{"prepared quiescence unknown", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=1,prepared_quiescent=2`},
		{"quiescent lease increase", `UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=1,prepared_quiescent=1`},
		{"prepared storage class", `UPDATE file_lease_recovery SET prepared_generation='bad',prepared_nanos=1,prepared_quiescent=0`},
		{"skipped generation", `UPDATE file_lease_recovery SET prepared_generation=2,prepared_nanos=1,prepared_quiescent=0`},
		{"decreasing duration", `UPDATE file_lease_recovery SET accepted_nanos=2,prepared_generation=1,prepared_nanos=1,prepared_quiescent=0`},
		{"overflowed generation", `UPDATE file_lease_recovery SET accepted_generation=9223372036854775807,prepared_generation=9223372036854775807,prepared_nanos=1,prepared_quiescent=0`},
		{"advanced pending", `UPDATE file_lease_initialization SET state=0; UPDATE file_lease_recovery SET accepted_generation=1`},
		{"nonzero pending duration", `UPDATE file_lease_initialization SET state=0; UPDATE file_lease_recovery SET accepted_nanos=1`},
		{"prepared pending", `UPDATE file_lease_initialization SET state=0; UPDATE file_lease_recovery SET prepared_generation=1,prepared_nanos=1,prepared_quiescent=0`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := testDatabase(t, 0)
			testVolume(t, db, "A")
			execute(t, db, `PRAGMA ignore_check_constraints=ON`)
			execute(t, db, initialFileLeaseRecord)
			execute(t, db, `UPDATE file_lease_initialization SET state=1`)
			execute(t, db, test.damage)
			before := migrationImage(t, db)
			if _, _, err := Prepare(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20); !errors.Is(err, syscall.EIO) {
				t.Fatalf("damaged file lease state opened: %v", err)
			}
			if after := migrationImage(t, db); after != before {
				t.Fatal("failed opening repaired or reset file lease evidence")
			}
		})
	}
}

func TestFileLeaseMigrationStartsPendingWithoutChangingStrongEvidence(t *testing.T) {
	for _, version := range []int{0, 1, 2, 3, 4, 5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			db := testDatabase(t, 0)
			if version != 0 {
				db = populatedMigrationSource(t, version)
			}
			if version >= 4 {
				execute(t, db, `INSERT INTO lease_recovery SELECT
					1,database_id,'22222222222222222222222222222222',7,99,8,100 FROM database_state`)
			}
			if _, _, err := Prepare(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20); err != nil {
				t.Fatal(err)
			}
			var marker, fileRows int
			if err := db.QueryRow(`SELECT state,(SELECT count(*) FROM file_lease_recovery)
				FROM file_lease_initialization WHERE singleton=1`).Scan(&marker, &fileRows); err != nil {
				t.Fatal(err)
			}
			if marker != 0 || fileRows != 0 {
				t.Fatalf("migration fabricated file lease authority: marker=%d rows=%d", marker, fileRows)
			}
			if version >= 4 {
				var stateID string
				var generation, nanos, preparedGeneration, preparedNanos int64
				if err := db.QueryRow(`SELECT state_id,accepted_generation,accepted_nanos,prepared_generation,prepared_nanos
					FROM lease_recovery`).Scan(&stateID, &generation, &nanos, &preparedGeneration, &preparedNanos); err != nil {
					t.Fatal(err)
				}
				if stateID != "22222222222222222222222222222222" || generation != 7 || nanos != 99 || preparedGeneration != 8 || preparedNanos != 100 {
					t.Fatalf("migration changed independent Strong evidence: %q %d/%d %d/%d", stateID, generation, nanos, preparedGeneration, preparedNanos)
				}
			}
		})
	}
}

func TestFileLeaseValidationPreservesQueryFailures(t *testing.T) {
	for _, table := range []string{"file_lease_initialization", "database_state", "file_lease_recovery", "removal_intents", "entries"} {
		t.Run(table, func(t *testing.T) {
			db := testDatabase(t, 0)
			testVolume(t, db, "A")
			execute(t, db, initialFileLeaseRecord)
			execute(t, db, `DROP TABLE `+table)
			if err := validateFileLeaseInitialization(t.Context(), db); err == nil {
				t.Fatal("missing state table was treated as initialization")
			}
		})
	}
	db := testDatabase(t, 0)
	testVolume(t, db, "A")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := validateFileLeaseInitialization(ctx, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost its cause: %v", err)
	}
}

func TestFileLeaseQuiescenceRequiresEveryVolumeToDrain(t *testing.T) {
	for _, evidence := range []string{"accepted", "prepared", "active"} {
		for _, obligation := range []string{"intention", "draining entry"} {
			t.Run(evidence+"/"+obligation, func(t *testing.T) {
				db := testDatabase(t, 0)
				volume, _ := testVolume(t, db, "A")
				other, root := testVolume(t, db, "B")
				node, _ := testFile(t, db, other, root, "file", 0, false)
				execute(t, db, initialFileLeaseRecord)
				execute(t, db, `UPDATE file_lease_initialization SET state=1`)
				execute(t, db, `UPDATE file_lease_recovery SET accepted_generation=7,accepted_nanos=99,accepted_quiescent=0`)
				switch evidence {
				case "accepted":
					execute(t, db, `UPDATE file_lease_recovery SET accepted_quiescent=1`)
				case "prepared":
					execute(t, db, `UPDATE file_lease_recovery SET prepared_generation=8,prepared_nanos=99,prepared_quiescent=1`)
				}
				if obligation == "intention" {
					execute(t, db, `INSERT INTO removal_intents(volume,reference,token,entry,if_empty,authority)
						SELECT volume,'reference','token',id,1,'authority' FROM entries WHERE node=?`, node)
				} else {
					execute(t, db, `UPDATE entries SET draining=1,drain_generation=1,drain_authority='authority' WHERE node=?`, node)
				}
				err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 1<<20)
				if evidence == "active" {
					if err != nil {
						t.Fatalf("active evidence rejected a pending obligation: %v", err)
					}
				} else if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "quiescent") {
					t.Fatalf("quiescent evidence ignored another volume's obligation: %v", err)
				}
				execute(t, db, `DELETE FROM removal_intents`)
				execute(t, db, `UPDATE entries SET draining=0,drain_authority=''`)
				if err := ValidateVolumeIntegrity(t.Context(), db, volume, 1000, 1<<20); err != nil {
					t.Fatalf("drained evidence rejected: %v", err)
				}
			})
		}
	}
}

func TestFileLeaseQuiescenceCannotBeMissing(t *testing.T) {
	db := testDatabase(t, 0)
	testVolume(t, db, "A")
	var ddl string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='file_lease_recovery'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	corruptDDL := strings.Replace(ddl, "accepted_quiescent  INTEGER NOT NULL", "accepted_quiescent  INTEGER", 1)
	if corruptDDL == ddl {
		t.Fatal("fixture did not remove the accepted quiescence NOT NULL constraint")
	}
	execute(t, db, `DROP TABLE file_lease_recovery`)
	execute(t, db, corruptDDL)
	execute(t, db, initialFileLeaseRecord)
	execute(t, db, `UPDATE file_lease_recovery SET accepted_quiescent=NULL`)
	before := migrationImage(t, db)
	if _, _, err := Prepare(t.Context(), db, "A", "", changes.DefaultWindow(), 1000, 1<<20); !errors.Is(err, syscall.EIO) {
		t.Fatalf("missing quiescence was given a default: %v", err)
	}
	if after := migrationImage(t, db); after != before {
		t.Fatal("missing quiescence was repaired during refusal")
	}
}

func TestFileLeaseGlobalDrainProofUsesItsIndex(t *testing.T) {
	db := testDatabase(t, 0)
	testVolume(t, db, "A")
	rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT 1 FROM entries WHERE draining=1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var indexed bool
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		indexed = indexed || strings.Contains(detail, "SEARCH entries USING COVERING INDEX entries_by_draining")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !indexed {
		t.Fatal("global drain proof scans entry rows")
	}
}
