package sqlite_test

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func writePreRetainedDatabase(t *testing.T, version int) string {
	t.Helper()
	path := writeHistoricalLeaseDatabase(t, true)
	if version == 4 {
		migration, err := os.ReadFile("migrations/0004_lease_recovery.sql")
		if err != nil {
			t.Fatal(err)
		}
		damageDatabase(t, path, string(migration))
		damageDatabase(t, path, `UPDATE schema_version SET version = 4`)
	}
	return path
}

func TestWitnessedRetainedMigrationPreservesNodesAndAcceptedState(t *testing.T) {
	path := writePreRetainedDatabase(t, 4)
	before := historicalLeaseRows(t, path)
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(t.Context(), path, "A", durableStoreID, 1024,
		sqlite.DefaultOptions(), sqlite.RequireExistingNamespace, sqlite.DurableStartup{
			Accepted: historicalLeaseDurableState, CheckpointedGeneration: historicalLeaseDurableState.Generation,
		}, witness)
	if err != nil {
		t.Fatalf("migrate witnessed version 4: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	assertHistoricalLeaseNamespace(t, store, "A")
	assertHistoricalLeaseRows(t, path, before)
	assertHistoricalLeaseSchemaVersion(t, path, 5)
	db := raw(t, path)
	defer db.Close()
	var total, initialized int
	if err := db.QueryRow(`SELECT count(*), count(CASE WHEN typeof(detached) = 'integer' AND detached = 0
		AND typeof(content_revision) = 'integer' AND content_revision = 1 THEN 1 END) FROM nodes`).Scan(&total, &initialized); err != nil {
		t.Fatal(err)
	}
	if total != 4 || initialized != total {
		t.Fatalf("migrated nodes = %d, valid linked revisions = %d", total, initialized)
	}
	accepted, visible := witness.accepts()
	want := historicalLeaseDurableState
	want.Generation++
	if len(accepted) != 1 || len(visible) != 1 || accepted[0] != want || visible[0] != want {
		t.Fatalf("migration acceptance = %+v, visible = %+v; want %+v", accepted, visible, want)
	}
}

func TestWitnessedRetainedMigrationRefusesOldSchemaCorruptionWithoutChanges(t *testing.T) {
	for _, version := range []int{3, 4} {
		for _, damage := range []struct {
			name string
			sql  string
		}{
			{"unmarked orphan", `DELETE FROM entries WHERE node = 2`},
			{"foreign namespace orphan", `DELETE FROM entries WHERE node = 4`},
			{"foreign namespace undercharge", `UPDATE namespaces SET used = 0 WHERE id = 2`},
			{"invalid node scalar", `UPDATE nodes SET atime_nsec = 'damaged' WHERE id = 2`},
			{"missing object", `DELETE FROM objects WHERE key = 'historical-alpha-object'`},
			{"invalid log predecessor", `UPDATE changes SET previous_position = 2 WHERE position = 3`},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, damage.name), func(t *testing.T) {
				path := writePreRetainedDatabase(t, version)
				damageDatabase(t, path, damage.sql)
				assertRetainedMigrationRefused(t, path, version, sqlite.DefaultOptions(), syscall.EIO)
			})
		}
	}
}

func TestWitnessedRetainedMigrationBoundsOldSchemaValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		options sqlite.Options
	}{
		{"records", integrityOptions(3)},
		{"name bytes", integrityByteOptions(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writePreRetainedDatabase(t, 4)
			assertRetainedMigrationRefused(t, path, 4, test.options, syscall.EFBIG)
		})
	}
}

func assertRetainedMigrationRefused(t *testing.T, path string, version int, options sqlite.Options, want error) {
	t.Helper()
	beforeSchema := schemaOf(t, path)
	before := historicalLeaseRows(t, path)
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(t.Context(), path, "A", durableStoreID, 1024,
		options, sqlite.RequireExistingNamespace, sqlite.DurableStartup{
			Accepted: historicalLeaseDurableState, CheckpointedGeneration: historicalLeaseDurableState.Generation,
		}, witness)
	if store != nil {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if !errors.Is(err, want) {
		t.Fatalf("migrate invalid version %d = %v; want %v", version, err, want)
	}
	assertHistoricalLeaseSchemaVersion(t, path, version)
	if after := schemaOf(t, path); after != beforeSchema {
		t.Fatal("refused migration changed the schema")
	}
	assertHistoricalLeaseRows(t, path, before)
	state, err := sqlite.InspectDurableState(t.Context(), path)
	if err != nil || state != historicalLeaseDurableState {
		t.Fatalf("refused migration changed durable state: %+v, %v", state, err)
	}
	if accepted, _ := witness.accepts(); len(accepted) != 0 {
		t.Fatalf("refused migration accepted state: %+v", accepted)
	}
}
