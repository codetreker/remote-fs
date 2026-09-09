package integration_test

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/sqliteschema"
)

var historicalLeaseDurableState = sqlite.DurableState{
	DatabaseID: "33333333333333333333333333333333", Generation: 9,
	NodeHighWater: 4, ChangeHighWater: 4,
}

func writeHistoricalLeaseDatabase(t *testing.T, bound bool) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "historical.sqlite")
	fixture, err := os.ReadFile("testdata/version3.sql")
	if err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.ExecContext(t.Context(), string(fixture)); err != nil {
		db.Close()
		t.Fatalf("create frozen version 3 fixture: %v", err)
	}
	if bound {
		if _, err := db.ExecContext(t.Context(), `INSERT INTO backing_store (singleton, store_id) VALUES (1, ?)`, durableStoreID); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := sqlite.InspectDurableState(t.Context(), path)
	if err != nil || state != historicalLeaseDurableState {
		t.Fatalf("historical durable proof = %+v, %v", state, err)
	}
	return path
}

func TestHistoricalLeaseSchemaMatchesTheFirstThreeMigrations(t *testing.T) {
	historical := writeHistoricalLeaseDatabase(t, false)
	stated := database(t)
	db := raw(t, stated)
	for _, name := range []string{"0001_tree.sql", "0002_replication.sql", "0003_durable_state.sql"} {
		migration, err := os.ReadFile(filepath.Join("..", "schema", "migrations", name))
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), string(migration)); err != nil {
			db.Close()
			t.Fatalf("execute historical migration %s: %v", name, err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE schema_version (version INTEGER NOT NULL)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := sqliteschema.Structure(schemaOf(t, stated)), sqliteschema.Structure(schemaOf(t, historical)); got != want {
		t.Fatalf("the first three migrations changed the historical version 3 layout\nproduced:\n%s\nhistorical:\n%s", got, want)
	}
}

func TestHistoricalLeaseMigrationPreservesAcceptedDurableProof(t *testing.T) {
	path := writeHistoricalLeaseDatabase(t, true)
	before := historicalLeaseRows(t, path)
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(t.Context(), path, "A", durableStoreID, 1024,
		sqlite.DefaultOptions(), sqlite.RequireExistingNamespace, sqlite.DurableStartup{
			Accepted: historicalLeaseDurableState, CheckpointedGeneration: historicalLeaseDurableState.Generation,
		}, witness)
	if err != nil {
		t.Fatalf("migrate witnessed version 3: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	assertHistoricalLeaseNamespace(t, store, "A")
	assertHistoricalLeaseRows(t, path, before)
	accepted, visible := witness.accepts()
	want := historicalLeaseDurableState
	want.Generation++
	if len(accepted) != 1 || len(visible) != 1 || accepted[0] != want || visible[0] != want {
		t.Fatalf("migration witness acceptance = %+v, visible = %+v; want %+v", accepted, visible, want)
	}
	assertHistoricalLeaseSchemaVersion(t, path, 5)
}

func TestHistoricalLeaseMigrationRefusesRollbackBeforeChangingSchema(t *testing.T) {
	for _, damage := range []string{"generation", "identity", "node-watermark", "change-watermark"} {
		t.Run(damage, func(t *testing.T) {
			path := writeHistoricalLeaseDatabase(t, true)
			before := historicalLeaseRows(t, path)
			beforeSchema := schemaOf(t, path)
			accepted := historicalLeaseDurableState
			switch damage {
			case "generation":
				accepted.Generation++
			case "identity":
				accepted.DatabaseID = "44444444444444444444444444444444"
			case "node-watermark":
				accepted.NodeHighWater++
			case "change-watermark":
				accepted.ChangeHighWater++
			}
			witness := &recordingWitness{database: path}
			store, err := sqlite.OpenBoundDurableWithOptions(t.Context(), path, "A", durableStoreID, 1024,
				sqlite.DefaultOptions(), sqlite.RequireExistingNamespace,
				sqlite.DurableStartup{Accepted: accepted, CheckpointedGeneration: accepted.Generation}, witness)
			if store != nil {
				if closeErr := store.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("migrate rolled-back version 3 = %v", err)
			}
			assertHistoricalLeaseSchemaVersion(t, path, 3)
			if afterSchema := schemaOf(t, path); afterSchema != beforeSchema {
				t.Fatal("rejected durable preflight changed the historical schema")
			}
			assertHistoricalLeaseRows(t, path, before)
			state, err := sqlite.InspectDurableState(t.Context(), path)
			if err != nil || state != historicalLeaseDurableState {
				t.Fatalf("rejected migration changed durable proof: %+v, %v", state, err)
			}
			if accepted, _ := witness.accepts(); len(accepted) != 0 {
				t.Fatalf("rejected migration acknowledged state: %+v", accepted)
			}
		})
	}
}

func TestHistoricalLeaseMigrationProtectsEveryNamespaceAcrossReopen(t *testing.T) {
	path := writeHistoricalLeaseDatabase(t, false)
	before := historicalLeaseRows(t, path)
	config := sqlite.LockingConfig{
		Database: path, Namespace: "A", Allowance: 1024, SQLite: sqlite.DefaultOptions(),
		Locks: locking.DefaultOptions(), Initialize: true,
	}
	first, err := sqlite.OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatalf("enable locks while migrating historical namespace A: %v", err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Error(err)
		}
	})
	assertHistoricalLeaseNamespace(t, first.Store, "A")
	assertHistoricalLeaseRows(t, path, before)
	assertHistoricalLeaseSchemaVersion(t, path, 5)
	acquireHistoricalLease(t, first.LockService(), "alpha.txt")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if bypass, err := sqlite.OpenWithOptions(t.Context(), path, "B", 1024, sqlite.DefaultOptions()); !errors.Is(err, syscall.EIO) {
		if bypass != nil {
			if closeErr := bypass.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
		t.Fatalf("raw namespace B bypassed migrated database protection: %v", err)
	}
	config.Initialize = false
	config.Locks.MaxLease = time.Second
	for _, namespace := range []string{"B", "A"} {
		config.Namespace = namespace
		reopened, err := sqlite.OpenLocking(t.Context(), config)
		if err != nil {
			t.Fatalf("reopen historical namespace %s under database-wide recovery: %v", namespace, err)
		}
		t.Cleanup(func() {
			if err := reopened.Close(); err != nil {
				t.Error(err)
			}
		})
		assertHistoricalLeaseNamespace(t, reopened.Store, namespace)
		if maximum, err := reopened.MaxLease(t.Context()); err != nil || maximum != time.Minute {
			t.Fatalf("namespace %s maximum = %v, %v", namespace, maximum, err)
		}
		if err := reopened.Create(t.Context(), "unsafe"); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("namespace %s mutation during recovery = %v", namespace, err)
		}
		status, err := reopened.LockService().(locking.StatusService).Status(t.Context())
		if err != nil || !status.Recovering || status.RecoveryRemainingMillis <= time.Second.Milliseconds() {
			t.Fatalf("namespace %s lost historical protection: recovering=%v, remaining_ms=%d, err=%v",
				namespace, status.Recovering, status.RecoveryRemainingMillis, err)
		}
		assertHistoricalLeaseRows(t, path, before)
		state, err := reopened.DurableState(t.Context())
		if err != nil || state.DatabaseID != historicalLeaseDurableState.DatabaseID ||
			state.Generation <= historicalLeaseDurableState.Generation || state.NodeHighWater != 4 || state.ChangeHighWater != 4 {
			t.Fatalf("namespace %s changed historical lineage: %+v, %v", namespace, state, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func acquireHistoricalLease(t *testing.T, service locking.Service, path string) {
	t.Helper()
	ticket, err := service.BeginEnrollment(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.OpenSession(t.Context(), ticket)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.CreateOwner(t.Context(), session.ID, "historical-owner")
	if err != nil {
		t.Fatal(err)
	}
	resource, err := service.Resolve(t.Context(), owner.Ref, path)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := service.Acquire(t.Context(), locking.AcquireRequest{
		Owner: owner.Ref, Request: "historical-grant", Resource: resource, Mode: locking.Exclusive, TTL: time.Minute,
	})
	if err != nil || grant.Receipt.Outcome != locking.Granted {
		t.Fatalf("acquire historical namespace lease: outcome=%v, err=%v", grant.Receipt.Outcome, err)
	}
}

func assertHistoricalLeaseNamespace(t *testing.T, store *sqlite.Store, namespace string) {
	t.Helper()
	name, id, root, size, mode := "alpha.txt", int64(2), int64(1), int64(5), os.FileMode(0o640)
	content := metastore.Key("historical-alpha-object")
	seconds, accessNanos, modifiedNanos := int64(1700000100), int64(201), int64(202)
	incarnation := metastore.Incarnation("11111111111111111111111111111111")
	positions := []metastore.Position{1, 3}
	if namespace == "B" {
		name, id, root, size, mode = "bravo.txt", 4, 3, 7, 0o600
		content = "historical-bravo-object"
		seconds, accessNanos, modifiedNanos = 1700000300, 401, 402
		incarnation = "22222222222222222222222222222222"
		positions = []metastore.Position{2, 4}
	}
	node, err := store.Stat(t.Context(), name)
	if err != nil || node.ID != id || node.Size != size || node.Mode != mode || node.Content != content ||
		!node.AccessTime.Equal(time.Unix(seconds, accessNanos)) || !node.ModTime.Equal(time.Unix(seconds, modifiedNanos)) {
		t.Fatalf("namespace %s historical node changed: %+v, %v", namespace, node, err)
	}
	children, err := store.List(t.Context(), "")
	if err != nil || len(children) != 1 || string(children[0].Name) != name || children[0].Node.ID != id {
		t.Fatalf("namespace %s historical directory changed: %+v, %v", namespace, children, err)
	}
	gotIncarnation, err := store.Incarnation(t.Context(), 64)
	if err != nil || gotIncarnation != incarnation {
		t.Fatalf("namespace %s historical log incarnation changed: %q, %v", namespace, gotIncarnation, err)
	}
	changes, retained, err := readChanges(t.Context(), store, 0, 10)
	if err != nil || len(changes) != 2 || retained.Oldest != positions[0] || retained.Tail != positions[1] || retained.TrimmedThrough != 0 {
		t.Fatalf("namespace %s historical retention changed: %d changes, %+v, %v", namespace, len(changes), retained, err)
	}
	for i, change := range changes {
		kind := metastore.Created
		if i == 1 {
			kind = metastore.Modified
		}
		if change.Position != positions[i] || change.Kind != kind || change.Parent != root || string(change.Name) != name ||
			change.From != nil || change.Node == nil || change.Node.ID != id || change.Node.Content != content || change.Node.Size != size {
			t.Fatalf("namespace %s historical change %d was not preserved", namespace, i)
		}
	}
}

func historicalLeaseReadOnly(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func historicalLeaseRows(t *testing.T, path string) map[string][][]any {
	t.Helper()
	db := historicalLeaseReadOnly(t, path)
	result := make(map[string][][]any)
	for table, query := range map[string]string{
		"backing_store": `SELECT * FROM backing_store ORDER BY singleton`,
		"namespaces":    `SELECT * FROM namespaces ORDER BY id`,
		"nodes": `SELECT id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content
			FROM nodes ORDER BY id`,
		"entries":         `SELECT * FROM entries ORDER BY namespace, parent, name`,
		"objects":         `SELECT * FROM objects ORDER BY key`,
		"logs":            `SELECT * FROM logs ORDER BY namespace`,
		"changes":         `SELECT * FROM changes ORDER BY position`,
		"sqlite_sequence": `SELECT * FROM sqlite_sequence ORDER BY name`,
	} {
		result[table] = nil
		rows, err := db.QueryContext(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, targets := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if blob, ok := value.([]byte); ok {
					values[i] = bytes.Clone(blob)
				}
			}
			result[table] = append(result[table], values)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertHistoricalLeaseRows(t *testing.T, path string, before map[string][][]any) {
	t.Helper()
	after := historicalLeaseRows(t, path)
	for table, original := range before {
		if !reflect.DeepEqual(original, after[table]) {
			t.Fatalf("historical %s rows changed during lease migration or recovery", table)
		}
	}
}

func assertHistoricalLeaseSchemaVersion(t *testing.T, path string, want int) {
	t.Helper()
	db := historicalLeaseReadOnly(t, path)
	var version, leaseTables int
	if err := db.QueryRowContext(t.Context(), `SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='lease_recovery'`).Scan(&leaseTables); err != nil {
		t.Fatal(err)
	}
	if version != want || (want == 3 && leaseTables != 0) || (want == 4 && leaseTables != 1) {
		t.Fatalf("schema version=%d, lease tables=%d; want version %d", version, leaseTables, want)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
