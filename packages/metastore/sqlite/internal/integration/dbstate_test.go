package integration_test

import (
	"errors"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func TestOpenRefusesLoweredNodeSequenceAfterHighestNodeDeletion(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Create(t.Context(), "highest"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var highest int64
	if err := tx.QueryRow(`SELECT max(id) FROM nodes`).Scan(&highest); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM entries WHERE node = ?`, highest); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM nodes WHERE id = ?`, highest); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM sqlite_sequence WHERE name = 'nodes'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("opening after deletion of the node sequence succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening after deletion of the node sequence: %v, want EIO", err)
	}
}

func TestOpenRefusesDeletedChangeSequenceAfterHistoryWasIssued(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	if _, err := db.Exec(`DELETE FROM sqlite_sequence WHERE name = 'changes'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("opening after deletion of the change sequence succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening after deletion of the change sequence: %v, want EIO", err)
	}
}

func TestAcceptedHighWaterRefusesCoordinatedNodeSequenceRollback(t *testing.T) {
	path := database(t)
	options := sqlite.DefaultOptions()
	options.Window = sqlite.Window{Floor: 1, Cap: 1, Age: time.Hour}
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, "workspace", durableStoreID, 0, options,
		sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "highest"); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(t.Context(), "highest"); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Complete {
		t.Fatalf("full checkpoint is incomplete: %+v", checkpoint)
	}
	if checkpoint.State.NodeHighWater < 2 {
		t.Fatalf("create and delete reached node high-water %d, want at least 2", checkpoint.State.NodeHighWater)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	var maximumLiveNode, maximumLoggedNode int64
	if err := db.QueryRow(`SELECT coalesce(max(id), 0) FROM nodes`).Scan(&maximumLiveNode); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT coalesce(max(node), 0) FROM changes`).Scan(&maximumLoggedNode); err != nil {
		t.Fatal(err)
	}
	if maximumLiveNode >= checkpoint.State.NodeHighWater || maximumLoggedNode >= checkpoint.State.NodeHighWater {
		t.Fatalf("highest deleted identity remains directly visible in nodes=%d or changes=%d below high-water %d",
			maximumLiveNode, maximumLoggedNode, checkpoint.State.NodeHighWater)
	}
	if _, err := db.Exec(`UPDATE database_state SET node_high_water = ? WHERE singleton = 1`, maximumLiveNode); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE sqlite_sequence SET seq = ? WHERE name = 'nodes'`, maximumLiveNode); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	requireOpenEIO(t, path, "workspace", sqlite.DurableStartup{
		Accepted: checkpoint.State, CheckpointedGeneration: checkpoint.State.Generation,
	})
}

func TestGlobalHighWaterValidationIncludesOtherNamespaces(t *testing.T) {
	path := database(t)
	first := open(t, path, "first", 0)
	second := open(t, path, "second", 0)
	if err := second.Create(t.Context(), "retained-elsewhere"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		UPDATE database_state SET node_high_water = 1, change_high_water = 0 WHERE singleton = 1;
		UPDATE sqlite_sequence SET seq = 1 WHERE name = 'nodes';
		UPDATE sqlite_sequence SET seq = 0 WHERE name = 'changes'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.Open(t.Context(), path, "first", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("opening one namespace ignored identities retained by another namespace")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening after global high-water rollback returned %v, want EIO", err)
	}
}

func TestAppendRefusesALogTailAboveTheAllocatedPosition(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurableWithoutCleanup(t, path, witness)
	t.Cleanup(func() {
		if err := store.Abort(); err != nil {
			t.Errorf("aborting corrupt durable store: %v", err)
		}
	})
	state, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	acceptedBefore, _ := witness.accepts()
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE logs SET committed_position = ?`, state.ChangeHighWater+100); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Create(t.Context(), "must-roll-back"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("appending after a raised log tail returned %v, want EIO", err)
	}
	acceptedAfter, _ := witness.accepts()
	if len(acceptedAfter) != len(acceptedBefore) {
		t.Fatalf("refused append published %d additional states", len(acceptedAfter)-len(acceptedBefore))
	}
	db = raw(t, path)
	defer db.Close()
	var generation, nodeHighWater, changeHighWater, nodeSequence, changeSequence, created int64
	if err := db.QueryRow(`
		SELECT generation, node_high_water, change_high_water,
		       (SELECT seq FROM sqlite_sequence WHERE name = 'nodes'),
		       (SELECT seq FROM sqlite_sequence WHERE name = 'changes'),
		       (SELECT count(*) FROM entries WHERE name = CAST('must-roll-back' AS BLOB))
		FROM database_state WHERE singleton = 1`).Scan(
		&generation, &nodeHighWater, &changeHighWater, &nodeSequence, &changeSequence, &created,
	); err != nil {
		t.Fatal(err)
	}
	if generation != state.Generation || nodeHighWater != state.NodeHighWater ||
		changeHighWater != state.ChangeHighWater || nodeSequence != state.NodeHighWater ||
		changeSequence != state.ChangeHighWater || created != 0 {
		t.Fatalf("refused append changed durable state to generation=%d node=%d/%d change=%d/%d created=%d; want %+v",
			generation, nodeHighWater, nodeSequence, changeHighWater, changeSequence, created, state)
	}
}

func TestNodeIdentityExhaustionReturnsENOSPCWithoutCreatingAName(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	if _, err := db.Exec(`UPDATE database_state SET node_high_water = ? WHERE singleton = 1`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE sqlite_sequence SET seq = ? WHERE name = 'nodes'`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := open(t, path, "workspace", 0)
	if err := reopened.Create(t.Context(), "never-created"); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("creating after node identity exhaustion returned %v, want ENOSPC", err)
	}
	if _, err := reopened.Stat(t.Context(), "never-created"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed exhausted creation left a visible name: %v, want ENOENT", err)
	}
}

func TestChangePositionExhaustionRollsBackNodeAllocation(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE database_state SET change_high_water = ? WHERE singleton = 1`, int64(math.MaxInt64)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE sqlite_sequence SET seq = ? WHERE name = 'changes'`, int64(math.MaxInt64)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, path, "workspace", 0)
	before, err := reopened.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Create(t.Context(), "never-created"); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("creating after change position exhaustion returned %v, want ENOSPC", err)
	}
	if _, err := reopened.Stat(t.Context(), "never-created"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed change allocation left a visible name: %v, want ENOENT", err)
	}
	after, err := reopened.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if after.NodeHighWater != before.NodeHighWater {
		t.Fatalf("failed change allocation moved node high-water from %d to %d",
			before.NodeHighWater, after.NodeHighWater)
	}
}
