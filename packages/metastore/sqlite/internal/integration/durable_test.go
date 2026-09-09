package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
)

const durableStoreID = "durable-test-store"

type recordingWitness struct {
	mu sync.Mutex

	database      string
	acceptErr     error
	checkpointErr error
	accepted      []sqlite.DurableState
	visible       []sqlite.DurableState
	checkpointed  []sqlite.DurableState
}

type blockingWitness struct {
	recordingWitness

	blockMu sync.Mutex
	entered chan struct{}
	release chan struct{}
	failure error
}

type blockingCheckpointWitness struct {
	recordingWitness
	entered chan struct{}
	release chan struct{}
}

func (w *blockingCheckpointWitness) Checkpoint(state sqlite.DurableState) error {
	select {
	case <-w.entered:
	default:
		close(w.entered)
	}
	<-w.release
	return w.recordingWitness.Checkpoint(state)
}

func (w *blockingWitness) Accept(state sqlite.DurableState) error {
	if err := w.recordingWitness.Accept(state); err != nil {
		return err
	}
	w.blockMu.Lock()
	entered, release, failure := w.entered, w.release, w.failure
	w.blockMu.Unlock()
	if entered == nil {
		return nil
	}
	select {
	case <-entered:
	default:
		close(entered)
	}
	<-release
	return failure
}

func (w *blockingWitness) blockNextAccept(err error) (<-chan struct{}, chan<- struct{}) {
	w.blockMu.Lock()
	defer w.blockMu.Unlock()
	w.entered = make(chan struct{})
	w.release = make(chan struct{})
	w.failure = err
	return w.entered, w.release
}

func (w *recordingWitness) Accept(state sqlite.DurableState) error {
	var visible sqlite.DurableState
	var inspectErr error
	if w.database != "" {
		visible, inspectErr = sqlite.InspectDurableState(context.Background(), w.database)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.accepted = append(w.accepted, state)
	w.visible = append(w.visible, visible)
	if inspectErr != nil {
		return fmt.Errorf("inspecting committed state from Accept: %w", inspectErr)
	}
	return w.acceptErr
}

func (w *recordingWitness) Checkpoint(state sqlite.DurableState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checkpointed = append(w.checkpointed, state)
	return w.checkpointErr
}

func (w *recordingWitness) failAcceptWith(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.acceptErr = err
}

func (w *recordingWitness) failCheckpointWith(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checkpointErr = err
}

func (w *recordingWitness) accepts() ([]sqlite.DurableState, []sqlite.DurableState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sqlite.DurableState(nil), w.accepted...), append([]sqlite.DurableState(nil), w.visible...)
}

func (w *recordingWitness) checkpoints() []sqlite.DurableState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sqlite.DurableState(nil), w.checkpointed...)
}

func openDurable(
	t *testing.T,
	path, namespace string,
	mode sqlite.NamespaceOpenMode,
	startup sqlite.DurableStartup,
	witness sqlite.CommitWitness,
) *sqlite.Store {
	t.Helper()
	store, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, namespace, durableStoreID, 0, sqlite.DefaultOptions(), mode, startup, witness,
	)
	if err != nil {
		t.Fatalf("opening durable namespace %q: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing durable namespace %q: %v", namespace, err)
		}
	})
	return store
}

func openDurableWithoutCleanup(
	t *testing.T,
	path string,
	witness sqlite.CommitWitness,
) *sqlite.Store {
	t.Helper()
	store, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, "workspace", durableStoreID, 0, sqlite.DefaultOptions(),
		sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness,
	)
	if err != nil {
		t.Fatalf("opening durable namespace: %v", err)
	}
	return store
}

func initializeCheckpointedDurableStore(t *testing.T, path string) sqlite.DurableState {
	t.Helper()
	witness := &recordingWitness{database: path}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	result, err := store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
	if err != nil {
		t.Fatalf("checkpointing initialized database: %v", err)
	}
	if !result.Complete {
		t.Fatalf("initial full checkpoint is incomplete: %+v", result)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing initialized database: %v", err)
	}
	return result.State
}

func requireOpenEIO(
	t *testing.T,
	path, namespace string,
	startup sqlite.DurableStartup,
) {
	t.Helper()
	store, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, namespace, durableStoreID, 0, sqlite.DefaultOptions(),
		sqlite.RequireExistingNamespace, startup, &recordingWitness{database: path},
	)
	if err == nil {
		store.Close()
		t.Fatal("opening inconsistent durable state succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening inconsistent durable state: %v, want EIO", err)
	}
}

func TestRequireExistingDurableNamespaceRefusesToCreateAMissingNamespace(t *testing.T) {
	path := database(t)
	accepted := initializeCheckpointedDurableStore(t, path)
	witness := &recordingWitness{database: path}
	store, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, "missing", durableStoreID, 0, sqlite.DefaultOptions(),
		sqlite.RequireExistingNamespace,
		sqlite.DurableStartup{Accepted: accepted, CheckpointedGeneration: accepted.Generation},
		witness,
	)
	if err == nil {
		store.Close()
		t.Fatal("requiring a missing namespace succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("requiring a missing namespace: %v, want EIO", err)
	}
	if acceptedAfter, _ := witness.accepts(); len(acceptedAfter) != 0 {
		t.Fatalf("a refused open published accepted states: %+v", acceptedAfter)
	}

	db := raw(t, path)
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM namespaces WHERE name = 'missing'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("a refused open created %d missing namespace rows", count)
	}
}

func TestRequireExistingWithoutAnAcceptedWitnessDoesNotRebuildALostNamespace(t *testing.T) {
	path := database(t)
	initializeCheckpointedDurableStore(t, path)
	db := raw(t, path)
	if _, err := db.Exec(`DELETE FROM namespaces WHERE name = 'workspace'`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, "workspace", durableStoreID, 0, sqlite.DefaultOptions(),
		sqlite.RequireExistingNamespace, sqlite.DurableStartup{}, &recordingWitness{database: path},
	)
	if err == nil {
		store.Close()
		t.Fatal("requiring a lost unwitnessed namespace rebuilt it")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("requiring a lost unwitnessed namespace: %v, want EIO", err)
	}
	db = raw(t, path)
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM namespaces WHERE name = 'workspace'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("refused recovery created %d replacement workspace rows", count)
	}
}

func TestAcceptObservesCommittedMonotonicDurableState(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)

	for _, mutate := range []func() error{
		func() error { return store.Create(t.Context(), "file") },
		func() error { return store.Mkdir(t.Context(), "directory") },
		func() error { return store.Rename(t.Context(), "file", "renamed") },
	} {
		if err := mutate(); err != nil {
			t.Fatal(err)
		}
	}

	accepted, visible := witness.accepts()
	if len(accepted) != 4 {
		t.Fatalf("open plus three commits published %d accepted states, want 4: %+v", len(accepted), accepted)
	}
	for i, state := range accepted {
		if visible[i] != state {
			t.Fatalf("Accept %d received %+v before SQLite exposed it; inspection saw %+v", i, state, visible[i])
		}
		if i == 0 {
			continue
		}
		previous := accepted[i-1]
		if state.DatabaseID != previous.DatabaseID || state.Generation <= previous.Generation ||
			state.NodeHighWater < previous.NodeHighWater || state.ChangeHighWater < previous.ChangeHighWater {
			t.Fatalf("accepted state moved backward from %+v to %+v", previous, state)
		}
	}
	current, err := store.DurableState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if current != accepted[len(accepted)-1] {
		t.Fatalf("store exposes %+v after Accept published %+v", current, accepted[len(accepted)-1])
	}
}

func TestAcceptFailureReturnsEIOAndPoisonsReadsAndWrites(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurableWithoutCleanup(t, path, witness)
	witness.failAcceptWith(errors.New("witness unavailable"))

	if err := store.Create(t.Context(), "committed-but-unaccepted"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("mutation with failed Accept returned %v, want EIO", err)
	}

	checks := map[string]func() error{
		"stat": func() error {
			_, err := store.Stat(t.Context(), "")
			return err
		},
		"list": func() error {
			_, err := store.List(t.Context(), "")
			return err
		},
		"create": func() error {
			return store.Create(t.Context(), "after-poison")
		},
		"space": func() error {
			_, err := store.Space(t.Context())
			return err
		},
		"object status": func() error {
			_, err := store.ObjectStatus(t.Context())
			return err
		},
		"garbage": func() error {
			_, err := store.Garbage(t.Context(), 1)
			return err
		},
		"bounded list": func() error {
			result, err := storage.NewListResult(1024, 0, func(_ int, nameBytes int64, _ storage.Attr) (int64, error) {
				return nameBytes, nil
			})
			if err != nil {
				return err
			}
			return store.ListBounded(t.Context(), "", result)
		},
		"durable state": func() error {
			_, err := store.DurableState(t.Context())
			return err
		},
		"committed position": func() error {
			_, err := store.CommittedPosition(t.Context())
			return err
		},
		"since": func() error {
			_, _, err := readChanges(t.Context(), store, 0, 10)
			return err
		},
		"snapshot": func() error {
			snapshot, _, err := store.Snapshot(t.Context())
			if err == nil {
				err = snapshot.Close()
			}
			return err
		},
		"checkpoint": func() error {
			_, err := store.Checkpoint(t.Context(), sqlite.PassiveCheckpoint)
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("poisoned %s returned %v, want EIO", name, err)
			}
		})
	}
}

func TestCommittedStateIsNotVisibleWhileAcceptIsUnresolved(t *testing.T) {
	path := database(t)
	witness := &blockingWitness{recordingWitness: recordingWitness{database: path}}
	store := openDurableWithoutCleanup(t, path, witness)
	entered, release := witness.blockNextAccept(errors.New("witness publication failed"))

	mutation := make(chan error, 1)
	go func() { mutation <- store.Create(t.Context(), "unaccepted") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("mutation did not reach its post-commit witness")
	}

	read := make(chan error, 1)
	go func() {
		_, err := store.Stat(t.Context(), "unaccepted")
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("a read crossed unresolved witness publication with %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-mutation; !errors.Is(err, syscall.EIO) {
		t.Fatalf("failed witness mutation returned %v, want EIO", err)
	}
	if err := <-read; !errors.Is(err, syscall.EIO) {
		t.Fatalf("read released after witness failure returned %v, want EIO", err)
	}
}

func TestCheckpointWitnessFailureCanBeRetried(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	witness.failCheckpointWith(errors.New("checkpoint witness unavailable"))
	if _, err := store.Checkpoint(t.Context(), sqlite.FullCheckpoint); !errors.Is(err, syscall.EIO) {
		t.Fatalf("checkpoint witness failure returned %v, want EIO", err)
	}
	if _, err := store.Stat(t.Context(), "file"); err != nil {
		t.Fatalf("checkpoint witness failure poisoned an accepted namespace: %v", err)
	}
	witness.failCheckpointWith(nil)
	result, err := store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
	if err != nil {
		t.Fatalf("retrying checkpoint witness publication: %v", err)
	}
	if !result.Complete {
		t.Fatalf("retried checkpoint is incomplete: %+v", result)
	}
}

func TestDurableStartupReconciliationRefusesLostOrUnacceptedState(t *testing.T) {
	t.Run("accepted generation is newer than visible database", func(t *testing.T) {
		path := database(t)
		visible := initializeCheckpointedDurableStore(t, path)
		accepted := visible
		accepted.Generation++
		requireOpenEIO(t, path, "workspace", sqlite.DurableStartup{
			Accepted: accepted, CheckpointedGeneration: accepted.Generation,
		})
	})

	t.Run("accepted state needs a missing WAL", func(t *testing.T) {
		path := database(t)
		accepted := initializeCheckpointedDurableStore(t, path)
		requireOpenEIO(t, path, "workspace", sqlite.DurableStartup{
			Accepted: accepted, CheckpointedGeneration: accepted.Generation - 1,
			WALPresent: false, WALNonEmpty: false,
		})
	})

	t.Run("visible state is newer than accepted without a WAL", func(t *testing.T) {
		path := database(t)
		witness := &recordingWitness{database: path}
		store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
		if err := store.Create(t.Context(), "newer"); err != nil {
			t.Fatal(err)
		}
		result, err := store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Complete {
			t.Fatalf("full checkpoint is incomplete: %+v", result)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		accepted, _ := witness.accepts()
		if len(accepted) < 2 {
			t.Fatalf("open and mutation published %d states, want at least 2", len(accepted))
		}
		older := accepted[0]
		if result.State.Generation <= older.Generation {
			t.Fatalf("visible state %+v is not newer than accepted state %+v", result.State, older)
		}
		requireOpenEIO(t, path, "workspace", sqlite.DurableStartup{
			Accepted: older, CheckpointedGeneration: older.Generation,
			WALPresent: false, WALNonEmpty: false,
		})
	})
}

func TestOpenAcceptFailurePreservesWALForTheNextRecovery(t *testing.T) {
	path := database(t)
	initialWitness := &recordingWitness{database: path}
	initial := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, initialWitness)
	if err := initial.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	accepted, _ := initialWitness.accepts()
	stable := accepted[len(accepted)-1]

	failingWitness := &recordingWitness{database: path, acceptErr: errors.New("publish failed")}
	failed, err := sqlite.OpenBoundDurableWithOptions(
		t.Context(), path, "workspace", durableStoreID, 0, sqlite.DefaultOptions(),
		sqlite.RequireExistingNamespace,
		sqlite.DurableStartup{Accepted: stable, CheckpointedGeneration: stable.Generation},
		failingWitness,
	)
	if failed != nil {
		failed.Abort()
		t.Fatal("open with a failed Accept returned a Store")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("open with a failed Accept returned %v, want EIO", err)
	}
	wal, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatalf("failed open removed the recovery WAL: %v", err)
	}
	if wal.Size() == 0 {
		t.Fatal("failed open left an empty recovery WAL")
	}

	recovered := openDurable(
		t, path, "workspace", sqlite.RequireExistingNamespace,
		sqlite.DurableStartup{
			Accepted: stable, CheckpointedGeneration: stable.Generation,
			WALPresent: true, WALNonEmpty: true,
		},
		&recordingWitness{database: path},
	)
	if _, err := recovered.Stat(t.Context(), "file"); err != nil {
		t.Fatalf("recovery after open-time Accept failure lost the namespace: %v", err)
	}
}

func TestSuccessfulDurableCloseReleasesPersistentWALAllocation(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurable(
		t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness,
	)
	snapshot, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 64 {
		if err := store.Create(t.Context(), fmt.Sprintf("file-%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	walPath := path + "-wal"
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() <= 32 {
		t.Fatalf("pinned snapshot did not grow the WAL: %d bytes", before.Size())
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	walPresent := false
	walNonEmpty := false
	if after, err := os.Stat(walPath); err == nil {
		walPresent = true
		walNonEmpty = after.Size() > 32
		if walNonEmpty {
			t.Fatalf("successful close retained a %d-byte WAL after %d-byte growth",
				after.Size(), before.Size())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	accepted, _ := witness.accepts()
	latest := accepted[len(accepted)-1]
	reopened := openDurable(
		t, path, "workspace", sqlite.RequireExistingNamespace,
		sqlite.DurableStartup{
			Accepted: latest, CheckpointedGeneration: latest.Generation,
			WALPresent: walPresent, WALNonEmpty: walNonEmpty,
		},
		&recordingWitness{database: path},
	)
	if _, err := reopened.Stat(t.Context(), "file-063"); err != nil {
		t.Fatalf("reopening after WAL release lost committed metadata: %v", err)
	}
}

func TestCheckpointPublishesOnlyCompleteState(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	if err := store.Create(t.Context(), "before-snapshot"); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "after-snapshot"); err != nil {
		t.Fatal(err)
	}

	pinned, err := store.Checkpoint(t.Context(), sqlite.PassiveCheckpoint)
	if err != nil {
		t.Fatalf("passive checkpoint with pinned snapshot: %v", err)
	}
	if pinned.Complete {
		t.Fatalf("checkpoint crossed a pinned snapshot: %+v", pinned)
	}
	if checkpoints := witness.checkpoints(); len(checkpoints) != 0 {
		t.Fatalf("incomplete checkpoint published %+v", checkpoints)
	}

	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	complete, err := store.Checkpoint(t.Context(), sqlite.FullCheckpoint)
	if err != nil {
		t.Fatalf("full checkpoint after releasing snapshot: %v", err)
	}
	if !complete.Complete {
		t.Fatalf("full checkpoint remained incomplete after releasing snapshot: %+v", complete)
	}
	checkpoints := witness.checkpoints()
	if len(checkpoints) != 1 || checkpoints[0] != complete.State {
		t.Fatalf("complete checkpoint published %+v, want exactly %+v", checkpoints, complete.State)
	}
}

func TestDurableCloseKeepsTheDatabaseOpenUntilCheckpointIsWitnessed(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	if err := store.Create(t.Context(), "before-snapshot"); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "after-snapshot"); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("closing with an uncheckpointable WAL returned %v, want EBUSY", err)
	}
	if _, err := store.Stat(t.Context(), "after-snapshot"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("a store after Close began returned %v, want EIO", err)
	}
	if checkpoints := witness.checkpoints(); len(checkpoints) != 0 {
		t.Fatalf("a refused close published checkpoint state %+v", checkpoints)
	}

	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing after releasing the WAL reader: %v", err)
	}
	if checkpoints := witness.checkpoints(); len(checkpoints) != 1 {
		t.Fatalf("successful close published %d checkpoint states, want 1: %+v", len(checkpoints), checkpoints)
	}
	accepted, _ := witness.accepts()
	latest := accepted[len(accepted)-1]
	reopenedWitness := &recordingWitness{database: path}
	reopened := openDurable(
		t, path, "workspace", sqlite.RequireExistingNamespace,
		sqlite.DurableStartup{Accepted: latest, CheckpointedGeneration: latest.Generation},
		reopenedWitness,
	)
	if _, err := reopened.Stat(t.Context(), "after-snapshot"); err != nil {
		t.Fatalf("reopening after witnessed close lost committed metadata: %v", err)
	}
}

func TestDurableCloseRetriesCheckpointWitnessBeforeReleasingSQLite(t *testing.T) {
	path := database(t)
	witness := &recordingWitness{database: path}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	witness.failCheckpointWith(errors.New("temporary witness failure"))
	if err := store.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("close with failed checkpoint witness returned %v, want EIO", err)
	}
	witness.failCheckpointWith(nil)
	if err := store.Close(); err != nil {
		t.Fatalf("retrying close after witness recovery: %v", err)
	}

	accepted, _ := witness.accepts()
	latest := accepted[len(accepted)-1]
	reopened := openDurable(
		t, path, "workspace", sqlite.RequireExistingNamespace,
		sqlite.DurableStartup{Accepted: latest, CheckpointedGeneration: latest.Generation},
		&recordingWitness{database: path},
	)
	if _, err := reopened.Stat(t.Context(), "file"); err != nil {
		t.Fatalf("reopening after retried close lost the accepted namespace: %v", err)
	}
}

func TestDurableCloseExcludesANewerMutationThroughWriterClose(t *testing.T) {
	path := database(t)
	witness := &blockingCheckpointWitness{
		recordingWitness: recordingWitness{database: path},
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	if err := store.Create(t.Context(), "before-close"); err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	select {
	case <-witness.entered:
	case <-time.After(time.Second):
		t.Fatal("Close did not reach checkpoint witness publication")
	}
	mutated := make(chan error, 1)
	go func() { mutated <- store.Create(t.Context(), "racing") }()
	select {
	case err := <-mutated:
		t.Fatalf("mutation crossed checkpoint-to-writer-close serialization with %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(witness.release)
	if err := <-closed; err != nil {
		t.Fatalf("closing the durable store: %v", err)
	}
	if err := <-mutated; !errors.Is(err, syscall.EIO) {
		t.Fatalf("mutation released after Close returned %v, want EIO", err)
	}
	accepted, _ := witness.accepts()
	if len(accepted) != 2 {
		t.Fatalf("racing mutation published a durable state: %+v", accepted)
	}
}

func TestCommitGateAndCloseContextHonorCancellation(t *testing.T) {
	path := database(t)
	witness := &blockingWitness{recordingWitness: recordingWitness{database: path}}
	store := openDurable(t, path, "workspace", sqlite.CreateNamespaceIfMissing, sqlite.DurableStartup{}, witness)
	entered, release := witness.blockNextAccept(nil)
	first := make(chan error, 1)
	go func() { first <- store.Create(t.Context(), "holding-gate") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first mutation did not hold the commit gate in Accept")
	}

	mutationContext, cancelMutation := context.WithCancel(t.Context())
	cancelMutation()
	if err := store.Create(mutationContext, "canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("mutation canceled behind the commit gate returned %v", err)
	}
	closeContext, cancelClose := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelClose()
	if err := store.CloseContext(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext blocked behind the commit gate returned %v", err)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("releasing the first mutation: %v", err)
	}
	if err := store.CloseContext(t.Context()); err != nil {
		t.Fatalf("retrying CloseContext after the gate was released: %v", err)
	}
}
