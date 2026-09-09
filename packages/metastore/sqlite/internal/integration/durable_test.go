package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
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

func TestMiddleRetainedChangeDeletionIsRefusedByOpenAndSince(t *testing.T) {
	deleteMiddle := func(t *testing.T, path string) {
		t.Helper()
		db := raw(t, path)
		defer db.Close()
		result, err := db.Exec(`
			DELETE FROM changes
			WHERE position = (
				SELECT position FROM changes
				WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
				ORDER BY position
				LIMIT 1 OFFSET 1
			)`)
		if err != nil {
			t.Fatal(err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if deleted != 1 {
			t.Fatalf("deleted %d middle changes, want 1", deleted)
		}
	}
	populate := func(t *testing.T, path string) *sqlite.Store {
		t.Helper()
		store := open(t, path, "workspace", 0)
		for _, name := range []string{"first", "middle", "last"} {
			if err := store.Create(t.Context(), name); err != nil {
				t.Fatal(err)
			}
		}
		return store
	}

	t.Run("open", func(t *testing.T) {
		path := database(t)
		store := populate(t, path)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		deleteMiddle(t, path)

		reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
		if err == nil {
			reopened.Close()
			t.Fatal("opening a log missing a middle retained change succeeded")
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("opening a log missing a middle retained change: %v, want EIO", err)
		}
	})

	t.Run("since", func(t *testing.T) {
		path := database(t)
		store := populate(t, path)
		deleteMiddle(t, path)

		changes, _, err := readChanges(t.Context(), store, 0, 100)
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("reading a log missing a middle retained change returned %v, want EIO", err)
		}
		if changes != nil {
			t.Fatalf("failed Since exposed changes: %+v", changes)
		}
	})

	t.Run("snapshot and status", func(t *testing.T) {
		path := database(t)
		store := populate(t, path)
		deleteMiddle(t, path)

		if snapshot, _, err := store.Snapshot(t.Context()); !errors.Is(err, syscall.EIO) {
			if snapshot != nil {
				snapshot.Close()
			}
			t.Fatalf("snapshot over a missing middle change returned %v, want EIO", err)
		}
		if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
			t.Fatalf("object status over a missing middle change returned %v, want EIO", err)
		}
	})
}

func TestFullIntegrityRejectsPredecessorAndTrimAnchorCorruption(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, string) *sqlite.Store
		damage  string
	}{
		{
			name: "previous position",
			prepare: func(t *testing.T, path string) *sqlite.Store {
				store := open(t, path, "workspace", 0)
				for _, name := range []string{"first", "second"} {
					if err := store.Create(t.Context(), name); err != nil {
						t.Fatal(err)
					}
				}
				return store
			},
			damage: `
				UPDATE changes SET previous_position = 0
				WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
				  AND position = (
					SELECT position FROM changes
					WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
					ORDER BY position LIMIT 1 OFFSET 1
				  )`,
		},
		{
			name: "trim anchor",
			prepare: func(t *testing.T, path string) *sqlite.Store {
				workspace := open(t, path, "workspace", 0)
				neighbour := open(t, path, "neighbour", 0)
				if err := neighbour.Create(t.Context(), "gap"); err != nil {
					t.Fatal(err)
				}
				if err := workspace.Create(t.Context(), "file"); err != nil {
					t.Fatal(err)
				}
				return workspace
			},
			damage: `
				UPDATE logs SET trimmed_through = 1
				WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			store := test.prepare(t, path)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if snapshot, _, err := store.Snapshot(t.Context()); !errors.Is(err, syscall.EIO) {
				if snapshot != nil {
					snapshot.Close()
				}
				t.Fatalf("snapshot over corrupt continuity returned %v, want EIO", err)
			}
			if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("status over corrupt continuity returned %v, want EIO", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
			if err == nil {
				reopened.Close()
				t.Fatal("opening corrupt continuity succeeded")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("opening corrupt continuity returned %v, want EIO", err)
			}
		})
	}
}

func TestSinceBoundsItsFullContinuityCheck(t *testing.T) {
	path := database(t)
	options := sqlite.DefaultOptions()
	options.MaxIntegrityRecords = sqlite.MinIntegrityRecords
	store, err := sqlite.OpenWithOptions(t.Context(), path, "workspace", 0, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	for _, name := range []string{"first", "second"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	changes, _, err := readChanges(t.Context(), store, 0, 100)
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("reading an over-limit continuity chain returned %v, want EFBIG", err)
	}
	if changes != nil {
		t.Fatalf("an over-limit continuity check exposed changes: %+v", changes)
	}
}

func TestSinceDoesNotHoldTheHealthGateWhileChargingAResult(t *testing.T) {
	store := open(t, database(t), "workspace", 0)
	if err := store.Create(t.Context(), "before"); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	result, err := metastore.NewChangeResult(1<<20, 0,
		func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return 256 + lengths.Name + lengths.FromName + lengths.Content, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() {
		_, err := store.Since(t.Context(), 0, 1, result)
		read <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Since did not reach caller-owned result accounting")
	}

	written := make(chan error, 1)
	go func() { written <- store.Create(t.Context(), "during-read") }()
	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("writer running beside a pinned Since snapshot: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller-owned Since accounting blocked the writer commit boundary")
	}
	close(release)
	if err := <-read; err != nil {
		t.Fatalf("Since after releasing result accounting: %v", err)
	}
}

func TestSinceDetectsAGapWhenTheNextPageReachesIt(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	for _, name := range []string{"first", "second", "third"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	firstPage, _, err := readChanges(t.Context(), store, 0, 1)
	if err != nil || len(firstPage) != 1 {
		t.Fatalf("reading the first page returned %d changes, %v", len(firstPage), err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		DELETE FROM changes
		WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
		  AND position = (
			SELECT position FROM changes
			WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
			  AND position > ?
			ORDER BY position LIMIT 1
		  )`, firstPage[0].Position); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	nextPage, _, err := readChanges(t.Context(), store, firstPage[0].Position, 1)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading the page which crosses a missing change returned %v, want EIO", err)
	}
	if nextPage != nil {
		t.Fatalf("the failed page exposed changes: %+v", nextPage)
	}
}

func TestSinceCancellationDuringResultAccountingFailsTheWholePage(t *testing.T) {
	store := open(t, database(t), "workspace", 0)
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	result, err := metastore.NewChangeResult(1<<20, 0,
		func(_ int, _ metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) {
			close(entered)
			<-release
			return 1, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := store.Since(ctx, 0, 1, result)
		done <- err
	}()
	<-entered
	cancel()
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceling Since during result accounting returned %v", err)
	}
	if changes, err := result.Changes(); changes != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Since exposed %+v, %v", changes, err)
	}
}

func TestSinceRejectsScalarStorageCorruptionOnTheCrossedPage(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	for _, name := range []string{"first", "second", "third"} {
		if err := store.Create(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	firstPage, _, err := readChanges(t.Context(), store, 0, 2)
	if err != nil || len(firstPage) != 2 {
		t.Fatalf("reading the first page returned %d changes, %v", len(firstPage), err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`
		UPDATE changes SET previous_position = 'broken'
		WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
		  AND position = (
			SELECT position FROM changes
			WHERE namespace = (SELECT id FROM namespaces WHERE name = 'workspace')
			  AND position > ? ORDER BY position LIMIT 1
		  )`, firstPage[len(firstPage)-1].Position); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	page, _, err := readChanges(t.Context(), store, firstPage[len(firstPage)-1].Position, 2)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading a text predecessor returned %v, want EIO", err)
	}
	if page != nil {
		t.Fatalf("scalar-corrupt page exposed changes: %+v", page)
	}
}

func TestLiveLogReadersRejectScalarStorageCorruption(t *testing.T) {
	tests := []struct {
		name   string
		damage string
		read   func(*sqlite.Store) error
	}{
		{
			"barrier position text",
			`UPDATE logs SET committed_position = 'broken'`,
			func(store *sqlite.Store) error {
				_, err := store.Barrier(t.Context(), 1024)
				return err
			},
		},
		{
			"committed position blob",
			`UPDATE logs SET committed_position = CAST(committed_position AS BLOB)`,
			func(store *sqlite.Store) error {
				_, err := store.CommittedPosition(t.Context())
				return err
			},
		},
		{
			"barrier incarnation blob",
			`UPDATE logs SET incarnation = CAST(incarnation AS BLOB)`,
			func(store *sqlite.Store) error {
				_, err := store.Barrier(t.Context(), 1024)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			store := open(t, path, "workspace", 0)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := test.read(store); !errors.Is(err, syscall.EIO) {
				t.Fatalf("reading a corrupt live log returned %v, want EIO", err)
			}
		})
	}
}

func TestLiveReadersRejectLargeBlobScalarsBeforeMaterializingThem(t *testing.T) {
	tests := []struct {
		name   string
		damage string
		read   func(*sqlite.Store) error
	}{
		{
			"database identity",
			`PRAGMA ignore_check_constraints = ON;
			 UPDATE database_state SET database_id = CAST(zeroblob(4 * 1024 * 1024) AS TEXT)`,
			func(store *sqlite.Store) error {
				_, err := store.DurableState(t.Context())
				return err
			},
		},
		{
			"global root identity",
			`UPDATE namespaces SET root = zeroblob(4 * 1024 * 1024) WHERE name = 'workspace'`,
			func(store *sqlite.Store) error {
				_, err := store.DurableState(t.Context())
				return err
			},
		},
		{
			"change mode",
			`UPDATE changes SET mode = zeroblob(4 * 1024 * 1024)
			 WHERE position = (SELECT min(position) FROM changes)`,
			func(store *sqlite.Store) error {
				result, err := metastore.NewChangeResult(1<<20, 0,
					func(_ int, _ metastore.Change, _ metastore.ChangePayloadLengths) (int64, error) {
						return 1, nil
					})
				if err != nil {
					return err
				}
				_, err = store.Since(t.Context(), 0, 100, result)
				if changes, resultErr := result.Changes(); changes != nil || !errors.Is(resultErr, syscall.EIO) {
					return fmt.Errorf("failed Since exposed %+v, %v", changes, resultErr)
				}
				return err
			},
		},
		{
			"log age flag",
			`UPDATE logs SET trimmed_by_age = zeroblob(4 * 1024 * 1024)`,
			func(store *sqlite.Store) error {
				_, err := store.Barrier(t.Context(), 1024)
				return err
			},
		},
		{
			"snapshot committed position",
			`UPDATE logs SET committed_position = zeroblob(4 * 1024 * 1024)`,
			func(store *sqlite.Store) error {
				snapshot, _, err := store.Snapshot(t.Context())
				if snapshot != nil {
					snapshot.Close()
				}
				return err
			},
		},
		{
			"append committed position",
			`UPDATE logs SET committed_position = zeroblob(4 * 1024 * 1024)`,
			func(store *sqlite.Store) error {
				return store.Create(t.Context(), "after-corruption")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			store := open(t, path, "workspace", 0)
			if err := store.Create(t.Context(), "file"); err != nil {
				t.Fatal(err)
			}
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := test.read(store); !errors.Is(err, syscall.EIO) {
				t.Fatalf("reading a large corrupt scalar returned %v, want EIO", err)
			}
		})
	}
}

func TestLegacyMigrationRejectsLargeBlobIdentityScalars(t *testing.T) {
	tests := []struct {
		name   string
		write  func(*testing.T, string)
		damage string
	}{
		{
			"version one root",
			writeVersionOne,
			`UPDATE namespaces SET root = zeroblob(4 * 1024 * 1024)`,
		},
		{
			"version two committed position",
			writeVersionTwo,
			`UPDATE logs SET committed_position = zeroblob(4 * 1024 * 1024)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			test.write(t, path)
			db := raw(t, path)
			if _, err := db.Exec(test.damage); err != nil {
				db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
			if err == nil {
				store.Close()
				t.Fatal("migrating a large BLOB identity scalar succeeded")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("migrating a large BLOB identity scalar returned %v, want EIO", err)
			}
		})
	}
}

func TestOpenRejectsLargeBlobSchemaVersionBeforeMigration(t *testing.T) {
	path := database(t)
	writeVersionOne(t, path)
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE schema_version SET version = zeroblob(4 * 1024 * 1024)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening a large BLOB schema version succeeded")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening a large BLOB schema version returned %v, want EIO", err)
	}
}

func TestBoundOpenComparesALargeStoredIdentityWithoutReturningIt(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`UPDATE backing_store SET store_id = CAST(zeroblob(4 * 1024 * 1024) AS TEXT)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenBound(t.Context(), path, "workspace", "store-b", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening a database with another large stored identity succeeded")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a database with another large stored identity returned %v, want EINVAL", err)
	}
}

func TestEmptySinceAcceptsALargeRequestedLimitWithinActualWorkBound(t *testing.T) {
	options := sqlite.DefaultOptions()
	options.MaxIntegrityRecords = sqlite.MinIntegrityRecords
	store, err := sqlite.OpenWithOptions(t.Context(), database(t), "workspace", 0, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	changes, retention, err := readChanges(t.Context(), store, 0, 1_000_000)
	if err != nil {
		t.Fatalf("reading an empty log under a large requested limit: %v", err)
	}
	if len(changes) != 0 || retention.Tail != 0 {
		t.Fatalf("empty log returned %d changes and %+v", len(changes), retention)
	}
}

func TestSmallSinceCapsALargeRequestedLimitToActualWorkBudget(t *testing.T) {
	options := sqlite.DefaultOptions()
	options.MaxIntegrityRecords = 5
	store, err := sqlite.OpenWithOptions(t.Context(), database(t), "workspace", 0, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	changes, retention, err := readChanges(t.Context(), store, 0, 1_000_000)
	if err != nil {
		t.Fatalf("reading a small log under a large requested limit: %v", err)
	}
	if len(changes) != 2 || changes[len(changes)-1].Position != retention.Tail {
		t.Fatalf("small bounded log returned %d changes at tail %d, retention %+v",
			len(changes), changes[len(changes)-1].Position, retention)
	}
}
