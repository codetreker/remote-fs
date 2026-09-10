package localstore

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	bootstrapWriterEnvironment = "REMOTE_FS_BOOTSTRAP_WRITER"
	bootstrapRootEnvironment   = "REMOTE_FS_BOOTSTRAP_ROOT"
)

func TestRootAnchorDetectsPathReplacement(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := anchor.Close(); err != nil {
			t.Fatal(err)
		}
	})

	if err := os.Rename(root, root+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := anchor.VerifyPath(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("VerifyPath returned %v, want EIO", err)
	}
}

func TestOpenCleansAnInterruptedCompletionStage(t *testing.T) {
	newConfig := func(t *testing.T) Config {
		t.Helper()
		root := filepath.Join(t.TempDir(), "store")
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		return Config{
			Root: root, Volume: "workspace", Quota: 1 << 20,
			Window: sqlite.DefaultWindow(),
			Maintenance: objectstore.Options{
				SweepInterval: time.Hour,
				SweepBatch:    8,
			},
		}
	}
	initialize := func(t *testing.T, config Config) {
		t.Helper()
		store, err := Open(t.Context(), config)
		if err != nil {
			t.Fatalf("initialize local store: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close initialized local store: %v", err)
		}
	}

	t.Run("regular stage", func(t *testing.T) {
		config := newConfig(t)
		initialize(t, config)
		stage := filepath.Join(config.Root, completionStage)
		if err := os.WriteFile(stage, []byte("interrupted marker"), 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := Open(t.Context(), config)
		if err != nil {
			t.Fatalf("reopen with completion stage: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("completion stage still exists after reopen: %v", err)
		}
	})

	t.Run("directory stage", func(t *testing.T) {
		config := newConfig(t)
		initialize(t, config)
		stage := filepath.Join(config.Root, completionStage)
		if err := os.Mkdir(stage, 0o700); err != nil {
			t.Fatal(err)
		}
		store, err := Open(t.Context(), config)
		if err == nil {
			store.Close()
			t.Fatal("Open removed a directory posing as the completion stage")
		}
		if !errors.Is(err, syscall.EISDIR) {
			t.Fatalf("Open returned %v, want EISDIR", err)
		}
		if info, err := os.Lstat(stage); err != nil || !info.IsDir() {
			t.Fatalf("failed cleanup changed the stage directory: %v, %v", info, err)
		}
		objects, err := localdisk.Open(t.Context(), config.Root, localdisk.Options{})
		if err != nil {
			t.Fatalf("failed Open retained the object-store lock: %v", err)
		}
		if err := objects.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestInitializationIntentRecoversInterruptedFirstOpen(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{CompositeInitialization: true})
	if err != nil {
		t.Fatal(err)
	}
	if objects.CompositeInitializationState() != localdisk.CompositeInitializationStarted {
		t.Fatalf("initialization state is %v, want started", objects.CompositeInitializationState())
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(t.Context(), Config{
		Root:   root,
		Volume: "workspace",
		Quota:  1 << 20,
		Window: sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{
			SweepInterval: time.Hour,
			SweepBatch:    8,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, localdisk.InitializationMarkerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed initialization left its intent behind: %v", err)
	}
}

func TestBoundInitializationRecoversSchemaLessSQLiteHeader(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	prepareBoundInitialization(t, config)
	databasePath := filepath.Join(config.Root, metastoreFilename)
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `PRAGMA journal_mode=WAL`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(databasePath, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("opening the SQLite writer did not publish a database header")
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		t.Fatal(err)
	}
	rootState, err := anchor.InspectMetastore()
	if closeErr := anchor.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if rootState.WALNonEmpty {
		t.Fatal("schema-less bootstrap unexpectedly contains WAL frames")
	}

	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatalf("recover schema-less SQLite bootstrap: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundInitializationRecoversUncommittedSchemaWAL(t *testing.T) {
	if os.Getenv(bootstrapWriterEnvironment) == "1" {
		runUncommittedBootstrapWriter(t)
		return
	}
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	prepareBoundInitialization(t, config)
	command := exec.Command(os.Args[0], "-test.run=^TestBoundInitializationRecoversUncommittedSchemaWAL$")
	command.Env = append(os.Environ(),
		bootstrapWriterEnvironment+"=1",
		bootstrapRootEnvironment+"="+config.Root,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("wait for uncommitted bootstrap WAL: %v", err)
	}
	if line != "uncommitted\n" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("bootstrap writer printed %q", line)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed bootstrap writer exited successfully")
	}
	wal, err := os.Stat(filepath.Join(config.Root, metastoreFilename+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	if wal.Size() <= sqliteWALHeaderBytes {
		t.Fatalf("uncommitted bootstrap WAL has no frames: %d bytes", wal.Size())
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatalf("recover uncommitted schema WAL: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func runUncommittedBootstrapWriter(t *testing.T) {
	root := os.Getenv(bootstrapRootEnvironment)
	if root == "" {
		t.Fatal("bootstrap writer has no root")
	}
	database, err := sql.Open("sqlite", filepath.Join(root, metastoreFilename))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `PRAGMA cache_size=1`); err != nil {
		t.Fatal(err)
	}
	tx, err := database.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `CREATE TABLE unfinished (payload BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for range 1024 {
		if _, err := tx.ExecContext(t.Context(), `INSERT INTO unfinished VALUES (zeroblob(4096))`); err != nil {
			t.Fatal(err)
		}
	}
	wal, err := os.Stat(filepath.Join(root, metastoreFilename+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	if wal.Size() <= sqliteWALHeaderBytes {
		t.Fatalf("writer did not spill uncommitted WAL frames: %d bytes", wal.Size())
	}
	if _, err := fmt.Fprintln(os.Stdout, "uncommitted"); err != nil {
		t.Fatal(err)
	}
	if err := os.Stdout.Sync(); err != nil {
		t.Fatal(err)
	}
	select {}
}

func prepareBoundInitialization(t *testing.T, config Config) {
	t.Helper()
	objects, err := localdisk.Open(t.Context(), config.Root, localdisk.Options{CompositeInitialization: true})
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		objects.Close()
		t.Fatal(err)
	}
	if err := anchor.BindInitialization(objects.ID(), config.Volume, false); err != nil {
		anchor.Close()
		objects.Close()
		t.Fatal(err)
	}
	if err := errors.Join(anchor.Close(), objects.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestPinnedSnapshotDefersButDoesNotBlockMetastoreCheckpoint(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	waitForWitness(t, store, func(record metastoreWitnessRecord) bool {
		return record.CheckpointedGeneration == record.State.Generation
	})

	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "while-pinned"); err != nil {
		snapshot.Close()
		t.Fatalf("mutation while a snapshot pins the WAL: %v", err)
	}
	pinned := waitForWitness(t, store, func(record metastoreWitnessRecord) bool {
		return record.State.Generation > record.CheckpointedGeneration
	})
	status, err := store.Status(t.Context())
	if err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	if !status.Checkpoint.Pending ||
		status.Checkpoint.AcceptedGeneration != pinned.State.Generation ||
		status.Checkpoint.CheckpointedGeneration != pinned.CheckpointedGeneration {
		snapshot.Close()
		t.Fatalf("checkpoint status does not report pinned WAL state: %+v", status.Checkpoint)
	}
	time.Sleep(2 * checkpointRetryInterval)
	stillPinned := readWitnessRecord(t, store)
	if stillPinned.CheckpointedGeneration != pinned.CheckpointedGeneration ||
		stillPinned.State.Generation != pinned.State.Generation {
		snapshot.Close()
		t.Fatalf("checkpoint passed a live snapshot: before %+v, after %+v", pinned, stillPinned)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	waitForWitness(t, store, func(record metastoreWitnessRecord) bool {
		return record.CheckpointedGeneration == record.State.Generation &&
			record.State.Generation == pinned.State.Generation
	})
	status, err = store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Checkpoint.Pending || status.Checkpoint.LastError != nil {
		t.Fatalf("checkpoint status remained degraded after catch-up: %+v", status.Checkpoint)
	}
}

func TestPinnedSnapshotCoalescesCheckpointSignals(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
	waitForWitness(t, store, func(record metastoreWitnessRecord) bool {
		return record.CheckpointedGeneration == record.State.Generation
	})
	store.durable.mu.Lock()
	store.durable.retryInterval = 5 * time.Second
	store.durable.mu.Unlock()

	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "first-pinned"); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	waitForCheckpointRetry(t, store.durable)
	store.durable.mu.Lock()
	attempts := store.durable.checkpointAttempts
	store.durable.mu.Unlock()
	for index := range 16 {
		if err := store.Create(t.Context(), fmt.Sprintf("pinned-%d", index)); err != nil {
			snapshot.Close()
			t.Fatal(err)
		}
	}
	store.durable.mu.Lock()
	after := store.durable.checkpointAttempts
	store.durable.mu.Unlock()
	if after != attempts {
		snapshot.Close()
		t.Fatalf("accepted mutations bypassed checkpoint retry interval: attempts %d -> %d", attempts, after)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
}

func waitForCheckpointRetry(t *testing.T, durable *durableMetastore) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		durable.mu.Lock()
		waiting := durable.retryWaiting
		durable.mu.Unlock()
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("checkpoint worker did not enter its bounded retry interval")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStatusSurfacesCheckpointWorkerFailure(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	})
	waitForWitness(t, store, func(record metastoreWitnessRecord) bool {
		return record.CheckpointedGeneration == record.State.Generation
	})
	store.durable.mu.Lock()
	store.durable.retryInterval = 2 * time.Second
	store.durable.mu.Unlock()
	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "pending-checkpoint"); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	waitForCheckpointRetry(t, store.durable)
	stage := filepath.Join(config.Root, metastoreWitnessStage)
	if err := os.Mkdir(stage, 0o700); err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var status Status
	for {
		status, err = store.Status(t.Context())
		if status.Checkpoint.LastError != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint worker failure was not reported; status=%+v err=%v", status.Checkpoint, err)
		}
		time.Sleep(time.Millisecond)
	}
	if err == nil || !errors.Is(err, syscall.EIO) || !status.Checkpoint.Pending {
		t.Fatalf("Status during checkpoint failure = %+v, %v", status.Checkpoint, err)
	}
	if err := os.Remove(stage); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		status, err = store.Status(t.Context())
		if err == nil && status.Checkpoint.LastError == nil && !status.Checkpoint.Pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint worker did not recover; status=%+v err=%v", status.Checkpoint, err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestActiveReaderCloseRetryWithoutPendingWAL(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	waitForWitness(t, store, func(record metastoreWitnessRecord) bool {
		return record.CheckpointedGeneration == record.State.Generation
	})
	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err == nil || !errors.Is(err, syscall.EBUSY) {
		snapshot.Close()
		t.Fatalf("Close with an active reader returned %v, want EBUSY", err)
	}
	if _, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk); !errors.Is(err, syscall.EBUSY) {
		snapshot.Close()
		t.Fatalf("failed Close released object-store ownership: %v", err)
	}
	status, err := store.Status(t.Context())
	if err == nil || status.Checkpoint.LastError == nil {
		snapshot.Close()
		t.Fatalf("Status after failed Close = %+v, %v; want checkpoint failure", status.Checkpoint, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("retry Close after releasing snapshot: %v", err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatalf("successful Close retained object-store ownership: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFailureAbortsUnexposedMetastoreBeforeReleasingOwnership(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	injected := errors.New("post-metastore open failure")
	store, err := open(t.Context(), config, openHooks{
		afterDurableMetastore: func(durable *durableMetastore) error {
			stage := filepath.Join(config.Root, metastoreWitnessStage)
			deadline := time.Now().Add(5 * time.Second)
			for {
				err := os.Mkdir(stage, 0o700)
				if err == nil {
					break
				}
				if !errors.Is(err, os.ErrExist) || time.Now().After(deadline) {
					return fmt.Errorf("occupy witness stage: %w", err)
				}
				time.Sleep(time.Millisecond)
			}
			mutationErr := durable.Create(t.Context(), "committed-before-open-failure")
			removeErr := os.Remove(stage)
			if mutationErr == nil || !errors.Is(mutationErr, syscall.EIO) {
				return fmt.Errorf("witness-failed mutation returned %v", mutationErr)
			}
			if removeErr != nil {
				return removeErr
			}
			return injected
		},
	})
	if store != nil {
		store.Close()
		t.Fatal("injected Open failure returned a Store")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("Open returned %v, want injected failure", err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatalf("Open failure leaked root ownership: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFailureAfterVolumeCompositionClosesAllOwnership(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	injected := errors.New("post-volume composition failure")
	store, err := open(t.Context(), config, openHooks{
		afterVolume: func(*objectstore.Storage, *durableMetastore) error {
			return injected
		},
	})
	if store != nil {
		store.Close()
		t.Fatal("injected volume-composition failure returned a Store")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("Open returned %v, want injected failure", err)
	}
	if _, err := os.Stat(filepath.Join(config.Root, completionFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed Open published completion: %v", err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatalf("volume cleanup retained root ownership: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableConstructorCleanupFailureRetainsRootOwnership(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	injected := errors.New("SQLite pool close left a native handle open")
	store, err := open(t.Context(), config, openHooks{
		openDurable: func() (*sqlite.Store, error) { return nil, injected },
		retainsOwnership: func(err error) bool {
			return errors.Is(err, injected)
		},
	})
	if store != nil {
		store.Close()
		t.Fatal("constructor cleanup failure returned a Store")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("Open returned %v, want constructor cleanup failure", err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err == nil {
		objects.Close()
		t.Fatal("constructor cleanup uncertainty released root ownership")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("opening retained root returned %v, want EBUSY", err)
	}
}

func TestMetastoreWitnessStageRecoveryIsFailClosed(t *testing.T) {
	t.Run("duplicate stage", func(t *testing.T) {
		config := Config{
			Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
			Window:      sqlite.DefaultWindow(),
			Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
		}
		store, err := Open(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		final := filepath.Join(config.Root, metastoreWitnessFilename)
		stage := filepath.Join(config.Root, metastoreWitnessStage)
		content, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stage, content, 0o600); err != nil {
			t.Fatal(err)
		}
		recovered, err := Open(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stage remained after recovery: %v", err)
		}
	})

	t.Run("stage without acknowledged witness", func(t *testing.T) {
		config := Config{
			Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
			Window:      sqlite.DefaultWindow(),
			Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
		}
		store, err := Open(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		final := filepath.Join(config.Root, metastoreWitnessFilename)
		stage := filepath.Join(config.Root, metastoreWitnessStage)
		content, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stage, content, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(final); err != nil {
			t.Fatal(err)
		}
		store, err = Open(t.Context(), config)
		if err == nil {
			store.Close()
			t.Fatal("Open promoted a stage to the acknowledged witness beside READY")
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open returned %v, want EIO", err)
		}
	})

	t.Run("damaged stage", func(t *testing.T) {
		config := Config{
			Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
			Window:      sqlite.DefaultWindow(),
			Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
		}
		store, err := Open(t.Context(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(config.Root, metastoreWitnessStage), []byte("damaged"), 0o600); err != nil {
			t.Fatal(err)
		}
		store, err = Open(t.Context(), config)
		if err == nil {
			store.Close()
			t.Fatal("Open accepted a damaged metastore witness stage")
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open returned %v, want EIO", err)
		}
	})
}

func TestInterruptedWitnessStageCleanupIsIdempotentAndRefusesDirectories(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		t.Fatal(err)
	}
	witness := &metastoreWitness{anchor: anchor}
	if err := witness.RemoveInterruptedStage(); err != nil {
		anchor.Close()
		t.Fatalf("idempotent stage cleanup: %v", err)
	}
	stage := filepath.Join(config.Root, metastoreWitnessStage)
	if err := os.Mkdir(stage, 0o700); err != nil {
		anchor.Close()
		t.Fatal(err)
	}
	if err := witness.RemoveInterruptedStage(); err == nil || !errors.Is(err, syscall.EISDIR) {
		os.Remove(stage)
		anchor.Close()
		t.Fatalf("directory stage cleanup returned %v, want EISDIR", err)
	}
	if info, err := os.Stat(stage); err != nil || !info.IsDir() {
		os.Remove(stage)
		anchor.Close()
		t.Fatalf("failed cleanup changed stage directory: %v, %v", info, err)
	}
	if err := os.Remove(stage); err != nil {
		anchor.Close()
		t.Fatal(err)
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundInitializationRejectsStageWithLostMetastore(t *testing.T) {
	for name, leavePristine := range map[string]bool{
		"missing":  false,
		"pristine": true,
	} {
		t.Run(name, func(t *testing.T) {
			config := boundInitializationWithWitnessStage(t, leavePristine)
			store, err := Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open replaced committed metastore state with an empty volume")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
			if _, err := os.Stat(filepath.Join(config.Root, metastoreWitnessStage)); err != nil {
				t.Fatalf("failed recovery removed the commit-evidence stage: %v", err)
			}
			info, err := os.Stat(filepath.Join(config.Root, metastoreFilename))
			switch {
			case leavePristine && err != nil:
				t.Fatalf("failed recovery removed the pristine metastore: %v", err)
			case leavePristine && info.Size() != 0:
				t.Fatalf("failed recovery initialized a %d-byte replacement metastore", info.Size())
			case !leavePristine && !errors.Is(err, os.ErrNotExist):
				t.Fatalf("failed recovery created replacement metadata: %v", err)
			}
		})
	}
}

func boundInitializationWithWitnessStage(t *testing.T, leavePristine bool) Config {
	t.Helper()
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		objects.Close()
		t.Fatal(err)
	}
	if err := anchor.publishInitializationBinding(objects.ID(), config.Volume); err != nil {
		anchor.Close()
		objects.Close()
		t.Fatal(err)
	}
	if err := errors.Join(anchor.Close(), objects.Close()); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(config.Root, metastoreWitnessFilename)
	stage := filepath.Join(config.Root, metastoreWitnessStage)
	content, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stage, content, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range append([]string{metastoreWitnessFilename, completionFilename}, metastoreAuxiliaryFilenames[:]...) {
		if err := os.Remove(filepath.Join(config.Root, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	database := filepath.Join(config.Root, metastoreFilename)
	if leavePristine {
		if err := os.Truncate(database, 0); err != nil {
			t.Fatal(err)
		}
	} else if err := os.Remove(database); err != nil {
		t.Fatal(err)
	}
	return config
}

func TestWitnessRenameFailureCleansStageForRetry(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		objects.Close()
		t.Fatal(err)
	}
	witness, exists, stageExists, err := anchor.InspectMetastoreWitness(objects.ID(), config.Volume)
	if err != nil {
		anchor.Close()
		objects.Close()
		t.Fatal(err)
	}
	if !exists || stageExists {
		anchor.Close()
		objects.Close()
		t.Fatalf("unexpected witness state: exists=%t stage=%t", exists, stageExists)
	}
	final := filepath.Join(config.Root, metastoreWitnessFilename)
	stage := filepath.Join(config.Root, metastoreWitnessStage)
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(final, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := witness.publish(witness.record); err == nil {
		t.Fatal("witness publication replaced a directory")
	}
	if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed witness rename left its stage behind: %v", err)
	}
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeMetastoreWitness(witness.record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stage, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := witness.publish(witness.record); err != nil {
		t.Fatalf("witness publication could not recover its valid leftover stage: %v", err)
	}
	if err := errors.Join(anchor.Close(), objects.Close()); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMetastoreWitnessRejectsValidChecksumIdentityMismatch(t *testing.T) {
	tests := map[string]func(*metastoreWitnessRecord){
		"object store": func(record *metastoreWitnessRecord) { record.StoreID[0] ^= 0xff },
		"volume":       func(record *metastoreWitnessRecord) { record.Volume = "another-workspace" },
		"database": func(record *metastoreWitnessRecord) {
			identity := []byte(record.State.DatabaseID)
			if identity[0] == '0' {
				identity[0] = '1'
			} else {
				identity[0] = '0'
			}
			record.State.DatabaseID = string(identity)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := Config{
				Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
				Window:      sqlite.DefaultWindow(),
				Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
			}
			store, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
			if err != nil {
				t.Fatal(err)
			}
			anchor, err := openRootAnchor(config.Root)
			if err != nil {
				objects.Close()
				t.Fatal(err)
			}
			witness, exists, stageExists, err := anchor.InspectMetastoreWitness(objects.ID(), config.Volume)
			if err != nil {
				anchor.Close()
				objects.Close()
				t.Fatal(err)
			}
			if !exists || stageExists {
				t.Fatalf("unexpected witness state: exists=%t stage=%t", exists, stageExists)
			}
			record := witness.record
			mutate(&record)
			encoded, err := encodeMetastoreWitness(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(config.Root, metastoreWitnessFilename), encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(anchor.Close(), objects.Close()); err != nil {
				t.Fatal(err)
			}

			store, err = Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open accepted a valid-checksum witness with mismatched identity")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func TestBoundPreflightRejectsAdditionalVolume(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(config.Root, metastoreFilename))
	if err != nil {
		t.Fatal(err)
	}
	var root int64
	if err := database.QueryRowContext(t.Context(), `SELECT root FROM volumes WHERE name = ?`, config.Volume).Scan(&root); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`INSERT INTO volumes (name, root, used) VALUES ('additional', ?, 0)`, root); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("Open accepted an additional volume in the bound database")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	database, err = sql.Open("sqlite", "file:"+filepath.Join(config.Root, metastoreFilename)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM volumes`).Scan(&count); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("failed preflight changed volume count to %d", count)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatalf("failed preflight retained object-store ownership: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundPreflightRejectsOversizedValuesBeforeLoadingThem(t *testing.T) {
	for name, corrupt := range map[string]string{
		"blob store identity": `UPDATE backing_store SET store_id = zeroblob(2097152)`,
		"text store identity": `UPDATE backing_store SET store_id = CAST(zeroblob(2097152) AS TEXT)`,
		"blob volume":         `UPDATE volumes SET name = zeroblob(2097152)`,
		"text volume":         `UPDATE volumes SET name = CAST(zeroblob(2097152) AS TEXT)`,
	} {
		t.Run(name, func(t *testing.T) {
			config := Config{
				Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
				Window:      sqlite.DefaultWindow(),
				Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
			}
			store, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			database, err := sql.Open("sqlite", filepath.Join(config.Root, metastoreFilename))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.ExecContext(t.Context(), corrupt); err != nil {
				database.Close()
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open loaded an oversized binding value")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
			objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
			if err != nil {
				t.Fatalf("failed bounded preflight retained object-store ownership: %v", err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBoundInitializationNeverRecreatesMissingVolume(t *testing.T) {
	config := Config{
		Root: privateTestRoot(t), Volume: "workspace", Quota: 1 << 20,
		Window:      sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{SweepInterval: time.Hour, SweepBatch: 8},
	}
	store, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(config.Root)
	if err != nil {
		objects.Close()
		t.Fatal(err)
	}
	if err := anchor.publishInitializationBinding(objects.ID(), config.Volume); err != nil {
		anchor.Close()
		objects.Close()
		t.Fatal(err)
	}
	if err := errors.Join(anchor.Close(), objects.Close()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(config.Root, completionFilename)); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(config.Root, metastoreFilename))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `DELETE FROM volumes`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("Open recreated a missing volume during bound initialization recovery")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	if _, err := os.Stat(filepath.Join(config.Root, completionFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed recovery published READY: %v", err)
	}
	database, err = sql.Open("sqlite", "file:"+filepath.Join(config.Root, metastoreFilename)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM volumes`).Scan(&count); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed recovery created %d volumes", count)
	}
	objects, err = localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err != nil {
		t.Fatalf("failed recovery retained object-store ownership: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
}

func privateTestRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func waitForWitness(
	t *testing.T,
	store *Store,
	accept func(metastoreWitnessRecord) bool,
) metastoreWitnessRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		record := readWitnessRecord(t, store)
		if accept(record) {
			return record
		}
		if time.Now().After(deadline) {
			t.Fatalf("metastore witness did not reach the expected state; last record: %+v", record)
		}
		time.Sleep(time.Millisecond)
	}
}

func readWitnessRecord(t *testing.T, store *Store) metastoreWitnessRecord {
	t.Helper()
	anchor, err := openRootAnchor(store.anchor.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := anchor.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	record, exists, err := anchor.readMetastoreWitnessEntry(
		metastoreWitnessFilename,
		store.objects.ID(),
		store.volumeName,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("metastore witness is missing")
	}
	return record
}

func TestInterruptedInitializationRejectsAnotherVolume(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{CompositeInitialization: true})
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := openRootAnchor(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := anchor.BindInitialization(objects.ID(), "workspace", false); err != nil {
		t.Fatal(err)
	}
	if err := anchor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(t.Context(), Config{
		Root:   root,
		Volume: "workspaec",
		Quota:  1 << 20,
		Window: sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{
			SweepInterval: time.Hour,
			SweepBatch:    8,
		},
	})
	if err == nil {
		store.Close()
		t.Fatal("Open accepted another volume while initialization was incomplete")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	if _, err := os.Stat(filepath.Join(root, metastoreFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the refused volume created metadata: %v", err)
	}
}

func TestPrivateMetadataMustShareTheRootFilesystem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
	})
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		t.Fatal(err)
	}
	mount, err := mountID(fd, unix.Statx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validatePrivateFile(
		fd, path, uint64(stat.Dev)+1, mount, false, unix.Statx,
	); !errors.Is(err, syscall.EIO) {
		t.Fatalf("validatePrivateFile returned %v, want EIO", err)
	}
	fakeStatx := func(fd int, path string, flags, mask int, stat *unix.Statx_t) error {
		if err := unix.Statx(fd, path, flags, mask, stat); err != nil {
			return err
		}
		stat.Mnt_id++
		return nil
	}
	if _, err := validatePrivateFile(
		fd, path, uint64(stat.Dev), mount, false, fakeStatx,
	); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("validatePrivateFile across a bind mount returned %v, want EOPNOTSUPP", err)
	}
}

func TestClampStatusSpaceValidatesAndCombinesMeasurements(t *testing.T) {
	logical := storage.Space{Total: 100, Used: 40, Avail: 60}
	combined, err := clampStatusSpace(logical, localdisk.Status{PhysicalAvailable: 25}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := (storage.Space{Total: 100, Used: 40, Avail: 25}); combined != want {
		t.Fatalf("combined space = %+v, want %+v", combined, want)
	}

	for name, space := range map[string]storage.Space{
		"negative":         {Total: -1},
		"excess available": {Total: 100, Used: 40, Avail: 61},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := clampStatusSpace(space, localdisk.Status{}, nil); !errors.Is(err, syscall.EIO) {
				t.Fatalf("clampStatusSpace returned %v, want EIO", err)
			}
		})
	}
	if _, err := clampStatusSpace(logical, localdisk.Status{PhysicalAvailable: -1}, nil); !errors.Is(err, syscall.EIO) {
		t.Fatalf("negative physical availability returned %v, want EIO", err)
	}
	localFailure := errors.New("local status failed")
	unclamped, err := clampStatusSpace(logical, localdisk.Status{}, localFailure)
	if err != nil || unclamped != logical {
		t.Fatalf("failed physical measurement changed logical space to %+v, %v", unclamped, err)
	}
}
