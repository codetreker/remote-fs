package localstore_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/storage/storagetest"
)

const databaseName = "metastore.sqlite"

func TestStorageContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		store := open(t, testConfig(privateRoot(t)))
		t.Cleanup(func() { closeStore(t, store) })
		return store
	})
}

func TestBoundedStorageContract(t *testing.T) {
	storagetest.RunBounded(t, func(t *testing.T) storage.BoundedStorage {
		store := open(t, testConfig(privateRoot(t)))
		t.Cleanup(func() { closeStore(t, store) })
		return store
	})
}

func TestOpenCreatesAndReopensABoundVolume(t *testing.T) {
	config := testConfig(privateRoot(t))
	first := open(t, config)
	if err := first.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	incarnation, err := first.Log().Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := open(t, config)
	t.Cleanup(func() { closeStore(t, second) })
	if _, err := second.Stat(t.Context(), "held"); err != nil {
		t.Fatalf("the reopened volume lost its file: %v", err)
	}
	reopenedIncarnation, err := second.Log().Incarnation(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if reopenedIncarnation != incarnation {
		t.Fatalf("the log incarnation changed from %q to %q across reopen", incarnation, reopenedIncarnation)
	}
	for _, name := range []string{"FORMAT", "OWNER.lock", "objects", databaseName, "METASTORE", "LOCALSTORE"} {
		if _, err := os.Lstat(filepath.Join(config.Root, name)); err != nil {
			t.Errorf("the local store did not create %s: %v", name, err)
		}
	}
}

func TestRootIsBoundToExactlyOneVolume(t *testing.T) {
	config := testConfig(privateRoot(t))
	closeStore(t, open(t, config))

	wrong := config
	wrong.Volume = "workspaec"
	store, err := localstore.Open(t.Context(), wrong)
	if err == nil {
		store.Close()
		t.Fatal("Open accepted a different volume in the same root")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}

	database := rawDatabase(t, filepath.Join(config.Root, databaseName), true)
	t.Cleanup(func() { closeStore(t, database) })
	var created int
	if err := database.QueryRowContext(t.Context(),
		`SELECT count(*) FROM volumes WHERE name = ?`, wrong.Volume).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatal("the rejected volume was created in SQLite")
	}

	reopened := open(t, config)
	closeStore(t, reopened)
}

func TestPreexistingObjectStoreWithoutInitializationIntentFailsClosed(t *testing.T) {
	root := privateRoot(t)
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !objects.NewlyInitialized() {
		t.Fatal("the empty root was not initialized")
	}
	closeStore(t, objects)

	store, err := localstore.Open(t.Context(), testConfig(root))
	if err == nil {
		store.Close()
		t.Fatal("Open invented metadata for an object store with no initialization intent")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestUnprovenEmptyDatabaseFailsClosed(t *testing.T) {
	root := privateRoot(t)
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	closeStore(t, objects)
	if err := os.WriteFile(filepath.Join(root, databaseName), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := localstore.Open(t.Context(), testConfig(root))
	if err == nil {
		store.Close()
		t.Fatal("Open initialized an empty database with no initialization intent")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestUnboundInitializationIntentCannotAdoptAPreexistingDatabase(t *testing.T) {
	root := privateRoot(t)
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{CompositeInitialization: true})
	if err != nil {
		t.Fatal(err)
	}
	closeStore(t, objects)
	if err := os.WriteFile(filepath.Join(root, databaseName), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		store, err := localstore.Open(t.Context(), testConfig(root))
		if err == nil {
			store.Close()
			t.Fatalf("Open attempt %d adopted a database that predates its volume-bound initialization intent", attempt)
		}
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open attempt %d returned %v, want EIO", attempt, err)
		}
	}
}

func TestContentPersistsAcrossRestart(t *testing.T) {
	config := testConfig(privateRoot(t))
	first := open(t, config)
	want := []byte("durable contents")
	if err := first.Write(t.Context(), "artifact", want); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := open(t, config)
	t.Cleanup(func() { closeStore(t, second) })
	got, err := second.Read(t.Context(), "artifact")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("Read returned %q, want %q", got, want)
	}
}

func TestSweepRetriesGarbageAfterBackgroundFailure(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.Write(t.Context(), "obsolete", []byte("garbage payload")); err != nil {
		t.Fatal(err)
	}

	database := rawDatabase(t, filepath.Join(config.Root, databaseName), true)
	var key string
	if err := database.QueryRowContext(t.Context(),
		`SELECT key FROM objects WHERE state = 1`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(key))
	shard := filepath.Join(config.Root, "objects", hex.EncodeToString(digest[:1]))
	object := filepath.Join(shard, "k"+base64.RawURLEncoding.EncodeToString([]byte(key)))
	if err := os.Chmod(shard, 0o500); err != nil {
		t.Fatal(err)
	}
	shardRestored := false
	t.Cleanup(func() {
		if !shardRestored {
			_ = os.Chmod(shard, 0o700)
		}
	})

	if err := store.Remove(t.Context(), "obsolete"); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for store.MaintenanceStatus().LastSweepError == nil {
		select {
		case <-deadline.C:
			t.Fatal("background maintenance did not retain the shard-permission failure")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := os.Stat(object); err != nil {
		t.Fatalf("failed background maintenance removed the object: %v", err)
	}

	if err := os.Chmod(shard, 0o700); err != nil {
		t.Fatal(err)
	}
	shardRestored = true
	removed, err := store.Sweep(t.Context(), 1)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if removed != 1 {
		t.Fatalf("Sweep removed %d objects, want 1", removed)
	}
	if _, err := os.Stat(object); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Sweep left the garbage object behind: %v", err)
	}
	status, err := store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Objects.GarbageCount != 0 || status.Maintenance.LastSweepError != nil ||
		status.Maintenance.LastSweepRemoved != 1 {
		t.Fatalf("Status after Sweep = %+v", status)
	}
	if removed, err := store.Sweep(t.Context(), -1); removed != 0 || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative Sweep returned %d, %v; want 0, EINVAL", removed, err)
	}
}

func TestCorruptReferencedObjectStateFailsBeforeStartupSweep(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	want := []byte("durable contents")
	if err := store.Write(t.Context(), "artifact", want); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)

	database := rawDatabase(t, filepath.Join(config.Root, databaseName), false)
	var key string
	if err := database.QueryRowContext(t.Context(),
		`SELECT key FROM objects WHERE state = 1`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(),
		`UPDATE objects SET state = 2 WHERE key = ?`, key); err != nil {
		t.Fatal(err)
	}
	closeStore(t, database)

	digest := sha256.Sum256([]byte(key))
	objectPath := filepath.Join(
		config.Root,
		"objects",
		hex.EncodeToString(digest[:1]),
		"k"+base64.RawURLEncoding.EncodeToString([]byte(key)),
	)
	before, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := localstore.Open(t.Context(), config)
	if err == nil {
		reopened.Close()
		t.Fatal("Open accepted a file that references an object marked as garbage")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	after, readErr := os.ReadFile(objectPath)
	if readErr != nil {
		t.Fatalf("startup validation removed the referenced object: %v", readErr)
	}
	if string(after) != string(before) {
		t.Fatal("startup validation modified the referenced object")
	}
}

func TestASecondOwnerIsRefusedUntilClose(t *testing.T) {
	config := testConfig(privateRoot(t))
	first := open(t, config)

	second, err := localstore.Open(t.Context(), config)
	if err == nil {
		second.Close()
		t.Fatal("a second owner opened the same root")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("opening the same root twice returned %v, want EBUSY", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := open(t, config)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentInitializersProduceOneOwner(t *testing.T) {
	config := testConfig(privateRoot(t))
	start := make(chan struct{})
	type result struct {
		store *localstore.Store
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			store, err := localstore.Open(t.Context(), config)
			results <- result{store: store, err: err}
		}()
	}
	close(start)
	var owner *localstore.Store
	var refusal error
	for range 2 {
		result := <-results
		if result.store != nil {
			if owner != nil {
				result.store.Close()
				owner.Close()
				t.Fatal("both concurrent initializers became owners")
			}
			owner = result.store
		} else {
			refusal = result.err
		}
	}
	if owner == nil {
		t.Fatalf("neither concurrent initializer opened the store; refusal was %v", refusal)
	}
	closeStore(t, owner)
	if !errors.Is(refusal, syscall.EBUSY) {
		t.Fatalf("the losing initializer returned %v, want EBUSY", refusal)
	}
	if _, err := os.Stat(filepath.Join(config.Root, localdisk.InitializationMarkerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("concurrent initialization left a recovery intent behind: %v", err)
	}
}

func TestQuotaMustMeetTheStorageMinimumBeforeTheRootIsTouched(t *testing.T) {
	for name, quota := range map[string]int64{
		"negative":      -1,
		"zero":          0,
		"positive":      1,
		"below minimum": limited.MinLimit - 1,
	} {
		t.Run(name, func(t *testing.T) {
			root := privateRoot(t)
			config := testConfig(root)
			config.Quota = quota
			store, err := localstore.Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatalf("Open accepted quota %d", quota)
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("Open with quota %d returned %v, want EINVAL", quota, err)
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("invalid configuration created %d root entries", len(entries))
			}
		})
	}
}

func TestRootIsRequired(t *testing.T) {
	config := testConfig("")
	store, err := localstore.Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("Open accepted an empty root")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Open returned %v, want EINVAL", err)
	}
}

func TestVolumeNameIsLengthBoundedBeforeTheRootIsTouched(t *testing.T) {
	root := privateRoot(t)
	config := testConfig(root)
	config.Volume = string(make([]byte, localstore.MaxVolumeBytes+1))
	store, err := localstore.Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("Open accepted an oversized volume name")
	}
	if !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("Open returned %v, want ENAMETOOLONG", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid volume name created %d root entries", len(entries))
	}
}

func TestLargestVolumeNameCanReopen(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.Volume = strings.Repeat("w", localstore.MaxVolumeBytes)
	closeStore(t, open(t, config))
	closeStore(t, open(t, config))
}

func TestConfigurationIsValidatedBeforeTheRootIsTouched(t *testing.T) {
	tests := map[string]func(*localstore.Config){
		"volume":    func(config *localstore.Config) { config.Volume = "" },
		"log floor": func(config *localstore.Config) { config.Window.Floor = 0 },
		"log cap":   func(config *localstore.Config) { config.Window.Cap = config.Window.Floor - 1 },
		"log age":   func(config *localstore.Config) { config.Window.Age = 0 },
		"sweep interval": func(config *localstore.Config) {
			config.Maintenance.SweepInterval = 0
		},
		"sweep batch":      func(config *localstore.Config) { config.Maintenance.SweepBatch = 0 },
		"local disk limit": func(config *localstore.Config) { config.LocalDisk.MaxObjectBytes = -1 },
		"pending object limit": func(config *localstore.Config) {
			config.ObjectLimits.MaxPendingObjects = -1
		},
		"pending byte limit": func(config *localstore.Config) {
			config.ObjectLimits.MaxPendingBytes = math.MaxInt64
		},
		"pending bytes below one object": func(config *localstore.Config) {
			config.LocalDisk.MaxObjectBytes = 1024
			config.ObjectLimits.MaxPendingBytes = 512
		},
		"reader connection limit": func(config *localstore.Config) {
			config.MaxReaderConnections = -1
		},
		"unbounded reader connections": func(config *localstore.Config) {
			config.MaxReaderConnections = math.MaxInt
		},
		"snapshot reader connection limit": func(config *localstore.Config) {
			config.MaxSnapshotReaderConnections = -1
		},
		"unbounded snapshot reader connections": func(config *localstore.Config) {
			config.MaxSnapshotReaderConnections = math.MaxInt
		},
		"integrity node limit": func(config *localstore.Config) {
			config.MaxIntegrityRecords = -1
		},
		"integrity nodes below an empty volume": func(config *localstore.Config) {
			config.MaxIntegrityRecords = sqlite.MinIntegrityRecords - 1
		},
		"unbounded integrity nodes": func(config *localstore.Config) {
			config.MaxIntegrityRecords = math.MaxInt64
		},
		"integrity byte limit": func(config *localstore.Config) {
			config.MaxIntegrityBytes = -1
		},
		"unbounded integrity bytes": func(config *localstore.Config) {
			config.MaxIntegrityBytes = math.MaxInt64
		},
	}
	for name, invalidate := range tests {
		t.Run(name, func(t *testing.T) {
			root := privateRoot(t)
			config := testConfig(root)
			invalidate(&config)
			store, err := localstore.Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open accepted invalid configuration")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("Open returned %v, want EINVAL", err)
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("invalid configuration created %d root entries", len(entries))
			}
		})
	}
}

func TestConfiguredLogWindowBoundsTheDurableLog(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.Window = sqlite.Window{Floor: 1, Cap: 1, Age: time.Hour}
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.Create(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), "second"); err != nil {
		t.Fatal(err)
	}

	result, err := metastore.NewChangeResult(1<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return 256 + lengths.Name + lengths.FromName + lengths.Content, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	retention, err := store.Log().Since(t.Context(), 0, 10, result)
	if err != nil {
		t.Fatal(err)
	}
	changes, err := result.Changes()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || retention.TrimmedThrough == 0 {
		t.Fatalf("one-entry log returned %d changes with retention %+v", len(changes), retention)
	}
}

func TestStatusCombinesLogicalObjectAndPhysicalState(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.ObjectLimits = sqlite.ObjectLimits{MaxPendingObjects: 8, MaxPendingBytes: 2 << 30}
	config.MaxSnapshotReaderConnections = 7
	config.MaxIntegrityRecords = 100
	config.MaxIntegrityBytes = 1 << 20
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })

	content := []byte("status payload")
	if err := store.Write(t.Context(), "artifact", content); err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Volume != config.Volume {
		t.Fatalf("Status.Volume = %q, want %q", status.Volume, config.Volume)
	}
	if status.Space.Total != config.Quota || status.Space.Used != int64(len(content)) {
		t.Fatalf("Status.Space = %+v, want total %d and used %d", status.Space, config.Quota, len(content))
	}
	if !status.Space.Coherent() {
		t.Fatalf("Status.Space is incoherent: %+v", status.Space)
	}
	if status.Objects.ReservedCount != 0 || status.Objects.ReservedBytes != 0 ||
		status.Objects.GarbageCount != 0 || status.Objects.GarbageBytes != 0 {
		t.Fatalf("a completed write left object maintenance state: %+v", status.Objects)
	}
	if status.ObjectLimits != config.ObjectLimits {
		t.Fatalf("Status.ObjectLimits = %+v, want %+v", status.ObjectLimits, config.ObjectLimits)
	}
	if status.MaxIntegrityRecords != config.MaxIntegrityRecords {
		t.Fatalf("Status.MaxIntegrityRecords = %d, want %d", status.MaxIntegrityRecords, config.MaxIntegrityRecords)
	}
	if status.MaxIntegrityBytes != config.MaxIntegrityBytes {
		t.Fatalf("Status.MaxIntegrityBytes = %d, want %d", status.MaxIntegrityBytes, config.MaxIntegrityBytes)
	}
	if status.MaxSnapshotReaderConnections != config.MaxSnapshotReaderConnections {
		t.Fatalf("Status.MaxSnapshotReaderConnections = %d, want %d",
			status.MaxSnapshotReaderConnections, config.MaxSnapshotReaderConnections)
	}
	if status.LocalDisk.StoreID == (localdisk.ID{}) {
		t.Fatal("Status.LocalDisk has no durable store ID")
	}
	if status.LocalDisk.PhysicalAvailable < 0 {
		t.Fatalf("Status.LocalDisk reports negative physical availability: %+v", status.LocalDisk)
	}
	if status.Maintenance.LastSweepError != nil {
		t.Fatalf("maintenance failed: %+v", status.Maintenance)
	}
}

func TestMetadataAndCompletionFilesRemainPrivate(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.Write(t.Context(), "artifact", []byte("content")); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{databaseName, databaseName + "-wal", databaseName + "-shm", "METASTORE", "LOCALSTORE"} {
		path := filepath.Join(config.Root, name)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Errorf("%s has mode %v, want a private regular file", filepath.Base(path), info.Mode())
		}
	}
}

func TestOpenRepairsMetadataPermissionsBeforeServing(t *testing.T) {
	config := testConfig(privateRoot(t))
	closeStore(t, open(t, config))
	database := filepath.Join(config.Root, databaseName)
	if err := os.Chmod(database, 0o644); err != nil {
		t.Fatal(err)
	}

	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	info, err := os.Stat(database)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("Open left metadata mode %04o, want 0600", info.Mode().Perm())
	}
}

func TestObjectBacklogLimitsRemainObservableWithoutBlockingShrinkOperations(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	if err := store.Write(t.Context(), "victim", []byte("victim")); err != nil {
		t.Fatal(err)
	}
	if err := store.Mkdir(t.Context(), "empty"); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), "source", []byte("source")); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), "destination", []byte("destination")); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)

	database := rawDatabase(t, filepath.Join(config.Root, databaseName), false)
	var volume int64
	if err := database.QueryRowContext(t.Context(),
		`SELECT id FROM volumes WHERE name = ?`, config.Volume).Scan(&volume); err != nil {
		t.Fatal(err)
	}
	for index := range 2 {
		key := strings.Repeat("x", localdisk.MaxKeyBytes+1) + string(rune('a'+index))
		if _, err := database.ExecContext(t.Context(), `
			INSERT INTO objects (key, volume, state, size, digest, created_sec, created_nsec)
			VALUES (?, ?, 2, 10, NULL, 0, ?)`, key, volume, index); err != nil {
			t.Fatal(err)
		}
	}
	closeStore(t, database)

	config.LocalDisk.MaxObjectBytes = 1 << 20
	config.ObjectLimits = sqlite.ObjectLimits{MaxPendingObjects: 1, MaxPendingBytes: 1 << 20}
	store = open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	status, err := store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !status.Objects.OverLimit {
		t.Fatalf("Status.Objects = %+v, want an over-limit backlog", status.Objects)
	}
	if status.ObjectLimits != config.ObjectLimits {
		t.Fatalf("Status.ObjectLimits = %+v, want %+v", status.ObjectLimits, config.ObjectLimits)
	}
	if err := store.Remove(t.Context(), "victim"); err != nil {
		t.Fatalf("Remove under backlog pressure: %v", err)
	}
	if err := store.RemoveDir(t.Context(), "empty"); err != nil {
		t.Fatalf("RemoveDir under backlog pressure: %v", err)
	}
	if err := store.Rename(t.Context(), "source", "destination"); err != nil {
		t.Fatalf("Rename under backlog pressure: %v", err)
	}
	content, err := store.Read(t.Context(), "destination")
	if err != nil || string(content) != "source" {
		t.Fatalf("renamed destination = %q, %v", content, err)
	}
}

func TestStatusReportsFailureWhenDurableComponentsAreClosed(t *testing.T) {
	store := open(t, testConfig(privateRoot(t)))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(t.Context())
	if err == nil {
		t.Fatalf("Status after Close succeeded with %+v", status)
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Status after Close returned %v, want EIO", err)
	}
	if status.LocalDisk.StoreID == (localdisk.ID{}) {
		t.Fatal("failed status lost the durable store identity")
	}
}

func TestStatusReportsEffectiveDefaultObjectLimits(t *testing.T) {
	store := open(t, testConfig(privateRoot(t)))
	t.Cleanup(func() { closeStore(t, store) })
	status, err := store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := sqlite.DefaultObjectLimits(); status.ObjectLimits != want {
		t.Fatalf("Status.ObjectLimits = %+v, want defaults %+v", status.ObjectLimits, want)
	}
	if status.MaxReaderConnections != sqlite.DefaultMaxReaderConnections {
		t.Fatalf("Status.MaxReaderConnections = %d, want default %d",
			status.MaxReaderConnections, sqlite.DefaultMaxReaderConnections)
	}
	if status.MaxIntegrityRecords != sqlite.DefaultMaxIntegrityRecords {
		t.Fatalf("Status.MaxIntegrityRecords = %d, want default %d",
			status.MaxIntegrityRecords, sqlite.DefaultMaxIntegrityRecords)
	}
	if status.MaxIntegrityBytes != sqlite.DefaultMaxIntegrityBytes {
		t.Fatalf("Status.MaxIntegrityBytes = %d, want default %d",
			status.MaxIntegrityBytes, sqlite.DefaultMaxIntegrityBytes)
	}
	if status.MaxSnapshotReaderConnections != sqlite.DefaultMaxSnapshotReaderConnections {
		t.Fatalf("Status.MaxSnapshotReaderConnections = %d, want default %d",
			status.MaxSnapshotReaderConnections, sqlite.DefaultMaxSnapshotReaderConnections)
	}
}

func TestConfiguredReaderConnectionLimitIsReported(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.MaxReaderConnections = 3
	config.MaxSnapshotReaderConnections = 5
	config.MaxIntegrityRecords = 101
	config.MaxIntegrityBytes = 202
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	status, err := store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.MaxReaderConnections != config.MaxReaderConnections {
		t.Fatalf("Status.MaxReaderConnections = %d, want %d",
			status.MaxReaderConnections, config.MaxReaderConnections)
	}
	if status.MaxIntegrityRecords != config.MaxIntegrityRecords {
		t.Fatalf("Status.MaxIntegrityRecords = %d, want %d",
			status.MaxIntegrityRecords, config.MaxIntegrityRecords)
	}
	if status.MaxIntegrityBytes != config.MaxIntegrityBytes {
		t.Fatalf("Status.MaxIntegrityBytes = %d, want %d",
			status.MaxIntegrityBytes, config.MaxIntegrityBytes)
	}
	if status.MaxSnapshotReaderConnections != config.MaxSnapshotReaderConnections {
		t.Fatalf("Status.MaxSnapshotReaderConnections = %d, want %d",
			status.MaxSnapshotReaderConnections, config.MaxSnapshotReaderConnections)
	}
}

func TestStatusUsesControlAdmissionWhileTheDataPlaneIsSaturated(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.Quota = 256 << 20
	config.LocalDisk.MaxObjectBytes = 128 << 20
	config.LocalDisk.MaxInFlightOperations = 1
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.Write(context.Background(), "large", make([]byte, 128<<20))
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	var status localstore.Status
	for {
		statusContext, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		observed, err := store.Status(statusContext)
		cancel()
		if err != nil {
			t.Fatalf("Status under saturated data admission: %v", err)
		}
		if observed.LocalDisk.InFlightOperations == 1 {
			status = observed
			break
		}
		select {
		case err := <-writeDone:
			t.Fatalf("the data operation finished before saturation was observed: %v", err)
		case <-deadline.C:
			t.Fatal("the data operation never occupied its single admission slot")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case err := <-writeDone:
		t.Fatalf("Status waited for the saturated data operation to finish: %v", err)
	default:
	}
	if status.Objects.ReservedCount != 1 || status.Objects.ReservedBytes != 128<<20 {
		t.Fatalf("Status.Objects = %+v, want the admitted write reservation", status.Objects)
	}
	if status.Space.Total != config.Quota || status.Space.Used != 0 || !status.Space.Coherent() {
		t.Fatalf("Status.Space = %+v, want a coherent unused logical quota", status.Space)
	}
	if status.Space.Avail > status.LocalDisk.PhysicalAvailable {
		t.Fatalf("Status.Space.Avail = %d exceeds physical availability %d",
			status.Space.Avail, status.LocalDisk.PhysicalAvailable)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("saturating Write: %v", err)
	}
}

func TestSpaceClampsTheVolumeAllowanceToPhysicalAvailability(t *testing.T) {
	config := testConfig(privateRoot(t))
	config.LocalDisk.MaintenanceReserveBytes = math.MaxInt64
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })

	space, err := store.Space(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if space.Total != config.Quota || space.Used != 0 || space.Avail != 0 {
		t.Fatalf("Space = %+v, want the logical quota clamped to zero physical availability", space)
	}
	status, err := store.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.LocalDisk.PhysicalAvailable != 0 {
		t.Fatalf("local disk availability is %d, want zero", status.LocalDisk.PhysicalAvailable)
	}
}

func TestDatabaseBoundToAnotherObjectStoreIsRefused(t *testing.T) {
	config := testConfig(privateRoot(t))
	closeStore(t, open(t, config))
	database := rawDatabase(t, filepath.Join(config.Root, databaseName), false)
	if _, err := database.ExecContext(t.Context(),
		`UPDATE backing_store SET store_id = 'another-store' WHERE singleton = 1`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := localstore.Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("metadata bound to another object root was accepted")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening malformed mismatched metadata returned %v, want EIO", err)
	}

	objects, openErr := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if openErr != nil {
		t.Fatalf("the failed composite open retained the object-store lock: %v", openErr)
	}
	closeStore(t, objects)
}

func TestCompletionMarkerCannotBeMovedToAnotherObjectRoot(t *testing.T) {
	firstConfig := testConfig(privateRoot(t))
	secondConfig := testConfig(privateRoot(t))
	closeStore(t, open(t, firstConfig))
	closeStore(t, open(t, secondConfig))
	copyFile(t,
		filepath.Join(secondConfig.Root, "LOCALSTORE"),
		filepath.Join(firstConfig.Root, "LOCALSTORE"),
	)

	store, err := localstore.Open(t.Context(), firstConfig)
	if err == nil {
		store.Close()
		t.Fatal("a completion marker from another object root was accepted")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestMalformedCompletionMarkerFailsClosed(t *testing.T) {
	tests := map[string]func([]byte) []byte{
		"truncated": func(marker []byte) []byte { return marker[:len(marker)-1] },
		"magic": func(marker []byte) []byte {
			marker[0] ^= 0xff
			return marker
		},
		"total length": func(marker []byte) []byte {
			binary.BigEndian.PutUint32(marker[12:16], uint32(len(marker)+1))
			return marker
		},
		"volume length": func(marker []byte) []byte {
			binary.BigEndian.PutUint32(marker[16:20], localstore.MaxVolumeBytes+1)
			return marker
		},
		"checksum": func(marker []byte) []byte {
			marker[len(marker)-1] ^= 0xff
			return marker
		},
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			config := testConfig(privateRoot(t))
			closeStore(t, open(t, config))
			path := filepath.Join(config.Root, "LOCALSTORE")
			marker, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, corrupt(marker), 0o600); err != nil {
				t.Fatal(err)
			}

			store, err := localstore.Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open accepted a malformed completion marker")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func TestMissingMetadataNeverReopensAsAnEmptyVolume(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	if err := store.Write(t.Context(), "artifact", []byte("content")); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)
	if err := os.Remove(filepath.Join(config.Root, databaseName)); err != nil {
		t.Fatal(err)
	}

	reopened, err := localstore.Open(t.Context(), config)
	if err == nil {
		reopened.Close()
		t.Fatal("Open recreated missing metadata and exposed an empty volume")
	}
	if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Open returned %v, want EIO without ENOENT", err)
	}
	objects, openErr := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if openErr != nil {
		t.Fatalf("the refused reopen retained object-store ownership: %v", openErr)
	}
	closeStore(t, objects)
}

func TestEmptyReplacementMetadataNeverReopensAsAnEmptyVolume(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	if err := store.Write(t.Context(), "artifact", []byte("content")); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)
	if err := os.WriteFile(filepath.Join(config.Root, databaseName), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := localstore.Open(t.Context(), config)
	if err == nil {
		reopened.Close()
		t.Fatal("Open initialized empty replacement metadata")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestLosingCompletionAndMetadataNeverReopensAsAnEmptyVolume(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	if err := store.Write(t.Context(), "artifact", []byte("content")); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)
	for _, name := range []string{"LOCALSTORE", databaseName} {
		if err := os.Remove(filepath.Join(config.Root, name)); err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := localstore.Open(t.Context(), config)
	if err == nil {
		reopened.Close()
		t.Fatal("Open recreated both completion and metadata as an empty volume")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestLosingTheBoundVolumeRowNeverRecreatesAnEmptyVolume(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	if err := store.Write(t.Context(), "artifact", []byte("content")); err != nil {
		t.Fatal(err)
	}
	closeStore(t, store)

	database := rawDatabase(t, filepath.Join(config.Root, databaseName), false)
	database.SetMaxOpenConns(1)
	connection, err := database.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(t.Context(), `DELETE FROM volumes`); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := localstore.Open(t.Context(), config)
	if err == nil {
		reopened.Close()
		t.Fatal("Open recreated the missing bound volume")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	database = rawDatabase(t, filepath.Join(config.Root, databaseName), true)
	t.Cleanup(func() { closeStore(t, database) })
	var volumes int
	if err := database.QueryRowContext(t.Context(), `SELECT count(*) FROM volumes`).Scan(&volumes); err != nil {
		t.Fatal(err)
	}
	if volumes != 0 {
		t.Fatalf("the refused reopen created %d volumes", volumes)
	}
}

func TestMetastoreSymlinkIsRefused(t *testing.T) {
	config := testConfig(privateRoot(t))
	closeStore(t, open(t, config))
	database := filepath.Join(config.Root, databaseName)
	external := filepath.Join(filepath.Dir(config.Root), "external.sqlite")
	if err := os.Rename(database, external); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, database); err != nil {
		t.Fatal(err)
	}

	store, err := localstore.Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("Open followed a metastore symlink")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestSQLiteURIMetacharactersAreRefusedBeforeInitialization(t *testing.T) {
	for _, name := range []string{"question?query", "fragment#name", "encoded%2Fseparator"} {
		t.Run(name, func(t *testing.T) {
			parent := privateRoot(t)
			root := filepath.Join(parent, name)
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			store, err := localstore.Open(t.Context(), testConfig(root))
			if err == nil {
				store.Close()
				t.Fatal("Open accepted a root SQLite interprets as a file URI")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("Open returned %v, want EINVAL", err)
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("refused root contains %d entries", len(entries))
			}
		})
	}
}

func TestWritableParentIsRefusedBeforeInitialization(t *testing.T) {
	parent := privateRoot(t)
	if err := os.Chmod(parent, 0o770); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := localstore.Open(t.Context(), testConfig(root))
	if err == nil {
		store.Close()
		t.Fatal("Open accepted a root beneath a group-writable parent")
	}
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("Open returned %v, want EACCES", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("refused root contains %d entries", len(entries))
	}
}

func TestNonStickyWritableAncestorIsRefused(t *testing.T) {
	ancestor := privateRoot(t)
	if err := os.Chmod(ancestor, 0o777); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(ancestor, "parent")
	root := filepath.Join(parent, "store")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := localstore.Open(t.Context(), testConfig(root))
	if err == nil {
		store.Close()
		t.Fatal("Open accepted a path through a replaceable ancestor")
	}
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("Open returned %v, want EACCES", err)
	}
}

func TestStickyWritableAncestorProtectingAnOwnedEntryIsAllowed(t *testing.T) {
	ancestor := privateRoot(t)
	if err := os.Chmod(ancestor, fs.ModeSticky|0o777); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(ancestor, "store")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	store := open(t, testConfig(root))
	closeStore(t, store)
}

func TestADatabaseOpenFailureReleasesObjectStoreOwnership(t *testing.T) {
	root := privateRoot(t)
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	closeStore(t, objects)
	if err := os.WriteFile(filepath.Join(root, databaseName), []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := localstore.Open(t.Context(), testConfig(root))
	if err == nil {
		store.Close()
		t.Fatal("Open accepted a corrupt metadata database")
	}

	reopened, openErr := localdisk.Open(t.Context(), root, localdisk.Options{})
	if openErr != nil {
		t.Fatalf("the failed metadata open retained object-store ownership: %v", openErr)
	}
	closeStore(t, reopened)
}

func TestCloseIsConcurrentIdempotentAndAllowsReopen(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	if err := store.Write(t.Context(), "artifact", []byte("content")); err != nil {
		t.Fatal(err)
	}

	const callers = 16
	start := make(chan struct{})
	errorsByCaller := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			errorsByCaller <- store.Close()
		}()
	}
	ready.Wait()
	close(start)
	for range callers {
		if err := <-errorsByCaller; err != nil {
			t.Fatalf("Close returned %v", err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("a later Close returned %v", err)
	}

	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	content, err := reopened.Read(t.Context(), "artifact")
	if err != nil {
		t.Fatalf("reopening after Close: %v", err)
	}
	if string(content) != "content" {
		t.Fatalf("the close sequence lost content: %q", content)
	}
}

func testConfig(root string) localstore.Config {
	return localstore.Config{
		Root:   root,
		Volume: "workspace",
		Quota:  1 << 20,
		Window: sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{
			SweepInterval: time.Hour,
			SweepBatch:    8,
		},
	}
}

func privateRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func open(t *testing.T, config localstore.Config) *localstore.Store {
	t.Helper()
	store, err := localstore.Open(t.Context(), config)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store
}

type closer interface {
	Close() error
}

func closeStore(t *testing.T, value closer) {
	t.Helper()
	if err := value.Close(); err != nil {
		t.Fatal(err)
	}
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	content, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func rawDatabase(t *testing.T, path string, readOnly bool) *sql.DB {
	t.Helper()
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)"
	if readOnly {
		dsn += "&mode=ro"
	}
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return database
}
