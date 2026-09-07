package localstore_test

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	crashHelperEnvironment = "REMOTE_FS_LOCALSTORE_CRASH_HELPER"
	crashRootEnvironment   = "REMOTE_FS_LOCALSTORE_CRASH_ROOT"
)

func TestKilledStoreRequiresItsAcknowledgedWAL(t *testing.T) {
	if os.Getenv(crashHelperEnvironment) == "1" {
		runCrashHelper(t)
		return
	}

	t.Run("intact WAL recovers", func(t *testing.T) {
		root := privateRoot(t)
		killStoreWithPinnedWAL(t, root)
		assertNonemptyWAL(t, root)

		store := open(t, testConfig(root))
		defer closeStore(t, store)
		if _, err := store.Stat(t.Context(), "acknowledged"); err != nil {
			t.Fatalf("recovered store lost the acknowledged mutation: %v", err)
		}
	})

	for name, damage := range map[string]func(string) error{
		"missing WAL": os.Remove,
		"empty WAL":   func(path string) error { return os.Truncate(path, 0) },
		"header only": func(path string) error { return os.Truncate(path, 32) },
	} {
		t.Run(name+" fails closed", func(t *testing.T) {
			root := privateRoot(t)
			killStoreWithPinnedWAL(t, root)
			wal := assertNonemptyWAL(t, root)
			if err := damage(wal); err != nil {
				t.Fatal(err)
			}

			store, err := localstore.Open(t.Context(), testConfig(root))
			if err == nil {
				store.Close()
				t.Fatal("Open accepted a completed store after its acknowledged WAL was lost")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func runCrashHelper(t *testing.T) {
	root := os.Getenv(crashRootEnvironment)
	if root == "" {
		t.Fatal("crash helper has no root")
	}
	store := open(t, testConfig(root))
	snapshot, _, err := store.Log().Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := store.Create(t.Context(), "acknowledged"); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "acknowledged"); err != nil {
		t.Fatal(err)
	}
	if err := os.Stdout.Sync(); err != nil {
		t.Fatal(err)
	}
	select {}
}

func killStoreWithPinnedWAL(t *testing.T, root string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestKilledStoreRequiresItsAcknowledgedWAL$")
	command.Env = append(os.Environ(),
		crashHelperEnvironment+"=1",
		crashRootEnvironment+"="+root,
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
		t.Fatalf("wait for crash helper acknowledgement: %v", err)
	}
	if strings.TrimSpace(line) != "acknowledged" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("crash helper printed %q", line)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed crash helper exited successfully")
	}
}

func assertNonemptyWAL(t *testing.T, root string) string {
	t.Helper()
	wal := filepath.Join(root, databaseName+"-wal")
	info, err := os.Stat(wal)
	if err != nil {
		t.Fatalf("stat acknowledged WAL: %v", err)
	}
	if info.Size() <= 32 {
		t.Fatalf("acknowledged WAL has no frames: %d bytes", info.Size())
	}
	return wal
}

func TestCompletedStoreRejectsImpossibleInitializationIntent(t *testing.T) {
	for name, content := range map[string][]byte{
		"empty":   nil,
		"damaged": []byte("not a binding"),
	} {
		t.Run(name, func(t *testing.T) {
			config := testConfig(privateRoot(t))
			closeStore(t, open(t, config))
			if err := os.WriteFile(
				filepath.Join(config.Root, localdisk.InitializationMarkerName),
				content,
				0o600,
			); err != nil {
				t.Fatal(err)
			}

			store, err := localstore.Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open accepted an impossible initialization intent beside READY")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func TestCompletedStoreRejectsInitializationBindingStage(t *testing.T) {
	config := testConfig(privateRoot(t))
	closeStore(t, open(t, config))
	if err := os.WriteFile(filepath.Join(config.Root, ".LOCALSTORE.init.stage"), []byte("interrupted"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := localstore.Open(t.Context(), config)
	if err == nil {
		store.Close()
		t.Fatal("Open accepted an initialization stage beside READY")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestMetastoreAuxiliaryFilesCannotBootstrapMetadata(t *testing.T) {
	for name, entries := range map[string][]string{
		"missing main":  {databaseName + "-wal"},
		"pristine main": {databaseName, databaseName + "-shm"},
	} {
		t.Run(name, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := localdisk.Open(t.Context(), root, localdisk.Options{CompositeInitialization: true})
			if err != nil {
				t.Fatal(err)
			}
			closeStore(t, objects)
			for _, entry := range entries {
				if err := os.WriteFile(filepath.Join(root, entry), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}

			store, err := localstore.Open(t.Context(), testConfig(root))
			if err == nil {
				store.Close()
				t.Fatal("Open accepted SQLite auxiliary state without initialized metadata")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func TestCompletedStoreRequiresAnIntactMetastoreWitness(t *testing.T) {
	for name, corrupt := range map[string]func(string) error{
		"missing": os.Remove,
		"checksum": func(path string) error {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			content[len(content)-1] ^= 0xff
			return os.WriteFile(path, content, 0o600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := testConfig(privateRoot(t))
			closeStore(t, open(t, config))
			if err := corrupt(filepath.Join(config.Root, "METASTORE")); err != nil {
				t.Fatal(err)
			}
			store, err := localstore.Open(t.Context(), config)
			if err == nil {
				store.Close()
				t.Fatal("Open accepted a completed store without an intact metastore witness")
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func TestWitnessPublicationFailurePoisonsAndRetainsOwnership(t *testing.T) {
	config := testConfig(privateRoot(t))
	store := open(t, config)
	stage := filepath.Join(config.Root, ".METASTORE.stage")
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := os.Mkdir(stage, 0o700)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) || time.Now().After(deadline) {
			t.Fatalf("occupy witness stage: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	if err := store.Create(t.Context(), "committed"); err == nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("Create with failed witness publication returned %v, want EIO", err)
	}
	if _, err := store.Stat(t.Context(), "committed"); err == nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("poisoned store Stat returned %v, want EIO", err)
	}
	if err := os.Remove(stage); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err == nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("poisoned store Close returned %v, want EIO", err)
	}

	objects, err := localdisk.Open(t.Context(), config.Root, config.LocalDisk)
	if err == nil {
		objects.Close()
		t.Fatal("failed metastore close released object-store ownership")
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("opening the retained object store returned %v, want EBUSY", err)
	}
}
