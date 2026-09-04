package localstore

import (
	"errors"
	"os"
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
			Root: root, Workspace: "workspace", Quota: 1 << 20,
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
		Root:      root,
		Workspace: "workspace",
		Quota:     1 << 20,
		Window:    sqlite.DefaultWindow(),
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

func TestInterruptedInitializationRejectsAnotherWorkspace(t *testing.T) {
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
		Root:      root,
		Workspace: "workspaec",
		Quota:     1 << 20,
		Window:    sqlite.DefaultWindow(),
		Maintenance: objectstore.Options{
			SweepInterval: time.Hour,
			SweepBatch:    8,
		},
	})
	if err == nil {
		store.Close()
		t.Fatal("Open accepted another workspace while initialization was incomplete")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	if _, err := os.Stat(filepath.Join(root, metastoreFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the refused workspace created metadata: %v", err)
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
