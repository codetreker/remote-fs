package localdisk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/objectstoretest"
)

func privateRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, privateDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return root
}

func openForTest(t *testing.T, options Options) *Objects {
	t.Helper()
	objects, err := Open(t.Context(), privateRoot(t), options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := objects.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return objects
}

func TestObjectsContract(t *testing.T) {
	objectstoretest.Run(t, func(t *testing.T) objectstore.Objects {
		objects, err := Open(t.Context(), privateRoot(t), Options{})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return objects
	})
}

func TestInitializationPersistsIdentityAndPrivateLayout(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	id := objects.ID()
	if !objects.NewlyInitialized() {
		t.Fatal("the first Open did not report initialization")
	}
	if id == (ID{}) || len(id.String()) != 36 {
		t.Fatalf("ID is %q, want a UUID", id.String())
	}
	if _, err := objects.Put(t.Context(), "a/key/../with\x00bytes", []byte("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, path := range []string{root, filepath.Join(root, objectsDirectory)} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != privateDirectoryMode {
			t.Errorf("%s mode is %04o, want %04o", path, got, privateDirectoryMode)
		}
	}
	for _, path := range []string{filepath.Join(root, manifestName), filepath.Join(root, ownerLockName)} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != privateFileMode {
			t.Errorf("%s mode is %04o, want %04o", path, got, privateFileMode)
		}
	}

	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if reopened.NewlyInitialized() {
		t.Fatal("reopen reported a new initialization")
	}
	if reopened.ID() != id {
		t.Fatalf("reopened ID is %s, want %s", reopened.ID(), id)
	}
	got, err := reopened.Get(t.Context(), "a/key/../with\x00bytes")
	if err != nil || string(got) != "payload" {
		t.Fatalf("Get after reopen returned %q, %v", got, err)
	}
}

func TestOpenRequiresAnOwnedRecognizedRoot(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		root := filepath.Join(privateRoot(t), "missing")
		if _, err := Open(t.Context(), root, Options{}); err == nil {
			t.Fatal("Open succeeded")
		}
	})
	t.Run("not a directory", func(t *testing.T) {
		root := filepath.Join(privateRoot(t), "file")
		if err := os.WriteFile(root, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.ENOTDIR) {
			t.Fatalf("Open returned %v, want ENOTDIR", err)
		}
	})
	t.Run("public mode", func(t *testing.T) {
		root := privateRoot(t)
		if err := os.Chmod(root, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("Open returned %v, want EACCES", err)
		}
	})
	t.Run("unrecognized nonempty", func(t *testing.T) {
		root := privateRoot(t)
		if err := os.WriteFile(filepath.Join(root, "somebody-elses-file"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.ENOTEMPTY) {
			t.Fatalf("Open returned %v, want ENOTEMPTY", err)
		}
	})
}

func TestCompositeInitializationMarkerIsTheOnlyReservedPreFormatEntry(t *testing.T) {
	t.Run("private regular marker", func(t *testing.T) {
		root := privateRoot(t)
		marker := filepath.Join(root, InitializationMarkerName)
		if err := os.WriteFile(marker, []byte("composite intent"), privateFileMode); err != nil {
			t.Fatal(err)
		}
		objects, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if !objects.NewlyInitialized() {
			t.Fatal("Open did not initialize the marked root")
		}
		if got := objects.CompositeInitializationState(); got != NoCompositeInitialization {
			t.Fatalf("raw Open state is %v, want NoCompositeInitialization", got)
		}
		if err := objects.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(marker); err != nil || string(got) != "composite intent" {
			t.Fatalf("localdisk changed the composite marker to %q, %v", got, err)
		}
	})

	for _, test := range []struct {
		name   string
		create func(*testing.T, string)
	}{
		{name: "public file", create: func(t *testing.T, path string) {
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "directory", create: func(t *testing.T, path string) {
			if err := os.Mkdir(path, privateDirectoryMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", create: func(t *testing.T, path string) {
			if err := os.Symlink("elsewhere", path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fifo", create: func(t *testing.T, path string) {
			if err := unix.Mkfifo(path, privateFileMode); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRoot(t)
			test.create(t, filepath.Join(root, InitializationMarkerName))
			if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
		})
	}
}

func TestCompositeInitializationIntentIsCreatedUnderTheRootLock(t *testing.T) {
	root := privateRoot(t)
	options := Options{CompositeInitialization: true}
	ops := systemFileOperations
	originalLink := ops.linkat
	observedBeforePublication := false
	ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if _, err := os.Stat(filepath.Join(root, InitializationMarkerName)); err != nil {
			return fmt.Errorf("publication began without a durable composite intent: %w", err)
		}
		observedBeforePublication = true
		return originalLink(oldFD, old, newFD, new, flags)
	}
	objects, err := open(t.Context(), root, options, ops)
	if err != nil {
		t.Fatal(err)
	}
	if !observedBeforePublication {
		t.Fatal("the publication seam did not observe the initialization intent")
	}
	if got := objects.CompositeInitializationState(); got != CompositeInitializationStarted {
		t.Fatalf("state is %v, want CompositeInitializationStarted", got)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), root, options)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.CompositeInitializationState(); got != CompositeInitializationResumed {
		t.Fatalf("reopen state is %v, want CompositeInitializationResumed", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, InitializationMarkerName)); err != nil {
		t.Fatal(err)
	}
	complete, err := Open(t.Context(), root, options)
	if err != nil {
		t.Fatal(err)
	}
	defer complete.Close()
	if got := complete.CompositeInitializationState(); got != NoCompositeInitialization {
		t.Fatalf("completed state is %v, want NoCompositeInitialization", got)
	}
}

func TestCompositeInitializerCannotRewriteIntentHeldByAnotherOwner(t *testing.T) {
	root := privateRoot(t)
	marker := filepath.Join(root, InitializationMarkerName)
	const intent = "original composite intent"
	if err := os.WriteFile(marker, []byte(intent), privateFileMode); err != nil {
		t.Fatal(err)
	}
	options := Options{CompositeInitialization: true}
	first, err := Open(t.Context(), root, options)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if got := first.CompositeInitializationState(); got != CompositeInitializationResumed {
		t.Fatalf("state is %v, want CompositeInitializationResumed", got)
	}
	if _, err := Open(t.Context(), root, options); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("second initializer returned %v, want EBUSY", err)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != intent {
		t.Fatalf("intent changed to %q, %v", got, err)
	}
}

func TestCompositeIntentMustBeDirectorySyncedBeforeFormat(t *testing.T) {
	root := privateRoot(t)
	ops := systemFileOperations
	originalSync := ops.fsync
	failed := false
	ops.fsync = func(fd int) error {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return err
		}
		if !failed && st.Mode&unix.S_IFMT == unix.S_IFDIR {
			if _, err := os.Stat(filepath.Join(root, InitializationMarkerName)); err == nil {
				failed = true
				return syscall.EIO
			}
		}
		return originalSync(fd)
	}
	if _, err := open(t.Context(), root, Options{CompositeInitialization: true}, ops); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	if !failed {
		t.Fatal("fault seam did not reach the intent directory sync")
	}
	if _, err := os.Stat(filepath.Join(root, manifestName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("FORMAT was published after intent sync failure: %v", err)
	}
}

func TestRawObjectsOpenDoesNotCreateCompositeIntent(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	if got := objects.CompositeInitializationState(); got != NoCompositeInitialization {
		t.Fatalf("state is %v, want NoCompositeInitialization", got)
	}
	if _, err := os.Stat(filepath.Join(root, InitializationMarkerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw Open created a composite intent: %v", err)
	}
}

func TestLifetimeLockIsExclusiveAndReleasedByClose(t *testing.T) {
	root := privateRoot(t)
	first, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("second Open returned %v, want EBUSY", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseIsConcurrentAndClosedOperationsFailLoudly(t *testing.T) {
	objects := openForTest(t, Options{})
	const callers = 16
	var group sync.WaitGroup
	errorsSeen := make(chan error, callers)
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsSeen <- objects.Close()
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	}
	if _, err := objects.Get(t.Context(), "key"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Get after Close returned %v, want EIO", err)
	}
}

func objectPath(t *testing.T, root, key string) string {
	t.Helper()
	location, err := locate(key)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, objectsDirectory, location.first, location.final)
}

func stagingPath(t *testing.T, root, key string) string {
	t.Helper()
	location, err := locate(key)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, objectsDirectory, location.first, location.staging)
}

func writeRecoveryMarkerForTest(t *testing.T, root string, id ID, key string, deleting bool) string {
	t.Helper()
	name, err := markerName(key, deleting)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, objectsDirectory, name)
	encoded := encodeRecoveryMarker(id, key, deleting)
	if err := os.WriteFile(path, encoded[:], privateFileMode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestKeyEncodingIsInjectiveBoundedAndConfined(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	keys := []string{"", ".", "..", "a/b", "/absolute", "a\\b", "\x00", "\xff\xfe", strings.Repeat("x", MaxKeyBytes)}
	for i, key := range keys {
		content := []byte(fmt.Sprintf("object-%d", i))
		if _, err := objects.Put(t.Context(), key, content); err != nil {
			t.Fatalf("Put of key %q: %v", key, err)
		}
		got, err := objects.Get(t.Context(), key)
		if err != nil || !bytes.Equal(got, content) {
			t.Fatalf("Get of key %q returned %q, %v", key, got, err)
		}
		location, err := locate(key)
		if err != nil {
			t.Fatal(err)
		}
		for _, component := range []string{location.first, location.final, location.staging} {
			if component == "." || component == ".." || strings.ContainsAny(component, "/\\\x00") {
				t.Errorf("key %q produced unsafe component %q", key, component)
			}
		}
		path := objectPath(t, root, key)
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Errorf("key %q escaped root as %q", key, path)
		}
	}
	tooLong := strings.Repeat("y", MaxKeyBytes+1)
	if _, err := objects.Put(t.Context(), tooLong, nil); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("Put of an oversized key returned %v, want ENAMETOOLONG", err)
	}
}

func TestGetFailsClosedOnEveryObjectCorruption(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, path string)
	}{
		{name: "truncated", mutate: func(t *testing.T, path string) {
			if err := os.Truncate(path, fixedEnvelopeBytes-1); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "appended", mutate: func(t *testing.T, path string) {
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte("extra")); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "magic", mutate: flipByte(0)},
		{name: "store ID", mutate: flipByte(12)},
		{name: "key", mutate: flipByte(fixedEnvelopeBytes)},
		{name: "payload", mutate: flipByte(fixedEnvelopeBytes + len("corrupt"))},
		{name: "public permissions", mutate: func(t *testing.T, path string) {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", mutate: func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("somewhere", path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fifo", mutate: func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := unix.Mkfifo(path, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer objects.Close()
			if _, err := objects.Put(t.Context(), "corrupt", []byte("payload")); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, objectPath(t, root, "corrupt"))
			got, err := objects.Get(t.Context(), "corrupt")
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOENT) {
				t.Fatalf("Get returned %q, %v; want only EIO", got, err)
			}
			if got != nil {
				t.Errorf("Get returned %q alongside corruption", got)
			}
		})
	}
}

func TestGetBoundedRefusesBeforePayloadAdmission(t *testing.T) {
	objects := openForTest(t, Options{})
	key := "bounded-read"
	content := []byte("four")
	if _, err := objects.Put(t.Context(), key, content); err != nil {
		t.Fatal(err)
	}
	got, err := objects.GetBounded(t.Context(), key, int64(len(content)))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("GetBounded at the boundary returned %q, %v", got, err)
	}
	if got, err := objects.GetBounded(t.Context(), key, int64(len(content)-1)); !errors.Is(err, syscall.EFBIG) || got != nil {
		t.Fatalf("GetBounded above the boundary returned %q, %v; want EFBIG", got, err)
	}
	for _, limit := range []int64{0, -1} {
		if _, err := objects.GetBounded(t.Context(), key, limit); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("GetBounded limit %d returned %v, want EINVAL", limit, err)
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := objects.GetBounded(cancelled, key, int64(len(content))); !errors.Is(err, syscall.EINTR) {
		t.Fatalf("cancelled GetBounded returned %v, want EINTR", err)
	}
	operations, inFlightBytes, _ := objects.gate.snapshot()
	if operations != 0 || inFlightBytes != 0 {
		t.Fatalf("refused bounded read retained %d operations and %d bytes", operations, inFlightBytes)
	}
}

func flipByte(offset int) func(*testing.T, string) {
	return func(t *testing.T, path string) {
		t.Helper()
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		var value [1]byte
		if _, err := file.ReadAt(value[:], int64(offset)); err != nil {
			t.Fatal(err)
		}
		value[0] ^= 0xff
		if _, err := file.WriteAt(value[:], int64(offset)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMissingLeafIsDistinctFromMissingOrCorruptStoreState(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Get(t.Context(), "missing"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missing leaf returned %v, want ENOENT", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, objectsDirectory), filepath.Join(root, "objects-away")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missing objects root returned %v, want only EIO", err)
	}

	root = privateRoot(t)
	objects, err = Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	flipByte(20)(t, filepath.Join(root, manifestName))
	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt FORMAT returned %v, want EIO", err)
	}
}

func TestOpenRecoversOnlyDurableStagingResidues(t *testing.T) {
	t.Run("put residue", func(t *testing.T) {
		root := privateRoot(t)
		objects, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatal(err)
		}
		key := "interrupted-put"
		location, _ := locate(key)
		shardFD, _, err := objects.openShard(location, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Close(shardFD); err != nil {
			t.Fatal(err)
		}
		if err := objects.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stagingPath(t, root, key), []byte("partial envelope"), privateFileMode); err != nil {
			t.Fatal(err)
		}
		markerPath := writeRecoveryMarkerForTest(t, root, objects.ID(), key, false)
		marker := filepath.Base(markerPath)

		reopened, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer reopened.Close()
		if _, err := os.Lstat(stagingPath(t, root, key)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("staging file remains: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(root, objectsDirectory, marker)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("recovery record remains: %v", err)
		}
		if _, err := reopened.Get(t.Context(), key); !errors.Is(err, syscall.ENOENT) {
			t.Errorf("interrupted Put became visible: %v", err)
		}
	})

	t.Run("delete residue", func(t *testing.T) {
		root := privateRoot(t)
		objects, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatal(err)
		}
		key := "interrupted-delete"
		if _, err := objects.Put(t.Context(), key, []byte("garbage")); err != nil {
			t.Fatal(err)
		}
		if err := objects.Close(); err != nil {
			t.Fatal(err)
		}
		writeRecoveryMarkerForTest(t, root, objects.ID(), key, true)
		reopened, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer reopened.Close()
		if _, err := reopened.Get(t.Context(), key); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("delete recovery left the object visible: %v", err)
		}
	})
}

func TestRecoveryWalkIsBoundedAndRejectsUnknownEntries(t *testing.T) {
	options := Options{MaxObjectBytes: 1, MaxInFlightBytes: fixedEnvelopeBytes + MaxKeyBytes + 1,
		MaxInFlightOperations: 1, MaxRecoveryEntries: 1}
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"one", "two"} {
		writeRecoveryMarkerForTest(t, root, objects.ID(), key, false)
	}
	if _, err := Open(t.Context(), root, options); !errors.Is(err, syscall.EOVERFLOW) {
		t.Fatalf("Open with too many residues returned %v, want EOVERFLOW", err)
	}

	root = privateRoot(t)
	objects, err = Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, objectsDirectory, "unrecognized"), nil, privateFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open with unknown object-root entry returned %v, want EIO", err)
	}
}

func TestFailedPublicationCleansItsResidueAndDoesNotStore(t *testing.T) {
	objects := openForTest(t, Options{})
	key := "link-fails"
	location, _ := locate(key)
	original := objects.ops.linkat
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if new == location.final {
			return syscall.EIO
		}
		return original(oldFD, old, newFD, new, flags)
	}
	if _, err := objects.Put(t.Context(), key, []byte("bytes")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want EIO", err)
	}
	objects.ops.linkat = original
	if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed Put stored an object: %v", err)
	}
	if _, err := os.Lstat(stagingPath(t, objects.rootPath, key)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed Put left staging state: %v", err)
	}
	marker, _ := markerName(key, false)
	if _, err := os.Lstat(filepath.Join(objects.rootPath, objectsDirectory, marker)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed Put left recovery state: %v", err)
	}
	if _, err := objects.Put(t.Context(), key, []byte("retry")); err != nil {
		t.Fatalf("retry after clean failure: %v", err)
	}
}

func TestCleanupFailureIsLoudAndRecoveredOnReopen(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "cleanup-fails"
	location, _ := locate(key)
	original := objects.ops.unlinkat
	objects.ops.unlinkat = func(dirFD int, name string, flags int) error {
		if name == location.staging {
			return syscall.EIO
		}
		return original(dirFD, name, flags)
	}
	if _, err := objects.Put(t.Context(), key, []byte("durably published")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want EIO", err)
	}
	status, err := objects.Status(t.Context())
	if !errors.Is(err, syscall.EIO) || status.Failure == "" || status.RecoveryRecords != 1 {
		t.Fatalf("Status after cleanup failure is %+v, %v", status, err)
	}
	if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Get after degraded cleanup returned %v, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get(t.Context(), key)
	if err != nil || string(got) != "durably published" {
		t.Fatalf("Get after recovery returned %q, %v", got, err)
	}
	if _, err := os.Lstat(stagingPath(t, root, key)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("reopen left staging state: %v", err)
	}
}

func TestFailedStagingDirectorySyncRetainsRecoveryRecord(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "cleanup-sync-fails"
	location, _ := locate(key)
	shardFD, _, err := objects.openShard(location, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(shardFD); err != nil {
		t.Fatal(err)
	}
	original := objects.ops.fsync
	shardSyncs := 0
	objects.ops.fsync = func(fd int) error {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return err
		}
		if st.Mode&unix.S_IFMT == unix.S_IFDIR && fd != objects.rootFD && fd != objects.objectsFD {
			shardSyncs++
			if shardSyncs >= 3 {
				return syscall.EIO
			}
		}
		return original(fd)
	}
	if _, err := objects.Put(t.Context(), key, []byte("published")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want EIO", err)
	}
	marker, _ := markerName(key, false)
	if _, err := os.Stat(filepath.Join(root, objectsDirectory, marker)); err != nil {
		t.Fatalf("recovery record was not retained: %v", err)
	}
	status, err := objects.Status(t.Context())
	if !errors.Is(err, syscall.EIO) || status.RecoveryRecords != 1 {
		t.Fatalf("Status is %+v, %v", status, err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Get(t.Context(), key)
	if err != nil || string(got) != "published" {
		t.Fatalf("Get after recovery returned %q, %v", got, err)
	}
}

func TestRecoveryRecordWithMissingShardIsCorruptionAndIsRetained(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleting=%t", deleting), func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			path := writeRecoveryMarkerForTest(t, root, objects.ID(), "missing-shard", deleting)
			if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOENT) {
				t.Fatalf("Open returned %v, want only EIO", err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("recovery record was removed: %v", err)
			}
		})
	}
}

func TestPublicationSyncFailurePoisonsTheOpenStoreAndReopensTruthfully(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "publication-sync"
	location, _ := locate(key)
	// Leave this shard present so the injected failure cannot be consumed by shard creation.
	if _, err := objects.Put(t.Context(), "seed", []byte("seed")); err != nil {
		t.Fatal(err)
	}
	seedLocation, _ := locate("seed")
	if seedLocation.first != location.first {
		for candidate := 0; ; candidate++ {
			seed := fmt.Sprintf("same-shard-%d", candidate)
			seedLocation, _ = locate(seed)
			if seedLocation.first == location.first {
				if _, err := objects.Put(t.Context(), seed, []byte("seed")); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
	}
	original := objects.ops.fsync
	failed := false
	shardSyncs := 0
	objects.ops.fsync = func(fd int) error {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil {
			return err
		}
		if st.Mode&unix.S_IFMT == unix.S_IFDIR && fd != objects.rootFD && fd != objects.objectsFD {
			shardSyncs++
			if !failed && shardSyncs == 2 {
				failed = true
				return syscall.EIO
			}
		}
		return original(fd)
	}
	if _, err := objects.Put(t.Context(), key, []byte("whole object")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want EIO", err)
	}
	if !failed {
		t.Fatal("fault seam did not reach the shard fsync")
	}
	if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.EIO) {
		t.Fatalf("degraded store answered Get with %v, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Get(t.Context(), key)
	if err != nil || string(got) != "whole object" {
		t.Fatalf("reopened Get returned %q, %v", got, err)
	}
}

func TestAdmissionIsContextAwareAndStatusReportsLiveUse(t *testing.T) {
	options := Options{
		MaxObjectBytes:        1024,
		MaxInFlightOperations: 1,
		MaxInFlightBytes:      fixedEnvelopeBytes + MaxKeyBytes + 1024,
		MaxRecoveryEntries:    1,
	}
	objects := openForTest(t, options)
	key := "blocked"
	location, _ := locate(key)
	started := make(chan struct{})
	release := make(chan struct{})
	original := objects.ops.linkat
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if new == location.final {
			close(started)
			<-release
		}
		return original(oldFD, old, newFD, new, flags)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := objects.Put(context.Background(), key, bytes.Repeat([]byte{'x'}, 900))
		putDone <- err
	}()
	<-started
	status, err := objects.Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.InFlightOperations != 1 || status.InFlightBytes != int64(fixedEnvelopeBytes+len(key)+900) {
		t.Errorf("Status is %+v, want one admitted Put", status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := objects.Put(ctx, "also-blocked", bytes.Repeat([]byte{'y'}, 900)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put waiting for bytes returned %v, want EIO", err)
	}
	close(release)
	if err := <-putDone; err != nil {
		t.Fatalf("blocked Put: %v", err)
	}
}

func TestCloseWaitsForAdmittedOperationsAndRejectsNewOnes(t *testing.T) {
	objects := openForTest(t, Options{})
	key := "held-open"
	location, _ := locate(key)
	started := make(chan struct{})
	release := make(chan struct{})
	original := objects.ops.linkat
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if new == location.final {
			close(started)
			<-release
		}
		return original(oldFD, old, newFD, new, flags)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := objects.Put(context.Background(), key, []byte("bytes"))
		putDone <- err
	}()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- objects.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the operation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := objects.Get(t.Context(), "new"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("operation admitted during Close returned %v, want EIO", err)
	}
	close(release)
	if err := <-putDone; err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStatusUsesDedicatedControlAdmissionAndCloseDrainsIt(t *testing.T) {
	options := Options{
		MaxObjectBytes:        1024,
		MaxInFlightOperations: 1,
		MaxInFlightBytes:      fixedEnvelopeBytes + MaxKeyBytes + 1024,
		MaxRecoveryEntries:    1,
	}
	objects := openForTest(t, options)
	key := "saturate-data-plane"
	location, _ := locate(key)
	putStarted := make(chan struct{})
	putRelease := make(chan struct{})
	originalLink := objects.ops.linkat
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if new == location.final {
			close(putStarted)
			<-putRelease
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := objects.Put(context.Background(), key, []byte("bytes"))
		putDone <- err
	}()
	<-putStarted

	statusStarted := make(chan struct{})
	statusRelease := make(chan struct{})
	originalStatfs := objects.ops.fstatfs
	var statusOnce sync.Once
	objects.ops.fstatfs = func(fd int, st *unix.Statfs_t) error {
		statusOnce.Do(func() { close(statusStarted) })
		<-statusRelease
		return originalStatfs(fd, st)
	}
	type statusResult struct {
		status Status
		err    error
	}
	statusDone := make(chan statusResult, 1)
	go func() {
		status, err := objects.Status(context.Background())
		statusDone <- statusResult{status: status, err: err}
	}()
	<-statusStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- objects.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while data and control operations were admitted: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	waiting, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := objects.Status(waiting); !errors.Is(err, syscall.EIO) {
		t.Fatalf("concurrent Status returned %v, want context-derived EIO", err)
	}

	close(putRelease)
	if err := <-putDone; err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the control operation drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(statusRelease)
	result := <-statusDone
	if result.err != nil {
		t.Fatalf("Status: %v", result.err)
	}
	if result.status.InFlightOperations != 1 || result.status.InFlightBytes != int64(fixedEnvelopeBytes+len(key)+len("bytes")) {
		t.Fatalf("saturated Status is %+v", result.status)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMaintenanceReserveIsReportedAndEnforced(t *testing.T) {
	objects := openForTest(t, Options{MaintenanceReserveBytes: int64(^uint64(0) >> 1)})
	available, err := objects.Available(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if available != 0 {
		t.Fatalf("Available is %d, want zero", available)
	}
	if _, err := objects.Put(t.Context(), "cannot-fit", nil); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Put returned %v, want ENOSPC", err)
	}
}

func TestInsufficientInodesReportZeroAndRefusePublication(t *testing.T) {
	objects := openForTest(t, Options{})
	original := objects.ops.fstatfs
	objects.ops.fstatfs = func(fd int, st *unix.Statfs_t) error {
		if err := original(fd, st); err != nil {
			return err
		}
		st.Files = 100
		st.Ffree = 1
		return nil
	}
	available, err := objects.Available(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if available != 0 {
		t.Fatalf("Available is %d with one free inode, want zero", available)
	}
	key := "inode-exhaustion"
	if _, err := objects.Put(t.Context(), key, []byte("bytes")); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Put returned %v, want ENOSPC", err)
	}
	marker, _ := markerName(key, false)
	if _, err := os.Stat(filepath.Join(objects.rootPath, objectsDirectory, marker)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused Put created recovery state: %v", err)
	}
	if _, err := os.Stat(stagingPath(t, objects.rootPath, key)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused Put created staging state: %v", err)
	}
}

func TestLimitsRejectImpossibleConfigurationAndOversizedInput(t *testing.T) {
	root := privateRoot(t)
	invalid := Options{MaxInFlightOperations: 2, MaxRecoveryEntries: 1}
	if err := invalid.Validate(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Validate returned %v, want EINVAL", err)
	}
	if err := (Options{}).Validate(); err != nil {
		t.Fatalf("zero Options Validate: %v", err)
	}
	if _, err := Open(t.Context(), root, invalid); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Open returned %v, want EINVAL", err)
	}
	objects := openForTest(t, Options{MaxObjectBytes: 4, MaxInFlightBytes: fixedEnvelopeBytes + MaxKeyBytes + 4})
	if _, err := objects.Put(t.Context(), "large", []byte("12345")); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("oversized Put returned %v, want EFBIG", err)
	}
}

func TestOpenHonorsContextAndProbesPublicationCapability(t *testing.T) {
	root := privateRoot(t)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Open(cancelled, root, Options{}); !errors.Is(err, syscall.EINTR) {
		t.Fatalf("cancelled Open returned %v, want EINTR", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cancelled Open created %v", entries)
	}

	ops := systemFileOperations
	ops.linkat = func(int, string, int, string, int) error { return syscall.EPERM }
	if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("Open without hard-link publication returned %v, want EOPNOTSUPP", err)
	}
	if _, err := os.Lstat(filepath.Join(root, manifestName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed capability probe published FORMAT: %v", err)
	}

	root = privateRoot(t)
	ops = systemFileOperations
	ops.linkat = func(int, string, int, string, int) error { return syscall.ENOSPC }
	if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("Open with a full publication filesystem returned %v, want only ENOSPC", err)
	}
	root = privateRoot(t)
	ops = systemFileOperations
	ops.statx = func(int, string, int, int, *unix.Statx_t) error { return syscall.EIO }
	if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("Open after statx I/O failure returned %v, want only EIO", err)
	}
}

func TestPublicationProbeVerifiesInodeIdentityAndDestinationPreservation(t *testing.T) {
	t.Run("copy instead of hard link", func(t *testing.T) {
		root := privateRoot(t)
		ops := systemFileOperations
		original := ops.linkat
		ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
			if new == probeTargetName {
				fd, err := unix.Openat(newFD, new,
					unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
				if err != nil {
					return err
				}
				return unix.Close(fd)
			}
			return original(oldFD, old, newFD, new, flags)
		}
		if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("Open returned %v, want EOPNOTSUPP", err)
		}
	})

	t.Run("occupied destination replaced before EEXIST", func(t *testing.T) {
		root := privateRoot(t)
		ops := systemFileOperations
		original := ops.linkat
		ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
			if new == probeOccupiedName {
				if err := unix.Unlinkat(newFD, new, 0); err != nil {
					return err
				}
				fd, err := unix.Openat(newFD, new,
					unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, privateFileMode)
				if err != nil {
					return err
				}
				if err := unix.Close(fd); err != nil {
					return err
				}
				return syscall.EEXIST
			}
			return original(oldFD, old, newFD, new, flags)
		}
		if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("Open returned %v, want EOPNOTSUPP", err)
		}
	})
}

func TestVerifyRootPathDetectsReplacement(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.VerifyRootPath(); err != nil {
		t.Fatalf("VerifyRootPath before replacement: %v", err)
	}
	moved := root + ".locked"
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, privateDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := objects.VerifyRootPath(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("VerifyRootPath returned %v, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, root); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyRootPathDetectsMountReplacement(t *testing.T) {
	objects := openForTest(t, Options{})
	original := objects.ops.statx
	objects.ops.statx = func(fd int, path string, flags int, mask int, st *unix.Statx_t) error {
		if err := original(fd, path, flags, mask, st); err != nil {
			return err
		}
		if fd == unix.AT_FDCWD {
			st.Mnt_id++
		}
		return nil
	}
	if err := objects.VerifyRootPath(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("VerifyRootPath returned %v, want EIO", err)
	}
}

func TestOpenRejectsKnownRemoteFilesystemsBeforeMutation(t *testing.T) {
	remoteTypes := []int64{
		unix.NFS_SUPER_MAGIC,
		unix.CIFS_SUPER_MAGIC,
		unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC,
		unix.AFS_FS_MAGIC,
	}
	for _, filesystemType := range remoteTypes {
		t.Run(fmt.Sprintf("%#x", filesystemType), func(t *testing.T) {
			root := privateRoot(t)
			ops := systemFileOperations
			ops.fstatfs = func(_ int, st *unix.Statfs_t) error {
				st.Type = filesystemType
				return nil
			}
			if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("Open returned %v, want EOPNOTSUPP", err)
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("remote-filesystem rejection created %v", entries)
			}
		})
	}

	root := privateRoot(t)
	ops := systemFileOperations
	originalFstatfs := ops.fstatfs
	ops.fstatfs = func(fd int, st *unix.Statfs_t) error {
		if err := originalFstatfs(fd, st); err != nil {
			return err
		}
		st.Type = 0x12345678
		return nil
	}
	objects, err := open(t.Context(), root, Options{}, ops)
	if err != nil {
		t.Fatalf("Open on an unknown probe-capable type: %v", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsAnObjectsDirectoryOnAnotherMount(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	ops := operationsWithMismatchedDeviceAt(t, filepath.Join(root, objectsDirectory))
	if _, err := open(t.Context(), root, Options{}, ops); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	for _, name := range []string{probeSourceName, probeTargetName} {
		if _, err := os.Stat(filepath.Join(root, objectsDirectory, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("identity rejection left publication probe %q: %v", name, err)
		}
	}
}

func TestPutRejectsAReplacedShardBeforeObjectMutation(t *testing.T) {
	for _, mismatch := range []string{"device", "filesystem type", "mount"} {
		t.Run(mismatch, func(t *testing.T) {
			objects := openForTest(t, Options{})
			key := "submount-replacement-" + mismatch
			location, _ := locate(key)
			shardFD, _, err := objects.openShard(location, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Close(shardFD); err != nil {
				t.Fatal(err)
			}
			switch mismatch {
			case "device":
				original := objects.ops.fstat
				objects.ops.fstat = func(fd int, st *unix.Stat_t) error {
					if err := original(fd, st); err != nil {
						return err
					}
					st.Dev++
					return nil
				}
			case "filesystem type":
				original := objects.ops.fstatfs
				objects.ops.fstatfs = func(fd int, st *unix.Statfs_t) error {
					if err := original(fd, st); err != nil {
						return err
					}
					st.Type ^= 1
					return nil
				}
			case "mount":
				original := objects.ops.statx
				objects.ops.statx = func(fd int, path string, flags int, mask int, st *unix.Statx_t) error {
					if err := original(fd, path, flags, mask, st); err != nil {
						return err
					}
					st.Mnt_id++
					return nil
				}
			}
			if _, err := objects.Put(t.Context(), key, []byte("must not be staged")); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Put returned %v, want EIO", err)
			}
			if _, err := os.Stat(stagingPath(t, objects.rootPath, key)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("identity rejection created staging state: %v", err)
			}
			marker, _ := markerName(key, false)
			if _, err := os.Stat(filepath.Join(objects.rootPath, objectsDirectory, marker)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("identity rejection created recovery state: %v", err)
			}
		})
	}
}

func TestGetRejectsAnObjectFileMountedFromElsewhere(t *testing.T) {
	objects := openForTest(t, Options{})
	key := "mounted-object"
	if _, err := objects.Put(t.Context(), key, []byte("bytes")); err != nil {
		t.Fatal(err)
	}
	original := objects.ops.statx
	calls := 0
	objects.ops.statx = func(fd int, path string, flags int, mask int, st *unix.Statx_t) error {
		if err := original(fd, path, flags, mask, st); err != nil {
			return err
		}
		calls++
		if calls == 2 {
			st.Mnt_id++
		}
		return nil
	}
	if _, err := objects.Get(t.Context(), key); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Get returned %v, want EIO", err)
	}
}

func operationsWithMismatchedDeviceAt(t *testing.T, path string) fileOperations {
	t.Helper()
	var target unix.Stat_t
	if err := unix.Stat(path, &target); err != nil {
		t.Fatal(err)
	}
	ops := systemFileOperations
	original := ops.fstat
	ops.fstat = func(fd int, st *unix.Stat_t) error {
		if err := original(fd, st); err != nil {
			return err
		}
		if st.Dev == target.Dev && st.Ino == target.Ino {
			st.Dev++
		}
		return nil
	}
	return ops
}

func TestOpenRejectsAControlFileFromAnotherFilesystem(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	format := filepath.Join(root, manifestName)
	if _, err := open(t.Context(), root, Options{}, operationsWithMismatchedDeviceAt(t, format)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
}

func TestCompositeIntentIsNotCreatedBeforePartialRootValidation(t *testing.T) {
	for _, entry := range []string{ownerLockName, manifestStageName, objectsDirectory, probeSourceName} {
		t.Run(entry, func(t *testing.T) {
			root := privateRoot(t)
			target := filepath.Join(root, entry)
			switch entry {
			case objectsDirectory:
				if err := os.Mkdir(target, privateDirectoryMode); err != nil {
					t.Fatal(err)
				}
			case probeSourceName:
				objectsPath := filepath.Join(root, objectsDirectory)
				if err := os.Mkdir(objectsPath, privateDirectoryMode); err != nil {
					t.Fatal(err)
				}
				target = filepath.Join(objectsPath, probeSourceName)
				if err := os.WriteFile(target, nil, privateFileMode); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.WriteFile(target, nil, privateFileMode); err != nil {
					t.Fatal(err)
				}
			}
			options := Options{CompositeInitialization: true}
			if _, err := open(t.Context(), root, options, operationsWithMismatchedDeviceAt(t, target)); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
			for _, unexpected := range []string{InitializationMarkerName, manifestName} {
				if _, err := os.Stat(filepath.Join(root, unexpected)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid root gained %s: %v", unexpected, err)
				}
			}
		})
	}
}

func TestRecoveryRejectsRecordsAndStagingFromAnotherFilesystem(t *testing.T) {
	for _, targetName := range []string{"recovery record", "staging object"} {
		t.Run(targetName, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			key := "mounted-recovery-" + targetName
			location, _ := locate(key)
			shardFD, _, err := objects.openShard(location, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Close(shardFD); err != nil {
				t.Fatal(err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			markerPath := writeRecoveryMarkerForTest(t, root, objects.ID(), key, false)
			stagePath := stagingPath(t, root, key)
			if err := os.WriteFile(stagePath, []byte("partial"), privateFileMode); err != nil {
				t.Fatal(err)
			}
			target := markerPath
			if targetName == "staging object" {
				target = stagePath
			}
			if _, err := open(t.Context(), root, Options{}, operationsWithMismatchedDeviceAt(t, target)); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
			for _, path := range []string{markerPath, stagePath} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("identity failure removed %s: %v", path, err)
				}
			}
		})
	}
}

func twoStoreRoots(t *testing.T) (string, string) {
	t.Helper()
	parent := t.TempDir()
	one := filepath.Join(parent, "one")
	two := filepath.Join(parent, "two")
	for _, root := range []string{one, two} {
		if err := os.Mkdir(root, privateDirectoryMode); err != nil {
			t.Fatal(err)
		}
	}
	return one, two
}

func TestOpenRejectsSameMountForeignObjectsAndShardDirectories(t *testing.T) {
	t.Run("objects directory", func(t *testing.T) {
		oneRoot, twoRoot := twoStoreRoots(t)
		one, err := Open(t.Context(), oneRoot, Options{})
		if err != nil {
			t.Fatal(err)
		}
		two, err := Open(t.Context(), twoRoot, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := one.Close(); err != nil {
			t.Fatal(err)
		}
		if err := two.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(oneRoot, objectsDirectory), filepath.Join(oneRoot, "original-objects")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(twoRoot, objectsDirectory), filepath.Join(oneRoot, objectsDirectory)); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(t.Context(), oneRoot, Options{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open returned %v, want EIO", err)
		}
	})

	t.Run("shard directory", func(t *testing.T) {
		oneRoot, twoRoot := twoStoreRoots(t)
		one, err := Open(t.Context(), oneRoot, Options{})
		if err != nil {
			t.Fatal(err)
		}
		two, err := Open(t.Context(), twoRoot, Options{})
		if err != nil {
			t.Fatal(err)
		}
		key := "same-shard"
		location, _ := locate(key)
		for _, objects := range []*Objects{one, two} {
			fd, _, err := objects.openShard(location, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Close(fd); err != nil {
				t.Fatal(err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
		}
		oneShard := filepath.Join(oneRoot, objectsDirectory, location.first)
		if err := os.Rename(oneShard, oneShard+"-original"); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(twoRoot, objectsDirectory, location.first), oneShard); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(t.Context(), oneRoot, Options{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open returned %v, want EIO", err)
		}
	})
}

func TestForeignRecoveryRecordCannotDeleteAValidObject(t *testing.T) {
	victimRoot, foreignRoot := twoStoreRoots(t)
	victim, err := Open(t.Context(), victimRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := Open(t.Context(), foreignRoot, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "must-survive"
	if _, err := victim.Put(t.Context(), key, []byte("victim bytes")); err != nil {
		t.Fatal(err)
	}
	if err := victim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := foreign.Close(); err != nil {
		t.Fatal(err)
	}
	writeRecoveryMarkerForTest(t, victimRoot, foreign.ID(), key, true)
	if _, err := Open(t.Context(), victimRoot, Options{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	content, err := os.ReadFile(objectPath(t, victimRoot, key))
	if err != nil || len(content) == 0 {
		t.Fatalf("foreign recovery record removed victim object: %d bytes, %v", len(content), err)
	}
}

func TestDeleteRecoveryRejectsImpossibleStagingBeforeDeletingFinal(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "delete-with-stage"
	if _, err := objects.Put(t.Context(), key, []byte("survives")); err != nil {
		t.Fatal(err)
	}
	id := objects.ID()
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	writeRecoveryMarkerForTest(t, root, id, key, true)
	if err := os.WriteFile(stagingPath(t, root, key), []byte("impossible"), privateFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	if _, err := os.Stat(objectPath(t, root, key)); err != nil {
		t.Fatalf("recovery deleted final before rejecting staging: %v", err)
	}
}

func TestOpenRejectsImpossibleManifestAndProbeResidues(t *testing.T) {
	t.Run("manifest staging inode", func(t *testing.T) {
		root := privateRoot(t)
		objects, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := objects.Close(); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(filepath.Join(root, manifestName))
		if err != nil {
			t.Fatal(err)
		}
		stage := filepath.Join(root, manifestStageName)
		if err := os.WriteFile(stage, content, privateFileMode); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open returned %v, want EIO", err)
		}
		if _, err := os.Stat(stage); err != nil {
			t.Fatalf("invalid manifest residue was removed: %v", err)
		}
	})

	t.Run("publication probe pair", func(t *testing.T) {
		root := privateRoot(t)
		objects, err := Open(t.Context(), root, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := objects.Close(); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{probeSourceName, probeTargetName} {
			if err := os.WriteFile(filepath.Join(root, objectsDirectory, name), []byte(name), privateFileMode); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("Open returned %v, want EIO", err)
		}
		for _, name := range []string{probeSourceName, probeTargetName} {
			if _, err := os.Stat(filepath.Join(root, objectsDirectory, name)); err != nil {
				t.Fatalf("invalid probe residue %q was removed: %v", name, err)
			}
		}
	})
}

func TestPutAndDeleteRefuseACorruptExistingObject(t *testing.T) {
	for _, operation := range []string{"put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			objects := openForTest(t, Options{})
			key := "corrupt-existing"
			if _, err := objects.Put(t.Context(), key, []byte("bytes")); err != nil {
				t.Fatal(err)
			}
			flipByte(0)(t, objectPath(t, objects.rootPath, key))
			var err error
			if operation == "put" {
				_, err = objects.Put(t.Context(), key, []byte("replacement"))
			} else {
				err = objects.Delete(t.Context(), key)
			}
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.ENOENT) {
				t.Fatalf("%s returned %v, want only EIO", operation, err)
			}
		})
	}
}

func TestPutAndDeleteClassifyAnUntrustedFinalAsIOFailure(t *testing.T) {
	for _, operation := range []string{"put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			objects := openForTest(t, Options{})
			key := "symlink-final"
			if _, err := objects.Put(t.Context(), key, []byte("bytes")); err != nil {
				t.Fatal(err)
			}
			path := objectPath(t, objects.rootPath, key)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("elsewhere", path); err != nil {
				t.Fatal(err)
			}
			var err error
			if operation == "put" {
				_, err = objects.Put(t.Context(), key, []byte("replacement"))
			} else {
				err = objects.Delete(t.Context(), key)
			}
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ELOOP) {
				t.Fatalf("%s returned %v, want only EIO", operation, err)
			}
		})
	}
}
