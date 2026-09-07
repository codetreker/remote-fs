package localdir

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/locking"
	"golang.org/x/sys/unix"
)

func nativeStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func nativeFile(t *testing.T, s *Storage, path string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(s.root, path), []byte(path), 0o600); err != nil {
		t.Fatal(err)
	}
}

func discoverNative(t *testing.T, s *Storage, path string, adopt bool) locking.BackendKey {
	t.Helper()
	var key locking.BackendKey
	if err := s.Discover(t.Context(), path, func(candidate locking.BackendKey) (bool, error) {
		key = candidate
		return adopt, nil
	}); err != nil {
		t.Fatal(err)
	}
	if key == "" {
		t.Fatal("empty native identity")
	}
	return key
}

func nativeFD(t *testing.T, s *Storage, path string) int {
	t.Helper()
	fd, err := s.openPath(path, unix.O_PATH|unix.O_NOFOLLOW)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Close(fd); err != nil {
			t.Error(err)
		}
	})
	return fd
}

func TestNativeDiscoveryAdoptionCapacityAndForget(t *testing.T) {
	s := nativeStorage(t)
	s.runtime.limits.MaxPinnedTargets = 1
	nativeFile(t, s, "first")
	nativeFile(t, s, "second")
	first := discoverNative(t, s, "first", true)
	pin := s.runtime.pins[first].fd
	if got := discoverNative(t, s, "first", false); got != first {
		t.Fatalf("repeated discovery changed identity: %q != %q", got, first)
	}
	called := false
	if err := s.Discover(t.Context(), "second", func(locking.BackendKey) (bool, error) {
		called = true
		return true, nil
	}); locking.CodeOf(err) != locking.Capacity || called {
		t.Fatalf("full pin table admitted discovery: %v, callback=%v", err, called)
	}
	if err := s.Forget(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(pin), unix.F_GETFD, 0); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("forgotten inode remains pinned: %v", err)
	}
	rejected := discoverNative(t, s, "second", false)
	if len(s.runtime.pins) != 0 || len(s.runtime.physical) != 0 {
		t.Fatal("unadopted candidate retained native state")
	}
	second := discoverNative(t, s, "second", true)
	if second == first || second == rejected {
		t.Fatal("native key was reused")
	}
	s.runtime.stop()
	if err := s.Forget(t.Context(), second); err != nil {
		t.Fatalf("Forget unavailable after admission stopped: %v", err)
	}
}

func TestNativeDiscoveryRejectsUnsupportedTargets(t *testing.T) {
	s := nativeStorage(t)
	nativeFile(t, s, "file")
	if err := os.Link(filepath.Join(s.root, "file"), filepath.Join(s.root, "hard")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("file", filepath.Join(s.root, "symbolic")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", "file", "hard", "symbolic", "missing", "file/child"} {
		t.Run(path, func(t *testing.T) {
			called := false
			err := s.Discover(t.Context(), path, func(locking.BackendKey) (bool, error) {
				called = true
				return true, nil
			})
			if locking.CodeOf(err) != locking.UnsupportedTarget || called {
				t.Fatalf("unsupported discovery: %v, callback=%v", err, called)
			}
		})
	}
}

func TestNativeReplacementTransfersIdentityAndDeleteRetiresIt(t *testing.T) {
	s := nativeStorage(t)
	nativeFile(t, s, "file")
	nativeFile(t, s, "staged")
	key := discoverNative(t, s, "file", true)
	oldFD, stagedFD := nativeFD(t, s, "file"), nativeFD(t, s, "staged")
	err := s.withPaths(t.Context(), []pathIntent{{path: "file", write: true}}, func() error {
		if err := os.Rename(filepath.Join(s.root, "staged"), filepath.Join(s.root, "file")); err != nil {
			return err
		}
		return s.replaceTarget(oldFD, stagedFD, "file")
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.target(stagedFD, "file"); err != nil || got != key {
		t.Fatalf("replacement did not preserve key: %q, %v", got, err)
	}
	if got, err := s.target(oldFD, "file"); err != nil || got != "" {
		t.Fatalf("old inode still bound: %q, %v", got, err)
	}
	flags, err := unix.FcntlInt(uintptr(s.runtime.pins[key].fd), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_PATH == 0 {
		t.Fatalf("replacement pin is not O_PATH: %x, %v", flags, err)
	}
	called := false
	if err := s.Guard(t.Context(), key, func() error { called = true; return nil }); err != nil || !called {
		t.Fatalf("replacement guard failed: %v, callback=%v", err, called)
	}
	err = s.withPaths(t.Context(), []pathIntent{{path: "file", write: true}}, func() error {
		if err := os.Remove(filepath.Join(s.root, "file")); err != nil {
			return err
		}
		retired, err := s.removeTarget(stagedFD)
		if err == nil && (len(retired) != 1 || retired[0] != key) {
			t.Fatalf("wrong retirement set: %v", retired)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Guard(t.Context(), key, func() error { t.Error("retired callback ran"); return nil }); locking.CodeOf(err) != locking.StaleResource {
		t.Fatalf("removed target remains live: %v", err)
	}
	nativeFile(t, s, "file")
	if recreated := discoverNative(t, s, "file", true); recreated == key {
		t.Fatal("delete/recreate rebound the retired resource")
	}
}

func TestNativeRenameMovesDescendantsAndRetiresDestination(t *testing.T) {
	s := nativeStorage(t)
	if err := os.MkdirAll(filepath.Join(s.root, "dir/deep"), 0o700); err != nil {
		t.Fatal(err)
	}
	nativeFile(t, s, "dir/file")
	nativeFile(t, s, "dir/deep/grandchild")
	nativeFile(t, s, "destination")
	file := discoverNative(t, s, "dir/file", true)
	grandchild := discoverNative(t, s, "dir/deep/grandchild", true)
	destination := discoverNative(t, s, "destination", true)
	directoryFD := nativeFD(t, s, "dir")
	if err := s.withPaths(t.Context(), []pathIntent{{path: "dir", write: true}, {path: "moved", write: true}}, func() error {
		if err := os.Rename(filepath.Join(s.root, "dir"), filepath.Join(s.root, "moved")); err != nil {
			return err
		}
		retired, err := s.renameTargets("dir", "moved", directoryFD, -1)
		if len(retired) != 0 {
			t.Errorf("directory move retired descendants: %v", retired)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []locking.BackendKey{file, grandchild} {
		if err := s.Guard(t.Context(), key, func() error { return nil }); err != nil {
			t.Fatalf("moved descendant lost identity: %v", err)
		}
	}
	sourceFD, targetFD := nativeFD(t, s, "moved/file"), nativeFD(t, s, "destination")
	if err := s.withPaths(t.Context(), []pathIntent{{path: "moved/file", write: true}, {path: "destination", write: true}}, func() error {
		if err := os.Rename(filepath.Join(s.root, "moved/file"), filepath.Join(s.root, "destination")); err != nil {
			return err
		}
		retired, err := s.renameTargets("moved/file", "destination", sourceFD, targetFD)
		if len(retired) != 1 || retired[0] != destination {
			t.Errorf("wrong replaced identity retired: %v", retired)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got := discoverNative(t, s, "destination", false); got != file {
		t.Fatalf("rename failed to preserve source: %q != %q", got, file)
	}
	if err := s.Guard(t.Context(), destination, func() error { t.Error("replaced callback ran"); return nil }); locking.CodeOf(err) != locking.StaleResource {
		t.Fatalf("replaced destination remains live: %v", err)
	}
}

func TestNativeGuardDetectsExternalIdentityAndLinkChanges(t *testing.T) {
	for _, mutation := range []string{"remove", "replace", "hardlink"} {
		t.Run(mutation, func(t *testing.T) {
			s := nativeStorage(t)
			nativeFile(t, s, "file")
			key := discoverNative(t, s, "file", true)
			var err error
			switch mutation {
			case "remove":
				err = os.Remove(filepath.Join(s.root, "file"))
			case "replace":
				nativeFile(t, s, "replacement")
				err = os.Rename(filepath.Join(s.root, "replacement"), filepath.Join(s.root, "file"))
			case "hardlink":
				err = os.Link(filepath.Join(s.root, "file"), filepath.Join(s.root, "hardlink"))
			}
			if err != nil {
				t.Fatal(err)
			}
			err = s.Guard(t.Context(), key, func() error { t.Error("invalid target callback ran"); return nil })
			if locking.CodeOf(err) != locking.StaleResource {
				t.Fatalf("changed native target accepted: %v", err)
			}
		})
	}
}

func TestNativeRenameBoundsEveryRetainedDescendantPath(t *testing.T) {
	s := nativeStorage(t)
	if err := os.Mkdir(filepath.Join(s.root, "dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	nativeFile(t, s, "dir/file")
	key := discoverNative(t, s, "dir/file", true)
	s.runtime.limits.MaxPathBytes = len("dir/file")
	if err := s.validateRenameTargets("dir", "long"); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("rename allowed unbounded retained path: %v", err)
	}
	if err := s.validateRenameTargets("dir", "new"); err != nil {
		t.Fatalf("rename refused bounded descendant: %v", err)
	}
	if s.runtime.pins[key].path != "dir/file" {
		t.Fatal("rename validation changed the resource binding")
	}
}

func TestNativeErrorsPreserveCallbacksAndDescriptorFailures(t *testing.T) {
	s := nativeStorage(t)
	nativeFile(t, s, "file")
	cause := errors.New("native callback failure")
	if err := s.Discover(t.Context(), "file", func(locking.BackendKey) (bool, error) { return false, cause }); !errors.Is(err, cause) {
		t.Fatalf("discovery discarded callback failure: %v", err)
	}
	if len(s.runtime.pins) != 0 {
		t.Fatal("failed adoption retained a pin")
	}
	key := discoverNative(t, s, "file", true)
	if err := s.Guard(t.Context(), key, func() error { return cause }); !errors.Is(err, cause) {
		t.Fatalf("guard discarded callback failure: %v", err)
	}
	invalidFD := int(^uint32(0) >> 1)
	if _, err := s.target(invalidFD, "file"); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("target discarded fstat failure: %v", err)
	}
	if _, err := s.removeTarget(invalidFD); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("remove discarded fstat failure: %v", err)
	}
	if err := s.replaceTarget(invalidFD, invalidFD, "file"); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("replacement discarded fstat failure: %v", err)
	}
	if _, err := s.renameTargets("file", "next", invalidFD, -1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("rename discarded fstat failure: %v", err)
	}
	if key, err := s.target(-1, "missing"); key != "" || err != nil {
		t.Fatalf("missing target has an identity: %q, %v", key, err)
	}
	if err := s.replaceTarget(-1, invalidFD, "missing"); err != nil {
		t.Fatalf("new file attempted identity transfer: %v", err)
	}
}

func TestNativeForgetWaitsForExistingDiscoveryCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := nativeStorage(t)
		nativeFile(t, s, "file")
		key := discoverNative(t, s, "file", true)
		entered := make(chan locking.BackendKey, 1)
		resume := make(chan struct{})
		discovery := make(chan error, 1)
		go func() {
			discovery <- s.Discover(t.Context(), "file", func(candidate locking.BackendKey) (bool, error) {
				entered <- candidate
				<-resume
				return false, nil
			})
		}()
		if got := <-entered; got != key {
			t.Fatalf("existing discovery changed key: %q", got)
		}
		forgotten := make(chan error, 1)
		go func() { forgotten <- s.Forget(t.Context(), key) }()
		synctest.Wait()
		select {
		case err := <-forgotten:
			t.Fatalf("Forget crossed the discovery callback: %v", err)
		default:
		}
		s.runtime.pinsMu.Lock()
		present := s.runtime.pins[key] != nil
		s.runtime.pinsMu.Unlock()
		if !present {
			t.Fatal("discovery callback lost its adopted pin")
		}
		close(resume)
		if err := <-discovery; err != nil {
			t.Fatal(err)
		}
		if err := <-forgotten; err != nil {
			t.Fatal(err)
		}
	})
}

func TestNativeForgetRetriesThePathAfterAncestorRename(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := nativeStorage(t)
		if err := os.Mkdir(filepath.Join(s.root, "dir"), 0o700); err != nil {
			t.Fatal(err)
		}
		nativeFile(t, s, "dir/file")
		key := discoverNative(t, s, "dir/file", true)
		directoryFD := nativeFD(t, s, "dir")
		releaseOld := claimPaths(t, s.runtime, pathIntent{path: "dir", write: true})
		releaseNew := claimPaths(t, s.runtime, pathIntent{path: "moved", write: true})
		forgotten := make(chan error, 1)
		go func() { forgotten <- s.Forget(t.Context(), key) }()
		synctest.Wait()
		if err := os.Rename(filepath.Join(s.root, "dir"), filepath.Join(s.root, "moved")); err != nil {
			t.Fatal(err)
		}
		if _, err := s.renameTargets("dir", "moved", directoryFD, -1); err != nil {
			t.Fatal(err)
		}
		releaseOld()
		synctest.Wait()
		select {
		case err := <-forgotten:
			t.Fatalf("Forget bypassed the renamed path's claim: %v", err)
		default:
		}
		s.runtime.gateMu.Lock()
		waitsForNew := len(s.runtime.pending) == 1 && s.runtime.pending[0].paths["moved/file"]
		s.runtime.gateMu.Unlock()
		if !waitsForNew {
			t.Fatal("Forget did not revalidate its path after rename")
		}
		releaseNew()
		if err := <-forgotten; err != nil {
			t.Fatal(err)
		}
	})
}

func TestNativeForgetCancellationKeepsPinForStoppedCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := nativeStorage(t)
		nativeFile(t, s, "file")
		key := discoverNative(t, s, "file", true)
		release := claimPaths(t, s.runtime, pathIntent{path: "file", write: true})
		ctx, cancel := context.WithCancel(t.Context())
		forgotten := make(chan error, 1)
		go func() { forgotten <- s.Forget(ctx, key) }()
		synctest.Wait()
		cancel()
		if err := <-forgotten; !errors.Is(err, context.Canceled) {
			t.Fatalf("Forget discarded wait cancellation: %v", err)
		}
		s.runtime.pinsMu.Lock()
		present := s.runtime.pins[key] != nil
		s.runtime.pinsMu.Unlock()
		if !present {
			t.Fatal("cancelled Forget detached the pin")
		}
		release()
		s.runtime.stop()
		if err := s.Forget(t.Context(), key); err != nil {
			t.Fatalf("stopped namespace refused pin cleanup: %v", err)
		}
	})
}
