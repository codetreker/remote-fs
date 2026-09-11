package localdisk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func shardTestKeys(t *testing.T) (string, string, string) {
	t.Helper()
	firstByShard := make(map[byte]string)
	for index := 0; ; index++ {
		key := fmt.Sprintf("shard-identity-key-%d", index)
		location, err := locate(key)
		if err != nil {
			t.Fatal(err)
		}
		if first, ok := firstByShard[location.shard]; ok {
			for shard, different := range firstByShard {
				if shard != location.shard {
					return first, key, different
				}
			}
		}
		firstByShard[location.shard] = key
	}
}

func TestConcurrentShardInitializationSerializesOnlyOneShard(t *testing.T) {
	objects := openForTest(t, Options{})
	first, second, different := shardTestKeys(t)
	firstLocation, _ := locate(first)
	secondLocation, _ := locate(second)
	differentLocation, _ := locate(different)
	if firstLocation.shard != secondLocation.shard || firstLocation.shard == differentLocation.shard {
		t.Fatal("test keys do not exercise one shared and one independent shard")
	}

	originalLink := objects.ops.linkat
	markerLinkEntered := make(chan struct{})
	releaseMarkerLink := make(chan struct{})
	var markerLinkReleased atomic.Bool
	release := func() {
		if markerLinkReleased.CompareAndSwap(false, true) {
			close(releaseMarkerLink)
		}
	}
	defer release()
	var blockedFirstMarker atomic.Bool
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if old == storeIdentityStageName && new == storeIdentityName && blockedFirstMarker.CompareAndSwap(false, true) {
			close(markerLinkEntered)
			<-releaseMarkerLink
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}

	put := func(key string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := objects.Put(t.Context(), key, []byte(key))
			done <- err
		}()
		return done
	}
	firstDone := put(first)
	<-markerLinkEntered
	secondDone := put(second)
	differentDone := put(different)
	select {
	case err := <-differentDone:
		if err != nil {
			t.Fatalf("Put in an independent shard: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an independent shard waited behind shard initialization")
	}
	release()
	for key, done := range map[string]<-chan error{first: firstDone, second: secondDone} {
		if err := <-done; err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}
	for _, key := range []string{first, second, different} {
		got, err := objects.Get(t.Context(), key)
		if err != nil || string(got) != key {
			t.Fatalf("Get %q returned %q, %v", key, got, err)
		}
	}
}

func TestShardInitializationWaitIsContextAware(t *testing.T) {
	locks := newShardLocker()
	unlock, err := locks.acquire(t.Context(), 42)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := locks.acquire(ctx, 42)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting acquisition returned %v, want context cancellation", err)
	}
}

func createShardIdentityStageForTest(t *testing.T, root string, id ID, key string) (string, string) {
	t.Helper()
	location, err := locate(key)
	if err != nil {
		t.Fatal(err)
	}
	shardPath := filepath.Join(root, objectsDirectory, location.first)
	if err := os.Mkdir(shardPath, privateDirectoryMode); err != nil {
		t.Fatal(err)
	}
	stagePath := filepath.Join(shardPath, storeIdentityStageName)
	encoded := encodeDirectoryMarker(id, directoryKindShard, location.shard)
	if err := os.WriteFile(stagePath, encoded[:], privateFileMode); err != nil {
		t.Fatal(err)
	}
	stage, err := os.Open(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	shard, err := os.Open(shardPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := shard.Sync(); err != nil {
		_ = shard.Close()
		t.Fatal(err)
	}
	if err := shard.Close(); err != nil {
		t.Fatal(err)
	}
	return stagePath, filepath.Join(shardPath, storeIdentityName)
}

func TestOpenRecoversShardIdentityPublicationResidue(t *testing.T) {
	for _, test := range []struct {
		name      string
		linkFinal bool
	}{
		{name: "stage only"},
		{name: "published inode retains stage", linkFinal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			id := objects.ID()
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			stagePath, finalPath := createShardIdentityStageForTest(t, root, id, test.name)
			if test.linkFinal {
				if err := os.Link(stagePath, finalPath); err != nil {
					t.Fatal(err)
				}
			}

			reopened, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovery left the staging name: %v", err)
			}
			got, err := os.ReadFile(finalPath)
			if err != nil {
				t.Fatal(err)
			}
			location, _ := locate(test.name)
			want := encodeDirectoryMarker(id, directoryKindShard, location.shard)
			if string(got) != string(want[:]) {
				t.Fatal("recovery changed the shard identity marker")
			}
		})
	}
}

func TestOpenPreservesImpossibleShardIdentityState(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	id := objects.ID()
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	stagePath, finalPath := createShardIdentityStageForTest(t, root, id, "different-inodes")
	encoded, err := os.ReadFile(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, encoded, privateFileMode); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	for _, path := range []string{stagePath, finalPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("identity validation removed evidence %q: %v", filepath.Base(path), err)
		}
	}
}

func TestOpenPreservesDamagedShardIdentityStage(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	id := objects.ID()
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}
	stagePath, _ := createShardIdentityStageForTest(t, root, id, "damaged-stage")
	file, err := os.OpenFile(stagePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var checksumByte [1]byte
	if _, err := file.ReadAt(checksumByte[:], 32); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	checksumByte[0] ^= 1
	if _, err := file.WriteAt(checksumByte[:], 32); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Open returned %v, want EIO", err)
	}
	if _, err := os.Stat(stagePath); err != nil {
		t.Fatalf("identity validation removed damaged evidence: %v", err)
	}
}

func TestOpenRejectsShardIdentityWithUncontrolledHardLinks(t *testing.T) {
	for _, state := range []string{"final only", "stage only", "final and stage"} {
		t.Run(state, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			id := objects.ID()
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			stagePath, finalPath := createShardIdentityStageForTest(t, root, id, "extra-link-"+state)
			extraPath := filepath.Join(filepath.Dir(stagePath), ".unexpected-identity-link")
			switch state {
			case "final only":
				if err := os.Link(stagePath, finalPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(stagePath); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(finalPath, extraPath); err != nil {
					t.Fatal(err)
				}
			case "stage only":
				if err := os.Link(stagePath, extraPath); err != nil {
					t.Fatal(err)
				}
			case "final and stage":
				if err := os.Link(stagePath, finalPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(stagePath, extraPath); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
			for _, path := range []string{finalPath, stagePath, extraPath} {
				if _, err := os.Lstat(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("inspect preserved evidence %q: %v", filepath.Base(path), err)
				}
			}
			if _, err := os.Stat(extraPath); err != nil {
				t.Fatalf("identity validation removed the uncontrolled link: %v", err)
			}
		})
	}
}

func TestOpenPreservesShardIdentityResidueWithUnexpectedEntry(t *testing.T) {
	for _, state := range []string{"stage only", "final and stage"} {
		t.Run(state, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			id := objects.ID()
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
			stagePath, finalPath := createShardIdentityStageForTest(t, root, id, "unexpected-entry-"+state)
			if state == "final and stage" {
				if err := os.Link(stagePath, finalPath); err != nil {
					t.Fatal(err)
				}
			}
			extraPath := filepath.Join(filepath.Dir(stagePath), "unexpected-object")
			if err := os.WriteFile(extraPath, []byte("evidence"), privateFileMode); err != nil {
				t.Fatal(err)
			}

			if _, err := Open(t.Context(), root, Options{}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Open returned %v, want EIO", err)
			}
			for _, path := range []string{stagePath, extraPath} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("identity validation removed %q: %v", filepath.Base(path), err)
				}
			}
			if state == "final and stage" {
				if _, err := os.Stat(finalPath); err != nil {
					t.Fatalf("identity validation removed final marker: %v", err)
				}
			}
		})
	}
}

func TestAmbiguousShardIdentityPublicationPoisonsUntilReopen(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "ambiguous-shard-marker"
	location, _ := locate(key)
	originalLink := objects.ops.linkat
	injected := false
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if !injected && old == storeIdentityStageName && new == storeIdentityName {
			injected = true
			if err := originalLink(oldFD, old, newFD, new, flags); err != nil {
				return err
			}
			return syscall.EIO
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}
	if _, err := objects.Put(t.Context(), key, []byte("not published")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want uncertain EIO", err)
	}
	if !injected {
		t.Fatal("publication fault was not injected")
	}
	stagePath := filepath.Join(root, objectsDirectory, location.first, storeIdentityStageName)
	finalPath := filepath.Join(root, objectsDirectory, location.first, storeIdentityName)
	var stage, final unix.Stat_t
	if err := unix.Stat(stagePath, &stage); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(finalPath, &final); err != nil {
		t.Fatal(err)
	}
	if stage.Dev != final.Dev || stage.Ino != final.Ino {
		t.Fatal("ambiguous publication did not preserve the linked inode as evidence")
	}
	if _, err := objects.Get(t.Context(), "unrelated-key"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("operation after ambiguous publication returned %v, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("Open after preserved publication: %v", err)
	}
	defer reopened.Close()
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open left the recovered staging name: %v", err)
	}
	if _, err := reopened.Put(t.Context(), key, []byte("published after recovery")); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
}

func TestRefusedShardIdentityPublicationCleansStageAndCanRetry(t *testing.T) {
	objects := openForTest(t, Options{})
	key := "refused-shard-marker"
	location, _ := locate(key)
	originalLink := objects.ops.linkat
	injected := false
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if !injected && old == storeIdentityStageName && new == storeIdentityName {
			injected = true
			return syscall.EIO
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}
	if _, err := objects.Put(t.Context(), key, []byte("not published")); !errors.Is(err, syscall.EIO) || errors.Is(err, errShardIdentityPublicationUncertain) {
		t.Fatalf("Put returned %v, want a known EIO publication failure", err)
	}
	if !injected {
		t.Fatal("publication fault was not injected")
	}
	shardPath := filepath.Join(objects.rootPath, objectsDirectory, location.first)
	for _, name := range []string{storeIdentityStageName, storeIdentityName} {
		if _, err := os.Lstat(filepath.Join(shardPath, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("known publication failure left %q: %v", name, err)
		}
	}
	objects.ops.linkat = originalLink
	if _, err := objects.Put(t.Context(), key, []byte("published on retry")); err != nil {
		t.Fatalf("Put after clean failure: %v", err)
	}
}

func TestShardIdentityCleanupFailurePoisonsUntilReopen(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "shard-marker-cleanup-failure"
	location, _ := locate(key)
	originalUnlink := objects.ops.unlinkat
	injected := false
	objects.ops.unlinkat = func(dirFD int, name string, flags int) error {
		if !injected && name == storeIdentityStageName {
			injected = true
			return syscall.EIO
		}
		return originalUnlink(dirFD, name, flags)
	}
	if _, err := objects.Put(t.Context(), key, []byte("not published")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want uncertain EIO", err)
	}
	if !injected {
		t.Fatal("cleanup fault was not injected")
	}
	shardPath := filepath.Join(root, objectsDirectory, location.first)
	stagePath := filepath.Join(shardPath, storeIdentityStageName)
	finalPath := filepath.Join(shardPath, storeIdentityName)
	var stage, final unix.Stat_t
	if err := unix.Stat(stagePath, &stage); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(finalPath, &final); err != nil {
		t.Fatal(err)
	}
	if stage.Dev != final.Dev || stage.Ino != final.Ino {
		t.Fatal("cleanup failure did not retain a recoverable linked inode")
	}
	if _, err := objects.Get(t.Context(), "unrelated-key"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("operation after cleanup failure returned %v, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("Open after cleanup failure: %v", err)
	}
	defer reopened.Close()
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open left the recovered staging name: %v", err)
	}
}

func TestShardIdentityPublicationSyncFailurePoisonsUntilReopen(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := "shard-marker-publication-sync-failure"
	location, _ := locate(key)
	originalLink := objects.ops.linkat
	originalSync := objects.ops.fsync
	markerLinked := false
	injected := false
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		err := originalLink(oldFD, old, newFD, new, flags)
		if err == nil && old == storeIdentityStageName && new == storeIdentityName {
			markerLinked = true
		}
		return err
	}
	objects.ops.fsync = func(fd int) error {
		if markerLinked && !injected {
			injected = true
			return syscall.EIO
		}
		return originalSync(fd)
	}
	if _, err := objects.Put(t.Context(), key, []byte("not published")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Put returned %v, want uncertain EIO", err)
	}
	if !injected {
		t.Fatal("publication sync fault was not injected")
	}
	shardPath := filepath.Join(root, objectsDirectory, location.first)
	stagePath := filepath.Join(shardPath, storeIdentityStageName)
	finalPath := filepath.Join(shardPath, storeIdentityName)
	var stage, final unix.Stat_t
	if err := unix.Stat(stagePath, &stage); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(finalPath, &final); err != nil {
		t.Fatal(err)
	}
	if stage.Dev != final.Dev || stage.Ino != final.Ino {
		t.Fatal("publication sync failure did not preserve the linked inode")
	}
	if _, err := objects.Get(t.Context(), "unrelated-key"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("operation after publication sync failure returned %v, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("Open after publication sync failure: %v", err)
	}
	defer reopened.Close()
	if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open left the recovered staging name: %v", err)
	}
}
