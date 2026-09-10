package localdisk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFactBearingResultsLinearizeAgainstGlobalPoison(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, *Objects, string, objectLocation)
		operate func(context.Context, *Objects, string) error
		fact    error
	}{
		{
			name: "missing object",
			prepare: func(t *testing.T, objects *Objects, _ string, location objectLocation) {
				fd, _, err := objects.openShard(t.Context(), location, true)
				if err != nil {
					t.Fatal(err)
				}
				if err := unix.Close(fd); err != nil {
					t.Fatal(err)
				}
			},
			operate: func(ctx context.Context, objects *Objects, key string) error {
				_, err := objects.Get(ctx, key)
				return err
			},
			fact: syscall.ENOENT,
		},
		{
			name: "existing object",
			prepare: func(t *testing.T, objects *Objects, key string, _ objectLocation) {
				if _, err := objects.Put(t.Context(), key, []byte("existing")); err != nil {
					t.Fatal(err)
				}
			},
			operate: func(ctx context.Context, objects *Objects, key string) error {
				_, err := objects.Put(ctx, key, []byte("replacement"))
				return err
			},
			fact: syscall.EEXIST,
		},
		{
			name: "bounded object",
			prepare: func(t *testing.T, objects *Objects, key string, _ objectLocation) {
				if _, err := objects.Put(t.Context(), key, []byte("larger than bound")); err != nil {
					t.Fatal(err)
				}
			},
			operate: func(ctx context.Context, objects *Objects, key string) error {
				_, err := objects.GetBounded(ctx, key, 1)
				return err
			},
			fact: syscall.EFBIG,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			target, _, poisonKey := shardTestKeys(t)
			targetLocation, _ := locate(target)
			poisonLocation, _ := locate(poisonKey)
			if targetLocation.shard == poisonLocation.shard {
				t.Fatal("test keys must use different shards")
			}
			test.prepare(t, objects, target, targetLocation)

			markerPath := filepath.Join(root, objectsDirectory, targetLocation.first, storeIdentityName)
			var targetMarker unix.Stat_t
			if err := unix.Stat(markerPath, &targetMarker); err != nil {
				t.Fatal(err)
			}
			originalFstat := objects.ops.fstat
			originalLink := objects.ops.linkat
			validationEntered := make(chan struct{})
			releaseValidation := make(chan struct{})
			var validationBlocked atomic.Bool
			var validationReleased atomic.Bool
			release := func() {
				if validationReleased.CompareAndSwap(false, true) {
					close(releaseValidation)
				}
			}
			defer release()
			objects.ops.fstat = func(fd int, st *unix.Stat_t) error {
				if err := originalFstat(fd, st); err != nil {
					return err
				}
				if st.Dev == targetMarker.Dev && st.Ino == targetMarker.Ino && validationBlocked.CompareAndSwap(false, true) {
					close(validationEntered)
					<-releaseValidation
				}
				return nil
			}
			var poisonInjected atomic.Bool
			objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
				if old == storeIdentityStageName && new == storeIdentityName && poisonInjected.CompareAndSwap(false, true) {
					if err := originalLink(oldFD, old, newFD, new, flags); err != nil {
						return err
					}
					return syscall.EIO
				}
				return originalLink(oldFD, old, newFD, new, flags)
			}

			result := make(chan error, 1)
			go func() { result <- test.operate(t.Context(), objects, target) }()
			<-validationEntered
			if _, err := objects.Put(t.Context(), poisonKey, []byte("poison")); !errors.Is(err, syscall.EIO) {
				release()
				<-result
				t.Fatalf("poisoning Put returned %v, want publication uncertainty", err)
			}
			release()
			if err := <-result; !errors.Is(err, syscall.EIO) || errors.Is(err, test.fact) {
				t.Fatalf("operation returned %v after poison, want only EIO instead of %v", err, test.fact)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInFlightSuccessAcknowledgesGlobalPoison(t *testing.T) {
	root := privateRoot(t)
	objects, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	first, _, poisonKey := shardTestKeys(t)
	firstLocation, _ := locate(first)
	poisonLocation, _ := locate(poisonKey)
	if firstLocation.shard == poisonLocation.shard {
		t.Fatal("test keys must use different shards")
	}
	shardFD, _, err := objects.openShard(t.Context(), firstLocation, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(shardFD); err != nil {
		t.Fatal(err)
	}

	originalLink := objects.ops.linkat
	objectPublished := make(chan struct{})
	releaseObject := make(chan struct{})
	var objectBlocked atomic.Bool
	var poisonInjected atomic.Bool
	objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
		if new == firstLocation.final && objectBlocked.CompareAndSwap(false, true) {
			if err := originalLink(oldFD, old, newFD, new, flags); err != nil {
				return err
			}
			close(objectPublished)
			<-releaseObject
			return nil
		}
		if old == storeIdentityStageName && new == storeIdentityName && poisonInjected.CompareAndSwap(false, true) {
			if err := originalLink(oldFD, old, newFD, new, flags); err != nil {
				return err
			}
			return syscall.EIO
		}
		return originalLink(oldFD, old, newFD, new, flags)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := objects.Put(t.Context(), first, []byte("durable first object"))
		firstDone <- err
	}()
	<-objectPublished
	if _, err := objects.Put(t.Context(), poisonKey, []byte("must not publish")); !errors.Is(err, syscall.EIO) {
		close(releaseObject)
		<-firstDone
		t.Fatalf("poisoning Put returned %v, want publication uncertainty", err)
	}
	close(releaseObject)
	if err := <-firstDone; !errors.Is(err, syscall.EIO) {
		t.Fatalf("in-flight Put returned %v after global poison, want EIO", err)
	}
	if err := objects.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(t.Context(), root, Options{})
	if err != nil {
		t.Fatalf("Open after poison: %v", err)
	}
	defer reopened.Close()
	content, err := reopened.Get(t.Context(), first)
	if err != nil || string(content) != "durable first object" {
		t.Fatalf("Get acknowledged object after reopen returned %q, %v", content, err)
	}
}

func TestShardIdentityLinkErrorsWithChangedNamesPoison(t *testing.T) {
	for _, state := range []string{"final only", "stage disappeared"} {
		t.Run(state, func(t *testing.T) {
			root := privateRoot(t)
			objects, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			key := "marker-link-error-" + state
			location, _ := locate(key)
			originalLink := objects.ops.linkat
			originalUnlink := objects.ops.unlinkat
			injected := false
			objects.ops.linkat = func(oldFD int, old string, newFD int, new string, flags int) error {
				if !injected && old == storeIdentityStageName && new == storeIdentityName {
					injected = true
					if state == "final only" {
						if err := originalLink(oldFD, old, newFD, new, flags); err != nil {
							return err
						}
					}
					if err := originalUnlink(oldFD, storeIdentityStageName, 0); err != nil {
						return err
					}
					return syscall.EIO
				}
				return originalLink(oldFD, old, newFD, new, flags)
			}
			if _, err := objects.Put(t.Context(), key, []byte("must not publish")); !errors.Is(err, syscall.EIO) {
				t.Fatalf("Put returned %v, want uncertain EIO", err)
			}
			if !injected {
				t.Fatal("link error state was not injected")
			}
			shardPath := filepath.Join(root, objectsDirectory, location.first)
			stagePath := filepath.Join(shardPath, storeIdentityStageName)
			finalPath := filepath.Join(shardPath, storeIdentityName)
			if _, err := os.Lstat(stagePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("publication error recreated or retained stage unexpectedly: %v", err)
			}
			_, finalErr := os.Stat(finalPath)
			if state == "final only" && finalErr != nil {
				t.Fatalf("publication error removed final evidence: %v", finalErr)
			}
			if state == "stage disappeared" && !errors.Is(finalErr, os.ErrNotExist) {
				t.Fatalf("publication error created final unexpectedly: %v", finalErr)
			}
			if _, err := objects.Get(t.Context(), "unrelated-key"); !errors.Is(err, syscall.EIO) {
				t.Fatalf("operation after changed marker names returned %v, want EIO", err)
			}
			if err := objects.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := Open(t.Context(), root, Options{})
			if err != nil {
				t.Fatalf("Open after changed marker names: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
