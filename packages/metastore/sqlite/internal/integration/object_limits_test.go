package integration_test

import (
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
)

func openWithObjectLimits(
	t *testing.T,
	path, namespace string,
	limits sqlite.ObjectLimits,
) *sqlite.Store {
	t.Helper()
	store, err := sqlite.OpenWithObjectLimits(
		t.Context(), path, namespace, 0, sqlite.DefaultWindow(), limits,
	)
	if err != nil {
		t.Fatalf("opening %q with object limits %+v: %v", namespace, limits, err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	return store
}

func TestObjectLimitsAreBoundedAndValidatedBeforeOpening(t *testing.T) {
	defaults := sqlite.DefaultObjectLimits()
	if defaults != (sqlite.ObjectLimits{
		MaxPendingObjects: sqlite.DefaultMaxPendingObjects,
		MaxPendingBytes:   sqlite.DefaultMaxPendingBytes,
	}) {
		t.Fatalf("default object limits are %+v", defaults)
	}
	if defaults.MaxPendingBytes != 8*1024*1024*1024 {
		t.Fatalf("the default pending byte limit is %d, want 8 GiB", defaults.MaxPendingBytes)
	}
	const largestSuppliedObject = int64(5000 * 1024 * 1024)
	if defaults.MaxPendingBytes < largestSuppliedObject {
		t.Fatalf("the default pending byte limit %d cannot hold the supplied blob backend's %d-byte maximum object",
			defaults.MaxPendingBytes, largestSuppliedObject)
	}
	if err := defaults.Validate(); err != nil {
		t.Fatalf("validating the defaults: %v", err)
	}
	if err := (sqlite.ObjectLimits{}).Validate(); err != nil {
		t.Fatalf("validating zero-valued defaults: %v", err)
	}
	effective, err := (sqlite.ObjectLimits{}).Effective()
	if err != nil {
		t.Fatalf("resolving zero-valued defaults: %v", err)
	}
	if effective != defaults {
		t.Fatalf("zero-valued limits resolve to %+v, want %+v", effective, defaults)
	}

	invalid := map[string]sqlite.ObjectLimits{
		"negative objects":    {MaxPendingObjects: -1},
		"negative bytes":      {MaxPendingBytes: -1},
		"unbounded objects":   {MaxPendingObjects: math.MaxInt64},
		"unbounded byte size": {MaxPendingBytes: math.MaxInt64},
	}
	for name, limits := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := limits.Validate(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("Validate returned %v, want EINVAL", err)
			}

			path := database(t)
			store, err := sqlite.OpenWithObjectLimits(
				t.Context(), path, "workspace", 0, sqlite.DefaultWindow(), limits,
			)
			if err == nil {
				store.Close()
				t.Fatal("OpenWithObjectLimits accepted invalid limits")
			}
			if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("OpenWithObjectLimits returned %v, want EINVAL", err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid limits touched the database path: %v", statErr)
			}
		})
	}
}

func TestOneReservationLargerThanThePendingByteLimitIsEFBIG(t *testing.T) {
	store := openWithObjectLimits(t, database(t), "workspace", sqlite.ObjectLimits{
		MaxPendingObjects: 1,
		MaxPendingBytes:   10,
	})
	if _, err := store.Reserve(t.Context(), "held", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Reserve(t.Context(), "oversized", 11); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("reserving one object larger than the byte limit beside a full backlog: %v, want EFBIG", err)
	}
	if _, err := store.Reserve(t.Context(), "temporarily-blocked", 1); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("reserving an object that fits by itself beside a full backlog: %v, want EAGAIN", err)
	}
	status, err := store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.ReservedCount != 1 || status.ReservedBytes != 1 {
		t.Fatalf("refused reservations changed status to %+v", status)
	}
}

func TestReserveEnforcesPendingObjectAndByteLimits(t *testing.T) {
	for _, test := range []struct {
		name        string
		limits      sqlite.ObjectLimits
		first       int64
		second      int64
		refused     int64
		wantObjects int64
		wantBytes   int64
	}{
		{
			name: "objects", limits: sqlite.ObjectLimits{MaxPendingObjects: 2, MaxPendingBytes: 100},
			first: 40, second: 40, refused: 1, wantObjects: 2, wantBytes: 80,
		},
		{
			name: "bytes", limits: sqlite.ObjectLimits{MaxPendingObjects: 10, MaxPendingBytes: 10},
			first: 6, second: 4, refused: 1, wantObjects: 2, wantBytes: 10,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openWithObjectLimits(t, database(t), "workspace", test.limits)
			if _, err := store.Reserve(t.Context(), "first", test.first); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Reserve(t.Context(), "second", test.second); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Reserve(t.Context(), "refused", test.refused); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("reserving past the %s limit: %v, want EAGAIN", test.name, err)
			}

			status, err := store.ObjectStatus(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if status.ReservedCount != test.wantObjects || status.ReservedBytes != test.wantBytes {
				t.Fatalf("the refused reservation left status %+v, want %d objects and %d bytes",
					status, test.wantObjects, test.wantBytes)
			}
			if status.OverLimit {
				t.Fatalf("a backlog exactly at its limit reports over-limit: %+v", status)
			}
		})
	}
}

func TestConcurrentReservationsCannotCrossThePendingLimits(t *testing.T) {
	const (
		callers = 64
		limit   = 8
		size    = 7
	)
	path := database(t)
	limits := sqlite.ObjectLimits{
		MaxPendingObjects: limit,
		MaxPendingBytes:   limit * size,
	}
	stores := []*sqlite.Store{
		openWithObjectLimits(t, path, "workspace", limits),
		openWithObjectLimits(t, path, "workspace", limits),
		openWithObjectLimits(t, path, "workspace", limits),
		openWithObjectLimits(t, path, "workspace", limits),
	}

	start := make(chan struct{})
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := range callers {
		go func() {
			ready.Done()
			<-start
			_, err := stores[i%len(stores)].Reserve(t.Context(), fmt.Sprintf("f-%d", i), size)
			results <- err
		}()
	}
	ready.Wait()
	close(start)

	var accepted, refused int
	for range callers {
		switch err := <-results; {
		case err == nil:
			accepted++
		case errors.Is(err, syscall.EAGAIN):
			refused++
		default:
			t.Fatalf("a concurrent reservation failed with %v", err)
		}
	}
	if accepted != limit || refused != callers-limit {
		t.Fatalf("concurrent reservations accepted %d and refused %d, want %d and %d",
			accepted, refused, limit, callers-limit)
	}
	status, err := stores[0].ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.ReservedCount != limit || status.ReservedBytes != limit*size || status.OverLimit {
		t.Fatalf("concurrent reservations left status %+v", status)
	}
}

func TestNamespaceSheddingMayPushTheBacklogOverLimitAndRecoveryReopensIt(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, *sqlite.Store)
		shed    func(*testing.T, *sqlite.Store)
	}{
		{
			name: "remove",
			prepare: func(t *testing.T, store *sqlite.Store) {
				commit(t, store, "victim", 10)
			},
			shed: func(t *testing.T, store *sqlite.Store) {
				if err := store.Remove(t.Context(), "victim"); err != nil {
					t.Fatalf("removing referenced content: %v", err)
				}
			},
		},
		{
			name: "rename",
			prepare: func(t *testing.T, store *sqlite.Store) {
				commit(t, store, "from", 10)
				commit(t, store, "victim", 20)
			},
			shed: func(t *testing.T, store *sqlite.Store) {
				if err := store.Rename(t.Context(), "from", "victim"); err != nil {
					t.Fatalf("renaming over referenced content: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := database(t)
			limits := sqlite.ObjectLimits{MaxPendingObjects: 1, MaxPendingBytes: 100}
			first, err := sqlite.OpenWithObjectLimits(
				t.Context(), path, "workspace", 0, sqlite.DefaultWindow(), limits,
			)
			if err != nil {
				t.Fatal(err)
			}
			test.prepare(t, first)
			pending, err := first.Reserve(t.Context(), "pending", 1)
			if err != nil {
				t.Fatal(err)
			}
			test.shed(t, first)

			status, err := first.ObjectStatus(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if !status.OverLimit || status.ReservedCount != 1 || status.GarbageCount != 1 {
				t.Fatalf("shedding namespace data left status %+v, want one reservation, one garbage object, and over-limit", status)
			}
			if _, err := first.Reserve(t.Context(), "blocked", 1); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("reserving while over-limit: %v, want EAGAIN", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}

			second, err := sqlite.OpenWithObjectLimits(
				t.Context(), path, "workspace", 0, sqlite.DefaultWindow(), limits,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			status, err = second.ObjectStatus(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if !status.OverLimit {
				t.Fatalf("the reopened backlog no longer reports over-limit: %+v", status)
			}
			if _, err := second.Reserve(t.Context(), "still-blocked", 1); !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("reserving after reopening an over-limit backlog: %v, want EAGAIN", err)
			}

			if err := second.Abandon(t.Context(), pending); err != nil {
				t.Fatalf("recording positive ownership proof for the pending object: %v", err)
			}
			keys, err := second.Garbage(t.Context(), 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(keys) != 2 {
				t.Fatalf("collecting the over-limit garbage returned %d keys, want 2", len(keys))
			}
			if err := second.Forget(t.Context(), keys); err != nil {
				t.Fatal(err)
			}
			if _, err := second.Reserve(t.Context(), "recovered", 100); err != nil {
				t.Fatalf("reserving after cleanup reduced the backlog: %v", err)
			}
		})
	}
}

func TestCommitMayAuthoritativelyReplaceAReservationWithLargerGarbage(t *testing.T) {
	path := database(t)
	first := openWithObjectLimits(t, path, "workspace", sqlite.ObjectLimits{
		MaxPendingObjects: 10,
		MaxPendingBytes:   100,
	})
	commit(t, first, "f", 20)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := sqlite.OpenWithObjectLimits(
		t.Context(), path, "workspace", 0, sqlite.DefaultWindow(), sqlite.ObjectLimits{
			MaxPendingObjects: 10,
			MaxPendingBytes:   10,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	replacement, err := second.Reserve(t.Context(), "f", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(t.Context(), "f", metastore.Object{
		Key: replacement, Size: 1, ModTime: time.Now(),
	}); err != nil {
		t.Fatalf("committing the authoritative replacement: %v", err)
	}

	status, err := second.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !status.OverLimit || status.ReservedCount != 0 ||
		status.GarbageCount != 1 || status.GarbageBytes != 20 {
		t.Fatalf("the replacement left status %+v, want 20 garbage bytes over the 10-byte limit", status)
	}
	if _, err := second.Reserve(t.Context(), "blocked", 1); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("reserving while authoritative garbage is over-limit: %v, want EAGAIN", err)
	}
	keys, err := second.Garbage(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("collecting authoritative garbage returned %d keys, want 1", len(keys))
	}
	if err := second.Forget(t.Context(), keys); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Reserve(t.Context(), "recovered", 10); err != nil {
		t.Fatalf("reserving after authoritative garbage was removed: %v", err)
	}
}
