package integration_test

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func TestObjectStatusAccountsForReservedAndGarbageBytesPerNamespace(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	other := open(t, path, "other", 0)

	first, err := store.Reserve(t.Context(), "f", 11)
	if err != nil {
		t.Fatal(err)
	}
	spare, err := store.Reserve(t.Context(), "spare", 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Reserve(t.Context(), "foreign", 100); err != nil {
		t.Fatal(err)
	}

	status, err := store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{ReservedCount: 2, ReservedBytes: 18}); status != want {
		t.Fatalf("two reservations report %+v, want %+v", status, want)
	}

	// A direct caller may commit a size different from the reservation. The committed size
	// becomes live content, so neither figure remains maintenance work.
	if err := store.Commit(t.Context(), "f", metastore.Object{
		Key: first, Size: 13, ModTime: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	status, err = store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{ReservedCount: 1, ReservedBytes: 7}); status != want {
		t.Fatalf("after committing one reservation status is %+v, want %+v", status, want)
	}

	replacement, err := store.Reserve(t.Context(), "f", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(t.Context(), "f", metastore.Object{
		Key: replacement, Size: 5, ModTime: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	status, err = store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{
		ReservedCount: 1,
		ReservedBytes: 7,
		GarbageCount:  1,
		GarbageBytes:  13,
	}); status != want {
		t.Fatalf("after replacing live content status is %+v, want %+v", status, want)
	}
	if err := store.Quarantine(t.Context(), spare); err != nil {
		t.Fatal(err)
	}

	garbage, err := store.Garbage(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(garbage) != 1 || garbage[0] != first {
		t.Fatalf("collecting explicit garbage returned %v, want only %q", garbage, first)
	}
	status, err = store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{
		UnresolvedCount: 1,
		UnresolvedBytes: 7,
		GarbageCount:    1,
		GarbageBytes:    13,
	}); status != want {
		t.Fatalf("after quarantining a reservation status is %+v, want %+v", status, want)
	}

	if err := store.Forget(t.Context(), []metastore.Key{first}); err != nil {
		t.Fatal(err)
	}
	status, err = store.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{UnresolvedCount: 1, UnresolvedBytes: 7}); status != want {
		t.Fatalf("after forgetting garbage status is %+v, want %+v", status, want)
	}

	foreign, err := other.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{ReservedCount: 1, ReservedBytes: 100}); foreign != want {
		t.Fatalf("the neighbouring namespace reports %+v, want %+v", foreign, want)
	}
}

func TestAReservationSizeSurvivesReopen(t *testing.T) {
	path := database(t)
	first, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reserve(t.Context(), "pending", 41); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	status, err := second.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (sqlite.ObjectStatus{ReservedCount: 1, ReservedBytes: 41}); status != want {
		t.Fatalf("the reopened reservation reports %+v, want %+v", status, want)
	}
}

func TestObjectStatusReportsADatabaseItCannotReach(t *testing.T) {
	store, err := sqlite.Open(t.Context(), database(t), "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("reading object status from a closed database: %v, want EIO", err)
	}
}

func TestObjectStatusRefusesMalformedObjectRecords(t *testing.T) {
	for name, damage := range map[string]string{
		"unknown state": `UPDATE objects SET state = 99`,
		"negative size hidden by a positive size": `
			UPDATE objects SET size = CASE key
				WHEN (SELECT min(key) FROM objects) THEN -10
				ELSE 20
			END`,
	} {
		t.Run(name, func(t *testing.T) {
			path := database(t)
			store := open(t, path, "workspace", 0)
			if _, err := store.Reserve(t.Context(), "one", 10); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Reserve(t.Context(), "two", 20); err != nil {
				t.Fatal(err)
			}

			db := raw(t, path)
			if _, err := db.Exec(damage); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := store.ObjectStatus(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("reading status after %s: %v, want EIO", name, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
			if err == nil {
				reopened.Close()
				t.Fatalf("reopening after %s succeeded, want EIO", name)
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("reopening after %s: %v, want EIO", name, err)
			}
		})
	}
}
