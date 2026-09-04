package sqlite_test

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func TestABackingStoreBindingSurvivesReopen(t *testing.T) {
	path := database(t)
	first, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("binding a new database: %v", err)
	}
	if err := first.Create(t.Context(), "held"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("reopening with the same backing store: %v", err)
	}
	defer second.Close()
	if _, err := second.Stat(t.Context(), "held"); err != nil {
		t.Fatalf("the namespace did not survive its bound reopen: %v", err)
	}

	db := raw(t, path)
	defer db.Close()
	var storeID string
	if err := db.QueryRow(`SELECT store_id FROM backing_store WHERE singleton = 1`).Scan(&storeID); err != nil {
		t.Fatal(err)
	}
	if storeID != "store-a" {
		t.Fatalf("the database is bound to %q, want store-a", storeID)
	}
}

func TestOpenBoundWithObjectLimitsUsesTheRequestedPendingLimit(t *testing.T) {
	store, err := sqlite.OpenBoundWithObjectLimits(
		t.Context(), database(t), "workspace", "store-a", 0, sqlite.DefaultWindow(),
		sqlite.ObjectLimits{MaxPendingObjects: 1, MaxPendingBytes: 1024},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Reserve(t.Context(), "first", 1); err != nil {
		t.Fatalf("reserving within the configured object limit: %v", err)
	}
	if _, err := store.Reserve(t.Context(), "second", 1); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("reserving beyond the configured object limit: %v, want EAGAIN", err)
	}
}

func TestABoundDatabaseRefusesADifferentBackingStoreWithoutCreatingANamespace(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	other, err := sqlite.OpenBound(t.Context(), path, "wrong", "store-b", 0, sqlite.DefaultWindow())
	if err == nil {
		other.Close()
		t.Fatal("opening a bound database with another backing store succeeded, want a refusal")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a bound database with another backing store: %v, want EINVAL", err)
	}

	db := raw(t, path)
	defer db.Close()
	var namespaces int
	if err := db.QueryRow(`SELECT count(*) FROM namespaces WHERE name = 'wrong'`).Scan(&namespaces); err != nil {
		t.Fatal(err)
	}
	if namespaces != 0 {
		t.Fatal("the refused opener created its namespace")
	}
}

func TestOpenCannotBypassABackingStoreBinding(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	bypass, err := sqlite.Open(t.Context(), path, "bypass", 0, sqlite.DefaultWindow())
	if err == nil {
		bypass.Close()
		t.Fatal("Open served a bound database without its backing store identity")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening a bound database without its backing store identity: %v, want EINVAL", err)
	}

	db := raw(t, path)
	defer db.Close()
	var namespaces int
	if err := db.QueryRow(`SELECT count(*) FROM namespaces WHERE name = 'bypass'`).Scan(&namespaces); err != nil {
		t.Fatal(err)
	}
	if namespaces != 0 {
		t.Fatal("the refused bypass created its namespace")
	}
}

func TestOpenCannotBypassACorruptBackingStoreBinding(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE backing_store SET singleton = 2`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	bypass, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err == nil {
		bypass.Close()
		t.Fatal("Open served a database whose backing-store binding was hidden under another singleton")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening a database with a corrupt backing-store binding: %v, want EIO", err)
	}
	db = raw(t, path)
	defer db.Close()
	var singleton int
	if err := db.QueryRow(`SELECT singleton FROM backing_store`).Scan(&singleton); err != nil {
		t.Fatal(err)
	}
	if singleton != 2 {
		t.Fatalf("a refused bypass rewrote the corrupt singleton to %d", singleton)
	}
}

func TestOpenRefusesMultipleBackingStoreBindings(t *testing.T) {
	path := database(t)
	store, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db := raw(t, path)
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO backing_store (singleton, store_id) VALUES (2, 'store-b')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err == nil {
		reopened.Close()
		t.Fatal("OpenBound accepted multiple backing-store bindings")
	}
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening multiple backing-store bindings: %v, want EIO", err)
	}
	db = raw(t, path)
	defer db.Close()
	var bindings int
	if err := db.QueryRow(`SELECT count(*) FROM backing_store`).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindings != 2 {
		t.Fatalf("a refused open rewrote %d backing-store bindings", bindings)
	}
}

func TestANonemptyUnboundDatabaseCannotBeBound(t *testing.T) {
	path := database(t)
	writeVersionOne(t, path)

	store, err := sqlite.OpenBound(t.Context(), path, "new", "store-a", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("binding a database that already holds an unbound namespace succeeded")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("binding a database that already holds an unbound namespace: %v, want EINVAL", err)
	}

	// The migration and binding shared the refused transaction, so neither was committed.
	db := raw(t, path)
	var version int
	if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("the refused binding moved schema version 1 to %d", version)
	}
	var bindingTable int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'backing_store'`).Scan(&bindingTable); err != nil {
		t.Fatal(err)
	}
	if bindingTable != 0 {
		t.Fatal("the refused binding committed its schema migration")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The original API remains the way an existing unbound database is reopened.
	unbound, err := sqlite.Open(t.Context(), path, "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("reopening the unbound database: %v", err)
	}
	defer unbound.Close()
	if _, err := unbound.Stat(t.Context(), "d/f"); err != nil {
		t.Fatalf("the refused binding disturbed the existing namespace: %v", err)
	}
}

func TestABindingRollsBackWhenNamespaceCreationFails(t *testing.T) {
	path := database(t)
	store := open(t, path, "temporary", 0)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	for _, statement := range []string{
		`DELETE FROM logs`,
		`DELETE FROM entries`,
		`DELETE FROM nodes`,
		`DELETE FROM objects`,
		`DELETE FROM namespaces`,
		`CREATE TRIGGER reject_namespace BEFORE INSERT ON namespaces
		 BEGIN SELECT RAISE(ABORT, 'namespace creation rejected'); END`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("preparing an empty database: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	bound, err := sqlite.OpenBound(t.Context(), path, "workspace", "store-a", 0, sqlite.DefaultWindow())
	if err == nil {
		bound.Close()
		t.Fatal("opening through a namespace creation failure succeeded")
	}

	db = raw(t, path)
	defer db.Close()
	var bindings int
	if err := db.QueryRow(`SELECT count(*) FROM backing_store`).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindings != 0 {
		t.Fatal("the backing store binding survived a failed namespace creation")
	}
}

func TestOpenBoundRefusesAnEmptyBackingStoreIdentity(t *testing.T) {
	store, err := sqlite.OpenBound(t.Context(), database(t), "workspace", "", 0, sqlite.DefaultWindow())
	if err == nil {
		store.Close()
		t.Fatal("opening with an empty backing store identity succeeded")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("opening with an empty backing store identity: %v, want EINVAL", err)
	}
}
