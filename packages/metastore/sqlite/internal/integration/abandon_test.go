package integration_test

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func TestAbandonReportsADatabaseItCannotReach(t *testing.T) {
	store, err := sqlite.Open(t.Context(), database(t), "workspace", 0, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Abandon(t.Context(), "reserved"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("abandoning through a closed database: %v, want EIO", err)
	}
	if err := store.Quarantine(t.Context(), "reserved"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("quarantining through a closed database: %v, want EIO", err)
	}
}

func TestAbandonRefusesAnObjectInAnUnknownState(t *testing.T) {
	path := database(t)
	store := open(t, path, "workspace", 0)
	key, err := store.Reserve(t.Context(), "f", 10)
	if err != nil {
		t.Fatal(err)
	}

	db := raw(t, path)
	if _, err := db.Exec(`UPDATE objects SET state = 99 WHERE key = ?`, string(key)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := store.Abandon(t.Context(), key); !errors.Is(err, syscall.EIO) {
		t.Fatalf("abandoning an object in an unknown state: %v, want EIO", err)
	}
	if err := store.Quarantine(t.Context(), key); !errors.Is(err, syscall.EIO) {
		t.Fatalf("quarantining an object in an unknown state: %v, want EIO", err)
	}
	if err := store.Forget(t.Context(), []metastore.Key{key}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("forgetting an object in an unknown state: %v, want EIO", err)
	}
}
