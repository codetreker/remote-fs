package management

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	temporary := filepath.Join(dir, "..", "..", ".tmp")
	if err := os.MkdirAll(temporary, 0700); err != nil {
		panic(err)
	}
	if err := os.Setenv("TMPDIR", temporary); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func managementFixture(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "management.sqlite")
	options := DefaultOptions()
	options.Create = true
	store, err := Open(t.Context(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, path
}

func TestManagementOpenRequiresExplicitInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	if _, err := Open(t.Context(), path, DefaultOptions()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing catalog: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing catalog was created: %v", err)
	}
	options := DefaultOptions()
	options.Create = true
	store, err := Open(t.Context(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	id := store.DatabaseID()
	if len(id) != 32 {
		t.Fatalf("invalid durable identity %q", id)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	options.Create = false
	options.ExpectedDatabaseID = id
	reopened, err := Open(t.Context(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.DatabaseID() != id {
		t.Fatal("catalog identity changed")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	options.ExpectedDatabaseID = ID(strings.Repeat("a", 32))
	if _, err := Open(t.Context(), path, options); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("accepted another root's database: %v", err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), path, options); err == nil {
		t.Fatal("accepted public database")
	}
}

func TestManagementOpenRejectsSymlinksAndInvalidOptions(t *testing.T) {
	store, path := managementFixture(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link.sqlite")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), link, DefaultOptions()); err == nil {
		t.Fatal("accepted database symlink")
	}
	if _, err := Open(t.Context(), "relative.sqlite", DefaultOptions()); err == nil {
		t.Fatal("accepted relative database")
	}
	for _, change := range []func(*Options){
		func(o *Options) { o.MaxReaders = 0 },
		func(o *Options) { o.MaxNamespaceParticipants = 0 },
		func(o *Options) { o.BusyTimeout = 0 },
		func(o *Options) { o.MaxSessionsPerUser = o.MaxSessions + 1 },
		func(o *Options) { o.SessionIdleTTL = o.SessionAbsoluteTTL + time.Second },
	} {
		options := DefaultOptions()
		change(&options)
		if err := options.Validate(); err == nil {
			t.Fatal("configuration accepted invalid limits")
		}
		if _, err := Open(t.Context(), path, options); err == nil {
			t.Fatal("accepted unbounded or inconsistent options")
		}
	}
}

func TestManagementRollbackAndCancellationRemainFailures(t *testing.T) {
	store, _ := managementFixture(t)
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("failure after account insert")
	err = store.writeTx(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), "INSERT INTO users(id,role,display_name,auth_epoch,next_action_namespace,retired_before_namespace,logical_time_high_water,created_at) VALUES(?,'user','example',1,1,0,0,0)", id)
		if err != nil {
			return err
		}
		return refusal
	})
	if !errors.Is(err, refusal) {
		t.Fatalf("lost transaction error: %v", err)
	}
	if _, err := store.GetUser(t.Context(), id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back account remains: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.writeTx(ctx, func(*sql.Tx) error { t.Fatal("canceled transaction admitted"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.writeTx(t.Context(), func(*sql.Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed writer accepted work: %v", err)
	}
}

func TestManagementReopenRefusesBrokenRelationshipsAndFutureSchema(t *testing.T) {
	for _, corruption := range []string{
		"INSERT INTO local_admin_credentials(user_id,login_name,password_hash,password_version,changed_at) VALUES('missing','admin','encoded',1,0)",
		"UPDATE schema_version SET version=2",
		"DELETE FROM database_identity",
		"DROP TABLE volumes",
	} {
		t.Run(corruption, func(t *testing.T) {
			store, path := managementFixture(t)
			if err := store.writeTx(t.Context(), func(tx *sql.Tx) error { _, err := tx.ExecContext(t.Context(), corruption); return err }); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(t.Context(), path, DefaultOptions()); err == nil {
				reopened.Close()
				t.Fatal("accepted corrupt catalog")
			}
		})
	}
}

func TestManagementDatabaseConstraintsProtectAccountAndVolumeIdentities(t *testing.T) {
	store, _ := managementFixture(t)
	user, err := store.BootstrapAdmin(t.Context(), "admin", "Administrator", "encoded-hash")
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := store.IssueNamespace(t.Context(), user.ID)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := store.CreateVolume(t.Context(), user.ID, ActionKey{Namespace: namespace.Sequence, Nonce: "create"}, "example", 4096)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"UPDATE volumes SET owner_user_id='another' WHERE id=?",
		"UPDATE volumes SET storage_name='another' WHERE id=?",
		"UPDATE volumes SET management_revision=0 WHERE id=?",
	} {
		if err := store.writeTx(t.Context(), func(tx *sql.Tx) error {
			_, err := tx.ExecContext(t.Context(), statement, operation.VolumeID)
			return err
		}); err == nil {
			t.Fatalf("accepted invalid write: %s", statement)
		}
	}
	volume, err := store.GetVolume(t.Context(), user.ID, operation.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if volume.OwnerUserID != user.ID || volume.ManagementRevision != 1 {
		t.Fatalf("failed mutation changed volume: %+v", volume)
	}
}

func TestManagementUnknownCommitKeepsDiagnosticCause(t *testing.T) {
	cause := errors.New("storage unavailable")
	err := &CommitUnknownError{Cause: cause}
	if !errors.Is(err, cause) || strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("unsafe or lost error: %v", err)
	}
}

func TestManagementIDsRejectUnsafeStorageNames(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if !id.Valid() {
		t.Fatalf("issued noncanonical identity %q", id)
	}
	for _, id := range []ID{"", "..", ID(strings.Repeat("A", 32)), ID(strings.Repeat("g", 32)), ID(strings.Repeat("0", 31)), ID(strings.Repeat("0", 31) + "/")} {
		if id.Valid() {
			t.Fatalf("accepted unsafe identity %q", id)
		}
	}
}

func TestManagementReopenRefusesMismatchedNamespaceParticipants(t *testing.T) {
	store, path := managementFixture(t)
	owner := authTestAdmin(t, store, "owner")
	other := authTestAdmin(t, store, "other")
	namespace := actionTestNamespace(t, store, owner.ID)
	operation, err := store.CreateVolume(t.Context(), owner.ID, ActionKey{Namespace: namespace.Sequence, Nonce: "create"}, "files", 0)
	if err != nil {
		t.Fatal(err)
	}
	otherNamespace := actionTestNamespace(t, store, other.ID)
	if _, err := store.write.ExecContext(t.Context(), "INSERT INTO namespace_volume_participants(actor_user_id,sequence,volume_id,created_at) VALUES(?,?,?,?)", other.ID, otherNamespace.Sequence, operation.VolumeID, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(t.Context(), path, DefaultOptions()); reopened != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("foreign owner participant passed startup integrity: store=%v error=%v", reopened, err)
	}
}
