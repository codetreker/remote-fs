package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestVolumeCreatedAtPersistsAcrossReopenAndRootBirthMutation(t *testing.T) {
	config := lockingTestConfig(t)
	before := time.Now()
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	created, err := store.VolumeCreatedAt(t.Context())
	if err != nil || created.Before(before) || created.After(time.Now()) || created.Location() != time.UTC {
		t.Fatalf("fresh volume creation = %v, %v", created, err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil || root.BirthTime == nil || !root.BirthTime.Equal(created) {
		t.Fatalf("initial root and volume creation disagree: %+v, %v", root, err)
	}
	changed := created.Add(-time.Hour)
	if err := store.SetAttr(t.Context(), "", storage.AttrChange{BirthTime: &changed}); err != nil {
		t.Fatal(err)
	}
	if got, err := store.VolumeCreatedAt(t.Context()); err != nil || !got.Equal(created) {
		t.Fatalf("root birth changed volume creation: %v, %v", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.VolumeCreatedAt(t.Context()); err != nil || !got.Equal(created) {
		t.Fatalf("reopened volume creation = %v, %v", got, err)
	}
	root, err = store.Stat(t.Context(), "")
	if err != nil || root.BirthTime == nil || !root.BirthTime.Equal(changed) {
		t.Fatalf("reopened root birth = %+v, %v", root, err)
	}
}

func TestVolumeCreatedAtHistoricalVersionTenRemainsUnknown(t *testing.T) {
	config := lockingTestConfig(t)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.Stat(t.Context(), "")
	if err != nil || root.BirthTime == nil {
		t.Fatalf("initial root = %+v, %v", root, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", config.Database)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`ALTER TABLE volumes DROP COLUMN created_nsec`,
		`ALTER TABLE volumes DROP COLUMN created_sec`,
		`DROP TRIGGER entries_summary_insert`, `DROP TRIGGER entries_summary_delete`, `DROP TRIGGER entries_summary_update`, `DROP TRIGGER nodes_summary_kind`,
		`DROP TABLE imports`, `DROP TABLE authority_actions`, `DROP TABLE action_namespaces`, `DROP TABLE action_subjects`,
		`ALTER TABLE volumes DROP COLUMN named_files`, `ALTER TABLE volumes DROP COLUMN named_directories`, `ALTER TABLE volumes DROP COLUMN quota_bytes`, `ALTER TABLE volumes DROP COLUMN quota_revision`,
		`UPDATE schema_version SET version=10`,
	} {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	store, err = OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for range 2 {
		if got, err := store.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || !got.IsZero() {
			t.Fatalf("historical creation = %v, %v", got, err)
		}
	}
	var seconds, nanoseconds any
	if err := store.read.QueryRowContext(t.Context(), `SELECT created_sec,created_nsec FROM volumes WHERE id=?`, store.volume).
		Scan(&seconds, &nanoseconds); err != nil || seconds != nil || nanoseconds != nil {
		t.Fatalf("historical query backfilled creation: %v/%v, %v", seconds, nanoseconds, err)
	}
	current, err := store.Stat(t.Context(), "")
	if err != nil || current.BirthTime == nil || !current.BirthTime.Equal(*root.BirthTime) {
		t.Fatalf("history migration changed root birth: %+v, %v", current, err)
	}
}

func TestVolumeCreatedAtRejectsMalformedStoredPair(t *testing.T) {
	store, file := openPublicationFile(t)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	created, err := store.VolumeCreatedAt(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := store.write.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints=ON`); err != nil {
		t.Fatal(err)
	}
	defer connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`)
	defer connection.ExecContext(context.Background(), `UPDATE volumes SET created_sec=?,created_nsec=? WHERE id=?`, created.Unix(), created.Nanosecond(), store.volume)
	for _, update := range []string{
		`created_sec=NULL,created_nsec=0`, `created_sec=0,created_nsec=NULL`,
		`created_sec=X'30',created_nsec=0`, `created_sec='bad',created_nsec=0`, `created_sec=0.5,created_nsec=0`,
		`created_sec=0,created_nsec=X'30'`, `created_sec=0,created_nsec='bad'`, `created_sec=0,created_nsec=0.5`,
		`created_sec=0,created_nsec=-1`, `created_sec=0,created_nsec=1000000000`,
		`created_sec=zeroblob(1048576),created_nsec=0`, `created_sec=0,created_nsec=zeroblob(1048576)`,
	} {
		t.Run(update, func(t *testing.T) {
			if _, err := connection.ExecContext(t.Context(), `UPDATE volumes SET `+update+` WHERE id=?`, store.volume); err != nil {
				t.Fatal(err)
			}
			if got, err := store.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EIO) || !got.IsZero() {
				t.Fatalf("malformed creation = %v, %v", got, err)
			}
		})
	}
	if _, err := connection.ExecContext(t.Context(), `UPDATE volumes SET created_sec=?,created_nsec=0 WHERE id=?`, time.Time{}.Unix(), store.volume); err != nil {
		t.Fatal(err)
	}
	if got, err := store.VolumeCreatedAt(t.Context()); err != nil || !got.IsZero() {
		t.Fatalf("canonical known year-one instant = %v, %v", got, err)
	}
}

func TestVolumeCreatedAtPreservesPublicationCancellationAndHealthFailures(t *testing.T) {
	store, file := openPublicationFile(t)
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	denied := errors.New("creation fact publication denied")
	for _, failAt := range []int{1, 2} {
		calls := 0
		ctx := metastore.WithFilePublicationGuard(t.Context(), func() error {
			calls++
			if calls == failAt {
				return denied
			}
			return nil
		})
		if got, err := store.VolumeCreatedAt(ctx); !errors.Is(err, denied) || !got.IsZero() || calls != failAt {
			t.Fatalf("publication guard %d = %v, %v, %d calls", failAt, got, err, calls)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := store.VolumeCreatedAt(ctx); !errors.Is(err, context.Canceled) || !got.IsZero() {
		t.Fatalf("pre-capture cancellation = %v, %v", got, err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	ctx = metastore.WithFilePublicationGuard(ctx, func() error {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil
	})
	if got, err := store.VolumeCreatedAt(ctx); !errors.Is(err, context.Canceled) || !got.IsZero() || calls != 2 {
		t.Fatalf("post-capture cancellation = %v, %v, %d calls", got, err, calls)
	}
	failure := errors.New("creation fact durability failure")
	store.coordinator.poisonWith(failure)
	t.Cleanup(func() {
		store.coordinator.health.Lock()
		store.coordinator.poison = nil
		store.coordinator.health.Unlock()
	})
	if got, err := store.VolumeCreatedAt(t.Context()); !errors.Is(err, failure) || !errors.Is(err, syscall.EIO) || !got.IsZero() {
		t.Fatalf("unhealthy creation fact = %v, %v", got, err)
	}
}

func TestVolumeCreatedAtRequiresLiveVerifiedAuthority(t *testing.T) {
	config := lockingTestConfig(t)
	plain, err := OpenWithOptions(t.Context(), config.Database, config.Volume, 0, config.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := plain.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || !got.IsZero() {
		t.Fatalf("plain metadata creation query = %v, %v", got, err)
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}
	config = lockingTestConfig(t)
	store, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET root=? WHERE id=?`, store.root+1, store.volume); err != nil {
		t.Fatal(err)
	}
	if got, err := store.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EIO) || !got.IsZero() {
		t.Fatalf("unverified root creation = %v, %v", got, err)
	}
	if _, err := store.write.ExecContext(t.Context(), `UPDATE volumes SET root=? WHERE id=?`, store.root, store.volume); err != nil {
		t.Fatal(err)
	}
	if err := store.leaseOwner.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := store.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.EIO) || !got.IsZero() {
		t.Fatalf("released native ownership creation = %v, %v", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := store.VolumeCreatedAt(t.Context()); !errors.Is(err, syscall.ESTALE) || !got.IsZero() {
		t.Fatalf("closed creation authority = %v, %v", got, err)
	}
}
