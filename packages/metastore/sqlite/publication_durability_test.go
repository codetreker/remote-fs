package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type failedPublicationWitness struct{ cause error }

func (w failedPublicationWitness) Accept(DurableState) error   { return w.cause }
func (failedPublicationWitness) Checkpoint(DurableState) error { return nil }

func TestSQLitePublicationDurabilityFailureFencesBothAuthorities(t *testing.T) {
	f := newPublicationFixture(t)
	f.put(t, t.Context(), "file", 3)
	object := f.stage(t, t.Context(), "file", 7)
	owner := f.owner(t)
	grant := f.grant(t, owner, "file", locking.Exclusive)
	witnessFailure := context.Canceled
	settlementFailure := errors.New("quota outcome could not be recorded")
	*f.authorityFence = witnessFailure
	f.store.witness = failedPublicationWitness{cause: witnessFailure}
	defer func() { f.store.witness = nil }()
	settlements := 0
	ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant),
		func(_, _ int64) (storage.PublicationSettlement, error) {
			return func(result storage.PublicationResult) error {
				settlements++
				if result != storage.PublicationUnknown {
					t.Errorf("unconfirmed durability settled as %v, want Unknown", result)
				}
				return settlementFailure
			}, nil
		})
	err := f.store.Commit(ctx, "file", object)
	if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, witnessFailure) || !errors.Is(err, settlementFailure) || settlements != 1 {
		t.Fatalf("unconfirmed commit = %v after %d settlements, want both causes and EIO", err, settlements)
	}
	status, err := f.store.locks.Status(t.Context())
	if err != nil || !status.Unavailable {
		t.Fatalf("authority accepted an unconfirmed namespace: %+v, %v", status, err)
	}
	if _, err := f.store.Stat(t.Context(), "file"); storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, witnessFailure) {
		t.Fatalf("namespace read after unconfirmed durability = %v", err)
	}
	var size int64
	if err := f.store.read.QueryRowContext(t.Context(),
		`SELECT size FROM nodes WHERE namespace = ? AND content = ?`, f.store.namespace, string(object.Key)).Scan(&size); err != nil {
		t.Fatal(err)
	}
	if size != object.Size {
		t.Fatalf("SQLite stored %d bytes before the witness failed, want %d", size, object.Size)
	}
}
