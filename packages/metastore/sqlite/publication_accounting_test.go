package sqlite

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSQLitePublicationDistinguishesPreparationFailureFromUncertainUnwind(t *testing.T) {
	for _, failedUnwind := range []bool{false, true} {
		t.Run(fmt.Sprintf("unwind failed %t", failedUnwind), func(t *testing.T) {
			f := newPublicationFixture(t)
			f.put(t, t.Context(), "file", 3)
			before := f.node(t, "file")
			object := f.stage(t, t.Context(), "file", 7)
			owner := f.owner(t)
			grant := f.grant(t, owner, "file", locking.Exclusive)
			primary := errors.New("quota preparation unavailable")
			var unwind error
			if failedUnwind {
				unwind = errors.New("quota reservation could not be released")
				*f.authorityFence = unwind
			}
			settlements := 0
			ctx := storage.WithPublicationAccounting(publicationScope(t.Context(), owner, grant),
				func(_, _ int64) (storage.PublicationSettlement, error) {
					return func(result storage.PublicationResult) error {
						settlements++
						if result != storage.PublicationNotApplied {
							t.Errorf("preparation rollback settled %v, want NotApplied", result)
						}
						return unwind
					}, primary
				})
			err := f.store.Commit(ctx, "file", object)
			if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, primary) || settlements != 1 ||
				storage.IsPublicationAccountingUncertain(err) != failedUnwind {
				t.Fatalf("preparation returned %v, settlements=%d, failed unwind=%v", err, settlements, failedUnwind)
			}
			if failedUnwind && !errors.Is(err, unwind) {
				t.Fatalf("preparation lost the unwind cause: %v", err)
			}
			status, statusErr := f.store.locks.Status(t.Context())
			if statusErr != nil || status.Unavailable != failedUnwind {
				t.Fatalf("preparation left authority status %+v, error %v", status, statusErr)
			}
			var size int64
			var content string
			if err := f.store.read.QueryRowContext(t.Context(),
				`SELECT size, content FROM nodes WHERE id = ?`, before.ID).Scan(&size, &content); err != nil ||
				size != before.Size || content != string(before.Content) {
				t.Fatalf("preparation changed native file to %d/%q: %v", size, content, err)
			}
			if failedUnwind {
				if _, err := f.store.Stat(t.Context(), "file"); storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, unwind) {
					t.Fatalf("uncertain accounting did not fence native reads: %v", err)
				}
			} else if state, err := f.store.locks.QueryGrant(t.Context(), owner, grant); err != nil || state.State != locking.Active {
				t.Fatalf("known preparation refusal displaced the grant: %+v, %v", state, err)
			}
		})
	}
}
