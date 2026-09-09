package sqlite

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCanceledCommitAndPoisonRemainDurabilityFailures(t *testing.T) {
	uncertain := sqlerr.NewUncertainCommit(context.Canceled)
	coordinator := new(databaseCoordinator)
	coordinator.poisonWith(uncertain)
	for _, err := range []error{uncertain, coordinator.healthy()} {
		if storage.ErrnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) {
			t.Fatalf("uncertain durability = %v (%v), want EIO retaining cancellation", err, storage.ErrnoOf(err))
		}
		for _, joined := range []error{
			errors.Join(err, context.Canceled),
			errors.Join(context.Canceled, err),
		} {
			if storage.ErrnoOf(joined) != syscall.EIO {
				t.Fatalf("joined cancellation displaced durability failure: %v", joined)
			}
		}
	}
}

func TestCoordinatorPoisonPreservesTheFirstDurabilityFailureAsEIO(t *testing.T) {
	coordinator := new(databaseCoordinator)
	coordinator.poisonWith(syscall.EEXIST)
	first := coordinator.healthy()
	if !errors.Is(first, syscall.EIO) || !errors.Is(first, syscall.EEXIST) {
		t.Fatalf("poisoned coordinator returned %v, want EIO retaining EEXIST", first)
	}
	coordinator.poisonWith(syscall.ENOSPC)
	second := coordinator.healthy()
	if !errors.Is(second, syscall.EEXIST) || errors.Is(second, syscall.ENOSPC) {
		t.Fatalf("second poison replaced the first durability failure: %v", second)
	}
}
