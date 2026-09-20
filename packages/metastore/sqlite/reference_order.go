package sqlite

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func (f *retainedFile) Order(ctx context.Context, transition func() error) error {
	if transition == nil {
		return syscall.EINVAL
	}
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer f.store.coordinator.commit.release()
	if err := f.check(); err != nil {
		return err
	}
	if err := f.store.checkFileOwnership(); err != nil {
		return err
	}
	if err := metastore.CheckFilePublication(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return transition()
}
