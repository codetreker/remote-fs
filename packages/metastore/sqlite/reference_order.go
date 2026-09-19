package sqlite

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// Order places a bounded access transition in the same order as native reads,
// publication and retirement. The transition must not perform I/O or reenter
// the native gate; an access coordinator calls Order without holding its mutex.
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
