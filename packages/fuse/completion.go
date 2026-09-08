package fuse

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// A close consumes the kernel descriptor even if its request was interrupted. Cleanup
// therefore has a finite independent budget, bounded by an earlier request deadline.
func (ns *namespace) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(ns.flushTimeout)
	if requestDeadline, bounded := ctx.Deadline(); bounded && requestDeadline.Before(deadline) {
		deadline = requestDeadline
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

func (ns *namespace) closeUnreturnedFile(ctx context.Context, file storage.File, cause error, changed bool) error {
	completion, cancel := ns.cleanupContext(ctx)
	defer cancel()
	closeErr := ns.closeError(file.Close(completion))
	if closeErr != nil {
		return afterMutation(changed, errors.Join(cause, closeErr))
	}
	return afterMutation(changed, cause)
}

func (ns *namespace) closeError(err error) error {
	if err == nil {
		return nil
	}
	// Normal session teardown retires references before draining them. Its worker
	// already owns final cleanup, so a late release is not lost continuity.
	if errnoOf(err) == syscall.ESTALE && errnoOf(ns.check()) == syscall.ESTALE {
		return err
	}
	ns.fence(err)
	return afterMutation(true, err)
}
