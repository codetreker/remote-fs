package replicated

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type nodeReference struct {
	session *fileSession
	remote  httprest.NodeReferenceWithBarrier
	retainedClose
}

func referenceCall[C, R any](ctx context.Context, session *fileSession, remote any, call func(context.Context, C) (R, error)) (R, error) {
	return fileCall(ctx, session, true, func(ctx context.Context) (R, error) {
		capability, err := optional[C](remote)
		if err != nil {
			var zero R
			return zero, err
		}
		return call(ctx, capability)
	})
}

func (r *nodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	return fileCall(ctx, r.session, true, r.remote.Stat)
}

func (r *nodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	return fileCall(ctx, r.session, true, func(ctx context.Context) (storage.Attr, error) {
		return capabilityMutation(ctx, r.session, "set-reference-attr", func(ctx context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
			return r.remote.SetAttrWithBarrier(ctx, change)
		})
	})
}

func (r *nodeReference) Close(ctx context.Context) error {
	_, err := r.CloseWithResult(ctx)
	return err
}

func (r *nodeReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	return r.retainedClose.close(ctx, r.session, r.remote, "reference", nil)
}

func (r *nodeReference) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	return r.retainedClose.closeWithAction(ctx, r.session, r.remote, "reference", attempt)
}

func (r *nodeReference) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	return queryCloseAttempt(ctx, r.remote, attempt)
}

func (r *nodeReference) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	return r.retainedClose.ownerStatus(ctx, r.session, r.remote)
}

func (r *nodeReference) CheckScopedReference() error {
	return capabilityCheck(r.remote, func(capability storage.ScopedReference) error { return capability.CheckScopedReference() })
}

func (r *nodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return referenceCall(ctx, r.session, r.remote, func(ctx context.Context, capability storage.ScopedReference) (storage.UseScope, error) {
		return capability.Scope(ctx)
	})
}

func (r *nodeReference) CheckReferenceState() error {
	return capabilityCheck(r.remote, func(capability storage.ReferenceStateAccess) error { return capability.CheckReferenceState() })
}

func (r *nodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	return referenceCall(ctx, r.session, r.remote, func(ctx context.Context, capability storage.ReferenceStateAccess) (storage.ReferenceState, error) {
		return capability.State(ctx)
	})
}

func (r *nodeReference) CheckMetadataAccess() error {
	return capabilityCheck(r.remote, func(capability httprest.ReferenceMetadataAccessWithBarrier) error {
		return capability.CheckMetadataAccess()
	})
}

func (r *nodeReference) SetMetadata(ctx context.Context, namespace string, version, payload []byte) (storage.OpaquePayload, error) {
	return referenceCall(ctx, r.session, r.remote, func(ctx context.Context, capability httprest.ReferenceMetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		return capabilityMutation(ctx, r.session, "set-reference-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return capability.SetMetadataWithBarrier(ctx, namespace, version, payload)
		})
	})
}

func (r *nodeReference) CheckDeleteIntent() error {
	return capabilityCheck(r.remote, func(capability httprest.DeleteIntentWithBarrier) error { return capability.CheckDeleteIntent() })
}

func (r *nodeReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCall(ctx, r.session, r.remote, func(ctx context.Context, capability httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		var result storage.ReferenceState
		err := r.session.confirm(ctx, "set-pending-unlink", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.SetPendingUnlinkWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

func (r *nodeReference) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCall(ctx, r.session, r.remote, func(ctx context.Context, capability httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		var result storage.ReferenceState
		err := r.session.confirm(ctx, "clear-pending-unlink", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.ClearPendingUnlinkWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

func (r *nodeReference) CheckConditionalFileMutation() error {
	return capabilityCheck(r.remote, func(capability httprest.ConditionalFileMutationWithBarrier) error {
		return capability.CheckConditionalFileMutation()
	})
}

func (r *nodeReference) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	return referenceCall(ctx, r.session, r.remote, func(ctx context.Context, capability httprest.ConditionalFileMutationWithBarrier) (storage.Attr, error) {
		var result storage.Attr
		err := r.session.confirm(ctx, "mutate-file", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.MutateFileWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

func (f *retainedFile) CheckReferenceState() error {
	return capabilityCheck(f.remote, func(capability storage.ReferenceStateAccess) error { return capability.CheckReferenceState() })
}

func (f *retainedFile) State(ctx context.Context) (storage.ReferenceState, error) {
	return referenceCall(ctx, f.session, f.remote, func(ctx context.Context, capability storage.ReferenceStateAccess) (storage.ReferenceState, error) {
		return capability.State(ctx)
	})
}

func (f *retainedFile) CheckDeleteIntent() error {
	return capabilityCheck(f.remote, func(capability httprest.DeleteIntentWithBarrier) error { return capability.CheckDeleteIntent() })
}

func (f *retainedFile) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCall(ctx, f.session, f.remote, func(ctx context.Context, capability httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		var result storage.ReferenceState
		err := f.session.confirm(ctx, "set-pending-unlink", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.SetPendingUnlinkWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

func (f *retainedFile) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCall(ctx, f.session, f.remote, func(ctx context.Context, capability httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		var result storage.ReferenceState
		err := f.session.confirm(ctx, "clear-pending-unlink", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.ClearPendingUnlinkWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

func (f *retainedFile) CheckConditionalFileMutation() error {
	return capabilityCheck(f.remote, func(capability httprest.ConditionalFileMutationWithBarrier) error {
		return capability.CheckConditionalFileMutation()
	})
}

func (f *retainedFile) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	return referenceCall(ctx, f.session, f.remote, func(ctx context.Context, capability httprest.ConditionalFileMutationWithBarrier) (storage.Attr, error) {
		var result storage.Attr
		err := f.session.confirm(ctx, "mutate-file", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.MutateFileWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

// A failed capability negotiation can still transfer a reference whose cleanup
// is unsettled. Cleanup and immutable identity remain available; data and
// metadata access retain the original failure.
type failedOpenReference struct {
	native      any
	close       func(context.Context, error) (storage.ReferenceCloseResult, bool, error)
	failure     error
	mu          sync.Mutex
	run         chan struct{}
	result      storage.ReferenceCloseResult
	err         error
	originalErr error
	settled     bool
}

func newFailedOpenReference(native storage.NodeReference, failure error, result storage.ReferenceCloseResult, closeErr error, close func(context.Context, error) (storage.ReferenceCloseResult, bool, error)) storage.NodeReference {
	reference := &failedOpenReference{native: native, close: close, failure: failure, result: result}
	if result.Released {
		reference.originalErr = persistentCloseError(closeErr)
	}
	return reference
}

func newFailedOpenFile(native storage.File, failure error, result storage.ReferenceCloseResult, closeErr error, close func(context.Context, error) (storage.ReferenceCloseResult, bool, error)) storage.File {
	file := &failedOpenFile{failedOpenReference: failedOpenReference{native: native, close: close, failure: failure, result: result}}
	if result.Released {
		file.originalErr = persistentCloseError(closeErr)
	}
	return file
}

func (r *failedOpenReference) ReferenceNodeID() (uint64, error) {
	return storage.ReferenceNodeID(r.native)
}

func (r *failedOpenReference) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, r.failure
}

func (r *failedOpenReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, r.failure
}

func (r *failedOpenReference) CheckScopedReference() error { return r.failure }
func (r *failedOpenReference) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{}, r.failure
}
func (r *failedOpenReference) CheckReferenceState() error { return r.failure }
func (r *failedOpenReference) State(context.Context) (storage.ReferenceState, error) {
	return storage.ReferenceState{}, r.failure
}
func (r *failedOpenReference) Close(ctx context.Context) error {
	_, err := r.CloseWithResult(ctx)
	return err
}

func (r *failedOpenReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	r.mu.Lock()
	for r.run != nil {
		run := r.run
		r.mu.Unlock()
		select {
		case <-run:
		case <-ctx.Done():
			return storage.ReferenceCloseResult{}, ctx.Err()
		}
		r.mu.Lock()
	}
	if r.settled {
		result, err := r.result, r.err
		r.mu.Unlock()
		return result, err
	}
	previous := r.result
	originalErr := r.originalErr
	r.run = make(chan struct{})
	r.mu.Unlock()
	result, settled, err := r.close(ctx, originalErr)
	if previous.Released && !result.Released {
		result = previous
		settled = false
		err = &storage.CloseSettlementError{
			State: storage.CloseSettlementUnknown, SemanticErr: originalErr,
			Cause: errors.Join(closeErrorPart(err, false), fmt.Errorf("released failed-open reference lost barrier replay: %w", syscall.EIO)),
		}
	}
	err = errors.Join(err, result.Check(err))
	r.mu.Lock()
	if result.Released {
		r.result = result
		if !previous.Released {
			r.originalErr = persistentCloseError(err)
		}
	}
	if settled {
		r.err = err
		r.settled = true
	}
	close(r.run)
	r.run = nil
	r.mu.Unlock()
	return result, err
}

type failedOpenFile struct{ failedOpenReference }

func (f *failedOpenFile) ReadAt(context.Context, int64, int) (storage.FileRead, error) {
	return storage.FileRead{}, f.failure
}

func (f *failedOpenFile) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	return storage.Attr{}, f.failure
}

func (f *failedOpenFile) Truncate(context.Context, int64) (storage.Attr, error) {
	return storage.Attr{}, f.failure
}

func (f *failedOpenFile) Sync(context.Context) error { return f.failure }

var (
	_ storage.NodeReference           = (*nodeReference)(nil)
	_ storage.ScopedReference         = (*nodeReference)(nil)
	_ storage.ReferenceStateAccess    = (*nodeReference)(nil)
	_ storage.ReferenceMetadataAccess = (*nodeReference)(nil)
	_ storage.DeleteIntent            = (*nodeReference)(nil)
	_ storage.ConditionalFileMutation = (*nodeReference)(nil)
	_ storage.ReferenceStateAccess    = (*retainedFile)(nil)
	_ storage.DeleteIntent            = (*retainedFile)(nil)
	_ storage.ConditionalFileMutation = (*retainedFile)(nil)
	_ storage.NodeReference           = (*failedOpenReference)(nil)
	_ storage.File                    = (*failedOpenFile)(nil)
	_ storage.ReferenceIdentity       = (*failedOpenReference)(nil)
	_ storage.ReferenceIdentity       = (*failedOpenFile)(nil)
)
