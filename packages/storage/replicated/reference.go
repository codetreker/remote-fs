package replicated

import (
	"context"
	"sync"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

var (
	_ storage.NodeReference           = (*nodeReference)(nil)
	_ storage.ScopedReference         = (*nodeReference)(nil)
	_ storage.ReferenceStateAccess    = (*nodeReference)(nil)
	_ storage.ReferenceMetadataAccess = (*nodeReference)(nil)
	_ storage.DeleteIntent            = (*nodeReference)(nil)
	_ storage.ScopedReference         = (*retainedFile)(nil)
	_ storage.ReferenceStateAccess    = (*retainedFile)(nil)
	_ storage.ReferenceMetadataAccess = (*retainedFile)(nil)
	_ storage.DeleteIntent            = (*retainedFile)(nil)
	_ storage.ConditionalFileMutation = (*retainedFile)(nil)
)

type nodeReference struct {
	session *fileSession
	remote  httprest.NodeReferenceWithBarrier
	mu      sync.Mutex
	closed  bool
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
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	r.session.mu.Lock()
	closed = closed || r.session.closed
	r.session.mu.Unlock()
	if closed {
		return nil
	}
	_, err := fileCall(ctx, r.session, false, func(ctx context.Context) (struct{}, error) {
		barrier, err := r.remote.CloseWithBarrier(ctx)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, r.session.confirmCleanup(ctx, "close-reference", barrier)
	})
	if err == nil {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
	}
	return err
}

func referenceCapability[C, R any](ctx context.Context, session *fileSession, remote any, call func(context.Context, C) (R, error)) (R, error) {
	return fileCall(ctx, session, true, func(ctx context.Context) (R, error) {
		capability, err := optional[C](remote)
		if err != nil {
			var zero R
			return zero, err
		}
		return call(ctx, capability)
	})
}

func (r *retainedFile) CheckScopedReference() error {
	return capabilityCheck(r.remote, func(c storage.ScopedReference) error { return c.CheckScopedReference() })
}
func (r *retainedFile) Scope(ctx context.Context) (storage.UseScope, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c storage.ScopedReference) (storage.UseScope, error) { return c.Scope(ctx) })
}
func (r *retainedFile) CheckReferenceState() error {
	return capabilityCheck(r.remote, func(c storage.ReferenceStateAccess) error { return c.CheckReferenceState() })
}
func (r *retainedFile) State(ctx context.Context) (storage.ReferenceState, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c storage.ReferenceStateAccess) (storage.ReferenceState, error) {
		return c.State(ctx)
	})
}
func (r *retainedFile) CheckMetadataAccess() error {
	return capabilityCheck(r.remote, func(c httprest.ReferenceMetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}
func (r *retainedFile) SetMetadata(ctx context.Context, namespace string, version, payload []byte) (storage.OpaquePayload, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c httprest.ReferenceMetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		return capabilityMutation(ctx, r.session, "set-reference-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return c.SetMetadataWithBarrier(ctx, namespace, version, payload)
		})
	})
}
func (r *retainedFile) CheckDeleteIntent() error {
	return capabilityCheck(r.remote, func(c httprest.DeleteIntentWithBarrier) error { return c.CheckDeleteIntent() })
}
func (r *retainedFile) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		return capabilityMutation(ctx, r.session, "set-pending-unlink", func(ctx context.Context) (storage.ReferenceState, *httprest.MutationBarrier, error) {
			return c.SetPendingUnlinkWithBarrier(ctx, command)
		})
	})
}
func (r *retainedFile) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		return capabilityMutation(ctx, r.session, "clear-pending-unlink", func(ctx context.Context) (storage.ReferenceState, *httprest.MutationBarrier, error) {
			return c.ClearPendingUnlinkWithBarrier(ctx, command)
		})
	})
}

func (r *nodeReference) CheckScopedReference() error {
	return capabilityCheck(r.remote, func(c storage.ScopedReference) error { return c.CheckScopedReference() })
}
func (r *nodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c storage.ScopedReference) (storage.UseScope, error) { return c.Scope(ctx) })
}
func (r *nodeReference) CheckReferenceState() error {
	return capabilityCheck(r.remote, func(c storage.ReferenceStateAccess) error { return c.CheckReferenceState() })
}
func (r *nodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c storage.ReferenceStateAccess) (storage.ReferenceState, error) {
		return c.State(ctx)
	})
}
func (r *nodeReference) CheckMetadataAccess() error {
	return capabilityCheck(r.remote, func(c httprest.ReferenceMetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}
func (r *nodeReference) SetMetadata(ctx context.Context, namespace string, version, payload []byte) (storage.OpaquePayload, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c httprest.ReferenceMetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		return capabilityMutation(ctx, r.session, "set-reference-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return c.SetMetadataWithBarrier(ctx, namespace, version, payload)
		})
	})
}
func (r *nodeReference) CheckDeleteIntent() error {
	return capabilityCheck(r.remote, func(c httprest.DeleteIntentWithBarrier) error { return c.CheckDeleteIntent() })
}
func (r *nodeReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		return capabilityMutation(ctx, r.session, "set-pending-unlink", func(ctx context.Context) (storage.ReferenceState, *httprest.MutationBarrier, error) {
			return c.SetPendingUnlinkWithBarrier(ctx, command)
		})
	})
}
func (r *nodeReference) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return referenceCapability(ctx, r.session, r.remote, func(ctx context.Context, c httprest.DeleteIntentWithBarrier) (storage.ReferenceState, error) {
		return capabilityMutation(ctx, r.session, "clear-pending-unlink", func(ctx context.Context) (storage.ReferenceState, *httprest.MutationBarrier, error) {
			return c.ClearPendingUnlinkWithBarrier(ctx, command)
		})
	})
}

func (f *retainedFile) CheckConditionalFileMutation() error {
	return capabilityCheck(f.remote, func(c httprest.ConditionalFileMutationWithBarrier) error { return c.CheckConditionalFileMutation() })
}
func (f *retainedFile) MutateFile(ctx context.Context, mutation storage.FileMutation) (storage.Attr, error) {
	return referenceCapability(ctx, f.session, f.remote, func(ctx context.Context, c httprest.ConditionalFileMutationWithBarrier) (storage.Attr, error) {
		return capabilityMutation(ctx, f.session, "mutate-file", func(ctx context.Context) (storage.Attr, *httprest.MutationBarrier, error) {
			return c.MutateFileWithBarrier(ctx, mutation)
		})
	})
}

// A failed capability negotiation can still transfer a reference whose cleanup
// failed. Only Close remains usable until that retained resource is released.
type failedOpenReference struct {
	native  storage.NodeReference
	failure error
}

func (r *failedOpenReference) Stat(context.Context) (storage.Attr, error) {
	return storage.Attr{}, r.failure
}
func (r *failedOpenReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, r.failure
}
func (r *failedOpenReference) Close(ctx context.Context) error { return r.native.Close(ctx) }

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
