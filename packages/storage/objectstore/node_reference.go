package objectstore

import (
	"bytes"
	"context"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type nodeReference struct {
	session    *fileSession
	native     metastore.NodeReference
	uses       referenceUses
	active     bool
	operations sync.WaitGroup
	retireMu   sync.Mutex
	retired    bool
	closeMu    sync.Mutex
	closeDone  chan struct{}
	closeErr   error
	closeFinal bool
}

func (r *nodeReference) begin(ctx context.Context) (context.Context, func(), error) {
	return r.admit(ctx, fileDataOperation)
}

func (r *nodeReference) admit(ctx context.Context, class fileOperationClass) (context.Context, func(), error) {
	done, err := r.session.admit(ctx, false, class)
	if err != nil {
		return nil, nil, err
	}
	r.session.mu.Lock()
	if !r.active {
		r.session.mu.Unlock()
		done()
		return nil, nil, syscall.EBADF
	}
	r.operations.Add(1)
	r.session.mu.Unlock()
	operation, cancel := r.session.operationContext(ctx)
	operation = metastore.WithFilePublicationGuard(operation, r.session.publicationAllowed)
	return operation, func() { cancel(); r.operations.Done(); done() }, nil
}

func (r *nodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	state, err := r.native.Node(ctx)
	return state.Attr(), err
}

func (r *nodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	if err := change.Check(); err != nil {
		return storage.Attr{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	state, err := r.native.SetAttr(ctx, change)
	return state.Attr(), err
}

func (r *nodeReference) retire() error {
	r.session.mu.Lock()
	r.active = false
	r.uses.retiring = true
	r.session.mu.Unlock()
	r.retireMu.Lock()
	defer r.retireMu.Unlock()
	if r.retired {
		return nil
	}
	ctx, cancel := r.session.operationContext(r.session.cleanup)
	defer cancel()
	if err := r.native.Retire(ctx); err != nil {
		return err
	}
	r.retired = true
	return nil
}

func (r *nodeReference) drainAndRelease() error {
	r.operations.Wait()
	ctx, cancel := r.session.operationContext(r.session.cleanup)
	defer cancel()
	if err := r.native.DropUse(ctx); err != nil {
		return err
	}
	return r.session.retireReferenceOwners(&r.uses)
}

func (r *nodeReference) startClose() <-chan struct{} {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closeDone != nil {
		select {
		case <-r.closeDone:
			if r.closeFinal {
				return r.closeDone
			}
		default:
			return r.closeDone
		}
	}
	r.closeDone = make(chan struct{})
	go r.finishClose()
	return r.closeDone
}

func (r *nodeReference) finishClose() {
	err := r.retire()
	if err == nil {
		err = r.drainAndRelease()
	}
	if err == nil {
		ctx, cancel := r.session.operationContext(r.session.cleanup)
		err = r.native.Close(ctx)
		cancel()
	}
	final := err == nil || storage.ErrnoOf(err) == syscall.ENOTEMPTY
	if final {
		r.session.mu.Lock()
		delete(r.session.files, r)
		r.session.mu.Unlock()
		r.session.storage.sweepAfterMutation()
	}
	r.closeMu.Lock()
	r.closeErr = err
	r.closeFinal = final
	close(r.closeDone)
	r.closeMu.Unlock()
}

func (r *nodeReference) Close(ctx context.Context) error {
	done := r.startClose()
	select {
	case <-done:
		r.closeMu.Lock()
		defer r.closeMu.Unlock()
		return r.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *nodeReference) CheckScopedReference() error {
	capability, ok := r.native.(storage.ScopedReference)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckScopedReference()
}

func (r *nodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	if err := r.CheckScopedReference(); err != nil {
		return storage.UseScope{}, err
	}
	ctx, done, err := r.admit(ctx, fileAdvisoryOperation)
	if err != nil {
		return storage.UseScope{}, err
	}
	defer done()
	scope, err := r.native.(storage.ScopedReference).Scope(ctx)
	if err != nil {
		return storage.UseScope{}, err
	}
	if err := scope.Check(); err != nil {
		return storage.UseScope{}, err
	}
	r.session.mu.Lock()
	r.uses.scope = scope
	r.session.mu.Unlock()
	return scope, nil
}

func (r *nodeReference) CheckMetadataAccess() error {
	capability, ok := r.native.(storage.ReferenceMetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckMetadataAccess()
}

func (r *nodeReference) SetMetadata(ctx context.Context, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := r.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	defer done()
	return r.native.(storage.ReferenceMetadataAccess).SetMetadata(ctx, namespace, expected, data)
}

func (r *nodeReference) CheckReferenceState() error {
	capability, ok := r.native.(metastore.ReferenceStateAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckReferenceState()
}

func publicReferenceState(state metastore.ReferenceState) storage.ReferenceState {
	return storage.ReferenceState{
		LinkTarget:        bytes.Clone(state.LinkTarget),
		Attr:              state.State.Attr(),
		Detached:          state.State.Detached,
		PendingUnlink:     state.PendingUnlink,
		PendingGeneration: bytes.Clone(state.PendingGeneration),
	}
}

func (r *nodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	if err := r.CheckReferenceState(); err != nil {
		return storage.ReferenceState{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := r.native.(metastore.ReferenceStateAccess).State(ctx)
	return publicReferenceState(state), err
}

func (r *nodeReference) CheckDeleteIntent() error {
	capability, ok := r.native.(metastore.DeleteIntent)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckDeleteIntent()
}

func (r *nodeReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := r.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	return runFileAction(ctx, r.session, command.Action, storage.OpFileSetPendingUnlink, command, cloneReferenceState,
		func(result storage.ReferenceState) bool { return result.Attr.ID != 0 },
		func() (storage.ReferenceState, error) { return r.setPendingUnlink(ctx, command) })
}

func (r *nodeReference) setPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := r.native.(metastore.DeleteIntent).SetPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}

func (r *nodeReference) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := r.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	return runFileAction(ctx, r.session, command.Action, storage.OpFileClearPendingUnlink, command, cloneReferenceState,
		func(result storage.ReferenceState) bool { return result.Attr.ID != 0 },
		func() (storage.ReferenceState, error) { return r.clearPendingUnlink(ctx, command) })
}

func (r *nodeReference) clearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := r.native.(metastore.DeleteIntent).ClearPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}

var (
	_ storage.NodeReference           = (*nodeReference)(nil)
	_ storage.ScopedReference         = (*nodeReference)(nil)
	_ storage.ReferenceMetadataAccess = (*nodeReference)(nil)
	_ storage.ReferenceStateAccess    = (*nodeReference)(nil)
	_ storage.DeleteIntent            = (*nodeReference)(nil)
)
