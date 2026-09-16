package objectstore

import (
	"bytes"
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceUses struct {
	retiring bool
	nodeID   uint64
	scope    storage.UseScope
	owners   map[storage.UseOwner]struct{}
}

func checkReferenceState(native metastore.NodeReference) error {
	capability, ok := native.(metastore.ReferenceStateAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckReferenceState()
}

func publicReferenceState(state metastore.ReferenceState) storage.ReferenceState {
	return storage.ReferenceState{Attr: state.State.Attr(), Detached: state.State.Detached,
		PendingUnlink: state.PendingUnlink, PendingGeneration: bytes.Clone(state.PendingGeneration),
		LinkTarget: bytes.Clone(state.State.LinkTarget)}
}

func (f *openFile) CheckReferenceState() error      { return checkReferenceState(f.native) }
func (r *nodeReference) CheckReferenceState() error { return checkReferenceState(r.native) }

func (f *openFile) State(ctx context.Context) (storage.ReferenceState, error) {
	if err := f.CheckReferenceState(); err != nil {
		return storage.ReferenceState{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := f.native.(metastore.ReferenceStateAccess).State(ctx)
	return publicReferenceState(state), err
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

func checkScopedReference(native metastore.NodeReference) error {
	capability, ok := native.(storage.ScopedReference)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckScopedReference()
}

func (f *openFile) CheckScopedReference() error      { return checkScopedReference(f.native) }
func (r *nodeReference) CheckScopedReference() error { return checkScopedReference(r.native) }

func (f *openFile) Scope(ctx context.Context) (storage.UseScope, error) {
	if err := f.CheckScopedReference(); err != nil {
		return storage.UseScope{}, err
	}
	ctx, done, err := f.admit(ctx, fileAdvisoryOperation)
	if err != nil {
		return storage.UseScope{}, err
	}
	defer done()
	scope, err := f.native.(storage.ScopedReference).Scope(ctx)
	if err == nil {
		f.session.mu.Lock()
		nodeID := f.uses.nodeID
		f.session.mu.Unlock()
		if nodeID == 0 {
			state, stateErr := f.native.Node(ctx)
			if stateErr != nil {
				return storage.UseScope{}, stateErr
			}
			nodeID = uint64(state.ID)
		}
		f.session.mu.Lock()
		f.uses.scope = scope
		f.uses.nodeID = nodeID
		f.session.mu.Unlock()
	}
	return scope, err
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
	if err == nil {
		r.session.mu.Lock()
		r.uses.scope = scope
		r.session.mu.Unlock()
	}
	return scope, err
}

func checkReferenceMetadata(native metastore.NodeReference) error {
	capability, ok := native.(storage.ReferenceMetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckMetadataAccess()
}

func (f *openFile) CheckMetadataAccess() error      { return checkReferenceMetadata(f.native) }
func (r *nodeReference) CheckMetadataAccess() error { return checkReferenceMetadata(r.native) }

func (f *openFile) SetMetadata(ctx context.Context, namespace string, expected, data []byte) (storage.OpaquePayload, error) {
	if err := f.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.OpaquePayload{}, err
	}
	defer done()
	return f.native.(storage.ReferenceMetadataAccess).SetMetadata(ctx, namespace, expected, data)
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

func checkDeleteIntent(native metastore.NodeReference) error {
	capability, ok := native.(metastore.DeleteIntent)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckDeleteIntent()
}

func (f *openFile) CheckDeleteIntent() error      { return checkDeleteIntent(f.native) }
func (r *nodeReference) CheckDeleteIntent() error { return checkDeleteIntent(r.native) }

func (f *openFile) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := f.native.(metastore.DeleteIntent).SetPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}

func (f *openFile) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
	ctx, done, err := f.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := f.native.(metastore.DeleteIntent).ClearPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}

func (r *nodeReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	if err := r.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, err
	}
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
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	state, err := r.native.(metastore.DeleteIntent).ClearPendingUnlink(ctx, command)
	return publicReferenceState(state), err
}
