package objectstore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type nodeReference struct {
	metadata   storage.MetadataPermissions
	session    *fileSession
	native     metastore.NodeReference
	uses       referenceUses
	active     bool
	operations sync.WaitGroup
	retireMu   sync.Mutex
	retired    bool
	closing    referenceCloseState
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

func (r *nodeReference) drainAndRetireOwners() error {
	r.operations.Wait()
	return r.session.retireReferenceOwners(&r.uses)
}

func (r *nodeReference) closeState() *referenceCloseState { return &r.closing }

func (r *nodeReference) performClose() (storage.ReferenceCloseResult, error) {
	err := r.retire()
	var result storage.ReferenceCloseResult
	if err == nil {
		err = r.drainAndRetireOwners()
		if err != nil {
			result.Determined = true
		}
	}
	if err == nil {
		ctx, cancel := r.session.operationContext(r.session.cleanup)
		result, err = r.native.CloseWithResult(ctx)
		cancel()
		result.Determined = result.Determined || result.Released
		err = errors.Join(err, result.Check(err))
	}
	if result.Released {
		r.session.mu.Lock()
		delete(r.session.files, r)
		r.session.mu.Unlock()
		r.session.storage.sweepAfterMutation()
	}
	return result, err
}

func (r *nodeReference) Close(ctx context.Context) error {
	_, err := r.CloseWithResult(ctx)
	return err
}

func (r *nodeReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	return r.closing.runImplicit(ctx, r.session, r.performClose)
}

func (r *nodeReference) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	return r.closing.run(ctx, r.session, attempt, r.performClose)
}

func (r *nodeReference) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	return r.closing.query(ctx, r.session, attempt)
}

func (r *nodeReference) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	return r.closing.status(ctx, r.session)
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
	target, err := r.session.referenceActionTarget(ctx, r)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	input := pendingUnlinkActionInput{Target: target, Command: command}
	return runFileAction(ctx, r.session, command.Action, storage.OpFileSetPendingUnlink, input, cloneReferenceState,
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
	target, err := r.session.referenceActionTarget(ctx, r)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	input := clearPendingUnlinkActionInput{Target: target, Command: command}
	return runFileAction(ctx, r.session, command.Action, storage.OpFileClearPendingUnlink, input, cloneReferenceState,
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

func (r *nodeReference) CheckConditionalFileMutation() error {
	capability, ok := r.native.(metastore.ConditionalFileMutation)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return capability.CheckConditionalFileMutation()
}

func (r *nodeReference) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	if err := r.CheckConditionalFileMutation(); err != nil {
		return storage.Attr{}, err
	}
	if err := command.CheckDataLimit(r.session.options.MaxFileSize); err != nil {
		return storage.Attr{}, err
	}
	target, err := r.session.referenceActionTarget(ctx, r)
	if err != nil {
		return storage.Attr{}, err
	}
	input := fileMutationActionInput{Target: target, Command: command}
	return runFileAction(ctx, r.session, command.Action, storage.OpFileMutate, input,
		func(attr storage.Attr) storage.Attr { return attr.Clone() },
		func(attr storage.Attr) bool { return attr.ID != 0 },
		func() (storage.Attr, error) { return r.mutateFile(ctx, command) })
}

func (r *nodeReference) mutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	if command.Kind != storage.MutateAttributes {
		return storage.Attr{}, syscall.EBADF
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.Attr{}, err
	}
	defer done()
	state, err := r.native.(metastore.ConditionalFileMutation).MutateFile(ctx, command)
	return state.Attr(), err
}

var (
	_ storage.NodeReference           = (*nodeReference)(nil)
	_ storage.ScopedReference         = (*nodeReference)(nil)
	_ storage.ReferenceMetadataAccess = (*nodeReference)(nil)
	_ storage.ReferenceStateAccess    = (*nodeReference)(nil)
	_ storage.DeleteIntent            = (*nodeReference)(nil)
	_ storage.ConditionalFileMutation = (*nodeReference)(nil)
)

func (r *nodeReference) State(ctx context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	if err := projection.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	if projection == storage.StateFull && r.metadata&storage.ReadMetadata == 0 {
		return storage.ReferenceState{}, syscall.EBADF
	}
	if err := r.CheckReferenceState(); err != nil {
		return storage.ReferenceState{}, err
	}
	id, err := storage.ReferenceNodeID(r.native)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	defer done()
	return observeNativeState(ctx, r.session, r.native, r.native, id, projection, &r.active)
}

func (r *nodeReference) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	if err := request.Check(); err != nil {
		return storage.MetadataObservation{}, err
	}
	if request.Purpose != storage.MetadataObservationNamespace || r.metadata&storage.WriteMetadata == 0 {
		return storage.MetadataObservation{}, syscall.EBADF
	}
	if err := r.CheckMetadataAccess(); err != nil {
		return storage.MetadataObservation{}, err
	}
	id, err := storage.ReferenceNodeID(r.native)
	if err != nil {
		return storage.MetadataObservation{}, err
	}
	ctx, done, err := r.begin(ctx)
	if err != nil {
		return storage.MetadataObservation{}, err
	}
	defer done()
	return observeNativeMetadata(ctx, r.session, r.native, id, request, nil, &r.active)
}

func observeNativeState(ctx context.Context, session *fileSession, capability metastore.ReferenceStateAccess, reference any, id uint64, projection storage.StateProjection, active *bool) (storage.ReferenceState, error) {
	ctx = metastore.WithReferenceSession(ctx, session.locks)
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return storage.ReferenceState{}, err
	}
	state, err := capability.State(ctx, projection)
	if err != nil {
		return storage.ReferenceState{}, err
	}
	var result storage.ReferenceState
	if projection == storage.StateFull {
		result = publicReferenceState(state)
	} else {
		if state.State.Metadata != nil || state.State.BirthTime != nil || state.State.ChangeTime != nil ||
			!state.State.AccessTime.IsZero() || !state.State.ModTime.IsZero() || state.State.LinkTarget != nil ||
			state.State.DirectoryRevision != nil || state.State.Content != "" || state.State.Revision != 0 ||
			state.LinkTarget != nil || state.PendingGeneration != nil {
			return storage.ReferenceState{}, syscall.EIO
		}
		result = storage.ReferenceState{Attr: storage.Attr{ID: uint64(state.State.ID), Kind: state.State.Kind, Size: state.State.Size, AllocationSize: state.State.AllocationSize, AllocationKnown: state.State.AllocationKnown}, Detached: state.State.Detached, PendingUnlink: state.PendingUnlink}
	}
	if err := result.CheckProjection(id, projection); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := checkObservedReference(ctx, session, reference, id, active); err != nil {
		return storage.ReferenceState{}, err
	}
	return result, nil
}

func observeNativeMetadata(ctx context.Context, session *fileSession, capability storage.ReferenceMetadataAccess, id uint64, request storage.MetadataObservationRequest, effect *storage.ContentMetadataEffect, active *bool) (storage.MetadataObservation, error) {
	ctx = metastore.WithReferenceSession(ctx, session.locks)
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return storage.MetadataObservation{}, err
	}
	result, err := capability.ObserveMetadata(ctx, request.Clone())
	if err != nil {
		return storage.MetadataObservation{}, err
	}
	if request.Purpose == storage.MetadataObservationNamespace {
		err = result.CheckNamespace(id, request.Namespace, request.MaxPayloadBytes)
	} else {
		err = result.CheckEffect(id, *effect)
	}
	if err != nil {
		return storage.MetadataObservation{}, err
	}
	if err := checkObservedReference(ctx, session, capability, id, active); err != nil {
		return storage.MetadataObservation{}, err
	}
	return result.Clone(), nil
}

func checkObservedReference(ctx context.Context, session *fileSession, reference any, expected uint64, active *bool) error {
	id, err := storage.ReferenceNodeID(reference)
	if err != nil {
		return err
	}
	if id != expected {
		return syscall.EIO
	}
	if err := session.locks.IOHealth(ctx, id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := session.publicationAllowed(); err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if !*active {
		return syscall.EBADF
	}
	return nil
}
