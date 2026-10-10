package smb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

const maxWriteConditionRetries = 2

type writeOwner struct {
	mu               sync.Mutex
	server           *Server
	volume           string
	id               WriteOwnerID
	handle           *fileHandle
	file             storage.File
	actions          storage.FileActions
	mutation         storage.ConditionalFileMutation
	command          storage.FileMutation
	effects          []storage.ContentMetadataEffect
	sequence         uint64
	authorize        func(context.Context) error
	reobserve        func(context.Context) (map[string][]byte, error)
	epoch            func() uint64
	result           storage.Attr
	captured         storage.Attr
	firstErr         error
	lastErr          error
	conditionRetries int
	dispatched       bool
	settled          bool
	retired          bool
	responseFinal    bool
}

func (s *Server) reserveWriteOwner(t *tree, h *fileHandle, sequence uint64, command storage.FileMutation, effects []storage.ContentMetadataEffect) (*writeOwner, error) {
	if h == nil || h.file == nil || t == nil || t.authority == nil || command.Action.Check() != nil || (command.Kind != storage.MutateWriteAt && command.Kind != storage.MutateAppend) {
		return nil, syscall.EINVAL
	}
	if !command.Attr.Empty() || len(command.Metadata) != 0 || len(command.Uses) != 0 || command.ExpectedSize != nil {
		return nil, syscall.EINVAL
	}
	if err := storage.CheckContentMutation(command, effects); err != nil {
		return nil, err
	}
	mutation, ok := h.file.(storage.ConditionalFileMutation)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := mutation.CheckConditionalFileMutation(); err != nil {
		return nil, err
	}
	actions, ok := t.authority.raw.(storage.FileActions)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := actions.CheckFileActions(); err != nil {
		return nil, err
	}
	charged := len(command.Data)
	for name, token := range command.ExpectedMetadata {
		charged += len(name) + max(len(token), storage.MaxObservationTokenBytes)
	}
	for _, effect := range effects {
		charged += len(effect.Namespace) + len(effect.PresentPrefix) + len(effect.AbsentPayload) + len(effect.ClearMask) + len(effect.SetMask)
	}
	id, err := s.reserveIORecord(t, h, charged, false)
	if err != nil {
		return nil, err
	}
	o := &writeOwner{server: s, volume: t.export.share.Volume, id: id, handle: h, file: h.file, actions: actions, mutation: mutation, command: cloneWriteCommand(command), effects: storage.CloneContentMetadataEffects(effects), sequence: sequence}
	s.writes.update(id, func(r *writeRecord) {
		r.fact.Action = command.Action
		r.fact.Operation = storage.OpFileMutate
		r.fact.Bytes = uint32(len(command.Data))
		r.fact.Digest = sha256.Sum256(command.Data)
	})
	return o, nil
}

func cloneWriteCommand(command storage.FileMutation) storage.FileMutation {
	command.Data = bytes.Clone(command.Data)
	command.ContentEffects = append([]uint16(nil), command.ContentEffects...)
	command.ExpectedMetadata = storage.CloneInitialMetadata(command.ExpectedMetadata)
	command.Metadata = storage.CloneMetadata(command.Metadata)
	command.Uses = append([]storage.TargetUse(nil), command.Uses...)
	if command.ExpectedSize != nil {
		size := *command.ExpectedSize
		command.ExpectedSize = &size
	}
	return command
}

func (o *writeOwner) execute(ctx context.Context, authorize func(context.Context) error, reobserve func(context.Context) (map[string][]byte, error), epoch func() uint64) (storage.Attr, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.authorize, o.reobserve, o.epoch = authorize, reobserve, epoch
	if authorize == nil {
		return o.failLocked(syscall.EINVAL)
	}
	for {
		if err := authorize(ctx); err != nil {
			if !o.dispatched {
				o.rejectLocked(err)
			}
			return o.failLocked(err)
		}
		o.dispatched = true
		attr, err := o.mutation.MutateFile(ctx, cloneWriteCommand(o.command))
		o.captureResultLocked(attr)
		if err == nil {
			if err = o.acceptResultLocked(attr); err == nil {
				return o.result.Clone(), nil
			}
		}
		o.lastErr = err
		receipt, queryErr := o.queryLocked(ctx)
		if queryErr != nil {
			return o.failLocked(errors.Join(err, queryErr))
		}
		if receipt.Outcome == storage.FileActionNotExecuted && receipt.Operation == storage.OpFileMutate {
			if errors.Is(err, storage.ErrConditionConflict) && o.conditionRetries < maxWriteConditionRetries && reobserve != nil && epoch != nil {
				conditions, observeErr := reobserve(ctx)
				if observeErr != nil {
					o.rejectLocked(observeErr)
					return o.failLocked(observeErr)
				}
				if !o.validRetryConditionsLocked(conditions) {
					o.rejectLocked(syscall.EIO)
					return o.failLocked(syscall.EIO)
				}
				newAction, mintErr := storage.NewFileActionID(epoch())
				if mintErr != nil {
					o.rejectLocked(mintErr)
					return o.failLocked(mintErr)
				}
				o.command.Action = newAction
				o.command.ExpectedMetadata = storage.CloneInitialMetadata(conditions)
				o.conditionRetries++
				o.dispatched = false
				o.server.writes.update(o.id, func(r *writeRecord) { r.fact.Action = newAction })
				continue
			}
			o.rejectLocked(err)
			return o.failLocked(err)
		}
		if receipt.Outcome == storage.FileActionCompleted || receipt.Outcome == storage.FileActionPending {
			attr, replayErr := o.replayLocked(ctx)
			if replayErr == nil {
				return attr, nil
			}
			return o.failLocked(errors.Join(err, replayErr))
		}
		return o.failLocked(errors.Join(err, syscall.EIO))
	}
}

func (o *writeOwner) queryLocked(ctx context.Context) (storage.FileActionReceipt, error) {
	if o.authorize == nil {
		return storage.FileActionReceipt{}, syscall.EIO
	}
	if err := o.authorize(ctx); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if session := o.handle.ownerSession; session != nil {
		session.identityMu.RLock()
		principal := session.principal
		session.identityMu.RUnlock()
		ctx = WithPrincipal(ctx, principal)
	}
	if err := o.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: o.volume, Operation: storage.OpFileQueryAction}); err != nil {
		return storage.FileActionReceipt{}, err
	}
	receipt, err := o.actions.QueryFileAction(ctx, o.command.Action)
	if err != nil {
		return receipt, err
	}
	if receipt.Check() != nil || receipt.Action != o.command.Action || receipt.Operation != "" && receipt.Operation != storage.OpFileMutate {
		return storage.FileActionReceipt{}, syscall.EIO
	}
	return receipt, nil
}

func (o *writeOwner) validRetryConditionsLocked(conditions map[string][]byte) bool {
	if len(conditions) != len(o.command.ExpectedMetadata) {
		return false
	}
	for name := range o.command.ExpectedMetadata {
		token, present := conditions[name]
		if !present || len(token) > storage.MaxObservationTokenBytes {
			return false
		}
	}
	return true
}

func (o *writeOwner) replayLocked(ctx context.Context) (storage.Attr, error) {
	if o.authorize == nil {
		return storage.Attr{}, syscall.EIO
	}
	if err := o.authorize(ctx); err != nil {
		return storage.Attr{}, err
	}
	attr, err := o.mutation.MutateFile(ctx, cloneWriteCommand(o.command))
	o.captureResultLocked(attr)
	if err != nil {
		o.lastErr = err
		return storage.Attr{}, err
	}
	if err := o.acceptResultLocked(attr); err != nil {
		o.lastErr = err
		return storage.Attr{}, err
	}
	result := o.result.Clone()
	if o.responseFinal {
		o.result = storage.Attr{}
	}
	return result, nil
}

func (o *writeOwner) acceptResultLocked(attr storage.Attr) error {
	if attr.ID != o.handle.nodeID || attr.ID == 0 || attr.Kind != storage.NodeRegular || attr.Size < 0 || attr.CheckAllocation() != nil || !attr.AllocationKnown {
		return syscall.EIO
	}
	if len(o.command.Data) != 0 {
		if o.command.Kind == storage.MutateWriteAt && attr.Size < o.command.Offset+int64(len(o.command.Data)) || o.command.Kind == storage.MutateAppend && attr.Size < int64(len(o.command.Data)) {
			return syscall.EIO
		}
		metadata, err := decodeWindowsMetadata(attr.Metadata)
		if err != nil || metadata.Attributes&dosArchive == 0 {
			return errors.Join(err, syscall.EIO)
		}
	}
	o.result = attr.Clone()
	o.settled = true
	o.lastErr = nil
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.terminal = true
		r.fact.Execution = WriteExecutionCompleted
		r.fact.ChainSettled = true
	})
	o.releasePayloadLocked()
	return nil
}

func (o *writeOwner) captureResultLocked(attr storage.Attr) {
	if attr.ID == 0 || attr.ID != o.handle.nodeID || attr.Kind != storage.NodeRegular || attr.Size < 0 || attr.CheckAllocation() != nil {
		return
	}
	attr.Metadata = nil
	o.captured = attr.Clone()
}

func (o *writeOwner) rejectLocked(err error) {
	o.settled = true
	o.lastErr = err
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.terminal = true
		r.fact.Execution = WriteExecutionNotExecuted
		r.fact.ChainSettled = true
	})
	o.releasePayloadLocked()
}

func (o *writeOwner) releasePayloadLocked() {
	o.command.Data = nil
	o.command.ExpectedMetadata = nil
	o.command.ContentEffects = nil
	o.effects = nil
	o.server.writes.releasePayload(o.id)
}

func (o *writeOwner) failLocked(err error) (storage.Attr, error) {
	if err == nil {
		err = syscall.EIO
	}
	if !o.settled {
		err = errors.Join(syscall.EIO, err)
	}
	if o.firstErr == nil {
		o.firstErr = err
	}
	o.lastErr = err
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.failed = true
		if r.fact.ErrorCategory == WriteErrorNone {
			r.fact.ErrorCategory = writeErrorCategory(err)
		}
	})
	return storage.Attr{}, err
}

func (o *writeOwner) reconcile(ctx context.Context) (storage.Attr, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.settled {
		return o.result.Clone(), o.lastErr
	}
	if o.retired || !o.dispatched || o.authorize == nil {
		return storage.Attr{}, syscall.EIO
	}
	if err := o.authorize(ctx); err != nil {
		return o.failLocked(err)
	}
	receipt, err := o.queryLocked(ctx)
	if err != nil {
		return o.failLocked(err)
	}
	if receipt.Operation == storage.OpFileMutate && receipt.Outcome == storage.FileActionNotExecuted {
		failure := o.lastErr
		if failure == nil {
			failure = syscall.EIO
		}
		o.rejectLocked(failure)
		return storage.Attr{}, failure
	}
	if receipt.Outcome != storage.FileActionCompleted && receipt.Outcome != storage.FileActionPending {
		return o.failLocked(syscall.EIO)
	}
	attr, err := o.replayLocked(ctx)
	if err != nil {
		return o.failLocked(err)
	}
	return attr, nil
}

func (o *writeOwner) pending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.settled && !o.retired
}

func (o *writeOwner) retire(released, settled bool) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !released || !settled {
		return o.firstErr
	}
	o.retired = true
	if !o.settled && o.firstErr == nil {
		o.firstErr = syscall.EIO
	}
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.terminal = true
		r.fact.ReferenceReleased = true
		r.fact.ChainSettled = true
		if o.firstErr != nil {
			r.failed = true
			if r.fact.ErrorCategory == WriteErrorNone {
				r.fact.ErrorCategory = writeErrorCategory(o.firstErr)
			}
		}
	})
	o.releasePayloadLocked()
	o.server.writes.removeSuccess(o.id)
	o.result = storage.Attr{}
	o.captured = storage.Attr{}
	return o.firstErr
}

func (o *writeOwner) recordResponse(disposition ResponseDisposition) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if disposition != ResponseSent && disposition != ResponseSendFailed && disposition != ResponseNotSent {
		return
	}
	if o.responseFinal {
		return
	}
	o.responseFinal = true
	if disposition != ResponseSent && o.firstErr == nil {
		o.firstErr = syscall.EIO
	}
	o.server.writes.update(o.id, func(r *writeRecord) {
		if r.fact.ResponseFinal {
			return
		}
		r.fact.Response = disposition
		r.fact.ResponseFinal = true
		if disposition != ResponseSent {
			r.failed = true
			if r.fact.ErrorCategory == WriteErrorNone {
				r.fact.ErrorCategory = WriteErrorIO
			}
		}
	})
	o.server.writes.removeSuccess(o.id)
	if o.settled {
		o.result = storage.Attr{}
	}
}

// These helpers run under the drained handle's close lifetime. Neither takes
// the FIFO lock or calls a close operation while holding an I/O owner lock.
func settlePendingFileIO(ctx context.Context, h *fileHandle) error {
	var errs []error
	if h.pendingWrite != nil && h.pendingWrite.pending() {
		if _, err := h.pendingWrite.reconcile(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if h.pendingFlush != nil && h.pendingFlush.pending() {
		if err := h.pendingFlush.reconcile(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func retirePendingFileIO(h *fileHandle, released, settled bool) error {
	var errs []error
	if h.pendingWrite != nil {
		if err := h.pendingWrite.retire(released, settled); err != nil {
			errs = append(errs, err)
		}
	}
	if h.pendingFlush != nil {
		if err := h.pendingFlush.retire(released, settled); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
