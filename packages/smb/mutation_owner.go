package smb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

const maxMutationConditionRetries = 2

type mutationOwner struct {
	mu               sync.Mutex
	server           *Server
	volume           string
	id               WriteOwnerID
	handle           *fileHandle
	nodeKind         storage.NodeKind
	actions          storage.FileActions
	mutation         storage.ConditionalFileMutation
	command          storage.FileMutation
	effects          []storage.ContentMetadataEffect
	sequence         uint64
	resultByteLimit  int64
	authorize        func(context.Context) error
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

func (s *Server) reserveMutationOwner(t *tree, h *fileHandle, sequence uint64, command storage.FileMutation, effects []storage.ContentMetadataEffect) (*mutationOwner, error) {
	if h == nil {
		return nil, syscall.EINVAL
	}
	kind := h.nodeKind
	if h == nil || h.reference() == nil || t == nil || t.authority == nil || t.export == nil ||
		(kind != storage.NodeRegular && kind != storage.NodeDirectory) || kind != storage.NodeRegular && command.Kind != storage.MutateAttributes {
		return nil, syscall.EINVAL
	}
	if len(command.Uses) != 0 || command.Kind != storage.MutateAttributes && (!command.Attr.Empty() || len(command.Metadata) != 0) {
		return nil, syscall.EINVAL
	}
	if err := storage.CheckContentMutation(command, effects); err != nil {
		return nil, err
	}
	if command.Kind == storage.MutateAttributes {
		if len(command.ExpectedMetadata) != 0 || len(command.Metadata) > 1 {
			return nil, syscall.EINVAL
		}
		if len(command.Metadata) != 0 {
			if _, present := command.Metadata[windowsMetadataKey]; !present {
				return nil, syscall.EINVAL
			}
		}
	}

	mutation, ok := h.reference().(storage.ConditionalFileMutation)
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

	for name := range command.ExpectedMetadata {
		charged += len(name) + storage.MaxObservationTokenBytes
	}
	for name, value := range command.Metadata {
		charged += len(name) + len(value.Data) + storage.MaxObservationTokenBytes
	}
	for _, instant := range []*time.Time{command.Attr.BirthTime, command.Attr.AccessTime, command.Attr.ModTime} {
		if instant != nil {
			charged += 24
		}
	}
	if command.ExpectedSize != nil {
		charged += 8
	}
	charged += 2 * len(command.ContentEffects)
	for _, effect := range effects {
		charged += len(effect.Namespace) + len(effect.PresentPrefix) + len(effect.AbsentPayload) + len(effect.ClearMask) + len(effect.SetMask)
	}
	id, err := s.reserveIORecord(t, h, charged, false)
	if err != nil {
		return nil, err
	}
	o := &mutationOwner{server: s, volume: t.export.share.Volume, id: id, handle: h, nodeKind: kind, actions: actions, mutation: mutation, command: cloneMutationCommand(command), effects: storage.CloneContentMetadataEffects(effects), sequence: sequence}

	s.writes.update(id, func(r *writeRecord) {
		r.fact.Action = command.Action
		r.fact.Operation = storage.OpFileMutate
		r.fact.Bytes = uint32(len(command.Data))
		r.fact.Digest = sha256.Sum256(command.Data)
	})
	return o, nil
}

func cloneMutationCommand(command storage.FileMutation) storage.FileMutation {
	command.Data = bytes.Clone(command.Data)
	command.ContentEffects = append([]uint16(nil), command.ContentEffects...)
	command.ExpectedMetadata = storage.CloneInitialMetadata(command.ExpectedMetadata)
	command.Metadata = storage.CloneMetadata(command.Metadata)
	cloneTime := func(value *time.Time) *time.Time {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	command.Attr.BirthTime = cloneTime(command.Attr.BirthTime)
	command.Attr.AccessTime = cloneTime(command.Attr.AccessTime)
	command.Attr.ModTime = cloneTime(command.Attr.ModTime)
	command.Uses = append([]storage.TargetUse(nil), command.Uses...)
	if command.ExpectedSize != nil {
		size := *command.ExpectedSize
		command.ExpectedSize = &size
	}
	return command
}

func (o *mutationOwner) executeMutation(ctx context.Context, authorize func(context.Context) error, reobserve func(context.Context) (windowsMetadataCondition, error), epoch func() uint64) (storage.Attr, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.authorize = authorize
	if limit, bounded := storage.AttrResultByteLimit(ctx); bounded {
		o.resultByteLimit = limit
	}
	ctx = o.resultContext(ctx)
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
		attr, err := o.mutation.MutateFile(ctx, cloneMutationCommand(o.command))
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
			if errors.Is(err, storage.ErrConditionConflict) && o.conditionRetries < maxMutationConditionRetries && reobserve != nil && epoch != nil {
				condition, observeErr := reobserve(ctx)
				if observeErr != nil {
					o.rejectLocked(observeErr)
					return o.failLocked(observeErr)
				}
				retryCommand, retryErr := applyWindowsMetadataCondition(cloneMutationCommand(o.command), condition, o.handle.nodeID == o.handle.identity.Backend.RootNodeID)
				if retryErr != nil {
					o.rejectLocked(retryErr)
					return o.failLocked(retryErr)
				}
				newAction, mintErr := storage.NewFileActionID(epoch())
				if mintErr != nil {
					o.rejectLocked(mintErr)
					return o.failLocked(mintErr)
				}
				o.command = retryCommand
				o.command.Action = newAction
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

func (o *mutationOwner) queryLocked(ctx context.Context) (storage.FileActionReceipt, error) {
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

func (o *mutationOwner) resultContext(ctx context.Context) context.Context {
	if o.resultByteLimit == 0 {
		return ctx
	}
	return storage.WithBoundedAttrResult(ctx, o.resultByteLimit, func(attr storage.Attr, metadataBytes int64) error {
		if err := o.checkResultIdentity(attr); err != nil {
			return err
		}
		if o.command.Kind != storage.MutateAttributes {
			if err := checkCreateAllocation(attr); err != nil {
				return err
			}
			for _, instant := range []*time.Time{attr.BirthTime, &attr.AccessTime, &attr.ModTime, attr.ChangeTime} {
				if instant != nil {
					if _, err := toFiletime(*instant); err != nil {
						return err
					}
				}
			}
		}
		retained, err := storage.MetadataRetentionBytes(metadataBytes)
		if err != nil {
			return err
		}
		if retained+512 > o.resultByteLimit {
			return syscall.EFBIG
		}
		return nil
	})
}

func (o *mutationOwner) replayLocked(ctx context.Context) (storage.Attr, error) {
	ctx = o.resultContext(ctx)
	if o.authorize == nil {
		return storage.Attr{}, syscall.EIO
	}
	if err := o.authorize(ctx); err != nil {
		return storage.Attr{}, err
	}
	attr, err := o.mutation.MutateFile(ctx, cloneMutationCommand(o.command))
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

func (o *mutationOwner) checkResultIdentity(attr storage.Attr) error {
	if attr.ID == 0 || attr.ID != o.handle.nodeID || attr.Kind != o.nodeKind || attr.CheckAllocation() != nil ||
		attr.Kind == storage.NodeRegular && attr.Size < 0 {
		return syscall.EIO
	}
	return nil
}

func (o *mutationOwner) acceptResultLocked(attr storage.Attr) error {
	if o.checkResultIdentity(attr) != nil {
		return syscall.EIO
	}
	switch o.command.Kind {
	case storage.MutateWriteAt, storage.MutateAppend, storage.MutateTruncate:
		if !attr.AllocationKnown {
			return syscall.EIO
		}
		if o.command.Kind == storage.MutateTruncate && attr.Size != o.command.Size ||
			o.command.Kind == storage.MutateWriteAt && len(o.command.Data) != 0 && attr.Size < o.command.Offset+int64(len(o.command.Data)) ||
			o.command.Kind == storage.MutateAppend && attr.Size < int64(len(o.command.Data)) {
			return syscall.EIO
		}
		if len(o.command.ContentEffects) != 0 {
			metadata, err := decodeWindowsMetadata(attr.Metadata)
			if err != nil || metadata.Attributes&dosArchive == 0 {
				return errors.Join(err, syscall.EIO)
			}
		}
	case storage.MutateAttributes:
		if storage.CheckMetadata(attr.Metadata) != nil {
			return syscall.EIO
		}
		for name, update := range o.command.Metadata {
			value, present := attr.Metadata[name]
			if !present || !bytes.Equal(value.Data, update.Data) {
				return syscall.EIO
			}
		}
		change := o.command.Attr
		if change.BirthTime != nil && (attr.BirthTime == nil || !attr.BirthTime.Equal(*change.BirthTime)) ||
			change.AccessTime != nil && !attr.AccessTime.Equal(*change.AccessTime) ||
			change.ModTime != nil && !attr.ModTime.Equal(*change.ModTime) {
			return syscall.EIO
		}
	default:
		return syscall.EIO
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

func (o *mutationOwner) captureResultLocked(attr storage.Attr) {
	if o.checkResultIdentity(attr) != nil {
		return
	}
	attr.Metadata = nil
	o.captured = attr.Clone()
}

func (o *mutationOwner) rejectLocked(err error) {
	o.settled = true
	o.lastErr = err
	o.server.writes.update(o.id, func(r *writeRecord) {
		r.terminal = true
		r.fact.Execution = WriteExecutionNotExecuted
		r.fact.ChainSettled = true
	})
	o.releasePayloadLocked()
}

func (o *mutationOwner) releasePayloadLocked() {
	o.command.Data = nil
	o.command.ExpectedMetadata = nil
	o.command.ContentEffects = nil
	o.command.Attr = storage.AttrChange{}
	o.command.Metadata = nil
	o.command.ExpectedSize = nil
	o.command.Uses = nil
	o.effects = nil
	o.authorize = nil
	o.server.writes.releasePayload(o.id)
}

func (o *mutationOwner) failLocked(err error) (storage.Attr, error) {
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

func (o *mutationOwner) reconcile(ctx context.Context) (storage.Attr, error) {
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

func (o *mutationOwner) pending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.settled && !o.retired
}

func (o *mutationOwner) retire(released, settled bool) error {
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

func (o *mutationOwner) recordResponse(disposition ResponseDisposition) {
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
