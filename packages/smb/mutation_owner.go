package smb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

const maxMutationConditionRetries = 2

type mutationCommand struct {
	file    *storage.FileMutation
	name    *storage.NameCommand
	pending *storage.PendingUnlinkCommand
	clear   *storage.ClearPendingUnlinkCommand
}

func (c mutationCommand) operation() storage.Operation {
	switch {
	case c.file != nil:
		return storage.OpFileMutate
	case c.name != nil:
		return storage.OpFileMutateName
	case c.pending != nil:
		return storage.OpFileSetPendingUnlink
	case c.clear != nil:
		return storage.OpFileClearPendingUnlink
	default:
		return ""
	}
}

func (c mutationCommand) action() storage.FileActionID {
	switch {
	case c.file != nil:
		return c.file.Action
	case c.name != nil:
		return c.name.Action
	case c.pending != nil:
		return c.pending.Action
	case c.clear != nil:
		return c.clear.Action
	default:
		return ""
	}
}

func (c mutationCommand) setAction(action storage.FileActionID) {
	switch {
	case c.file != nil:
		c.file.Action = action
	case c.name != nil:
		c.name.Action = action
	case c.pending != nil:
		c.pending.Action = action
	case c.clear != nil:
		c.clear.Action = action
	}
}

func (c mutationCommand) check(h *fileHandle, effects []storage.ContentMetadataEffect) error {
	variants := 0
	for _, present := range []bool{c.file != nil, c.name != nil, c.pending != nil, c.clear != nil} {
		if present {
			variants++
		}
	}
	if variants != 1 || c.file == nil && len(effects) != 0 {
		return syscall.EINVAL
	}
	switch {
	case c.file != nil:
		command := *c.file
		if h.nodeKind != storage.NodeRegular && command.Kind != storage.MutateAttributes ||
			len(command.Uses) != 0 || command.Kind != storage.MutateAttributes && (!command.Attr.Empty() || len(command.Metadata) != 0) {
			return syscall.EINVAL
		}
		if err := storage.CheckContentMutation(command, effects); err != nil {
			return err
		}
		if command.Kind == storage.MutateAttributes {
			if len(command.ExpectedMetadata) != 0 || len(command.Metadata) > 1 {
				return syscall.EINVAL
			}
			if len(command.Metadata) != 0 {
				if _, present := command.Metadata[windowsMetadataKey]; !present {
					return syscall.EINVAL
				}
			}
		}
		return nil
	case c.name != nil:
		if c.name.Kind != storage.NameRename && c.name.Kind != storage.NameRemove && c.name.Kind != storage.NameRemoveDir ||
			c.name.Target.State != storage.SameNode || c.name.Target.NodeID != h.nodeID ||
			c.name.Kind == storage.NameRemove && h.nodeKind != storage.NodeRegular ||
			c.name.Kind == storage.NameRemoveDir && h.nodeKind != storage.NodeDirectory {
			return syscall.EINVAL
		}
		return c.name.Check()
	case c.pending != nil:
		if c.pending.Condition == storage.UnlinkFile && h.nodeKind != storage.NodeRegular ||
			c.pending.Condition == storage.UnlinkIfEmpty && h.nodeKind != storage.NodeDirectory {
			return syscall.EINVAL
		}
		return c.pending.Check()
	default:
		return c.clear.Check()
	}
}

func cloneMutationName(name storage.ChildName) storage.ChildName {
	name.RawLeaf = bytes.Clone(name.RawLeaf)
	if name.Parent.Scope != nil {
		scope := *name.Parent.Scope
		name.Parent.Scope = &scope
	}
	return name
}

func (c mutationCommand) clone() mutationCommand {
	switch {
	case c.file != nil:
		copy := cloneMutationCommand(*c.file)
		return mutationCommand{file: &copy}
	case c.name != nil:
		copy := *c.name
		copy.Name = cloneMutationName(copy.Name)
		copy.Initial.LinkTarget = bytes.Clone(copy.Initial.LinkTarget)
		copy.Initial.Metadata = storage.CloneInitialMetadata(copy.Initial.Metadata)
		copy.Target.ExpectedMetadata = storage.CloneInitialMetadata(copy.Target.ExpectedMetadata)
		copy.Uses = append([]storage.TargetUse(nil), copy.Uses...)
		if copy.Guards != nil {
			copy.Guards = cloneNamespaceGuards(*copy.Guards)
		}
		if copy.Destination != nil {
			destination := *copy.Destination
			destination.ObservedLeaf = bytes.Clone(destination.ObservedLeaf)
			destination.OutputLeaf = bytes.Clone(destination.OutputLeaf)
			destination.Expected.ExpectedMetadata = storage.CloneInitialMetadata(destination.Expected.ExpectedMetadata)
			if destination.Parent.Scope != nil {
				scope := *destination.Parent.Scope
				destination.Parent.Scope = &scope
			}
			copy.Destination = &destination
		}
		return mutationCommand{name: &copy}
	case c.pending != nil:
		copy := *c.pending
		copy.ExpectedMetadata = storage.CloneInitialMetadata(copy.ExpectedMetadata)
		copy.Uses = append([]storage.TargetUse(nil), copy.Uses...)
		return mutationCommand{pending: &copy}
	case c.clear != nil:
		copy := *c.clear
		copy.Generation = bytes.Clone(copy.Generation)
		copy.Uses = append([]storage.TargetUse(nil), copy.Uses...)
		return mutationCommand{clear: &copy}
	default:
		return mutationCommand{}
	}
}

func (c mutationCommand) payloadBytes(effects []storage.ContentMetadataEffect) int {
	charged := 0
	conditions := func(values map[string][]byte) {
		for name := range values {
			charged += len(name) + storage.MaxObservationTokenBytes
		}
	}
	uses := func(values []storage.TargetUse) {
		for _, use := range values {
			charged += 24 + len(use.Scope.Token)
		}
	}
	parent := func(value storage.DirectoryTarget) {
		if value.Scope != nil {
			charged += len(value.Scope.Token)
		}
	}
	switch {
	case c.file != nil:
		command := c.file
		charged += len(command.Data)
		conditions(command.ExpectedMetadata)
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
	case c.name != nil:
		command := c.name
		charged += len(command.Name.RawLeaf)
		parent(command.Name.Parent)
		conditions(command.Target.ExpectedMetadata)
		uses(command.Uses)
		if command.Guards != nil {
			for _, directory := range command.Guards.Directories {
				charged += 32 + len(directory.Revision)
			}
			for _, edge := range command.Guards.Edges {
				charged += 40 + len(edge.RawLeaf)
			}
		}
		if command.Destination != nil {
			destination := command.Destination
			charged += len(destination.ObservedLeaf) + len(destination.OutputLeaf)
			parent(destination.Parent)
			conditions(destination.Expected.ExpectedMetadata)
		}
	case c.pending != nil:
		conditions(c.pending.ExpectedMetadata)
		uses(c.pending.Uses)
	case c.clear != nil:
		charged += len(c.clear.Generation)
		uses(c.clear.Uses)
	}
	for _, effect := range effects {
		charged += len(effect.Namespace) + len(effect.PresentPrefix) + len(effect.AbsentPayload) + len(effect.ClearMask) + len(effect.SetMask)
	}
	return charged
}

// Only a bound NotExecuted receipt permits replacing observations. Operation
// data and reference-use exemptions remain those of the admitted request.
func (c mutationCommand) sameEffect(fresh mutationCommand, root bool) bool {
	if c.operation() != fresh.operation() {
		return false
	}
	switch {
	case c.file != nil:
		old, next := *c.file, *fresh.file
		next.Action = old.Action
		old.ExpectedMetadata, next.ExpectedMetadata = nil, nil
		old.Metadata, next.Metadata = nil, nil
		if !reflect.DeepEqual(old, next) || len(c.file.Metadata) != len(fresh.file.Metadata) {
			return false
		}
		for name, value := range c.file.Metadata {
			update, present := fresh.file.Metadata[name]
			if !present {
				return false
			}
			if root && name == windowsMetadataKey {
				before, err := decodeWindowsMetadataPayload(value.Data)
				after, nextErr := decodeWindowsMetadataPayload(update.Data)
				if err != nil || nextErr != nil || before.Attributes&^(dosHidden|dosSystem) != after.Attributes&^(dosHidden|dosSystem) {
					return false
				}
			} else if !bytes.Equal(value.Data, update.Data) {
				return false
			}
		}
		return true
	case c.name != nil:
		if c.name.Kind != fresh.name.Kind || c.name.Target.State != fresh.name.Target.State ||
			c.name.Target.NodeID != fresh.name.Target.NodeID || c.name.RequireNoOpenDescendants != fresh.name.RequireNoOpenDescendants ||
			!reflect.DeepEqual(c.name.Uses, fresh.name.Uses) {
			return false
		}
		return c.name.Destination == nil && fresh.name.Destination == nil || c.name.Destination != nil && fresh.name.Destination != nil &&
			bytes.Equal(c.name.Destination.OutputLeaf, fresh.name.Destination.OutputLeaf)
	case c.pending != nil:
		return c.pending.Condition == fresh.pending.Condition && reflect.DeepEqual(c.pending.Uses, fresh.pending.Uses)
	case c.clear != nil:
		return reflect.DeepEqual(c.clear.Uses, fresh.clear.Uses)
	default:
		return false
	}
}

type mutationOwner struct {
	mu               sync.Mutex
	server           *Server
	volume           string
	id               WriteOwnerID
	handle           *fileHandle
	nodeKind         storage.NodeKind
	actions          storage.FileActions
	mutation         storage.ConditionalFileMutation
	namespace        storage.NamespaceAccess
	deleteIntent     storage.DeleteIntent
	command          mutationCommand
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
	return s.reserveMutationCommand(t, h, sequence, mutationCommand{file: &command}, effects)
}

func (s *Server) reserveMutationCommand(t *tree, h *fileHandle, sequence uint64, command mutationCommand, effects []storage.ContentMetadataEffect) (*mutationOwner, error) {
	if h == nil || h.reference() == nil || t == nil || t.authority == nil || t.export == nil ||
		(h.nodeKind != storage.NodeRegular && h.nodeKind != storage.NodeDirectory) {
		return nil, syscall.EINVAL
	}
	if err := command.check(h, effects); err != nil {
		return nil, err
	}
	o := &mutationOwner{server: s, volume: t.export.share.Volume, handle: h, nodeKind: h.nodeKind, sequence: sequence}
	var ok bool
	switch {
	case command.file != nil:
		o.mutation, ok = h.reference().(storage.ConditionalFileMutation)
		if !ok {
			return nil, syscall.EOPNOTSUPP
		}
		if err := o.mutation.CheckConditionalFileMutation(); err != nil {
			return nil, err
		}
	case command.name != nil:
		o.namespace, ok = t.authority.raw.(storage.NamespaceAccess)
		if !ok {
			return nil, syscall.EOPNOTSUPP
		}
		if err := o.namespace.CheckNamespaceAccess(); err != nil {
			return nil, err
		}
	default:
		o.deleteIntent, ok = h.reference().(storage.DeleteIntent)
		if !ok {
			return nil, syscall.EOPNOTSUPP
		}
		if err := o.deleteIntent.CheckDeleteIntent(); err != nil {
			return nil, err
		}
	}
	o.actions, ok = t.authority.raw.(storage.FileActions)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := o.actions.CheckFileActions(); err != nil {
		return nil, err
	}
	id, err := s.reserveIORecord(t, h, command.payloadBytes(effects), false)
	if err != nil {
		return nil, err
	}
	o.id, o.command, o.effects = id, command.clone(), storage.CloneContentMetadataEffects(effects)
	s.writes.update(id, func(r *writeRecord) {
		r.fact.Action = command.action()
		r.fact.Operation = command.operation()
		if command.file != nil {
			r.fact.Bytes = uint32(len(command.file.Data))
			r.fact.Digest = sha256.Sum256(command.file.Data)
		}
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
	var refresh func(context.Context) (mutationCommand, error)
	if reobserve != nil {
		refresh = func(call context.Context) (mutationCommand, error) {
			condition, err := reobserve(call)
			if err != nil {
				return mutationCommand{}, err
			}
			if o.command.file == nil {
				return mutationCommand{}, syscall.EINVAL
			}
			command, err := applyWindowsMetadataCondition(*o.command.file, condition, o.handle.nodeID == o.handle.identity.Backend.RootNodeID)
			return mutationCommand{file: &command}, err
		}
	}
	return o.executeCommand(ctx, authorize, refresh, epoch)
}

func (o *mutationOwner) executeCommand(ctx context.Context, authorize func(context.Context) error, reobserve func(context.Context) (mutationCommand, error), epoch func() uint64) (storage.Attr, error) {
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
		attr, err := o.dispatchLocked(ctx)
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
		if receipt.Outcome == storage.FileActionNotExecuted && receipt.Operation == o.command.operation() {
			if errors.Is(err, storage.ErrConditionConflict) && o.conditionRetries < maxMutationConditionRetries && reobserve != nil && epoch != nil {
				fresh, observeErr := reobserve(ctx)
				if observeErr == nil {
					observeErr = fresh.check(o.handle, o.effects)
				}
				if observeErr == nil && !o.command.sameEffect(fresh, o.handle.nodeID == o.handle.identity.Backend.RootNodeID) {
					observeErr = syscall.EINVAL
				}
				if observeErr != nil {
					o.rejectLocked(err)
					return o.failLocked(errors.Join(err, observeErr))
				}
				freshBytes := fresh.payloadBytes(o.effects)
				oldBytes, reserveErr := o.server.writes.reserveReplacementPayload(o.id, fresh.payloadBytes(nil), o.server.config.Limits.MaxRetainedWriteBytes)
				if reserveErr != nil {
					o.rejectLocked(err)
					return o.failLocked(errors.Join(err, reserveErr))
				}
				newAction, mintErr := storage.NewFileActionID(epoch())
				if mintErr != nil {
					o.server.writes.setPayloadCharge(o.id, oldBytes)
					o.rejectLocked(err)
					return o.failLocked(errors.Join(err, mintErr))
				}
				retryCommand := fresh.clone()
				retryCommand.setAction(newAction)
				o.command = retryCommand
				o.server.writes.setPayloadCharge(o.id, freshBytes)
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

func (o *mutationOwner) dispatchLocked(ctx context.Context) (storage.Attr, error) {
	command := o.command.clone()
	switch {
	case command.file != nil:
		return o.mutation.MutateFile(ctx, *command.file)
	case command.name != nil:
		result, err := o.namespace.MutateName(ctx, *command.name)
		if result.Attr == nil {
			if err == nil && command.name.Kind != storage.NameRemove && command.name.Kind != storage.NameRemoveDir {
				err = syscall.EIO
			}
			return storage.Attr{}, err
		}
		if err == nil && o.checkResultIdentity(*result.Attr) != nil {
			err = syscall.EIO
		}
		return *result.Attr, err
	default:
		var result storage.ReferenceState
		var err error
		if command.pending != nil {
			result, err = o.deleteIntent.SetPendingUnlink(ctx, *command.pending)
		} else {
			result, err = o.deleteIntent.ClearPendingUnlink(ctx, *command.clear)
		}
		if err == nil && (result.CheckProjection(o.handle.nodeID, storage.StateFull) != nil ||
			len(result.PendingGeneration) > storage.MaxObservationTokenBytes || result.PendingUnlink != (len(result.PendingGeneration) != 0) ||
			len(result.LinkTarget) != 0 || command.pending != nil && !result.PendingUnlink) {
			err = syscall.EIO
		}
		return result.Attr, err
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
	receipt, err := o.actions.QueryFileAction(ctx, o.command.action())
	if err != nil {
		return receipt, err
	}
	if receipt.Check() != nil || receipt.Action != o.command.action() || receipt.Operation != "" && receipt.Operation != o.command.operation() {
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
		if o.command.file != nil && o.command.file.Kind != storage.MutateAttributes {
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
	attr, err := o.dispatchLocked(ctx)
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
	if attr.ID == 0 && o.command.name != nil && (o.command.name.Kind == storage.NameRemove || o.command.name.Kind == storage.NameRemoveDir) {
		return o.completeLocked(attr)
	}
	if o.checkResultIdentity(attr) != nil || storage.CheckMetadata(attr.Metadata) != nil {
		return syscall.EIO
	}
	if o.command.file == nil {
		return o.completeLocked(attr)
	}
	command := o.command.file
	switch command.Kind {
	case storage.MutateWriteAt, storage.MutateAppend, storage.MutateTruncate:
		if !attr.AllocationKnown {
			return syscall.EIO
		}
		if command.Kind == storage.MutateTruncate && attr.Size != command.Size ||
			command.Kind == storage.MutateWriteAt && len(command.Data) != 0 && attr.Size < command.Offset+int64(len(command.Data)) ||
			command.Kind == storage.MutateAppend && attr.Size < int64(len(command.Data)) {
			return syscall.EIO
		}
		if len(command.ContentEffects) != 0 {
			metadata, err := decodeWindowsMetadata(attr.Metadata)
			if err != nil || metadata.Attributes&dosArchive == 0 {
				return errors.Join(err, syscall.EIO)
			}
		}
	case storage.MutateAttributes:
		if storage.CheckMetadata(attr.Metadata) != nil {
			return syscall.EIO
		}
		for name, update := range command.Metadata {
			value, present := attr.Metadata[name]
			if !present || !bytes.Equal(value.Data, update.Data) {
				return syscall.EIO
			}
		}
		change := command.Attr
		if change.BirthTime != nil && (attr.BirthTime == nil || !attr.BirthTime.Equal(*change.BirthTime)) ||
			change.AccessTime != nil && !attr.AccessTime.Equal(*change.AccessTime) ||
			change.ModTime != nil && !attr.ModTime.Equal(*change.ModTime) {
			return syscall.EIO
		}
	default:
		return syscall.EIO
	}

	return o.completeLocked(attr)
}

func (o *mutationOwner) completeLocked(attr storage.Attr) error {
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
	// Keep the variant and action for immutable diagnostic correlation.
	action := o.command.action()
	switch {
	case o.command.file != nil:
		o.command = mutationCommand{file: &storage.FileMutation{Action: action}}
	case o.command.name != nil:
		o.command = mutationCommand{name: &storage.NameCommand{Action: action}}
	case o.command.pending != nil:
		o.command = mutationCommand{pending: &storage.PendingUnlinkCommand{Action: action}}
	case o.command.clear != nil:
		o.command = mutationCommand{clear: &storage.ClearPendingUnlinkCommand{Action: action}}
	}
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
	if receipt.Operation == o.command.operation() && receipt.Outcome == storage.FileActionNotExecuted {
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
