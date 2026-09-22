package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type openAtActionInput struct {
	Selection storage.ChildSelection
	Options   storage.OpenAtOptions
}

type nodeRefActionInput struct {
	Node      uint64
	Selection *storage.ChildSelection
	Options   storage.NodeRefOptions
}

type referenceActionTarget struct {
	NodeID uint64
	Scope  storage.UseScope
}

type fileMutationActionInput struct {
	Target  referenceActionTarget
	Command storage.FileMutation
}

type pendingUnlinkActionInput struct {
	Target  referenceActionTarget
	Command storage.PendingUnlinkCommand
}

type clearPendingUnlinkActionInput struct {
	Target  referenceActionTarget
	Command storage.ClearPendingUnlinkCommand
}

type fileAction struct {
	operation storage.Operation
	digest    [sha256.Size]byte
	done      chan struct{}
	expires   time.Time
	outcome   storage.FileActionOutcome
	result    any
	err       error
}

func cloneOpenResult(result storage.OpenResult) storage.OpenResult {
	result.Attr = result.Attr.Clone()
	return result
}

func cloneNodeOpenResult(result storage.NodeOpenResult) storage.NodeOpenResult {
	result.Attr = result.Attr.Clone()
	return result
}

func cloneNameResult(result storage.NameResult) storage.NameResult {
	if result.Attr != nil {
		attr := result.Attr.Clone()
		result.Attr = &attr
	}
	return result
}

func cloneReferenceState(result storage.ReferenceState) storage.ReferenceState {
	result.Attr = result.Attr.Clone()
	result.LinkTarget = append([]byte{}, result.LinkTarget...)
	result.PendingGeneration = append([]byte{}, result.PendingGeneration...)
	return result
}

func fileActionDigest(operation storage.Operation, input any) ([sha256.Size]byte, error) {
	input = canonicalFileActionInput(input)
	body, err := json.Marshal(struct {
		Operation storage.Operation `json:"operation"`
		Input     any               `json:"input"`
	}{Operation: operation, Input: input})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(body), nil
}

func canonicalFileActionInput(input any) any {
	switch value := input.(type) {
	case openAtActionInput:
		value.Selection = canonicalChildSelection(value.Selection)
		value.Options.Target = canonicalChildCondition(value.Options.Target)
		value.Options.Initial = canonicalInitialState(value.Options.Initial)
		value.Options.CloseIntent = canonicalCloseIntent(value.Options.CloseIntent)
		return value
	case nodeRefActionInput:
		if value.Selection != nil {
			selection := canonicalChildSelection(*value.Selection)
			value.Selection = &selection
		}
		value.Options.Target = canonicalChildCondition(value.Options.Target)
		value.Options.InitialState = canonicalInitialState(value.Options.InitialState)
		value.Options.CloseIntent = canonicalCloseIntent(value.Options.CloseIntent)
		return value
	case storage.NameCommand:
		value.Name = canonicalChildName(value.Name)
		value.Target = canonicalChildCondition(value.Target)
		if value.Destination != nil {
			destination := *value.Destination
			destination.ObservedLeaf = bytes.Clone(destination.ObservedLeaf)
			destination.OutputLeaf = bytes.Clone(destination.OutputLeaf)
			destination.Expected = canonicalChildCondition(destination.Expected)
			if destination.Parent.Scope != nil {
				scope := *destination.Parent.Scope
				destination.Parent.Scope = &scope
			}
			value.Destination = &destination
		}
		value.Initial = canonicalInitialFields(value.Initial)
		value.Uses = canonicalTargetUses(value.Uses)
		return value
	case storage.PendingUnlinkCommand:
		value.ExpectedMetadata = canonicalMetadataConditions(value.ExpectedMetadata)
		value.Uses = canonicalTargetUses(value.Uses)
		return value
	case storage.ClearPendingUnlinkCommand:
		value.Generation = canonicalBytes(value.Generation)
		value.Uses = canonicalTargetUses(value.Uses)
		return value
	case storage.FileMutation:
		return canonicalFileMutation(value)
	case fileMutationActionInput:
		value.Command = canonicalFileMutation(value.Command)
		return value
	case pendingUnlinkActionInput:
		value.Command.ExpectedMetadata = canonicalMetadataConditions(value.Command.ExpectedMetadata)
		value.Command.Uses = canonicalTargetUses(value.Command.Uses)
		return value
	case clearPendingUnlinkActionInput:
		value.Command.Generation = canonicalBytes(value.Command.Generation)
		value.Command.Uses = canonicalTargetUses(value.Command.Uses)
		return value
	default:
		return input
	}
}

func canonicalFileMutation(value storage.FileMutation) storage.FileMutation {
	value.Data = canonicalBytes(value.Data)
	value.ExpectedMetadata = canonicalMetadataConditions(value.ExpectedMetadata)
	value.Metadata = canonicalMetadataUpdates(value.Metadata)
	value.Attr = canonicalAttrChange(value.Attr)
	value.Uses = canonicalTargetUses(value.Uses)
	return value
}

func canonicalChildName(name storage.ChildName) storage.ChildName {
	name.RawLeaf = bytes.Clone(name.RawLeaf)
	if name.Parent.Scope != nil {
		scope := *name.Parent.Scope
		name.Parent.Scope = &scope
	}
	return name
}

func canonicalChildSelection(selection storage.ChildSelection) storage.ChildSelection {
	selection = selection.Clone()
	if selection.Guards != nil {
		if selection.Guards.RootID == 0 && len(selection.Guards.Directories) == 0 && len(selection.Guards.Edges) == 0 {
			selection.Guards = nil
			return selection
		}
		sort.Slice(selection.Guards.Directories, func(left, right int) bool {
			return selection.Guards.Directories[left].ParentID < selection.Guards.Directories[right].ParentID
		})
		sort.Slice(selection.Guards.Edges, func(left, right int) bool {
			leftEdge, rightEdge := selection.Guards.Edges[left], selection.Guards.Edges[right]
			if leftEdge.ParentID != rightEdge.ParentID {
				return leftEdge.ParentID < rightEdge.ParentID
			}
			if compared := bytes.Compare(leftEdge.RawLeaf, rightEdge.RawLeaf); compared != 0 {
				return compared < 0
			}
			return leftEdge.ChildID < rightEdge.ChildID
		})
	}
	return selection
}

func canonicalChildCondition(condition storage.ChildCondition) storage.ChildCondition {
	condition.ExpectedMetadata = canonicalMetadataConditions(condition.ExpectedMetadata)
	return condition
}

func canonicalInitialState(state storage.InitialState) storage.InitialState {
	state.OnCreate = canonicalInitialFields(state.OnCreate)
	state.OnReset = canonicalInitialFields(state.OnReset)
	state.OnReplace = canonicalInitialFields(state.OnReplace)
	return state
}

func canonicalInitialFields(fields storage.InitialFields) storage.InitialFields {
	fields.LinkTarget = canonicalBytes(fields.LinkTarget)
	fields.Attr = canonicalAttrChange(fields.Attr)
	if len(fields.Metadata) == 0 {
		fields.Metadata = nil
		return fields
	}
	metadata := make(map[string][]byte, len(fields.Metadata))
	for namespace, payload := range fields.Metadata {
		metadata[namespace] = canonicalBytes(payload)
	}
	fields.Metadata = metadata
	return fields
}

func canonicalCloseIntent(intent *storage.CloseIntent) *storage.CloseIntent {
	if intent == nil {
		return nil
	}
	copy := *intent
	copy.ExpectedMetadata = canonicalMetadataConditions(copy.ExpectedMetadata)
	copy.Uses = canonicalTargetUses(copy.Uses)
	return &copy
}

func canonicalMetadataConditions(values map[string][]byte) map[string][]byte {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string][]byte, len(values))
	for namespace, version := range values {
		copy[namespace] = canonicalBytes(version)
	}
	return copy
}

func canonicalMetadataUpdates(values map[string]storage.OpaquePayload) map[string]storage.OpaquePayload {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]storage.OpaquePayload, len(values))
	for namespace, payload := range values {
		copy[namespace] = storage.OpaquePayload{Version: canonicalBytes(payload.Version), Data: canonicalBytes(payload.Data)}
	}
	return copy
}

func canonicalTargetUses(uses []storage.TargetUse) []storage.TargetUse {
	if len(uses) == 0 {
		return nil
	}
	copy := append([]storage.TargetUse(nil), uses...)
	sort.Slice(copy, func(i, j int) bool {
		if copy[i].NodeID != copy[j].NodeID {
			return copy[i].NodeID < copy[j].NodeID
		}
		return copy[i].Scope.Token < copy[j].Scope.Token
	})
	return copy
}

func canonicalBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return bytes.Clone(value)
}

func canonicalAttrChange(change storage.AttrChange) storage.AttrChange {
	canonical := func(value *time.Time) *time.Time {
		if value == nil {
			return nil
		}
		utc := value.UTC()
		return &utc
	}
	change.BirthTime = canonical(change.BirthTime)
	change.AccessTime = canonical(change.AccessTime)
	change.ModTime = canonical(change.ModTime)
	return change
}

func (fs *fileSession) pruneFileActionsLocked(now time.Time) {
	for id, action := range fs.actions {
		if action.outcome != storage.FileActionPending && !now.Before(action.expires) {
			delete(fs.actions, id)
		}
	}
}

func knownFileActionRefusal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, storage.ErrConditionConflict) || errors.Is(err, storage.ErrUseConflict) || errors.Is(err, storage.ErrRangeConflict) || errors.Is(err, storage.ErrInvalidScope) {
		return true
	}
	switch storage.ErrnoOf(err) {
	case syscall.EINVAL, syscall.EACCES, syscall.EPERM, syscall.EBADF, syscall.ESTALE,
		syscall.ENOENT, syscall.EEXIST, syscall.ENOTDIR, syscall.EISDIR, syscall.ENOTEMPTY,
		syscall.ELOOP, syscall.EBUSY, syscall.EFBIG, syscall.EMFILE, syscall.EDQUOT,
		syscall.ENOSPC, syscall.EOPNOTSUPP:
		return true
	default:
		return false
	}
}

func runFileAction[T any](
	ctx context.Context,
	fs *fileSession,
	id storage.FileActionID,
	operation storage.Operation,
	input any,
	clone func(T) T,
	hasEffect func(T) bool,
	call func() (T, error),
) (T, error) {
	if id == "" {
		return call()
	}
	if err := id.Check(); err != nil {
		var zero T
		return zero, err
	}
	digest, err := fileActionDigest(operation, input)
	if err != nil {
		var zero T
		return zero, err
	}
	for {
		fs.mu.Lock()
		if !fs.active || !time.Now().Before(fs.expires) {
			fs.mu.Unlock()
			var zero T
			return zero, syscall.ESTALE
		}
		if fs.actions == nil {
			fs.actions = make(map[storage.FileActionID]*fileAction)
		}
		fs.pruneFileActionsLocked(time.Now())
		if retained := fs.actions[id]; retained != nil {
			if retained.operation != operation || retained.digest != digest {
				fs.mu.Unlock()
				var zero T
				return zero, syscall.EINVAL
			}
			done := retained.done
			if retained.outcome == storage.FileActionPending {
				fs.mu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					var zero T
					return zero, ctx.Err()
				}
			}
			result, ok := retained.result.(T)
			retainedErr := retained.err
			fs.mu.Unlock()
			if !ok {
				var zero T
				return zero, syscall.EIO
			}
			return clone(result), retainedErr
		}
		fs.mu.Unlock()

		epoch, err := fs.locks.Epoch(ctx)
		if err != nil {
			var zero T
			return zero, err
		}
		requested, err := id.Epoch()
		if err != nil || requested != epoch {
			var zero T
			if err != nil {
				return zero, err
			}
			return zero, syscall.ESTALE
		}

		fs.mu.Lock()
		fs.pruneFileActionsLocked(time.Now())
		if fs.actions[id] != nil {
			fs.mu.Unlock()
			continue
		}
		if len(fs.actions) >= fs.options.MaxLockActions {
			fs.mu.Unlock()
			var zero T
			return zero, syscall.EAGAIN
		}
		action := &fileAction{operation: operation, digest: digest, done: make(chan struct{}), outcome: storage.FileActionPending}
		fs.actions[id] = action
		fs.mu.Unlock()

		result, callErr := call()
		outcome := storage.FileActionCompleted
		if callErr != nil {
			outcome = storage.FileActionUnknown
			if !hasEffect(result) && knownFileActionRefusal(callErr) {
				outcome = storage.FileActionNotExecuted
			}
		}
		stored := clone(result)
		fs.mu.Lock()
		action.result = stored
		action.err = callErr
		action.outcome = outcome
		action.expires = time.Now().Add(fs.options.History)
		close(action.done)
		fs.mu.Unlock()
		return clone(stored), callErr
	}
}

func (fs *fileSession) CheckFileActions() error {
	return fs.native.CheckFileStore()
}

func (fs *fileSession) QueryFileAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := id.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.beginControl(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	defer done()
	fs.mu.Lock()
	fs.pruneFileActionsLocked(time.Now())
	action := fs.actions[id]
	if action != nil {
		receipt := storage.FileActionReceipt{Action: id, Operation: action.operation, Outcome: action.outcome}
		fs.mu.Unlock()
		return receipt, nil
	}
	fs.mu.Unlock()
	requested, err := id.Epoch()
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	epoch, err := fs.locks.Epoch(ctx)
	if err != nil {
		return storage.FileActionReceipt{}, err
	}
	outcome := storage.FileActionNotExecuted
	if requested < epoch {
		outcome = storage.FileActionRetired
	} else if requested > epoch {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	return storage.FileActionReceipt{Action: id, Outcome: outcome}, nil
}

func (fs *fileSession) QueryDeleteIntent(ctx context.Context, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	if err := id.Check(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.beginControl(ctx)
	if err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	defer done()
	return fs.native.QueryDeleteIntent(ctx, id)
}

func (fs *fileSession) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	if err := command.Check(); err != nil {
		return err
	}
	_, err := runFileAction(ctx, fs, command.Action, storage.OpFileAcknowledgeDeleteIntent, command,
		func(result struct{}) struct{} { return result }, func(struct{}) bool { return false },
		func() (struct{}, error) {
			ctx, cancel := fs.operationContext(ctx)
			defer cancel()
			done, err := fs.begin(ctx, true)
			if err != nil {
				return struct{}{}, err
			}
			defer done()
			return struct{}{}, fs.native.AcknowledgeDeleteIntent(ctx, command)
		})
	return err
}

var _ storage.FileActions = (*fileSession)(nil)
