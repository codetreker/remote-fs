package httprest

import (
	"context"
	"errors"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func checkFileCapability(supported bool) error {
	if !supported {
		return syscall.EOPNOTSUPP
	}
	return nil
}

func (s *remoteFileSession) CheckAtomicFileOpen() error {
	return checkFileCapability(s.capabilities.AtomicOpen)
}
func (s *remoteFileSession) CheckNamespaceAccess() error {
	return checkFileCapability(s.capabilities.Namespace)
}
func (s *remoteFileSession) CheckNodeReferences() error {
	return checkFileCapability(s.capabilities.References)
}
func (s *remoteFileSession) CheckFileActions() error {
	return checkFileCapability(s.capabilities.Actions)
}

func (s *remoteFileSession) CheckMetadataAccess() error {
	return checkFileCapability(s.capabilities.Metadata)
}
func (s *remoteFileSession) CheckUseOwners() error { return checkFileCapability(s.capabilities.Owners) }
func (s *remoteFileSession) CheckRangeControl() error {
	return checkFileCapability(s.capabilities.Ranges)
}

func (s *remoteFileSession) QueryFileAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	if err := s.CheckFileActions(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if err := action.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileQueryAction, FileAction: action})
	if response.ActionReceipt == nil {
		if err == nil {
			err = unreachable(Request{Op: OpFile}, errors.New("file action query returned no receipt"))
		}
		return storage.FileActionReceipt{}, err
	}
	return *response.ActionReceipt, err
}

func (s *remoteFileSession) QueryDeleteIntent(ctx context.Context, owner storage.DeleteIntentOwner, intent storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	if err := s.CheckFileActions(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	if err := owner.Check(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	if err := intent.Check(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileQueryDeleteIntent, DeleteOwner: owner, DeleteIntent: intent})
	if response.DeleteStatus == nil {
		if err == nil {
			err = unreachable(Request{Op: OpFile}, errors.New("delete intent query returned no status"))
		}
		return storage.DeleteIntentStatus{}, err
	}
	result, decodeErr := response.DeleteStatus.storage()
	if decodeErr != nil {
		return storage.DeleteIntentStatus{}, unreachable(Request{Op: OpFileControl}, decodeErr)
	}
	return result, err
}

func (s *remoteFileSession) ListDeleteIntents(ctx context.Context, owner storage.DeleteIntentOwner, after storage.DeleteIntentCursor, limit int) (storage.DeleteIntentPage, error) {
	if err := s.CheckFileActions(); err != nil {
		return storage.DeleteIntentPage{}, err
	}
	if err := owner.Check(); err != nil {
		return storage.DeleteIntentPage{}, err
	}
	if after > storage.DeleteIntentCursor(math.MaxInt64) || limit < 1 || limit > storage.MaxDeleteIntentPageEntries {
		return storage.DeleteIntentPage{}, syscall.EINVAL
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileListDeleteIntents, DeleteOwner: owner, DeleteAfter: after, DeleteLimit: limit})
	if response.DeletePage == nil {
		if err == nil {
			err = unreachable(Request{Op: OpFileControl}, errors.New("delete intent listing returned no page"))
		}
		return storage.DeleteIntentPage{}, err
	}
	result, decodeErr := response.DeletePage.storage()
	if decodeErr != nil {
		return storage.DeleteIntentPage{}, unreachable(Request{Op: OpFileControl}, decodeErr)
	}
	return result, err
}

func (s *remoteFileSession) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	if err := s.CheckFileActions(); err != nil {
		return err
	}
	if err := command.Check(); err != nil {
		return err
	}
	_, err := s.call(ctx, fileRequest{Op: storage.OpFileAcknowledgeDeleteIntent, Acknowledge: acknowledgeDeleteIntentCommandOf(command)})
	return err
}

func (s *remoteFileSession) openCapability(ctx context.Context, req fileRequest) (*remoteFile, fileResponse, error) {
	response, callErr := s.call(ctx, req)
	if response.File == "" {
		return nil, response, callErr
	}
	if response.Capabilities == nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cleanupErr := s.closeUnclaimedFile(cleanup, response.File, response.Epoch)
		cancel()
		return nil, response, errors.Join(callErr, cleanupErr, unreachable(Request{Op: OpFile}, errors.New("retained reference response has no capabilities")))
	}
	if callErr == nil && (response.Attr == nil || response.Outcome < storage.Opened || response.Outcome > storage.Replaced) {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cleanupErr := s.closeUnclaimedFile(cleanup, response.File, response.Epoch)
		cancel()
		return nil, response, errors.Join(cleanupErr, unreachable(Request{Op: OpFile}, errors.New("retained reference response is incomplete")))
	}
	node := response.Node
	if response.Attr != nil {
		node = response.Attr.ID
	}
	reference := &remoteFile{session: s, id: response.File, node: node, capabilities: *response.Capabilities}
	_, ackErr := s.call(ctx, fileRequest{Op: storage.OpFileAck, File: response.File})
	if ackErr != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cleanupErr := s.closeUnclaimedFile(cleanup, response.File, response.Epoch)
		cancel()
		if cleanupErr != nil && !errors.Is(cleanupErr, syscall.ESTALE) {
			ackErr = errors.Join(ackErr, cleanupErr)
		}
		return nil, response, operationFailure(Request{Op: OpFile}, errors.Join(callErr, ackErr), false)
	}
	return reference, response, callErr
}

func (s *remoteFileSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	result, _, err := s.OpenAtWithBarrier(ctx, selection, options)
	return result, err
}

func (s *remoteFileSession) OpenAtWithBarrier(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, *MutationBarrier, error) {
	if err := s.CheckAtomicFileOpen(); err != nil {
		return storage.OpenResult{}, nil, err
	}
	if err := selection.Check(); err != nil {
		return storage.OpenResult{}, nil, err
	}
	if err := options.Check(); err != nil {
		return storage.OpenResult{}, nil, err
	}
	child, guards := childSelectionWire(selection)
	reference, response, err := s.openCapability(ctx, fileRequest{Op: storage.OpFileOpenAt, Child: child, Guards: guards, OpenAt: openAtOptionsOf(options)})
	result := openResultOf(reference, response)
	return result, response.Barrier, err
}

func openResultOf(reference *remoteFile, response fileResponse) storage.OpenResult {
	result := storage.OpenResult{Outcome: response.Outcome}
	if reference != nil {
		result.File = reference
	}
	if response.Attr != nil {
		result.Attr = response.Attr.Storage()
	}
	return result
}

func (s *remoteFileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	if err := s.CheckNamespaceAccess(); err != nil {
		return storage.Attr{}, err
	}
	if err := name.Check(); err != nil {
		return storage.Attr{}, err
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileLookupAt, Child: childNameOf(name)})
	return responseFileAttr(response, err)
}

func (s *remoteFileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	result, _, err := s.MutateNameWithBarrier(ctx, command)
	return result, err
}

func (s *remoteFileSession) MutateNameWithBarrier(ctx context.Context, command storage.NameCommand) (storage.NameResult, *MutationBarrier, error) {
	if err := s.CheckNamespaceAccess(); err != nil {
		return storage.NameResult{}, nil, err
	}
	if err := command.Check(); err != nil {
		return storage.NameResult{}, nil, err
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileMutateName, Name: nameCommandOf(command)})
	var result storage.NameResult
	if response.Attr != nil {
		attr := response.Attr.Storage()
		result.Attr = &attr
	}
	return result, response.Barrier, err
}

func (s *remoteFileSession) OpenNodeRef(ctx context.Context, node uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	result, _, err := s.OpenNodeRefWithBarrier(ctx, node, options)
	return result, err
}

func (s *remoteFileSession) OpenNodeRefWithBarrier(ctx context.Context, node uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, *MutationBarrier, error) {
	return s.openNodeReference(ctx, fileRequest{Op: storage.OpFileOpenNodeRef, Node: node, NodeRef: nodeRefOptionsOf(options)})
}

func (s *remoteFileSession) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	result, _, err := s.OpenChildRefWithBarrier(ctx, selection, options)
	return result, err
}

func (s *remoteFileSession) OpenChildRefWithBarrier(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, *MutationBarrier, error) {
	if err := s.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	if err := selection.Check(); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	if err := options.Check(); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	child, guards := childSelectionWire(selection)
	return s.openNodeReference(ctx, fileRequest{Op: storage.OpFileOpenChildRef, Child: child, Guards: guards, NodeRef: nodeRefOptionsOf(options)})
}

func (s *remoteFileSession) openNodeReference(ctx context.Context, req fileRequest) (storage.NodeOpenResult, *MutationBarrier, error) {
	if err := s.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	reference, response, err := s.openCapability(ctx, req)
	if err == nil && reference != nil && (!reference.capabilities.Scope || !reference.capabilities.State) {
		err = unreachable(Request{Op: OpFile}, errors.New("node reference omitted mandatory scope or state capability"))
	}
	result := storage.NodeOpenResult{Outcome: response.Outcome}
	if reference != nil {
		result.Reference = &remoteNodeReference{file: reference}
	}
	if response.Attr != nil {
		result.Attr = response.Attr.Storage()
	}
	return result, response.Barrier, err
}

func (s *remoteFileSession) SetMetadata(ctx context.Context, node uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	result, _, err := s.SetMetadataWithBarrier(ctx, node, namespace, version, data)
	return result, err
}

func (s *remoteFileSession) SetMetadataWithBarrier(ctx context.Context, node uint64, namespace string, version, data []byte) (storage.OpaquePayload, *MutationBarrier, error) {
	if err := s.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	req := fileRequest{Op: storage.OpFileSetNodeMetadata, Node: node, Namespace: namespace, Version: metadataVersion(version), Payload: metadataPayload(data)}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	response, err := s.call(ctx, req)
	if err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	return storage.OpaquePayload{Version: response.Metadata.Version, Data: response.Metadata.Data}, response.Barrier, nil
}

func (s *remoteFileSession) NewUseOwner(ctx context.Context, node uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	if err := s.CheckUseOwners(); err != nil {
		return 0, err
	}
	req := fileRequest{Op: storage.OpFileNewUseOwner, Node: node, Scope: &scope, OwnerOptions: options}
	if err := validateCapabilityArguments(req); err != nil {
		return 0, err
	}
	response, err := s.call(ctx, req)
	return response.Owner, err
}

func (s *remoteFileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	if err := s.CheckUseOwners(); err != nil {
		return err
	}
	req := fileRequest{Op: storage.OpFileRetireUseOwner, Owner: owner}
	if err := validateCapabilityArguments(req); err != nil {
		return err
	}
	_, err := s.call(ctx, req)
	return err
}

func (s *remoteFileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	if err := s.CheckRangeControl(); err != nil {
		return storage.RangeConflict{}, err
	}
	req := fileRequest{Op: storage.OpFileRangeGetConflict, Owner: owner, Commands: []storage.RangeCommand{command}}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.RangeConflict{}, err
	}
	response, err := s.call(ctx, req)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	return *response.Conflict, nil
}

func (s *remoteFileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, id storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.rangeCall(ctx, fileRequest{Op: storage.OpFileRangeApply, Owner: owner, Commands: commands, LockID: id})
}

func (s *remoteFileSession) Query(ctx context.Context, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.rangeCall(ctx, fileRequest{Op: storage.OpFileRangeQuery, Owner: owner, LockID: id})
}

func (s *remoteFileSession) Cancel(ctx context.Context, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	return s.rangeCall(ctx, fileRequest{Op: storage.OpFileRangeCancel, Owner: owner, LockID: id})
}

func (s *remoteFileSession) rangeCall(ctx context.Context, req fileRequest) (storage.RangeAttempt, error) {
	if err := s.CheckRangeControl(); err != nil {
		return storage.RangeAttempt{}, err
	}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.RangeAttempt{}, err
	}
	response, err := s.call(ctx, req)
	if err != nil {
		var operation *operationError
		if errors.As(err, &operation) && operation.attempt != nil {
			attempt := operation.attempt.Clone()
			if validationErr := validateFileAttempt(req, attempt); validationErr != nil {
				return storage.RangeAttempt{}, unreachable(Request{Op: OpFile}, validationErr)
			}
			return attempt, err
		}
		return storage.RangeAttempt{}, err
	}
	return response.Attempt.Clone(), nil
}

func (s *remoteFileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	if err := s.CheckRangeControl(); err != nil {
		return err
	}
	req := fileRequest{Op: storage.OpFileRangeDrop, Owner: owner, Domain: domain}
	if err := validateCapabilityArguments(req); err != nil {
		return err
	}
	_, err := s.call(ctx, req)
	return err
}

func (f *remoteFile) CheckScopedReference() error { return checkFileCapability(f.capabilities.Scope) }
func (f *remoteFile) CheckMetadataAccess() error  { return checkFileCapability(f.capabilities.Metadata) }
func (f *remoteFile) CheckReferenceState() error  { return checkFileCapability(f.capabilities.State) }
func (f *remoteFile) CheckDeleteIntent() error    { return checkFileCapability(f.capabilities.Delete) }
func (f *remoteFile) CheckConditionalFileMutation() error {
	return checkFileCapability(f.capabilities.Conditional)
}

type remoteNodeReference struct{ file *remoteFile }

var _ storage.NodeReference = (*remoteNodeReference)(nil)
var _ NodeReferenceWithBarrier = (*remoteNodeReference)(nil)

func (r *remoteNodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	return r.file.stat(ctx, false)
}
func (r *remoteNodeReference) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	attr, _, err := r.file.setAttrWithBarrier(ctx, change, false)
	return attr, err
}
func (r *remoteNodeReference) SetAttrWithBarrier(ctx context.Context, change storage.AttrChange) (storage.Attr, *MutationBarrier, error) {
	return r.file.setAttrWithBarrier(ctx, change, false)
}
func (r *remoteNodeReference) Close(ctx context.Context) error { return r.file.Close(ctx) }
func (r *remoteNodeReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	return r.file.CloseWithResult(ctx)
}
func (r *remoteNodeReference) CloseWithBarrier(ctx context.Context) (storage.ReferenceCloseResult, *MutationBarrier, error) {
	return r.file.CloseWithBarrier(ctx)
}
func (r *remoteNodeReference) CheckScopedReference() error { return r.file.CheckScopedReference() }
func (r *remoteNodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return r.file.Scope(ctx)
}
func (r *remoteNodeReference) CheckMetadataAccess() error { return r.file.CheckMetadataAccess() }
func (r *remoteNodeReference) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	return r.file.SetMetadata(ctx, namespace, version, data)
}
func (r *remoteNodeReference) SetMetadataWithBarrier(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, *MutationBarrier, error) {
	return r.file.SetMetadataWithBarrier(ctx, namespace, version, data)
}
func (r *remoteNodeReference) CheckReferenceState() error { return r.file.CheckReferenceState() }
func (r *remoteNodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	return r.file.State(ctx)
}
func (r *remoteNodeReference) CheckDeleteIntent() error { return r.file.CheckDeleteIntent() }
func (r *remoteNodeReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return r.file.SetPendingUnlink(ctx, command)
}
func (r *remoteNodeReference) SetPendingUnlinkWithBarrier(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	return r.file.SetPendingUnlinkWithBarrier(ctx, command)
}
func (r *remoteNodeReference) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return r.file.ClearPendingUnlink(ctx, command)
}
func (r *remoteNodeReference) ClearPendingUnlinkWithBarrier(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	return r.file.ClearPendingUnlinkWithBarrier(ctx, command)
}
func (r *remoteNodeReference) CheckConditionalFileMutation() error {
	return r.file.CheckConditionalFileMutation()
}
func (r *remoteNodeReference) MutateFile(ctx context.Context, mutation storage.FileMutation) (storage.Attr, error) {
	result, _, err := r.file.mutateFileWithBarrier(ctx, mutation, false)
	return result, err
}
func (r *remoteNodeReference) MutateFileWithBarrier(ctx context.Context, mutation storage.FileMutation) (storage.Attr, *MutationBarrier, error) {
	return r.file.mutateFileWithBarrier(ctx, mutation, false)
}

func (f *remoteFile) Scope(ctx context.Context) (storage.UseScope, error) {
	if err := f.CheckScopedReference(); err != nil {
		return storage.UseScope{}, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileScope})
	if err != nil {
		return storage.UseScope{}, err
	}
	return *response.Scope, nil
}

func (f *remoteFile) State(ctx context.Context) (storage.ReferenceState, error) {
	if err := f.CheckReferenceState(); err != nil {
		return storage.ReferenceState{}, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileState})
	if response.State == nil {
		return storage.ReferenceState{}, err
	}
	return response.State.storage(), err
}

func (f *remoteFile) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	result, _, err := f.SetMetadataWithBarrier(ctx, namespace, version, data)
	return result, err
}

func (f *remoteFile) SetMetadataWithBarrier(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, *MutationBarrier, error) {
	if err := f.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	req := fileRequest{Op: storage.OpFileSetMetadata, Namespace: namespace, Version: metadataVersion(version), Payload: metadataPayload(data)}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	response, err := f.call(ctx, req)
	if err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	return storage.OpaquePayload{Version: response.Metadata.Version, Data: response.Metadata.Data}, response.Barrier, nil
}

func (f *remoteFile) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	result, _, err := f.SetPendingUnlinkWithBarrier(ctx, command)
	return result, err
}

func (f *remoteFile) SetPendingUnlinkWithBarrier(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, nil, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, nil, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileSetPendingUnlink, Pending: pendingUnlinkCommandOf(command)})
	var result storage.ReferenceState
	if response.State != nil {
		result = response.State.storage()
	}
	return result, response.Barrier, err
}

func (f *remoteFile) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	result, _, err := f.ClearPendingUnlinkWithBarrier(ctx, command)
	return result, err
}

func (f *remoteFile) ClearPendingUnlinkWithBarrier(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, nil, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, nil, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileClearPendingUnlink, ClearPending: clearPendingUnlinkCommandOf(command)})
	var result storage.ReferenceState
	if response.State != nil {
		result = response.State.storage()
	}
	return result, response.Barrier, err
}

func (f *remoteFile) MutateFile(ctx context.Context, mutation storage.FileMutation) (storage.Attr, error) {
	result, _, err := f.mutateFileWithBarrier(ctx, mutation, true)
	return result, err
}

func (f *remoteFile) MutateFileWithBarrier(ctx context.Context, mutation storage.FileMutation) (storage.Attr, *MutationBarrier, error) {
	return f.mutateFileWithBarrier(ctx, mutation, true)
}

func (f *remoteFile) mutateFileWithBarrier(ctx context.Context, mutation storage.FileMutation, regular bool) (storage.Attr, *MutationBarrier, error) {
	if err := f.CheckConditionalFileMutation(); err != nil {
		return storage.Attr{}, nil, err
	}
	if err := mutation.CheckDataLimit(f.session.storage.maxWriteBytes); err != nil {
		return storage.Attr{}, nil, err
	}
	response, err := f.call(ctx, fileRequest{Op: storage.OpFileMutate, Mutation: fileMutationOf(mutation)})
	var result storage.Attr
	if response.Attr != nil {
		result = response.Attr.Storage()
		if regular && result.Kind != storage.NodeRegular {
			return storage.Attr{}, nil, unreachable(Request{Op: OpFile}, errors.New("file mutation returned nonregular attributes"))
		}
	}
	return result, response.Barrier, err
}
