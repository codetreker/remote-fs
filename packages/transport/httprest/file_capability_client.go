package httprest

import (
	"context"
	"errors"
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
func (s *remoteFileSession) CheckMetadataAccess() error {
	return checkFileCapability(s.capabilities.Metadata)
}
func (s *remoteFileSession) CheckUseOwners() error { return checkFileCapability(s.capabilities.Owners) }
func (s *remoteFileSession) CheckRangeControl() error {
	return checkFileCapability(s.capabilities.Ranges)
}

func (s *remoteFileSession) openCapability(ctx context.Context, req fileRequest) (*remoteFile, fileResponse, error) {
	r, err := s.call(ctx, req)
	if err != nil {
		return nil, r, err
	}
	_, err = s.call(ctx, fileRequest{Op: storage.OpFileAck, File: r.File})
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, cleanupErr := s.storage.fileCall(cleanup, fileRequest{Op: storage.OpFileClose, Session: s.id, File: r.File})
		cancel()
		unchanged := false
		if req.OpenAt != nil {
			unchanged = !req.OpenAt.Create && req.OpenAt.Existing == storage.Keep && req.OpenAt.CloseIntent == nil
		}
		if req.NodeRef != nil {
			unchanged = !req.NodeRef.Create && req.NodeRef.CloseIntent == nil
		}
		interruptible := unchanged && errors.Is(err, context.Canceled) && storage.ErrnoOf(err) == syscall.EINTR && cleanupErr == nil
		if cleanupErr != nil && !errors.Is(cleanupErr, syscall.ESTALE) {
			err = errors.Join(err, cleanupErr)
		}
		return nil, r, operationFailure(Request{Op: OpFile}, err, interruptible)
	}
	return &remoteFile{session: s, id: r.File, node: r.Attr.ID, capabilities: *r.Capabilities}, r, nil
}
func (s *remoteFileSession) OpenAt(ctx context.Context, name storage.ChildName, o storage.OpenAtOptions) (storage.OpenResult, error) {
	r, _, e := s.OpenAtWithBarrier(ctx, name, o)
	return r, e
}
func (s *remoteFileSession) OpenAtWithBarrier(ctx context.Context, name storage.ChildName, o storage.OpenAtOptions) (storage.OpenResult, *MutationBarrier, error) {
	if err := s.CheckAtomicFileOpen(); err != nil {
		return storage.OpenResult{}, nil, err
	}
	if err := name.Check(); err != nil {
		return storage.OpenResult{}, nil, err
	}
	if err := o.Check(); err != nil {
		return storage.OpenResult{}, nil, err
	}
	f, r, err := s.openCapability(ctx, fileRequest{Op: storage.OpFileOpenAt, Child: &name, OpenAt: openAtOptionsOf(o)})
	if err != nil {
		return storage.OpenResult{}, nil, err
	}
	return storage.OpenResult{File: f, Attr: r.Attr.Storage(), Outcome: r.Outcome}, r.Barrier, nil
}
func (s *remoteFileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	if err := s.CheckNamespaceAccess(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	if err := target.Check(); err != nil {
		return storage.ObservedDirectory{}, err
	}
	r, err := s.call(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target, ResultBytes: s.storage.maxBodyBytes})
	if err != nil {
		return storage.ObservedDirectory{}, err
	}
	return r.Directory.storage(), nil
}
func (s *remoteFileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	r, _, e := s.MutateNameWithBarrier(ctx, command)
	return r, e
}
func (s *remoteFileSession) MutateNameWithBarrier(ctx context.Context, command storage.NameCommand) (storage.NameResult, *MutationBarrier, error) {
	if err := s.CheckNamespaceAccess(); err != nil {
		return storage.NameResult{}, nil, err
	}
	if err := command.Check(); err != nil {
		return storage.NameResult{}, nil, err
	}
	r, err := s.call(ctx, fileRequest{Op: storage.OpFileMutateName, Name: nameCommandOf(command)})
	if err != nil {
		return storage.NameResult{}, nil, err
	}
	var result storage.NameResult
	if r.Attr != nil {
		attr := r.Attr.Storage()
		result.Attr = &attr
	}
	return result, r.Barrier, nil
}
func (s *remoteFileSession) OpenNodeRef(ctx context.Context, id uint64, o storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	r, _, e := s.OpenNodeRefWithBarrier(ctx, id, o)
	return r, e
}
func (s *remoteFileSession) OpenNodeRefWithBarrier(ctx context.Context, id uint64, o storage.NodeRefOptions) (storage.NodeOpenResult, *MutationBarrier, error) {
	return s.openNodeRef(ctx, fileRequest{Op: storage.OpFileOpenNodeRef, Node: id, NodeRef: nodeRefOptionsOf(o)})
}
func (s *remoteFileSession) OpenChildRef(ctx context.Context, name storage.ChildName, o storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	r, _, e := s.OpenChildRefWithBarrier(ctx, name, o)
	return r, e
}
func (s *remoteFileSession) OpenChildRefWithBarrier(ctx context.Context, name storage.ChildName, o storage.NodeRefOptions) (storage.NodeOpenResult, *MutationBarrier, error) {
	return s.openNodeRef(ctx, fileRequest{Op: storage.OpFileOpenChildRef, Child: &name, NodeRef: nodeRefOptionsOf(o)})
}
func (s *remoteFileSession) openNodeRef(ctx context.Context, req fileRequest) (storage.NodeOpenResult, *MutationBarrier, error) {
	if err := s.CheckNodeReferences(); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	f, r, err := s.openCapability(ctx, req)
	if err != nil {
		return storage.NodeOpenResult{}, nil, err
	}
	return storage.NodeOpenResult{Reference: &remoteNodeReference{file: f}, Attr: r.Attr.Storage(), Outcome: r.Outcome}, r.Barrier, nil
}
func (s *remoteFileSession) SetMetadata(ctx context.Context, node uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	r, _, e := s.SetMetadataWithBarrier(ctx, node, namespace, version, data)
	return r, e
}
func (s *remoteFileSession) SetMetadataWithBarrier(ctx context.Context, node uint64, namespace string, version, data []byte) (storage.OpaquePayload, *MutationBarrier, error) {
	if err := s.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	req := fileRequest{Op: storage.OpFileSetNodeMetadata, Node: node, Namespace: namespace, Version: version, Payload: data}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	r, err := s.call(ctx, req)
	if err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	return storage.OpaquePayload{Version: r.Metadata.Version, Data: r.Metadata.Data}, r.Barrier, nil
}
func (s *remoteFileSession) NewUseOwner(ctx context.Context, node uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	if err := s.CheckUseOwners(); err != nil {
		return 0, err
	}
	req := fileRequest{Op: storage.OpFileNewUseOwner, Node: node, Scope: &scope, OwnerOptions: options}
	if err := validateCapabilityArguments(req); err != nil {
		return 0, err
	}
	r, err := s.call(ctx, req)
	return r.Owner, err
}
func (s *remoteFileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	if err := s.CheckUseOwners(); err != nil {
		return err
	}
	if owner == 0 {
		return syscall.EINVAL
	}
	_, err := s.call(ctx, fileRequest{Op: storage.OpFileRetireUseOwner, Owner: owner})
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
	r, err := s.call(ctx, req)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	return *r.Conflict, nil
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
	r, err := s.call(ctx, req)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return *r.Attempt, nil
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

type remoteNodeReference struct{ file *remoteFile }

func (r *remoteNodeReference) Stat(ctx context.Context) (storage.Attr, error) {
	return r.file.Stat(ctx)
}
func (r *remoteNodeReference) SetAttr(ctx context.Context, c storage.AttrChange) (storage.Attr, error) {
	return r.file.SetAttr(ctx, c)
}
func (r *remoteNodeReference) SetAttrWithBarrier(ctx context.Context, c storage.AttrChange) (storage.Attr, *MutationBarrier, error) {
	return r.file.SetAttrWithBarrier(ctx, c)
}
func (r *remoteNodeReference) Close(ctx context.Context) error { return r.file.Close(ctx) }
func (r *remoteNodeReference) CloseWithBarrier(ctx context.Context) (*MutationBarrier, error) {
	return r.file.CloseWithBarrier(ctx)
}
func (f *remoteFile) CheckReferenceState() error  { return checkFileCapability(f.capabilities.State) }
func (f *remoteFile) CheckScopedReference() error { return checkFileCapability(f.capabilities.Scope) }
func (f *remoteFile) CheckMetadataAccess() error  { return checkFileCapability(f.capabilities.Metadata) }
func (f *remoteFile) CheckDeleteIntent() error    { return checkFileCapability(f.capabilities.Delete) }
func (f *remoteFile) CheckConditionalFileMutation() error {
	return checkFileCapability(f.capabilities.Conditional)
}
func (f *remoteFile) State(ctx context.Context) (storage.ReferenceState, error) {
	if err := f.CheckReferenceState(); err != nil {
		return storage.ReferenceState{}, err
	}
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileState})
	if err != nil {
		return storage.ReferenceState{}, err
	}
	return r.State.storage(), nil
}
func (f *remoteFile) Scope(ctx context.Context) (storage.UseScope, error) {
	if err := f.CheckScopedReference(); err != nil {
		return storage.UseScope{}, err
	}
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileScope})
	if err != nil {
		return storage.UseScope{}, err
	}
	return *r.Scope, nil
}
func (f *remoteFile) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	r, _, e := f.SetMetadataWithBarrier(ctx, namespace, version, data)
	return r, e
}
func (f *remoteFile) SetMetadataWithBarrier(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, *MutationBarrier, error) {
	if err := f.CheckMetadataAccess(); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	req := fileRequest{Op: storage.OpFileSetMetadata, Namespace: namespace, Version: version, Payload: data}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	r, err := f.call(ctx, req)
	if err != nil {
		return storage.OpaquePayload{}, nil, err
	}
	return storage.OpaquePayload{Version: r.Metadata.Version, Data: r.Metadata.Data}, r.Barrier, nil
}
func (f *remoteFile) SetPendingUnlink(ctx context.Context, c storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	r, _, e := f.SetPendingUnlinkWithBarrier(ctx, c)
	return r, e
}
func (f *remoteFile) SetPendingUnlinkWithBarrier(ctx context.Context, c storage.PendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	return f.pending(ctx, fileRequest{Op: storage.OpFileSetPendingUnlink, Pending: &c})
}
func (f *remoteFile) ClearPendingUnlink(ctx context.Context, c storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	r, _, e := f.ClearPendingUnlinkWithBarrier(ctx, c)
	return r, e
}
func (f *remoteFile) ClearPendingUnlinkWithBarrier(ctx context.Context, c storage.ClearPendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	return f.pending(ctx, fileRequest{Op: storage.OpFileClearPendingUnlink, ClearPending: &c})
}
func (f *remoteFile) pending(ctx context.Context, req fileRequest) (storage.ReferenceState, *MutationBarrier, error) {
	if err := f.CheckDeleteIntent(); err != nil {
		return storage.ReferenceState{}, nil, err
	}
	if err := validateCapabilityArguments(req); err != nil {
		return storage.ReferenceState{}, nil, err
	}
	r, err := f.call(ctx, req)
	if err != nil {
		return storage.ReferenceState{}, nil, err
	}
	return r.State.storage(), r.Barrier, nil
}
func (f *remoteFile) MutateFile(ctx context.Context, c storage.FileMutation) (storage.Attr, error) {
	r, _, e := f.MutateFileWithBarrier(ctx, c)
	return r, e
}
func (f *remoteFile) MutateFileWithBarrier(ctx context.Context, c storage.FileMutation) (storage.Attr, *MutationBarrier, error) {
	if err := f.CheckConditionalFileMutation(); err != nil {
		return storage.Attr{}, nil, err
	}
	if err := c.Check(); err != nil {
		return storage.Attr{}, nil, err
	}
	r, err := f.call(ctx, fileRequest{Op: storage.OpFileMutate, Mutation: fileMutationOf(c)})
	attr, err := responseFileAttr(r, err)
	return attr, r.Barrier, err
}
func (r *remoteNodeReference) CheckReferenceState() error { return r.file.CheckReferenceState() }
func (r *remoteNodeReference) State(ctx context.Context) (storage.ReferenceState, error) {
	return r.file.State(ctx)
}
func (r *remoteNodeReference) CheckScopedReference() error { return r.file.CheckScopedReference() }
func (r *remoteNodeReference) Scope(ctx context.Context) (storage.UseScope, error) {
	return r.file.Scope(ctx)
}
func (r *remoteNodeReference) CheckMetadataAccess() error { return r.file.CheckMetadataAccess() }
func (r *remoteNodeReference) SetMetadata(ctx context.Context, n string, v, d []byte) (storage.OpaquePayload, error) {
	return r.file.SetMetadata(ctx, n, v, d)
}
func (r *remoteNodeReference) SetMetadataWithBarrier(ctx context.Context, n string, v, d []byte) (storage.OpaquePayload, *MutationBarrier, error) {
	return r.file.SetMetadataWithBarrier(ctx, n, v, d)
}
func (r *remoteNodeReference) CheckDeleteIntent() error { return r.file.CheckDeleteIntent() }
func (r *remoteNodeReference) SetPendingUnlink(ctx context.Context, c storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	return r.file.SetPendingUnlink(ctx, c)
}
func (r *remoteNodeReference) SetPendingUnlinkWithBarrier(ctx context.Context, c storage.PendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	return r.file.SetPendingUnlinkWithBarrier(ctx, c)
}
func (r *remoteNodeReference) ClearPendingUnlink(ctx context.Context, c storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	return r.file.ClearPendingUnlink(ctx, c)
}
func (r *remoteNodeReference) ClearPendingUnlinkWithBarrier(ctx context.Context, c storage.ClearPendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error) {
	return r.file.ClearPendingUnlinkWithBarrier(ctx, c)
}

func (s *remoteFileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	if err := s.CheckNamespaceAccess(); err != nil {
		return storage.Attr{}, err
	}
	if err := name.Check(); err != nil {
		return storage.Attr{}, err
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileLookupAt, Child: &name})
	return responseFileAttr(response, err)
}

func (s *remoteFileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := s.CheckNamespaceAccess(); err != nil {
		return observation, err
	}
	if err := target.Check(); err != nil {
		return observation, err
	}
	limit := min(s.storage.maxBodyBytes, result.MaxBytes())
	if limit <= 0 {
		return observation, syscall.EFBIG
	}
	response, err := s.call(ctx, fileRequest{Op: storage.OpFileReadDirNode, Directory: &target, ResultBytes: limit})
	if err != nil {
		return observation, err
	}
	for _, entry := range response.Directory.Entries {
		if err := result.Add(storage.Entry{Name: string(entry.RawLeaf), Attr: entry.Attr.Storage()}); err != nil {
			return observation, err
		}
	}
	return response.Directory.Observation, nil
}
func fileResponseLimit(req fileRequest, limit int64) int64 {
	if req.Op == storage.OpFileReadDirNode || req.Op == fileObserveDirectoryMetadata || req.Op == fileObserveName || fileAttrResult(req.Op) {
		return min(limit, req.ResultBytes)
	}
	return limit
}
