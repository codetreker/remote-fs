package httprest

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func checkFileCapability(supported bool) error {
	if !supported {
		return syscall.EOPNOTSUPP
	}
	return nil
}

func (s *remoteFileSession) CheckMetadataAccess() error {
	return checkFileCapability(s.capabilities.Metadata)
}
func (s *remoteFileSession) CheckUseOwners() error { return checkFileCapability(s.capabilities.Owners) }
func (s *remoteFileSession) CheckRangeControl() error {
	return checkFileCapability(s.capabilities.Ranges)
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
