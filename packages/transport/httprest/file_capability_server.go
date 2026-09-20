package httprest

import (
	"bytes"
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func sessionCapabilitiesOf(value storage.FileSession) (*fileCapabilities, error) {
	caps := &fileCapabilities{}
	var failure error
	check := func(target *bool, call func() error) {
		err := call()
		if err == nil {
			*target = true
		} else if storage.ErrnoOf(err) != syscall.EOPNOTSUPP {
			failure = errors.Join(failure, err)
		}
	}
	if capability, ok := value.(storage.MetadataAccess); ok {
		check(&caps.Metadata, capability.CheckMetadataAccess)
	}
	if capability, ok := value.(storage.UseOwners); ok {
		check(&caps.Owners, capability.CheckUseOwners)
	}
	if capability, ok := value.(storage.RangeControl); ok {
		check(&caps.Ranges, capability.CheckRangeControl)
	}
	return caps, failure
}

func referenceCapabilitiesOf(value storage.File) (*fileCapabilities, error) {
	caps := &fileCapabilities{}
	var failure error
	check := func(target *bool, call func() error) {
		err := call()
		if err == nil {
			*target = true
		} else if storage.ErrnoOf(err) != syscall.EOPNOTSUPP {
			failure = errors.Join(failure, err)
		}
	}
	if capability, ok := value.(storage.ReferenceMetadataAccess); ok {
		check(&caps.Metadata, capability.CheckMetadataAccess)
	}
	if capability, ok := value.(storage.ScopedReference); ok {
		check(&caps.Scope, capability.CheckScopedReference)
	}
	return caps, failure
}

func (h *Handler) performSessionCapability(ctx context.Context, session storage.FileSession, req fileRequest) (fileResponse, error) {
	response := fileResponse{}
	switch req.Op {
	case storage.OpFileSetNodeMetadata:
		capability, ok := session.(storage.MetadataAccess)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckMetadataAccess(); err != nil {
			return response, err
		}
		value, err := capability.SetMetadata(ctx, req.Node, req.Namespace, []byte(req.Version), []byte(req.Payload))
		if err == nil {
			response.Metadata = &OpaquePayload{Version: bytes.Clone(value.Version), Data: append([]byte{}, value.Data...)}
		}
		return response, err
	case storage.OpFileNewUseOwner, storage.OpFileRetireUseOwner:
		capability, ok := session.(storage.UseOwners)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckUseOwners(); err != nil {
			return response, err
		}
		if req.Op == storage.OpFileNewUseOwner {
			owner, err := capability.NewUseOwner(ctx, req.Node, *req.Scope, req.OwnerOptions)
			response.Owner = owner
			return response, err
		}
		return response, capability.RetireUseOwner(ctx, req.Owner)
	case storage.OpFileRangeGetConflict, storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel, storage.OpFileRangeDrop:
		capability, ok := session.(storage.RangeControl)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckRangeControl(); err != nil {
			return response, err
		}
		switch req.Op {
		case storage.OpFileRangeGetConflict:
			conflict, err := capability.GetConflict(ctx, req.Owner, req.Commands[0])
			response.Conflict = &conflict
			return response, err
		case storage.OpFileRangeDrop:
			return response, capability.Drop(ctx, req.Owner, req.Domain)
		}
		var attempt storage.RangeAttempt
		var err error
		switch req.Op {
		case storage.OpFileRangeApply:
			attempt, err = capability.Apply(ctx, req.Owner, req.Commands, req.LockID)
		case storage.OpFileRangeQuery:
			attempt, err = capability.Query(ctx, req.Owner, req.LockID)
		case storage.OpFileRangeCancel:
			attempt, err = capability.Cancel(ctx, req.Owner, req.LockID)
		}
		if attempt.Request != "" {
			copy := attempt.Clone()
			response.Attempt = &copy
		}
		return response, err
	}
	return response, syscall.EINVAL
}

func performReferenceCapability(ctx context.Context, file storage.File, req fileRequest) (fileResponse, error) {
	response := fileResponse{}
	switch req.Op {
	case storage.OpFileScope:
		capability, ok := file.(storage.ScopedReference)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckScopedReference(); err != nil {
			return response, err
		}
		scope, err := capability.Scope(ctx)
		if err == nil {
			response.Scope = &scope
		}
		return response, err
	case storage.OpFileSetMetadata:
		capability, ok := file.(storage.ReferenceMetadataAccess)
		if !ok {
			return response, syscall.EOPNOTSUPP
		}
		if err := capability.CheckMetadataAccess(); err != nil {
			return response, err
		}
		value, err := capability.SetMetadata(ctx, req.Namespace, []byte(req.Version), []byte(req.Payload))
		if err == nil {
			response.Metadata = &OpaquePayload{Version: bytes.Clone(value.Version), Data: append([]byte{}, value.Data...)}
		}
		return response, err
	}
	return response, syscall.EINVAL
}
