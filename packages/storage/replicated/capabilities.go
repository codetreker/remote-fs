package replicated

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func optional[C any](remote any) (C, error) {
	capability, ok := remote.(C)
	if !ok {
		var zero C
		return zero, syscall.EOPNOTSUPP
	}
	return capability, nil
}

func capabilityCheck[C any](remote any, check func(C) error) error {
	capability, err := optional[C](remote)
	if err != nil {
		return err
	}
	return check(capability)
}

func sessionCapability[C, R any](ctx context.Context, session *fileSession, ordinary bool, call func(context.Context, C) (R, error)) (R, error) {
	return fileCall(ctx, session, ordinary, func(ctx context.Context) (R, error) {
		capability, err := optional[C](session.remote)
		if err != nil {
			var zero R
			return zero, err
		}
		return call(ctx, capability)
	})
}

func capabilityMutation[R any](ctx context.Context, session *fileSession, op string, send func(context.Context) (R, *httprest.MutationBarrier, error)) (R, error) {
	var result R
	err := session.confirm(ctx, op, func(ctx context.Context) (*httprest.MutationBarrier, error) {
		var barrier *httprest.MutationBarrier
		var err error
		result, barrier, err = send(ctx)
		return barrier, err
	})
	if err != nil {
		var zero R
		return zero, err
	}
	return result, nil
}

func (s *fileSession) CheckMetadataAccess() error {
	return capabilityCheck(s.remote, func(c httprest.MetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}

func (s *fileSession) SetMetadata(ctx context.Context, node uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.MetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		return capabilityMutation(ctx, s, "set-node-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return c.SetMetadataWithBarrier(ctx, node, namespace, version, data)
		})
	})
}

func (s *fileSession) CheckUseOwners() error {
	return capabilityCheck(s.remote, func(c storage.UseOwners) error { return c.CheckUseOwners() })
}

func (s *fileSession) NewUseOwner(ctx context.Context, node uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.UseOwners) (storage.UseOwner, error) {
		return c.NewUseOwner(ctx, node, scope, options)
	})
}

func (s *fileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	_, err := sessionCapability(ctx, s, false, func(ctx context.Context, c storage.UseOwners) (struct{}, error) {
		return struct{}{}, c.RetireUseOwner(ctx, owner)
	})
	return err
}

func (s *fileSession) CheckRangeControl() error {
	return capabilityCheck(s.remote, func(c storage.RangeControl) error { return c.CheckRangeControl() })
}

func (s *fileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeConflict, error) {
		return c.GetConflict(ctx, owner, command)
	})
}

func (s *fileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeAttempt, error) {
		return c.Apply(ctx, owner, commands, request)
	})
}

func (s *fileSession) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeAttempt, error) {
		return c.Query(ctx, owner, request)
	})
}

func (s *fileSession) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeAttempt, error) {
		return c.Cancel(ctx, owner, request)
	})
}

func (s *fileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	_, err := sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (struct{}, error) {
		return struct{}{}, c.Drop(ctx, owner, domain)
	})
	return err
}

var (
	_ storage.MetadataAccess = (*fileSession)(nil)
	_ storage.UseOwners      = (*fileSession)(nil)
	_ storage.RangeControl   = (*fileSession)(nil)
)

func referenceCapability[C, R any](ctx context.Context, file *retainedFile, call func(context.Context, C) (R, error)) (R, error) {
	return fileCall(ctx, file.session, true, func(ctx context.Context) (R, error) {
		capability, err := optional[C](file.remote)
		if err != nil {
			var zero R
			return zero, err
		}
		return call(ctx, capability)
	})
}

func (f *retainedFile) CheckScopedReference() error {
	return capabilityCheck(f.remote, func(c storage.ScopedReference) error { return c.CheckScopedReference() })
}

func (f *retainedFile) Scope(ctx context.Context) (storage.UseScope, error) {
	return referenceCapability(ctx, f, func(ctx context.Context, c storage.ScopedReference) (storage.UseScope, error) {
		return c.Scope(ctx)
	})
}

func (f *retainedFile) CheckMetadataAccess() error {
	return capabilityCheck(f.remote, func(c httprest.ReferenceMetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}

func (f *retainedFile) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	return referenceCapability(ctx, f, func(ctx context.Context, c httprest.ReferenceMetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		return capabilityMutation(ctx, f.session, "set-reference-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return c.SetMetadataWithBarrier(ctx, namespace, version, data)
		})
	})
}

var (
	_ storage.ScopedReference         = (*retainedFile)(nil)
	_ storage.ReferenceMetadataAccess = (*retainedFile)(nil)
)
