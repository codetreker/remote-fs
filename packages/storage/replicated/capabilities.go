package replicated

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

func (s *fileSession) CheckAtomicFileOpen() error {
	return capabilityCheck(s.remote, func(c httprest.AtomicFileOpenerWithBarrier) error { return c.CheckAtomicFileOpen() })
}

func (s *fileSession) CheckNamespaceAccess() error {
	return capabilityCheck(s.remote, func(c httprest.NamespaceAccessWithBarrier) error { return c.CheckNamespaceAccess() })
}

func (s *fileSession) CheckNodeReferences() error {
	return capabilityCheck(s.remote, func(c httprest.NodeReferencesWithBarrier) error { return c.CheckNodeReferences() })
}

func (s *fileSession) CheckFileActions() error {
	return capabilityCheck(s.remote, func(c storage.FileActions) error { return c.CheckFileActions() })
}

func (s *fileSession) QueryFileAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.FileActions) (storage.FileActionReceipt, error) {
		return c.QueryFileAction(ctx, action)
	})
}

func (s *fileSession) QueryDeleteIntent(ctx context.Context, intent storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.FileActions) (storage.DeleteIntentStatus, error) {
		return c.QueryDeleteIntent(ctx, intent)
	})
}

func (s *fileSession) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	_, err := sessionCapability(ctx, s, true, func(ctx context.Context, capability storage.FileActions) (struct{}, error) {
		return struct{}{}, capability.AcknowledgeDeleteIntent(ctx, command)
	})
	return err
}

func (s *fileSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, capability httprest.AtomicFileOpenerWithBarrier) (storage.OpenResult, error) {
		var result storage.OpenResult
		err := s.confirm(ctx, "open-at", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.OpenAtWithBarrier(ctx, name, options)
			return barrier, err
		})
		return s.wrapOpenResult(result, err)
	})
}

func nilReference(reference any) bool {
	if reference == nil {
		return true
	}
	value := reflect.ValueOf(reference)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (s *fileSession) wrapOpenResult(result storage.OpenResult, err error) (storage.OpenResult, error) {
	if nilReference(result.File) {
		result.File = nil
	}
	if err != nil {
		if result.File != nil {
			cleanup, done := s.base.fileCleanupContext()
			cleanupErr := result.File.Close(cleanup)
			done()
			if cleanupErr != nil {
				if remote, ok := result.File.(httprest.FileWithBarrier); ok {
					result.File = &retainedFile{session: s, remote: remote}
				} else {
					result.File = newFailedOpenFile(result.File, err)
				}
				return result, errors.Join(err, cleanupErr)
			}
		}
		return storage.OpenResult{}, err
	}
	remote, ok := result.File.(httprest.FileWithBarrier)
	if !ok {
		failure := fmt.Errorf("atomic open returned no barrier-capable file: %w", syscall.EIO)
		if result.File != nil {
			cleanup, done := s.base.fileCleanupContext()
			cleanupErr := result.File.Close(cleanup)
			done()
			if cleanupErr != nil {
				result.File = newFailedOpenFile(result.File, failure)
				return result, errors.Join(failure, cleanupErr)
			}
		}
		return storage.OpenResult{}, failure
	}
	result.File = &retainedFile{session: s, remote: remote}
	return result, nil
}

func (s *fileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, capability httprest.NamespaceAccessWithBarrier) (storage.Attr, error) {
		return capability.LookupAt(ctx, name)
	})
}

func (s *fileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, capability httprest.NamespaceAccessWithBarrier) (storage.NameResult, error) {
		var result storage.NameResult
		err := s.confirm(ctx, "mutate-name", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = capability.MutateNameWithBarrier(ctx, command)
			return barrier, err
		})
		return result, err
	})
}

func (s *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, capability httprest.NodeReferencesWithBarrier) (storage.NodeOpenResult, error) {
		return s.openReference(ctx, func(ctx context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
			return capability.OpenNodeRefWithBarrier(ctx, id, options)
		})
	})
}

func (s *fileSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, capability httprest.NodeReferencesWithBarrier) (storage.NodeOpenResult, error) {
		return s.openReference(ctx, func(ctx context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
			return capability.OpenChildRefWithBarrier(ctx, name, options)
		})
	})
}

func (s *fileSession) openReference(ctx context.Context, open func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error)) (storage.NodeOpenResult, error) {
	var result storage.NodeOpenResult
	err := s.confirm(ctx, "open-reference", func(ctx context.Context) (*httprest.MutationBarrier, error) {
		var barrier *httprest.MutationBarrier
		var err error
		result, barrier, err = open(ctx)
		return barrier, err
	})
	if nilReference(result.Reference) {
		result.Reference = nil
	}
	if err != nil {
		if result.Reference != nil {
			cleanup, done := s.base.fileCleanupContext()
			cleanupErr := result.Reference.Close(cleanup)
			done()
			if cleanupErr != nil {
				if remote, ok := result.Reference.(httprest.NodeReferenceWithBarrier); ok {
					result.Reference = &nodeReference{session: s, remote: remote}
				} else {
					result.Reference = newFailedOpenReference(result.Reference, err)
				}
				return result, errors.Join(err, cleanupErr)
			}
		}
		return storage.NodeOpenResult{}, err
	}
	remote, ok := result.Reference.(httprest.NodeReferenceWithBarrier)
	if !ok {
		failure := fmt.Errorf("node open returned no barrier-capable reference: %w", syscall.EIO)
		if result.Reference != nil {
			cleanup, done := s.base.fileCleanupContext()
			cleanupErr := result.Reference.Close(cleanup)
			done()
			if cleanupErr != nil {
				result.Reference = newFailedOpenReference(result.Reference, failure)
				return result, errors.Join(failure, cleanupErr)
			}
		}
		return storage.NodeOpenResult{}, failure
	}
	result.Reference = &nodeReference{session: s, remote: remote}
	return result, nil
}

func (s *fileSession) CheckMetadataAccess() error {
	return capabilityCheck(s.remote, func(c httprest.MetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}

func (s *fileSession) SetMetadata(ctx context.Context, node uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.MetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		if err := c.CheckMetadataAccess(); err != nil {
			return storage.OpaquePayload{}, err
		}
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
		if err := c.CheckUseOwners(); err != nil {
			return 0, err
		}
		return c.NewUseOwner(ctx, node, scope, options)
	})
}

func (s *fileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	_, err := sessionCapability(ctx, s, false, func(ctx context.Context, c storage.UseOwners) (struct{}, error) {
		if err := c.CheckUseOwners(); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, c.RetireUseOwner(ctx, owner)
	})
	return err
}

func (s *fileSession) CheckRangeControl() error {
	return capabilityCheck(s.remote, func(c storage.RangeControl) error { return c.CheckRangeControl() })
}

func (s *fileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeConflict, error) {
		if err := c.CheckRangeControl(); err != nil {
			return storage.RangeConflict{}, err
		}
		return c.GetConflict(ctx, owner, command)
	})
}

func (s *fileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeAttempt, error) {
		if err := c.CheckRangeControl(); err != nil {
			return storage.RangeAttempt{}, err
		}
		return c.Apply(ctx, owner, commands, request)
	})
}

func (s *fileSession) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeAttempt, error) {
		if err := c.CheckRangeControl(); err != nil {
			return storage.RangeAttempt{}, err
		}
		return c.Query(ctx, owner, request)
	})
}

func (s *fileSession) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (storage.RangeAttempt, error) {
		if err := c.CheckRangeControl(); err != nil {
			return storage.RangeAttempt{}, err
		}
		return c.Cancel(ctx, owner, request)
	})
}

func (s *fileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	_, err := sessionCapability(ctx, s, false, func(ctx context.Context, c storage.RangeControl) (struct{}, error) {
		if err := c.CheckRangeControl(); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, c.Drop(ctx, owner, domain)
	})
	return err
}

var (
	_ storage.AtomicFileOpener = (*fileSession)(nil)
	_ storage.NamespaceAccess  = (*fileSession)(nil)
	_ storage.NodeReferences   = (*fileSession)(nil)
	_ storage.FileActions      = (*fileSession)(nil)
	_ storage.MetadataAccess   = (*fileSession)(nil)
	_ storage.UseOwners        = (*fileSession)(nil)
	_ storage.RangeControl     = (*fileSession)(nil)
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
		if err := c.CheckScopedReference(); err != nil {
			return storage.UseScope{}, err
		}
		return c.Scope(ctx)
	})
}

func (f *retainedFile) CheckMetadataAccess() error {
	return capabilityCheck(f.remote, func(c httprest.ReferenceMetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}

func (f *retainedFile) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	return referenceCapability(ctx, f, func(ctx context.Context, c httprest.ReferenceMetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		if err := c.CheckMetadataAccess(); err != nil {
			return storage.OpaquePayload{}, err
		}
		return capabilityMutation(ctx, f.session, "set-reference-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return c.SetMetadataWithBarrier(ctx, namespace, version, data)
		})
	})
}

var (
	_ storage.ScopedReference         = (*retainedFile)(nil)
	_ storage.ReferenceMetadataAccess = (*retainedFile)(nil)
)
