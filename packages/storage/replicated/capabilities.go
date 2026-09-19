package replicated

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

var (
	_ storage.AtomicFileOpener = (*fileSession)(nil)
	_ storage.NamespaceAccess  = (*fileSession)(nil)
	_ storage.NodeReferences   = (*fileSession)(nil)
	_ storage.MetadataAccess   = (*fileSession)(nil)
	_ storage.UseOwners        = (*fileSession)(nil)
	_ storage.RangeControl     = (*fileSession)(nil)
)

func optional[C any](remote any) (C, error) {
	capability, ok := remote.(C)
	if !ok {
		return capability, syscall.EOPNOTSUPP
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

func (s *fileSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.AtomicFileOpenerWithBarrier) (storage.OpenResult, error) {
		var result storage.OpenResult
		err := s.confirm(ctx, "open-at", func(ctx context.Context) (*httprest.MutationBarrier, error) {
			var barrier *httprest.MutationBarrier
			var err error
			result, barrier, err = c.OpenAtWithBarrier(ctx, name, options)
			return barrier, err
		})
		if err != nil {
			if result.File != nil {
				cleanup, done := s.base.fileCleanupContext()
				cleanupErr := result.File.Close(cleanup)
				done()
				if cleanupErr != nil {
					if remote, ok := result.File.(httprest.FileWithBarrier); ok {
						result.File = &retainedFile{session: s, remote: remote}
					} else {
						result.File = &failedOpenFile{failedOpenReference: failedOpenReference{native: result.File, failure: err}}
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
					result.File = &failedOpenFile{failedOpenReference: failedOpenReference{native: result.File, failure: failure}}
					return result, errors.Join(failure, cleanupErr)
				}
			}
			return storage.OpenResult{}, failure
		}
		result.File = &retainedFile{session: s, remote: remote}
		return result, nil
	})
}

func (s *fileSession) CheckNamespaceAccess() error {
	return capabilityCheck(s.remote, func(c httprest.NamespaceAccessWithBarrier) error { return c.CheckNamespaceAccess() })
}
func (s *fileSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.NamespaceAccessWithBarrier) (storage.Attr, error) {
		return c.LookupAt(ctx, name)
	})
}
func (s *fileSession) ReadDirNode(ctx context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.NamespaceAccessWithBarrier) (storage.ObservedDirectory, error) {
		return c.ReadDirNode(ctx, target)
	})
}
func (s *fileSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.NamespaceAccessWithBarrier) (storage.NameResult, error) {
		return capabilityMutation(ctx, s, "mutate-name", func(ctx context.Context) (storage.NameResult, *httprest.MutationBarrier, error) {
			return c.MutateNameWithBarrier(ctx, command)
		})
	})
}
func (s *fileSession) CheckMetadataAccess() error {
	return capabilityCheck(s.remote, func(c httprest.MetadataAccessWithBarrier) error { return c.CheckMetadataAccess() })
}
func (s *fileSession) SetMetadata(ctx context.Context, id uint64, namespace string, version, payload []byte) (storage.OpaquePayload, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.MetadataAccessWithBarrier) (storage.OpaquePayload, error) {
		return capabilityMutation(ctx, s, "set-node-metadata", func(ctx context.Context) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
			return c.SetMetadataWithBarrier(ctx, id, namespace, version, payload)
		})
	})
}

func (s *fileSession) CheckNodeReferences() error {
	return capabilityCheck(s.remote, func(c httprest.NodeReferencesWithBarrier) error { return c.CheckNodeReferences() })
}
func (s *fileSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.NodeReferencesWithBarrier) (storage.NodeOpenResult, error) {
		return s.openReference(ctx, func(ctx context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
			return c.OpenNodeRefWithBarrier(ctx, id, options)
		})
	})
}
func (s *fileSession) OpenChildRef(ctx context.Context, name storage.ChildName, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.NodeReferencesWithBarrier) (storage.NodeOpenResult, error) {
		return s.openReference(ctx, func(ctx context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
			return c.OpenChildRefWithBarrier(ctx, name, options)
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
	if err != nil {
		if result.Reference != nil {
			cleanup, done := s.base.fileCleanupContext()
			cleanupErr := result.Reference.Close(cleanup)
			done()
			if cleanupErr != nil {
				if remote, ok := result.Reference.(httprest.NodeReferenceWithBarrier); ok {
					result.Reference = &nodeReference{session: s, remote: remote}
				} else {
					result.Reference = &failedOpenReference{native: result.Reference, failure: err}
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
				result.Reference = &failedOpenReference{native: result.Reference, failure: failure}
				return result, errors.Join(failure, cleanupErr)
			}
		}
		return storage.NodeOpenResult{}, failure
	}
	result.Reference = &nodeReference{session: s, remote: remote}
	return result, nil
}

func (s *fileSession) CheckUseOwners() error {
	return capabilityCheck(s.remote, func(c storage.UseOwners) error { return c.CheckUseOwners() })
}
func (s *fileSession) NewUseOwner(ctx context.Context, id uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	return sessionCapability(ctx, s, false, func(ctx context.Context, c storage.UseOwners) (storage.UseOwner, error) {
		return c.NewUseOwner(ctx, id, scope, options)
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

func (s *fileSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (observation storage.DirectoryObservation, returned error) {
	if result == nil {
		return observation, syscall.EINVAL
	}
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	return sessionCapability(ctx, s, true, func(ctx context.Context, c httprest.NamespaceAccessWithBarrier) (storage.DirectoryObservation, error) {
		return c.ReadDirNodeBounded(ctx, target, result)
	})
}
