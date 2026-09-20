package objectstore

import (
	"context"
	"errors"
	"syscall"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceUses struct {
	retiring bool
	nodeID   uint64
	scope    storage.UseScope
	owners   map[storage.UseOwner]struct{}
}

type orderedReference interface {
	Order(context.Context, func() error) error
}

type scopedRetainedReference interface {
	retainedReference
	storage.ScopedReference
	admit(context.Context, fileOperationClass) (context.Context, func(), error)
	nativeReference() orderedReference
	useBinding() *referenceUses
}

func (f *openFile) nativeReference() orderedReference { return f.native }
func (f *openFile) useBinding() *referenceUses        { return &f.uses }
func (r *nodeReference) nativeReference() orderedReference {
	return r.native
}
func (r *nodeReference) useBinding() *referenceUses { return &r.uses }

func (fs *fileSession) referenceActionTarget(ctx context.Context, ref scopedRetainedReference) (referenceActionTarget, error) {
	fs.mu.Lock()
	binding := *ref.useBinding()
	fs.mu.Unlock()
	if binding.nodeID != 0 && binding.scope.Check() == nil {
		return referenceActionTarget{NodeID: binding.nodeID, Scope: binding.scope}, nil
	}
	if _, err := ref.Scope(ctx); err != nil {
		return referenceActionTarget{}, err
	}
	fs.mu.Lock()
	binding = *ref.useBinding()
	fs.mu.Unlock()
	if binding.nodeID == 0 || binding.scope.Check() != nil {
		return referenceActionTarget{}, syscall.EIO
	}
	return referenceActionTarget{NodeID: binding.nodeID, Scope: binding.scope}, nil
}

var (
	_ storage.UseOwners    = (*fileSession)(nil)
	_ storage.RangeControl = (*fileSession)(nil)
)

func (fs *fileSession) CheckUseOwners() error {
	native, ok := fs.native.(useOwnerAuthority)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := native.CheckFileStore(); err != nil {
		return err
	}
	return native.CheckUseOwners()
}

func (fs *fileSession) CheckRangeControl() error {
	native, ok := fs.native.(rangeAuthority)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := native.CheckFileStore(); err != nil {
		return err
	}
	return native.CheckRangeControl()
}

func (fs *fileSession) scopedReference(scope storage.UseScope) (scopedRetainedReference, error) {
	if err := scope.Check(); err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for ref := range fs.files {
		scoped := ref.(scopedRetainedReference)
		if scoped.useBinding().scope == scope {
			return scoped, nil
		}
	}
	return nil, storage.ErrInvalidScope
}

func (fs *fileSession) referenceOrder(ref scopedRetainedReference) (advisory.Order, error) {
	native := ref.nativeReference()
	return func(ctx context.Context, transition func() error) error {
		ctx = metastore.WithReferenceSession(ctx, fs.locks)
		return native.Order(metastore.WithFilePublicationGuard(ctx, fs.publicationAllowed), transition)
	}, nil
}

func (fs *fileSession) NewUseOwner(ctx context.Context, node uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	if err := fs.CheckUseOwners(); err != nil {
		return 0, err
	}
	if err := options.Check(); err != nil {
		return 0, err
	}
	ref, err := fs.scopedReference(scope)
	if err != nil {
		return 0, err
	}
	ctx, done, err := ref.admit(ctx, fileAdvisoryOperation)
	if err != nil {
		return 0, err
	}
	defer done()
	fs.mu.Lock()
	nodeID := ref.useBinding().nodeID
	fs.mu.Unlock()
	if nodeID != node {
		return 0, storage.ErrInvalidScope
	}
	order, err := fs.referenceOrder(ref)
	if err != nil {
		return 0, err
	}
	var owner storage.UseOwner
	err = order(ctx, func() error {
		var err error
		owner, err = fs.locks.NewOwner(ctx, node, scope, options)
		return err
	})
	if err != nil {
		return 0, err
	}
	if options.Lifetime == storage.OwnerReference {
		fs.mu.Lock()
		uses := ref.useBinding()
		if uses.retiring {
			fs.mu.Unlock()
			cleanup, cancel := fs.operationContext(fs.cleanup)
			defer cancel()
			return 0, errors.Join(syscall.EBADF, fs.locks.RetireOwner(cleanup, owner))
		}
		if uses.owners == nil {
			uses.owners = make(map[storage.UseOwner]struct{})
		}
		uses.owners[owner] = struct{}{}
		fs.mu.Unlock()
	}
	return owner, nil
}

func (fs *fileSession) RetireUseOwner(ctx context.Context, owner storage.UseOwner) error {
	if err := fs.CheckUseOwners(); err != nil {
		return err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.admit(ctx, false, fileCleanupOperation)
	if err != nil {
		return err
	}
	defer done()
	if err := fs.locks.RetireOwner(ctx, owner); err != nil {
		return err
	}
	fs.mu.Lock()
	for ref := range fs.files {
		delete(ref.(scopedRetainedReference).useBinding().owners, owner)
	}
	fs.mu.Unlock()
	return nil
}

func (fs *fileSession) ownerOrder(ctx context.Context, owner storage.UseOwner) (uint64, advisory.Order, error) {
	node, err := fs.locks.OwnerNode(ctx, owner)
	if err != nil {
		return 0, nil, err
	}
	scope, err := fs.locks.OwnerScope(ctx, owner)
	if err != nil {
		return 0, nil, err
	}
	ref, err := fs.scopedReference(scope)
	if err != nil {
		return 0, nil, err
	}
	order, err := fs.referenceOrder(ref)
	return node, order, err
}

func (fs *fileSession) GetConflict(ctx context.Context, owner storage.UseOwner, command storage.RangeCommand) (storage.RangeConflict, error) {
	if err := fs.CheckRangeControl(); err != nil {
		return storage.RangeConflict{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.admit(ctx, false, fileAdvisoryOperation)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	defer done()
	node, order, err := fs.ownerOrder(ctx, owner)
	if err != nil {
		return storage.RangeConflict{}, err
	}
	return fs.locks.GetConflict(ctx, node, owner, command, order)
}

func (fs *fileSession) Apply(ctx context.Context, owner storage.UseOwner, commands []storage.RangeCommand, request storage.LockRequestID) (storage.RangeAttempt, error) {
	if err := fs.CheckRangeControl(); err != nil {
		return storage.RangeAttempt{}, err
	}
	class := fileAdvisoryOperation
	releasing := len(commands) > 0
	for _, command := range commands {
		releasing = releasing && (command.Edit == storage.Subtract || command.Edit == storage.RemoveExact)
	}
	if releasing {
		class = fileCleanupOperation
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.admit(ctx, false, class)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	defer done()
	node, order, err := fs.ownerOrder(ctx, owner)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return fs.locks.Apply(ctx, node, owner, commands, request, order)
}

func (fs *fileSession) Query(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	if err := fs.CheckRangeControl(); err != nil {
		return storage.RangeAttempt{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.admit(ctx, false, fileCleanupOperation)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	defer done()
	node, err := fs.locks.RequestNode(ctx, owner, request)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return fs.locks.Query(ctx, node, owner, request)
}

func (fs *fileSession) Cancel(ctx context.Context, owner storage.UseOwner, request storage.LockRequestID) (storage.RangeAttempt, error) {
	if err := fs.CheckRangeControl(); err != nil {
		return storage.RangeAttempt{}, err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.admit(ctx, false, fileCleanupOperation)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	defer done()
	node, err := fs.locks.RequestNode(ctx, owner, request)
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	return fs.locks.Cancel(ctx, node, owner, request)
}

func (fs *fileSession) Drop(ctx context.Context, owner storage.UseOwner, domain storage.ConflictDomain) error {
	if err := fs.CheckRangeControl(); err != nil {
		return err
	}
	ctx, cancel := fs.operationContext(ctx)
	defer cancel()
	done, err := fs.admit(ctx, false, fileCleanupOperation)
	if err != nil {
		return err
	}
	defer done()
	node, err := fs.locks.OwnerNode(ctx, owner)
	if err != nil {
		return err
	}
	return fs.locks.Drop(ctx, node, owner, domain)
}

func (fs *fileSession) retireReferenceOwners(uses *referenceUses) error {
	fs.mu.Lock()
	owners := make([]storage.UseOwner, 0, len(uses.owners))
	for owner := range uses.owners {
		owners = append(owners, owner)
	}
	fs.mu.Unlock()
	ctx, cancel := fs.operationContext(fs.cleanup)
	defer cancel()
	var failures []error
	for _, owner := range owners {
		if err := fs.locks.RetireOwner(ctx, owner); err != nil {
			failures = append(failures, err)
		} else {
			fs.mu.Lock()
			delete(uses.owners, owner)
			fs.mu.Unlock()
		}
	}
	return errors.Join(failures...)
}
