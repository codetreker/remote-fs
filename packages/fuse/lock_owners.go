package fuse

import (
	"context"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type lockOwnerKey struct {
	node, kernel uint64
	description  *handle
}

type lockGroup struct {
	id     uint64
	owners int
}
type localLockOwner struct {
	key        lockOwnerKey
	id         storage.UseOwner
	users      int
	persistent bool
	retired    bool
	retireDone chan struct{}
	retireErr  error
}

// The kernel owner is a process cookie for record locks and an open-description
// cookie for flock. The cookie remains local; the PID crosses only as diagnostic data.
func (h *handle) lockOwner(ctx context.Context, kernel uint64, domain storage.ConflictDomain, pid uint32, persistent bool) (*localLockOwner, error) {
	v := h.node.volume
	key := lockOwnerKey{node: h.node.id.node, kernel: kernel}
	if domain == storage.DomainWholeFile {
		key.description = h
	}
	v.ownerMu.Lock()
	if owner := v.lockOwners[key]; owner != nil {
		if owner.retireDone != nil {
			v.ownerMu.Unlock()
			errno, _ := h.lockOwnerRetirement(ctx, owner)
			return nil, errno
		}
		owner.users++
		owner.persistent = owner.persistent || persistent
		v.ownerMu.Unlock()
		return owner, nil
	}
	if len(v.lockOwners) >= v.sessionOptions.MaxLockOwners {
		v.ownerMu.Unlock()
		return nil, syscall.ENOLCK
	}
	v.ownerMu.Unlock()
	owners, ok := v.files.(storage.UseOwners)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := owners.CheckUseOwners(); err != nil {
		return nil, err
	}
	scoped, ok := h.file.(storage.ScopedReference)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	if err := scoped.CheckScopedReference(); err != nil {
		return nil, err
	}
	scope, err := scoped.Scope(ctx)
	if err != nil {
		return nil, err
	}
	if err := scope.Check(); err != nil {
		return nil, err
	}
	v.ownerMu.Lock()
	if existing := v.lockOwners[key]; existing != nil {
		if existing.retireDone != nil {
			v.ownerMu.Unlock()
			errno, _ := h.lockOwnerRetirement(ctx, existing)
			return nil, errno
		}
		existing.users++
		existing.persistent = existing.persistent || persistent
		v.ownerMu.Unlock()
		return existing, nil
	}
	if len(v.lockOwners) >= v.sessionOptions.MaxLockOwners {
		v.ownerMu.Unlock()
		return nil, syscall.ENOLCK
	}
	group := v.lockGroups[kernel]
	if group == nil {
		if v.nextLockGroup == math.MaxUint64 {
			v.ownerMu.Unlock()
			return nil, syscall.ENOLCK
		}
		v.nextLockGroup++
		group = &lockGroup{id: v.nextLockGroup}
	}
	lifetime := storage.OwnerExplicit
	if domain == storage.DomainWholeFile {
		lifetime = storage.OwnerReference
	}
	options := storage.OwnerOptions{Lifetime: lifetime, Group: group.id}
	if domain == storage.DomainRecord {
		options.Diagnostic = storage.OwnerDiagnostic(pid)
	}
	id, err := owners.NewUseOwner(ctx, key.node, scope, options)
	if err != nil {
		v.ownerMu.Unlock()
		return nil, err
	}
	if id == 0 {
		v.ownerMu.Unlock()
		return nil, syscall.EIO
	}
	owner := &localLockOwner{key: key, id: id, users: 1, persistent: persistent}
	if v.lockOwners == nil {
		v.lockOwners = make(map[lockOwnerKey]*localLockOwner)
		v.lockGroups = make(map[uint64]*lockGroup)
	}
	group.owners++
	v.lockGroups[kernel] = group
	v.lockOwners[key] = owner
	v.ownerMu.Unlock()
	return owner, nil
}

func (v *volume) retireLockOwner(ctx context.Context, owner *localLockOwner) error {
	v.ownerMu.Lock()
	if owner == nil || v.lockOwners[owner.key] != owner {
		v.ownerMu.Unlock()
		return syscall.EIO
	}
	if owner.retireDone != nil {
		done := owner.retireDone
		v.ownerMu.Unlock()
		select {
		case <-done:
			return owner.retireErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	group := v.lockGroups[owner.key.kernel]
	if group == nil || group.owners <= 0 {
		v.ownerMu.Unlock()
		return syscall.EIO
	}
	owner.retireDone = make(chan struct{})
	v.ownerMu.Unlock()

	owners, ok := v.files.(storage.UseOwners)
	var retireErr error
	if !ok {
		retireErr = syscall.EOPNOTSUPP
	} else if err := owners.CheckUseOwners(); err != nil {
		retireErr = err
	} else {
		retireErr = owners.RetireUseOwner(ctx, owner.id)
	}

	v.ownerMu.Lock()
	owner.retireErr = retireErr
	if retireErr == nil {
		owner.retired = true
		delete(v.lockOwners, owner.key)
		group.owners--
		if group.owners == 0 {
			delete(v.lockGroups, owner.key.kernel)
		}
	}
	close(owner.retireDone)
	v.ownerMu.Unlock()
	return retireErr
}

func (h *handle) releaseLockOwner(ctx context.Context, owner *localLockOwner) error {
	v := h.node.volume
	v.ownerMu.Lock()
	if owner == nil || owner.users <= 0 {
		v.ownerMu.Unlock()
		return syscall.EIO
	}
	owner.users--
	retire := owner.users == 0 && !owner.persistent && !owner.retired && owner.retireDone == nil && v.lockOwners[owner.key] == owner
	v.ownerMu.Unlock()
	if retire {
		return v.retireLockOwner(ctx, owner)
	}
	return nil
}

func (v *volume) lockOwnerRetirement(ctx context.Context, owner *localLockOwner) (bool, error) {
	v.ownerMu.Lock()
	if owner == nil || owner.retireDone == nil {
		v.ownerMu.Unlock()
		return false, nil
	}
	done := owner.retireDone
	v.ownerMu.Unlock()

	wait, cancel := v.cleanupContext(ctx)
	defer cancel()
	select {
	case <-done:
		v.ownerMu.Lock()
		retired, err := owner.retired, owner.retireErr
		v.ownerMu.Unlock()
		if err != nil {
			return false, err
		}
		if !retired {
			return false, syscall.EIO
		}
		return true, nil
	case <-wait.Done():
		return false, wait.Err()
	}
}

func (h *handle) dropRecordOwner(ctx context.Context, kernel uint64) error {
	v := h.node.volume
	v.ownerMu.Lock()
	owner := v.lockOwners[lockOwnerKey{node: h.node.id.node, kernel: kernel}]
	v.ownerMu.Unlock()
	if owner == nil {
		return nil
	}
	return v.retireLockOwner(ctx, owner)
}

func (h *handle) retireDescriptionOwners(ctx context.Context) error {
	v := h.node.volume
	v.ownerMu.Lock()
	owners := make([]*localLockOwner, 0, 1)
	for _, owner := range v.lockOwners {
		if owner.key.description == h {
			owners = append(owners, owner)
		}
	}
	v.ownerMu.Unlock()
	var result error
	for _, owner := range owners {
		result = errors.Join(result, v.retireLockOwner(ctx, owner))
	}
	return result
}
