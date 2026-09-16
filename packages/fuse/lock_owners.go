package fuse

import (
	"context"
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
	pid        uint32
	users      int
	persistent bool
	retired    bool
}

// The kernel owner is a process cookie for record locks and an open-description
// cookie for flock. Neither cookie nor the diagnostic PID crosses the wire.
func (h *handle) lockOwner(ctx context.Context, kernel uint64, domain storage.ConflictDomain, pid uint32, persistent bool) (*localLockOwner, error) {
	v := h.node.volume
	key := lockOwnerKey{node: h.node.id.node, kernel: kernel}
	if domain == storage.DomainWholeFile {
		key.description = h
	}
	v.ownerMu.Lock()
	defer v.ownerMu.Unlock()
	if owner := v.lockOwners[key]; owner != nil {
		owner.users++
		owner.persistent = owner.persistent || persistent
		if pid != 0 {
			owner.pid = pid
		}
		return owner, nil
	}
	if len(v.lockOwners) >= v.sessionOptions.MaxLockOwners {
		return nil, syscall.ENOLCK
	}
	owners, ok := v.files.(storage.UseOwners)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	scoped, ok := h.file.(storage.ScopedReference)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	scope, err := scoped.Scope(ctx)
	if err != nil {
		return nil, err
	}
	group := v.lockGroups[kernel]
	if group == nil {
		if v.nextLockGroup == math.MaxUint64 {
			return nil, syscall.ENOLCK
		}
		v.nextLockGroup++
		group = &lockGroup{id: v.nextLockGroup}
	}
	lifetime := storage.OwnerExplicit
	if domain == storage.DomainWholeFile {
		lifetime = storage.OwnerReference
	}
	id, err := owners.NewUseOwner(ctx, key.node, scope, storage.OwnerOptions{Lifetime: lifetime, Group: group.id})
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, syscall.EIO
	}
	owner := &localLockOwner{key: key, id: id, pid: pid, users: 1, persistent: persistent}
	if v.lockOwners == nil {
		v.lockOwners = make(map[lockOwnerKey]*localLockOwner)
		v.lockGroups = make(map[uint64]*lockGroup)
	}
	group.owners++
	v.lockGroups[kernel] = group
	v.lockOwners[key] = owner
	return owner, nil
}

func (v *volume) retireLockOwnerLocked(ctx context.Context, owner *localLockOwner) error {
	owners, ok := v.files.(storage.UseOwners)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := owners.RetireUseOwner(ctx, owner.id); err != nil {
		return err
	}
	owner.retired = true
	delete(v.lockOwners, owner.key)
	group := v.lockGroups[owner.key.kernel]
	group.owners--
	if group.owners == 0 {
		delete(v.lockGroups, owner.key.kernel)
	}
	return nil
}

func (h *handle) releaseLockOwner(ctx context.Context, owner *localLockOwner) error {
	v := h.node.volume
	v.ownerMu.Lock()
	defer v.ownerMu.Unlock()
	owner.users--
	if owner.users == 0 && !owner.persistent && !owner.retired && v.lockOwners[owner.key] == owner {
		return v.retireLockOwnerLocked(ctx, owner)
	}
	return nil
}

func (v *volume) lockOwnerRetired(owner *localLockOwner) bool {
	v.ownerMu.Lock()
	defer v.ownerMu.Unlock()
	return owner.retired
}

func (v *volume) lockPID(id storage.OwnerDiagnostic) uint32 {
	v.ownerMu.Lock()
	defer v.ownerMu.Unlock()
	for _, owner := range v.lockOwners {
		if storage.OwnerDiagnostic(owner.id) == id {
			return owner.pid
		}
	}
	return 0
}

func (h *handle) dropRecordOwner(ctx context.Context, kernel uint64) error {
	v := h.node.volume
	v.ownerMu.Lock()
	defer v.ownerMu.Unlock()
	owner := v.lockOwners[lockOwnerKey{node: h.node.id.node, kernel: kernel}]
	if owner == nil {
		return nil
	}
	return v.retireLockOwnerLocked(ctx, owner)
}

func (h *handle) retireDescriptionOwners(ctx context.Context) error {
	v := h.node.volume
	v.ownerMu.Lock()
	defer v.ownerMu.Unlock()
	for _, owner := range v.lockOwners {
		if owner.key.description == h {
			if err := v.retireLockOwnerLocked(ctx, owner); err != nil {
				return err
			}
		}
	}
	return nil
}
