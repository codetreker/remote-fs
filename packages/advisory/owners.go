package advisory

import (
	"context"
	"math"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

// NewOwner registers a caller-validated native reference scope. The caller owns
// reference-lifetime cleanup; explicit owners remain until RetireOwner or Retire.
// Group is an opaque, session-local deadlock participant, not a process ID.
func (s *Session) NewOwner(ctx context.Context, node uint64, scope storage.UseScope, options storage.OwnerOptions) (storage.UseOwner, error) {
	if err := checkCall(ctx, node); err != nil {
		return 0, err
	}
	if err := scope.Check(); err != nil {
		return 0, err
	}
	if options.Lifetime != storage.OwnerReference && options.Lifetime != storage.OwnerExplicit {
		return 0, syscall.EINVAL
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return 0, err
	}
	if c.registeredOwners+len(c.uses) >= c.config.MaxOwners || len(s.bindings) >= s.options.MaxLockOwners || s.nextOwner == math.MaxUint64 {
		return 0, syscall.ENOLCK
	}
	s.nextOwner++
	scope.Token = strings.Clone(scope.Token)
	s.bindings[s.nextOwner] = ownerBinding{node: node, scope: scope, options: options}
	s.owners++
	c.registeredOwners++
	return s.nextOwner, nil
}

// OwnerNode validates an active registration without conferring access to it.
func (s *Session) OwnerNode(ctx context.Context, owner storage.UseOwner) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return 0, err
	}
	binding, ok := s.bindings[owner]
	if !ok {
		return 0, syscall.ESTALE
	}
	return binding.node, nil
}

// OwnerScope returns the original reference binding. A caller must validate the
// reference itself before using this scope; registration is not proof of liveness.
func (s *Session) OwnerScope(ctx context.Context, owner storage.UseOwner) (storage.UseScope, error) {
	if err := ctx.Err(); err != nil {
		return storage.UseScope{}, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return storage.UseScope{}, err
	}
	binding, ok := s.bindings[owner]
	if !ok {
		return storage.UseScope{}, syscall.ESTALE
	}
	return binding.scope, nil
}

func (s *Session) checkOwnerLocked(node uint64, owner storage.UseOwner) error {
	binding, ok := s.bindings[owner]
	if !ok {
		return syscall.ESTALE
	}
	if binding.node != node {
		return syscall.EINVAL
	}
	return nil
}

// RetireOwner releases all domains and pending actions without requiring an
// action-history slot. A repeated retirement of an allocated owner is harmless.
// Native reference retirement must precede this call for reference-owned owners.
func (s *Session) RetireOwner(ctx context.Context, owner storage.UseOwner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := s.coordinator
	c.mu.Lock()
	if owner == 0 || owner > s.nextOwner {
		c.mu.Unlock()
		return syscall.EINVAL
	}
	if binding, ok := s.bindings[owner]; ok {
		for _, domain := range []storage.ConflictDomain{storage.DomainRecord, storage.DomainWholeFile, storage.DomainEnforced} {
			c.dropLocked(s.key(binding.node, owner, domain))
		}
		delete(s.bindings, owner)
		s.owners--
		c.registeredOwners--
	}
	c.mu.Unlock()
	c.pump(ctx)
	return nil
}
