package advisory

import (
	"context"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type useClaim struct {
	node  uint64
	claim storage.UseClaim
}

// AddUse is entered under native open/retention ordering with a freshly minted
// reference token. Tokens are never reused. This claim has the native reference's
// lifetime; it does not create a coordinator session or an independent lease.
func (c *Coordinator) AddUse(ctx context.Context, node uint64, scope storage.UseScope, claim storage.UseClaim) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	if err := scope.Check(); err != nil {
		return err
	}
	if err := claim.Check(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.uses[scope]; ok {
		if old.node != node || old.claim != claim {
			return syscall.EINVAL
		}
		return nil
	}
	for _, other := range c.uses {
		if other.node == node && (claim.Uses&other.claim.Deny != 0 || other.claim.Uses&claim.Deny != 0) {
			return storage.ErrUseConflict
		}
	}
	if len(c.uses)+c.registeredOwners >= c.config.MaxOwners {
		return syscall.ENOLCK
	}
	scope.Token = strings.Clone(scope.Token)
	c.uses[scope] = useClaim{node: node, claim: claim}
	return nil
}

// DropUse runs under the native gate only after reference fencing and accepted
// close-intent transitions succeed. It performs no callbacks or waiter pumping.
// Unknown cleanup must retain the claim and its resource charge.
func (c *Coordinator) DropUse(ctx context.Context, node uint64, scope storage.UseScope) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	if err := scope.Check(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.uses[scope]; ok {
		if old.node != node {
			return syscall.EINVAL
		}
		delete(c.uses, scope)
	}
	return nil
}

func (c *Coordinator) checkUseLocked(node uint64, scope storage.UseScope, uses storage.Uses) error {
	if scope.Token != "" {
		own, ok := c.uses[scope]
		if !ok || own.node != node {
			return storage.ErrInvalidScope
		}
		if uses&^own.claim.Uses != 0 {
			return syscall.EBADF
		}
	}
	for otherScope, other := range c.uses {
		if other.node == node && otherScope != scope && uses&other.claim.Deny != 0 {
			return storage.ErrUseConflict
		}
	}
	return nil
}

// CheckUse is called under native observation/publication ordering. A nonempty
// scope must already be validated against the actual reference and caller; an
// anonymous operation uses the zero scope and receives no holder exemption.
func (c *Coordinator) CheckUse(ctx context.Context, node uint64, scope storage.UseScope, uses storage.Uses) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	if uses&^storage.AllUses != 0 {
		return syscall.EINVAL
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkUseLocked(node, scope, uses)
}

// CheckIO applies use claims and enforced ranges to the actual byte operation.
// It shares the native gate with revision capture or final publication, and must
// be repeated when a read captures a different revision. Advisory domains do not
// participate. The actual native scope determines self-access for each claim.
func (c *Coordinator) CheckIO(ctx context.Context, node uint64, scope storage.UseScope, extent storage.Range, uses storage.Uses) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	if err := extent.Check(); err != nil {
		return err
	}
	if extent.Kind != storage.Bytes || uses == 0 || uses&^(storage.ReadData|storage.WriteData) != 0 {
		return syscall.EINVAL
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUseLocked(node, scope, uses); err != nil {
		return err
	}
	for key, owner := range c.owners {
		if key.node != node || key.domain != storage.DomainEnforced {
			continue
		}
		binding := c.sessions[key.session].bindings[key.owner]
		for _, held := range owner.ranges {
			if !overlaps(held.command.Range, extent) {
				continue
			}
			deny := held.command.Policy.DenyOthers
			if scope.Token != "" && binding.scope == scope {
				deny = held.command.Policy.DenySelf
			}
			if uses&deny != 0 {
				return storage.ErrRangeConflict
			}
		}
	}
	return nil
}
