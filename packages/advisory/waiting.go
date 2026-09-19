package advisory

import (
	"context"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *Coordinator) advance(ctx context.Context, action *request) error {
	c.mu.Lock()
	s := c.sessions[action.key.session]
	if s == nil || s.retiring || action.result.State != storage.Pending || action.order == nil {
		c.mu.Unlock()
		return nil
	}
	order := action.order
	c.mu.Unlock()
	return order(ctx, func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if action.result.State != storage.Pending || c.sessions[s.id] != s {
			return nil
		}
		if err := s.activeLocked(); err != nil {
			return err
		}
		if err := s.checkOwnerLocked(action.key.node, action.key.owner); err != nil {
			return err
		}
		if action.converting {
			action.converting = false
			c.startLocked(action)
			return nil
		}
		edit := c.draftLocked(action)
		if edit.rejected == storage.RangeBlocked {
			action.result.Conflict = edit.conflict
		} else if edit.rejected != "" {
			c.completeLocked(action, storage.Rejected, edit.rejected)
		} else {
			c.commitEditLocked(action, edit)
		}
		return nil
	})
}

// A wakeup selects candidates only. Each candidate independently reenters its
// native ordering before the state mutex is taken and conflicts are rechecked.
// A failed native check leaves that request pending for Query or Cancel to settle.
func (c *Coordinator) pump(ctx context.Context) {
	c.mu.Lock()
	limit := len(c.waiting)
	c.mu.Unlock()
	for pass := 0; pass <= limit; pass++ {
		c.mu.Lock()
		before := len(c.waiting)
		candidates := make([]*request, 0, before)
		for _, action := range c.waiting {
			if c.readyLocked(action) {
				candidates = append(candidates, action)
			}
		}
		c.mu.Unlock()
		if len(candidates) == 0 {
			return
		}
		for _, action := range candidates {
			if ctx.Err() != nil {
				return
			}
			_ = c.advance(ctx, action)
		}
		c.mu.Lock()
		remaining := len(c.waiting)
		c.mu.Unlock()
		if remaining >= before {
			return
		}
	}
}

func (c *Coordinator) readyLocked(action *request) bool {
	if s := c.sessions[action.key.session]; s == nil || s.retiring {
		return false
	}
	for _, command := range action.commands {
		if command.Edit != storage.Replace && command.Edit != storage.AddExact {
			continue
		}
		if c.conflictLocked(action.key, command).Found {
			return false
		}
	}
	return true
}

func (c *Coordinator) dropLocked(key ownerKey) {
	if c.owners[key] != nil {
		_ = c.replaceLocked(key, nil)
	}
	for _, action := range c.sessions[key.session].actions {
		if action.key != key {
			continue
		}
		switch action.result.State {
		case storage.Pending:
			c.completeLocked(action, storage.Cancelled, "")
		case storage.Granted:
			c.completeLocked(action, storage.Released, "")
		}
	}
	c.pruneOwnerLocked(key)
}

// Drop removes only the registered owner's selected domain. Call it after any
// native retirement gate has been released: waking a waiter can enter that gate.
func (s *Session) Drop(ctx context.Context, node uint64, owner storage.UseOwner, domain storage.ConflictDomain) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	if domain < storage.DomainRecord || domain > storage.DomainEnforced {
		return syscall.EINVAL
	}
	c := s.coordinator
	c.mu.Lock()
	if err := s.activeLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	if err := s.checkOwnerLocked(node, owner); err != nil {
		c.mu.Unlock()
		return err
	}
	c.dropLocked(s.key(node, owner, domain))
	c.mu.Unlock()
	c.pump(ctx)
	return nil
}
