package advisory

import (
	"context"
	"slices"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Session) key(node uint64, owner storage.UseOwner, domain storage.ConflictDomain) ownerKey {
	return ownerKey{actor: actor{session: s.id, owner: owner}, node: node, domain: domain}
}

// GetConflict observes an actual conflict or its confirmed absence under native
// ordering. It never registers a request or changes a protection.
func (s *Session) GetConflict(ctx context.Context, node uint64, owner storage.UseOwner, command storage.RangeCommand, order Order) (storage.RangeConflict, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.RangeConflict{}, err
	}
	if err := command.Check(); err != nil {
		return storage.RangeConflict{}, err
	}
	if order == nil || command.Edit == storage.Subtract || command.Edit == storage.RemoveExact {
		return storage.RangeConflict{}, syscall.EINVAL
	}
	var result storage.RangeConflict
	err := order(ctx, func() error {
		c := s.coordinator
		c.mu.Lock()
		defer c.mu.Unlock()
		if err := s.activeLocked(); err != nil {
			return err
		}
		if err := s.checkOwnerLocked(node, owner); err != nil {
			return err
		}
		result = c.conflictLocked(s.key(node, owner, command.Domain), command)
		return nil
	})
	return result, err
}

func (c *Coordinator) checkCommands(commands []storage.RangeCommand) error {
	if len(commands) == 0 || len(commands) > c.config.MaxCommands || len(commands) > storage.MaxRangeCommands {
		return syscall.EINVAL
	}
	for _, command := range commands {
		if err := command.Check(); err != nil {
			return err
		}
		if command.Domain != commands[0].Domain || command.Wait != commands[0].Wait {
			return syscall.EINVAL
		}
		if command.Conversion == storage.DropBeforeAcquire && len(commands) != 1 {
			return syscall.EINVAL
		}
		if command.Domain != storage.DomainEnforced && command.Range.Kind != storage.Bytes {
			return syscall.EINVAL
		}
	}
	return nil
}

// Apply retains the original command batch and result by request identity.
// order is retained only while waiting and receives each retry's current context.
// A reused request never reexecutes commands; changed intent is rejected.
func (s *Session) Apply(ctx context.Context, node uint64, owner storage.UseOwner, commands []storage.RangeCommand, id storage.LockRequestID, order Order) (storage.RangeAttempt, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.RangeAttempt{}, err
	}
	c := s.coordinator
	if err := c.checkCommands(commands); err != nil {
		return storage.RangeAttempt{}, err
	}
	epoch, err := id.Epoch()
	if err != nil || order == nil {
		if err != nil {
			return storage.RangeAttempt{}, err
		}
		return storage.RangeAttempt{}, syscall.EINVAL
	}
	commands = slices.Clone(commands)
	var result storage.RangeAttempt
	var current *request
	err = order(ctx, func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err := s.activeLocked(); err != nil {
			return err
		}
		if err := s.checkOwnerLocked(node, owner); err != nil {
			return err
		}
		now := c.now()
		if err := s.advanceLocked(now); err != nil {
			return err
		}
		key := s.key(node, owner, commands[0].Domain)
		if old := s.actions[id]; old != nil {
			if old.key != key || !slices.Equal(old.commands, commands) {
				return syscall.EINVAL
			}
			result = s.receiptLocked(old, now)
			current = old
			return nil
		}
		if epoch != s.epoch {
			return syscall.ESTALE
		}
		if c.requests >= c.config.MaxRequests {
			for _, session := range c.sessions {
				session.pruneHistoryLocked(now)
			}
		}
		if len(s.actions) >= s.options.MaxLockActions || c.requests >= c.config.MaxRequests {
			return syscall.ENOLCK
		}
		id = storage.LockRequestID(strings.Clone(string(id)))
		for i := range commands {
			commands[i].Claim = storage.ClaimID(strings.Clone(string(commands[i].Claim)))
		}
		action := &request{key: key, id: id, epoch: epoch, commands: commands, order: order,
			result:   storage.RangeAttempt{Request: id, Commands: slices.Clone(commands), State: storage.Pending},
			released: make([]bool, len(commands))}
		s.actions[id] = action
		current = action
		c.requests++
		c.startLocked(action)
		result = s.receiptLocked(action, c.now())
		return nil
	})
	if err == nil {
		c.pump(ctx)
		if err = c.advance(ctx, current); err == nil {
			c.mu.Lock()
			if err = s.activeLocked(); err == nil {
				if s.actions[id] != current {
					err = syscall.ESTALE
				} else {
					result = s.receiptLocked(current, c.now())
				}
			}
			if err != nil {
				result = storage.RangeAttempt{}
			}
			c.mu.Unlock()
		}
	}
	return result, err
}
