package advisory

import (
	"context"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Session) key(node uint64, owner storage.LockOwner, family storage.LockFamily) ownerKey {
	return ownerKey{actor: actor{session: s.id, owner: owner}, node: node, family: family}
}

// Get returns one actual conflicting range. Cross-session PIDs are reported as
// zero because a remote process ID has no meaning in the querying PID namespace.
func (s *Session) Get(ctx context.Context, node uint64, owner storage.LockOwner, lock storage.FileLock) (storage.LockConflict, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.LockConflict{}, err
	}
	if err := lock.Check(); err != nil {
		return storage.LockConflict{}, err
	}
	if lock.Type == storage.Unlock {
		return storage.LockConflict{}, syscall.EINVAL
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return storage.LockConflict{}, err
	}
	return c.conflictLocked(s.key(node, owner, lock.Family), lock), nil
}

// Set returns immediately, including for waiting locks. Reusing an action ID
// returns its retained outcome and never reapplies its lock. Unknown requests
// from an earlier epoch fail ESTALE even after their receipt has been reclaimed.
func (s *Session) Set(ctx context.Context, node uint64, owner storage.LockOwner, lock storage.FileLock, id storage.LockRequestID) (storage.LockAttempt, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.LockAttempt{}, err
	}
	if err := lock.Check(); err != nil {
		return storage.LockAttempt{}, err
	}
	epoch, err := id.Epoch()
	if err != nil {
		return storage.LockAttempt{}, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return storage.LockAttempt{}, err
	}
	now := c.now()
	if err := s.advanceLocked(now); err != nil {
		return storage.LockAttempt{}, err
	}
	key := s.key(node, owner, lock.Family)
	if old := s.actions[id]; old != nil {
		if old.key != key || old.lock != lock {
			return storage.LockAttempt{}, syscall.EINVAL
		}
		return s.receiptLocked(old, now), nil
	}
	if epoch != s.epoch {
		return storage.LockAttempt{}, syscall.ESTALE
	}
	if c.requests >= c.config.MaxRequests {
		for _, session := range c.sessions {
			session.pruneHistoryLocked(now)
		}
	}
	if len(s.actions) >= s.options.MaxLockActions || c.requests >= c.config.MaxRequests {
		return storage.LockAttempt{}, syscall.ENOLCK
	}
	action := &request{key: key, id: id, epoch: epoch, lock: lock,
		result: storage.LockAttempt{Request: id, Lock: lock}}
	s.actions[id] = action
	c.requests++
	state, err := c.ownerLocked(key)
	if err != nil {
		c.completeLocked(action, storage.LockRejected, syscall.ENOLCK)
		return s.receiptLocked(action, now), nil
	}
	defer c.pruneOwnerLocked(key)

	// Linux flock conversion drops the old whole-file lock before attempting
	// the new mode, including unsuccessful nonblocking conversion.
	if lock.Family == storage.Flock && len(state.ranges) > 0 && state.ranges[0].Type != lock.Type {
		_ = c.replaceLocked(key, nil)
		c.pumpLocked()
	}
	if lock.Type == storage.Unlock {
		if err := c.replaceLocked(key, replaceRanges(state.ranges, lock)); err != nil {
			c.completeLocked(action, storage.LockRejected, syscall.ENOLCK)
		} else {
			c.completeLocked(action, storage.LockReleased, 0)
			c.pumpLocked()
		}
		return s.receiptLocked(action, c.now()), nil
	}
	conflict := c.conflictLocked(key, lock)
	if !conflict.Found {
		if err := c.replaceLocked(key, replaceRanges(state.ranges, lock)); err != nil {
			c.completeLocked(action, storage.LockRejected, syscall.ENOLCK)
		} else {
			c.completeLocked(action, storage.LockGranted, 0)
			c.pumpLocked()
		}
		return s.receiptLocked(action, c.now()), nil
	}
	action.result.Conflict = conflict
	if !lock.Wait {
		c.completeLocked(action, storage.LockRejected, syscall.EAGAIN)
		return s.receiptLocked(action, c.now()), nil
	}
	if len(c.waiting) >= c.config.MaxWaiters || s.pending >= s.options.MaxPendingLocks || s.pending >= s.options.MaxWaiters {
		c.completeLocked(action, storage.LockRejected, syscall.ENOLCK)
		return s.receiptLocked(action, c.now()), nil
	}
	if lock.Family == storage.POSIX {
		deadlock, bounded := c.deadlockLocked(action)
		if deadlock || !bounded {
			errno := syscall.ENOLCK
			if deadlock {
				errno = syscall.EDEADLK
			}
			c.completeLocked(action, storage.LockRejected, errno)
			return s.receiptLocked(action, c.now()), nil
		}
	}
	action.result.State = storage.LockPending
	state.pending++
	s.pending++
	c.waiting = append(c.waiting, action)
	return s.receiptLocked(action, c.now()), nil
}

func (c *Coordinator) completeLocked(action *request, state storage.LockAttemptState, errno syscall.Errno) {
	action.result.State = state
	action.result.Errno = errno
	if state == storage.LockGranted {
		action.result.EverGranted = true
		action.result.Conflict = storage.LockConflict{}
	}
	action.expires = c.now().Add(c.sessions[action.key.session].options.History)
}

func (s *Session) receiptLocked(action *request, now time.Time) storage.LockAttempt {
	result := action.result
	if result.State == storage.LockPending {
		result.HistoryRemaining = s.options.History
	} else {
		result.HistoryRemaining = max(0, action.expires.Sub(now))
	}
	return result
}

func (s *Session) findLocked(node uint64, owner storage.LockOwner, id storage.LockRequestID) (*request, error) {
	if _, err := id.Epoch(); err != nil {
		return nil, err
	}
	if err := s.activeLocked(); err != nil {
		return nil, err
	}
	if err := s.advanceLocked(s.coordinator.now()); err != nil {
		return nil, err
	}
	action := s.actions[id]
	if action == nil {
		return nil, syscall.ESTALE
	}
	if action.key.node != node || action.key.owner != owner {
		return nil, syscall.EINVAL
	}
	return action, nil
}

func (s *Session) Query(ctx context.Context, node uint64, owner storage.LockOwner, id storage.LockRequestID) (storage.LockAttempt, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.LockAttempt{}, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	action, err := s.findLocked(node, owner, id)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	return s.receiptLocked(action, c.now()), nil
}

// Cancel settles a pending action without acquiring it. If granting won the
// race it returns Granted and leaves that acquisition intact; the caller must
// report success or explicitly unlock it before returning EINTR.
func (s *Session) Cancel(ctx context.Context, node uint64, owner storage.LockOwner, id storage.LockRequestID) (storage.LockAttempt, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.LockAttempt{}, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	action, err := s.findLocked(node, owner, id)
	if err != nil {
		return storage.LockAttempt{}, err
	}
	if action.result.State == storage.LockPending {
		c.completeLocked(action, storage.LockCancelled, 0)
		c.pumpLocked()
	}
	return s.receiptLocked(action, c.now()), nil
}

// Drop performs the kernel's owner cleanup for exactly one file and lock family.
// It cancels that owner's pending requests as well as releasing held ranges.
func (s *Session) Drop(ctx context.Context, node uint64, owner storage.LockOwner, family storage.LockFamily) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	if family != storage.Flock && family != storage.POSIX {
		return syscall.EINVAL
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return err
	}
	key := s.key(node, owner, family)
	if state := c.owners[key]; state != nil {
		_ = c.replaceLocked(key, nil)
	}
	for _, action := range s.actions {
		if action.key == key {
			switch action.result.State {
			case storage.LockPending:
				c.completeLocked(action, storage.LockCancelled, 0)
			case storage.LockGranted:
				c.completeLocked(action, storage.LockReleased, 0)
			}
		}
	}
	c.pumpLocked()
	c.pruneOwnerLocked(key)
	return nil
}

func (c *Coordinator) pumpLocked() {
	// A granted range replacement can downgrade an earlier exclusive range.
	// Each productive pass removes at least one request, bounding the passes
	// by the admitted waiter count while waking newly compatible predecessors.
	for len(c.waiting) > 0 {
		before := len(c.waiting)
		c.pumpPassLocked()
		if len(c.waiting) == before {
			return
		}
	}
}

func (c *Coordinator) pumpPassLocked() {
	remaining := c.waiting[:0]
	for _, action := range c.waiting {
		state := c.owners[action.key]
		session := c.sessions[action.key.session]
		if action.result.State == storage.LockPending && !session.retiring {
			conflict := c.conflictLocked(action.key, action.lock)
			if conflict.Found {
				action.result.Conflict = conflict
			} else if err := c.replaceLocked(action.key, replaceRanges(state.ranges, action.lock)); err != nil {
				c.completeLocked(action, storage.LockRejected, syscall.ENOLCK)
			} else {
				c.completeLocked(action, storage.LockGranted, 0)
			}
		}
		if action.result.State == storage.LockPending {
			remaining = append(remaining, action)
		} else {
			state.pending--
			session.pending--
			c.pruneOwnerLocked(action.key)
		}
	}
	clear(c.waiting[len(remaining):])
	c.waiting = remaining
}
