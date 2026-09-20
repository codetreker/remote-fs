package advisory

import (
	"context"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *Coordinator) completeLocked(action *request, state storage.AttemptState, rejection storage.RejectionCode) {
	action.result.State = state
	action.result.Rejection = rejection
	if state == storage.Granted {
		action.result.EverGranted = true
		action.result.Conflict = storage.RangeConflict{}
	}
	action.expires = c.now().Add(c.sessions[action.key.session].options.History)
	action.order = nil
	if action.pending {
		action.pending = false
		c.owners[action.key].pending--
		c.sessions[action.key.session].pending--
		for i, queued := range c.waiting {
			if queued == action {
				copy(c.waiting[i:], c.waiting[i+1:])
				c.waiting[len(c.waiting)-1] = nil
				c.waiting = c.waiting[:len(c.waiting)-1]
				break
			}
		}
		c.pruneOwnerLocked(action.key)
	}
}

func (s *Session) receiptLocked(action *request, now time.Time) storage.RangeAttempt {
	result := action.result.Clone()
	if result.State == storage.Pending {
		result.HistoryRemaining = s.options.History
	} else {
		result.HistoryRemaining = max(0, action.expires.Sub(now))
	}
	return result
}

func (s *Session) findLocked(node uint64, owner storage.UseOwner, id storage.LockRequestID) (*request, error) {
	action, err := s.requestLocked(owner, id)
	if err != nil {
		return nil, err
	}
	if action.key.node != node {
		return nil, syscall.EINVAL
	}
	return action, nil
}

func (s *Session) requestLocked(owner storage.UseOwner, id storage.LockRequestID) (*request, error) {
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
	if action.key.owner != owner {
		return nil, syscall.EINVAL
	}
	return action, nil
}

// RequestNode resolves retained action history even after its owner registration
// has been retired. It does not restore the owner or admit a new range action.
func (s *Session) RequestNode(ctx context.Context, owner storage.UseOwner, id storage.LockRequestID) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	action, err := s.requestLocked(owner, id)
	if err != nil {
		return 0, err
	}
	return action.key.node, nil
}

// Query reconciles a request and may grant its pending acquisition under the
// request's original native ordering callback. It does not extend any lease.
func (s *Session) Query(ctx context.Context, node uint64, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.RangeAttempt{}, err
	}
	c := s.coordinator
	c.mu.Lock()
	action, err := s.findLocked(node, owner, id)
	c.mu.Unlock()
	if err != nil {
		return storage.RangeAttempt{}, err
	}
	if err := c.advance(ctx, action); err != nil {
		return storage.RangeAttempt{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return storage.RangeAttempt{}, err
	}
	return s.receiptLocked(action, c.now()), nil
}

// Cancel proves cancellation only when acquisition has not won. A winning grant
// remains intact and is returned as Granted; no request-history slot is needed.
func (s *Session) Cancel(ctx context.Context, node uint64, owner storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	if err := checkCall(ctx, node); err != nil {
		return storage.RangeAttempt{}, err
	}
	c := s.coordinator
	c.mu.Lock()
	action, err := s.findLocked(node, owner, id)
	if err != nil {
		c.mu.Unlock()
		return storage.RangeAttempt{}, err
	}
	if action.result.State == storage.Pending {
		c.completeLocked(action, storage.Cancelled, "")
	}
	result := s.receiptLocked(action, c.now())
	c.mu.Unlock()
	c.pump(ctx)
	return result, nil
}
