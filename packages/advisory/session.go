package advisory

import (
	"context"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Session is one owner incarnation. Leases are enforced by its native owner,
// which calls Retire on expiry; receipt polling never extends a lease.
type Session struct {
	coordinator             *Coordinator
	id                      uint64
	options                 storage.FileSessionOptions
	fence                   func() error
	epoch                   uint64
	epochUntil              time.Time
	actions                 map[storage.LockRequestID]*request
	owners, ranges, pending int
	retiring, retired       bool
	retireDone              chan struct{}
	retireErr               error
}

func (s *Session) activeLocked() error {
	if s.retired {
		return syscall.ESTALE
	}
	if s.retiring {
		return fmt.Errorf("advisory session publication rights are retired: %w", syscall.EIO)
	}
	return nil
}

func (s *Session) advanceLocked(now time.Time) error {
	if !now.Before(s.epochUntil) {
		if s.epoch == math.MaxUint64 {
			return syscall.EOVERFLOW
		}
		s.epoch++
		s.epochUntil = now.Add(s.options.History)
	}
	s.pruneHistoryLocked(now)
	return nil
}

func (s *Session) pruneHistoryLocked(now time.Time) {
	for id, action := range s.actions {
		retiredEpoch := action.epoch < s.epoch || !now.Before(s.epochUntil)
		if retiredEpoch && action.result.State != storage.LockPending && !now.Before(action.expires) {
			delete(s.actions, id)
			s.coordinator.requests--
		}
	}
}

// History reports the current admission epoch and its remaining lifetime.
// Terminal receipts survive for at least History after completion, including
// requests that remained pending across earlier admission epochs.
func (s *Session) History(ctx context.Context) (uint64, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := s.activeLocked(); err != nil {
		return 0, 0, err
	}
	now := c.now()
	if err := s.advanceLocked(now); err != nil {
		return 0, 0, err
	}
	return s.epoch, s.epochUntil.Sub(now), nil
}

func (s *Session) Epoch(ctx context.Context) (uint64, error) {
	epoch, _, err := s.History(ctx)
	return epoch, err
}

// IOHealth validates session continuity only. Ordinary I/O never participates
// in advisory conflict checks; native reference health is the caller's check.
func (s *Session) IOHealth(ctx context.Context, node uint64) error {
	if err := checkCall(ctx, node); err != nil {
		return err
	}
	c := s.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	return s.activeLocked()
}

// Retire rejects new actions before invoking the native publication fence.
// No grants are freed unless that fence succeeds. Failed fencing can be retried;
// the session remains unusable in between. Cancellation only stops waiting for
// another retirement and never reverses a retirement already begun.
func (s *Session) Retire(ctx context.Context) error {
	c := s.coordinator
	c.mu.Lock()
	if s.retired {
		c.mu.Unlock()
		return nil
	}
	if s.retireDone != nil {
		done := s.retireDone
		c.mu.Unlock()
		select {
		case <-done:
			c.mu.Lock()
			err := s.retireErr
			c.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	s.retiring = true
	s.retireDone = make(chan struct{})
	c.mu.Unlock()

	err := s.fence()

	c.mu.Lock()
	defer c.mu.Unlock()
	s.retireErr = err
	if err == nil {
		for key, state := range c.owners {
			if key.session == s.id {
				c.ranges -= len(state.ranges)
				delete(c.owners, key)
			}
		}
		remaining := c.waiting[:0]
		for _, action := range c.waiting {
			if action.key.session != s.id {
				remaining = append(remaining, action)
			}
		}
		clear(c.waiting[len(remaining):])
		c.waiting = remaining
		c.requests -= len(s.actions)
		clear(s.actions)
		s.owners, s.ranges, s.pending = 0, 0, 0
		s.retired = true
		delete(c.sessions, s.id)
		c.pumpLocked()
	}
	close(s.retireDone)
	s.retireDone = nil
	return err
}
