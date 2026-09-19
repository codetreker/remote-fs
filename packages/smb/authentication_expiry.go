package smb

import (
	"context"
	"math"
	"syscall"
	"time"
)

// Each registered session owns one watcher. Exchange generations share this
// owner, so canceled timers cannot accumulate callbacks behind provider calls.
func (c *connection) startAuthenticationWatcherLocked(s *session) {
	s.authWake = make(chan struct{}, 1)
	s.authStop = make(chan struct{})
	s.authDone = make(chan struct{})
	s.authGeneration = 1
	s.authArmed = true
	c.wg.Add(1)
	go c.watchAuthentication(s)
}
func (s *session) wakeAuthenticationLocked() {
	if s.authWake != nil {
		select {
		case s.authWake <- struct{}{}:
		default:
		}
	}
}
func (s *session) stopAuthenticationWatcher() {
	if s.authStop != nil {
		s.authStopOnce.Do(func() { close(s.authStop) })
	}
}
func (s *session) armAuthenticationLocked(timeout time.Duration) error {
	if s.authGeneration == math.MaxUint64 {
		return syscall.ENOMEM
	}
	s.authGeneration++
	s.deadline = time.Now().Add(timeout)
	s.authArmed = true
	s.authExpired = false
	s.wakeAuthenticationLocked()
	return nil
}
func (s *session) disarmAuthenticationLocked(expired bool) {
	s.authArmed = false
	s.authExpired = expired
	s.wakeAuthenticationLocked()
}
func (s *session) closeAuthenticationLocked() error {
	if s.auth == nil {
		return nil
	}
	if err := s.auth.Close(); err != nil {
		return err
	}
	s.auth = nil
	return nil
}
func (c *connection) watchAuthentication(s *session) {
	defer c.wg.Done()
	defer func() { close(s.authDone); c.finishSessionRetirement(s) }()
	for {
		s.authMu.Lock()
		armed, generation, deadline := s.authArmed, s.authGeneration, s.deadline
		s.authMu.Unlock()
		var timer *time.Timer
		var expired <-chan time.Time
		if armed {
			timer = time.NewTimer(time.Until(deadline))
			expired = timer.C
		}
		timedOut := false
		select {
		case <-c.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-s.authStop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-s.authWake:
		case <-expired:
			timedOut = true
		}
		if timer != nil {
			timer.Stop()
		}
		if timedOut && c.expireAuthentication(s, generation) {
			return
		}
	}
}
func (c *connection) expireAuthentication(s *session, generation uint64) bool {
	s.authMu.Lock()
	s.mu.Lock()
	retired := s.retired
	s.mu.Unlock()
	if retired {
		s.authMu.Unlock()
		return true
	}
	if !s.authArmed || s.authGeneration != generation || time.Now().Before(s.deadline) {
		s.authMu.Unlock()
		return false
	}
	s.disarmAuthenticationLocked(true)
	s.identityMu.RLock()
	initial := s.signer == nil
	s.identityMu.RUnlock()
	err := s.closeAuthenticationLocked()
	s.authMu.Unlock()
	c.server.cleanupFailure(err)
	if initial {
		c.retireSessionRequests(s, requestFrame{})
		return true
	}
	return false
}

// Provider calls and final session installation share the same absolute
// deadline. Request cancellation still propagates independently of the watcher.
func (s *session) authenticationContextLocked(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, s.deadline)
}

func (s *session) waitAuthenticationWatcher(ctx context.Context) error {
	if s.authDone == nil {
		return nil
	}
	select {
	case <-s.authDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
