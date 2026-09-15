package smb

import (
	"context"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func (c *connection) logoff(ctx context.Context, s *session) error {
	s.logoffMu.Lock()
	defer s.logoffMu.Unlock()
	s.mu.Lock()
	if s.cleaned {
		s.mu.Unlock()
		return ErrStopped
	}
	s.retired = true
	s.mu.Unlock()
	frame, hasFrame := ctx.Value(pendingFrameKey{}).(requestFrame)
	c.mu.Lock()
	s.retirementMu.Lock()
	if s.retiringFrames == nil {
		s.retiringFrames = make(map[uint64]struct{})
	}
	for _, p := range c.pending {
		if p.sessionID == s.id {
			s.retiringFrames[p.frame] = struct{}{}
			if (!hasFrame || frame.connection != c || p.frame != frame.id) && p.command != wire.Logoff {
				p.cancel()
			}
		}
	}
	s.retirementMu.Unlock()
	c.mu.Unlock()
	s.authMu.Lock()
	if s.auth != nil {
		err := s.auth.Close()
		if err != nil {
			s.authMu.Unlock()
			return err
		}
		s.auth = nil
	}
	s.authMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.trees {
		if err := c.closeTree(t); err != nil {
			return err
		}
		delete(s.trees, id)
	}
	if err := c.closeOrphansLocked(ctx, s, nil); err != nil {
		return err
	}
	clear(s.authorities)
	s.cleaned = true
	s.retirementMu.Lock()
	s.resourcesClosed = true
	s.retirementMu.Unlock()
	c.mu.Lock()
	c.pruneRetiredSessionsLocked()
	c.mu.Unlock()
	return nil
}

func (c *connection) closeOrphansLocked(ctx context.Context, s *session, export *Export) error {
	for e, a := range s.authorities {
		if export != nil && e != export {
			continue
		}
		a.mu.Lock()
		if a.orphan {
			var err error
			if !a.isClosed() {
				err = a.close(ctx)
			}
			if err != nil {
				a.mu.Unlock()
				return err
			}
			a.orphan = false
			a.refs = 0
			c.server.mu.Lock()
			e.refs--
			c.server.mu.Unlock()
		}
		if a.isClosed() {
			a.releaseLeaseOrphans()
		}
		closed := a.isClosed() && a.refs == 0
		a.mu.Unlock()
		if closed {
			delete(s.authorities, e)
		}
	}
	return nil
}

func (c *connection) pruneRetiredSessionsLocked() {
	for _, s := range c.sessions {
		s.retirementMu.Lock()
		done := s.resourcesClosed && len(s.retiringFrames) == 0
		s.retirementMu.Unlock()
		if done {
			c.removeSessionLocked(s)
			s.identityMu.Lock()
			if s.signer != nil {
				s.signer.Destroy()
				s.signer = nil
			}
			s.identityMu.Unlock()
		}
	}
}
