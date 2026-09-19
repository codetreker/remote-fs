package smb

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (c *connection) cleanup() {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.cancel()
	_ = c.net.Close()
	c.wg.Wait()
	c.mu.Lock()
	c.disconnected = true
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
	defer cancel()
	c.server.cleanupFailure(c.retryDisconnected(ctx))
}
func (c *connection) retryDisconnected(ctx context.Context) error {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	c.mu.Lock()
	if !c.disconnected {
		c.mu.Unlock()
		return nil
	}
	ss := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		ss = append(ss, s)
	}
	c.mu.Unlock()
	var errs []error
	for _, s := range ss {
		if err := c.cleanSession(ctx, s, false); err != nil {
			errs = append(errs, err)
		}
	}
	c.releaseDisconnected()
	return errors.Join(errs...)
}
func (c *connection) releaseDisconnected() {
	c.mu.Lock()
	clean := c.disconnected && len(c.sessions) == 0
	c.mu.Unlock()
	if clean {
		c.server.mu.Lock()
		delete(c.server.connections, c)
		c.server.mu.Unlock()
	}
}
func (c *connection) logoff(ctx context.Context, s *session) error {
	return c.cleanSession(ctx, s, true)
}
func (c *connection) cleanSession(ctx context.Context, s *session, explicit bool) error {
	return s.cleanup.run(ctx, func() error {
		s.logoffMu.Lock()
		defer s.logoffMu.Unlock()
		s.mu.Lock()
		if s.cleaned {
			s.mu.Unlock()
			if explicit {
				return ErrStopped
			}
			return nil
		}
		s.mu.Unlock()
		current, _ := ctx.Value(pendingFrameKey{}).(requestFrame)
		c.retireSessionRequests(s, current)
		var errs []error
		s.authMu.Lock()
		s.disarmAuthenticationLocked(true)
		if err := s.closeAuthenticationLocked(); err != nil {
			errs = append(errs, err)
		}
		s.authMu.Unlock()
		if err := s.waitAuthenticationWatcher(ctx); err != nil {
			errs = append(errs, err)
		}
		s.mu.Lock()
		ts := make([]*tree, 0, len(s.trees))
		for _, t := range s.trees {
			ts = append(ts, t)
		}
		s.mu.Unlock()
		for _, t := range ts {
			var err error
			if explicit {
				err = c.closeTreeAuthorized(ctx, t)
			} else {
				err = c.closeTreeContext(ctx, t)
			}
			if err != nil {
				errs = append(errs, err)
			} else {
				s.mu.Lock()
				if s.trees[t.id] == t {
					delete(s.trees, t.id)
				}
				s.mu.Unlock()
			}
		}
		s.mu.Lock()
		if err := c.closeOrphansLocked(ctx, s, nil); err != nil {
			errs = append(errs, err)
		}
		s.mu.Unlock()
		c.finishSessionRetirement(s)
		return errors.Join(errs...)
	})
}
func (c *connection) closeTree(t *tree) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
	defer cancel()
	return c.closeTreeContext(ctx, t)
}
func (c *connection) closeTreeAuthorized(ctx context.Context, t *tree) error {
	if t.kind == volumeTree {
		if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileSessionClose}); err != nil {
			return err
		}
		t.files.mu.Lock()
		count := len(t.files.handles)
		t.files.mu.Unlock()
		for range count {
			if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: t.export.share.Volume, Operation: storage.OpFileClose}); err != nil {
				return err
			}
		}
	}
	return c.closeTreeContext(ctx, t)
}
func (c *connection) closeTreeContext(ctx context.Context, t *tree) error {
	return t.cleanup.run(ctx, func() error {
		t.closeMu.Lock()
		defer t.closeMu.Unlock()
		if t.closed {
			return nil
		}
		t.stopOnce.Do(func() {
			if t.done != nil {
				close(t.done)
			}
		})
		c.mu.Lock()
		for _, p := range c.pending {
			if p.treeID == t.id && p.sessionID == t.sessionID && p.command != wire.Logoff && p.command != wire.TreeDisconnect {
				p.cancel()
			}
		}
		c.mu.Unlock()
		if t.kind == controlTree {
			t.closed = true
			return nil
		}
		a := t.authority
		a.treeCloseMu.Lock()
		defer a.treeCloseMu.Unlock()
		ctx = WithPrincipal(ctx, a.principal)
		a.installMu.Lock()
		a.mu.Lock()
		last := a.refs == 1
		if last {
			a.stopping = true
		}
		a.mu.Unlock()
		a.installMu.Unlock()
		var errs []error
		if last {
			if err := a.close(ctx); err != nil {
				errs = append(errs, err)
			}
			if err := a.wait(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if err := t.files.close(ctx); err != nil {
			errs = append(errs, err)
		}
		if err := errors.Join(errs...); err != nil {
			return err
		}
		a.mu.Lock()
		a.refs--
		a.mu.Unlock()
		t.closed = true
		c.server.mu.Lock()
		t.export.refs--
		c.server.mu.Unlock()
		return nil
	})
}

// The caller owns s.mu; native calls and waits run while that lock is released.
func (c *connection) closeOrphansLocked(ctx context.Context, s *session, export *Export) error {
	type entry struct {
		e *Export
		a *authoritySession
	}
	entries := make([]entry, 0, len(s.authorities))
	for e, a := range s.authorities {
		if export == nil || export == e {
			entries = append(entries, entry{e, a})
		}
	}
	s.mu.Unlock()
	var errs []error
	for _, item := range entries {
		a := item.a
		select {
		case <-a.ready:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			continue
		}
		a.treeCloseMu.Lock()
		a.mu.Lock()
		empty := a.refs == 0
		a.mu.Unlock()
		if empty {
			err := a.close(ctx)
			if err == nil {
				err = a.wait(ctx)
			}
			if err != nil {
				errs = append(errs, err)
			} else {
				a.releaseExport()
				s.mu.Lock()
				if s.authorities[item.e] == a {
					delete(s.authorities, item.e)
				}
				s.mu.Unlock()
			}
		}
		a.treeCloseMu.Unlock()
	}
	s.mu.Lock()
	return errors.Join(errs...)
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
func (c *connection) closeExport(ctx context.Context, e *Export) error {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	c.mu.Lock()
	ss := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		ss = append(ss, s)
	}
	c.mu.Unlock()
	var errs []error
	for _, s := range ss {
		s.mu.Lock()
		ts := make([]*tree, 0)
		for _, t := range s.trees {
			if t.export == e {
				ts = append(ts, t)
			}
		}
		s.mu.Unlock()
		for _, t := range ts {
			if err := c.closeTreeContext(ctx, t); err != nil {
				errs = append(errs, err)
			} else {
				s.mu.Lock()
				if s.trees[t.id] == t {
					delete(s.trees, t.id)
				}
				s.mu.Unlock()
			}
		}
		s.mu.Lock()
		if err := c.closeOrphansLocked(ctx, s, e); err != nil {
			errs = append(errs, err)
		}
		s.mu.Unlock()
		c.finishSessionRetirement(s)
	}
	return errors.Join(errs...)
}

// Completion records already-known cleanup only. It never retries a provider or
// authority, and callers must release admission/provider locks before entering.
func (c *connection) finishSessionRetirement(s *session) {
	s.authMu.Lock()
	watcherDone := s.authDone == nil
	if !watcherDone {
		select {
		case <-s.authDone:
			watcherDone = true
		default:
		}
	}
	s.mu.Lock()
	complete := s.retired && !s.finalizing && s.auth == nil && watcherDone && s.openingTrees == 0 && len(s.trees) == 0 && len(s.authorities) == 0
	if complete {
		s.cleaned = true
	}
	s.mu.Unlock()
	s.authMu.Unlock()
	if !complete {
		return
	}
	s.retirementMu.Lock()
	s.resourcesClosed = true
	s.retirementMu.Unlock()
	c.mu.Lock()
	c.pruneRetiredSessionsLocked()
	c.mu.Unlock()
	c.releaseDisconnected()
}
func (c *connection) retireSessionRequests(s *session, except requestFrame) {
	s.mu.Lock()
	// Retirement and frame enrollment must become visible together to pruning.
	// The session-to-connection lock order also matches authentication finalization.
	c.mu.Lock()
	s.retired = true
	s.mu.Unlock()
	s.retirementMu.Lock()
	if s.retiringFrames == nil {
		s.retiringFrames = make(map[requestFrame]struct{})
	}
	for _, p := range c.pending {
		if p.sessionID != s.id {
			continue
		}
		frame := requestFrame{connection: c, id: p.frame}
		s.retiringFrames[frame] = struct{}{}
		if frame != except && p.command != wire.Logoff {
			p.cancel()
		}
	}
	s.retirementMu.Unlock()
	c.mu.Unlock()
	s.stopAuthenticationWatcher()
}
