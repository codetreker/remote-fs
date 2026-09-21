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
	sessions := make([]*session, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessions = append(sessions, session)
	}
	c.mu.Unlock()
	var errs []error
	for _, session := range sessions {
		if err := c.cleanSession(ctx, session, false); err != nil {
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
		trees := make([]*tree, 0, len(s.trees))
		for _, tree := range s.trees {
			trees = append(trees, tree)
		}
		s.mu.Unlock()
		for _, tree := range trees {
			var err error
			if explicit {
				err = c.closeTreeAuthorized(ctx, tree)
			} else {
				err = c.closeTreeContext(ctx, tree)
			}
			if err != nil {
				errs = append(errs, err)
				continue
			}
			s.mu.Lock()
			if s.trees[tree.id] == tree {
				delete(s.trees, tree.id)
			}
			s.mu.Unlock()
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

func (c *connection) closeTree(tree *tree) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.server.config.Limits.CleanupTimeout)
	defer cancel()
	return c.closeTreeContext(ctx, tree)
}

func (c *connection) closeTreeAuthorized(ctx context.Context, tree *tree) error {
	if tree.kind == volumeTree {
		if err := c.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
			Volume: tree.export.share.Volume, Operation: storage.OpFileSessionClose,
		}); err != nil {
			return err
		}
	}
	return c.closeTreeContext(ctx, tree)
}

func (c *connection) closeTreeContext(ctx context.Context, tree *tree) error {
	return tree.cleanup.run(ctx, func() error {
		tree.closeMu.Lock()
		defer tree.closeMu.Unlock()
		if tree.closed {
			return nil
		}
		tree.stopOnce.Do(func() { close(tree.done) })
		c.mu.Lock()
		for _, pending := range c.pending {
			if pending.treeID == tree.id && pending.sessionID == tree.sessionID &&
				pending.command != wire.Logoff && pending.command != wire.TreeDisconnect {
				pending.cancel()
			}
		}
		c.mu.Unlock()
		if tree.kind == controlTree {
			tree.closed = true
			return nil
		}

		authority := tree.authority
		authority.treeCloseMu.Lock()
		defer authority.treeCloseMu.Unlock()
		ctx = WithPrincipal(ctx, authority.principal)
		authority.installMu.Lock()
		authority.mu.Lock()
		last := authority.refs == 1
		if last {
			authority.stopping = true
		}
		authority.mu.Unlock()
		authority.installMu.Unlock()
		if last {
			if err := authority.close(ctx); err != nil {
				return err
			}
			if err := authority.wait(ctx); err != nil {
				return err
			}
		}
		authority.mu.Lock()
		authority.refs--
		authority.mu.Unlock()
		tree.closed = true
		c.server.mu.Lock()
		tree.export.refs--
		c.server.mu.Unlock()
		return nil
	})
}

// The caller owns s.mu; native calls and waits run while that lock is released.
func (c *connection) closeOrphansLocked(ctx context.Context, s *session, selected *Export) error {
	type entry struct {
		export    *Export
		authority *authoritySession
	}
	entries := make([]entry, 0, len(s.authorities))
	for export, authority := range s.authorities {
		if selected == nil || selected == export {
			entries = append(entries, entry{export: export, authority: authority})
		}
	}
	s.mu.Unlock()
	var errs []error
	for _, item := range entries {
		select {
		case <-item.authority.ready:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			continue
		}
		item.authority.treeCloseMu.Lock()
		item.authority.mu.Lock()
		empty := item.authority.refs == 0
		item.authority.mu.Unlock()
		if empty {
			err := item.authority.close(ctx)
			if err == nil {
				err = item.authority.wait(ctx)
			}
			if err != nil {
				errs = append(errs, err)
			} else {
				item.authority.releaseExport()
				s.mu.Lock()
				if s.authorities[item.export] == item.authority {
					delete(s.authorities, item.export)
				}
				s.mu.Unlock()
			}
		}
		item.authority.treeCloseMu.Unlock()
	}
	s.mu.Lock()
	return errors.Join(errs...)
}

func (c *connection) pruneRetiredSessionsLocked() {
	for _, s := range c.sessions {
		s.retirementMu.Lock()
		done := s.resourcesClosed && len(s.retiringFrames) == 0
		s.retirementMu.Unlock()
		if !done {
			continue
		}
		c.removeSessionLocked(s)
		s.identityMu.Lock()
		if s.signer != nil {
			s.signer.Destroy()
			s.signer = nil
		}
		s.identityMu.Unlock()
	}
}

func (c *connection) closeExport(ctx context.Context, export *Export) error {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	c.mu.Lock()
	sessions := make([]*session, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessions = append(sessions, session)
	}
	c.mu.Unlock()
	var errs []error
	for _, session := range sessions {
		session.mu.Lock()
		trees := make([]*tree, 0)
		for _, tree := range session.trees {
			if tree.export == export {
				trees = append(trees, tree)
			}
		}
		session.mu.Unlock()
		for _, tree := range trees {
			if err := c.closeTreeContext(ctx, tree); err != nil {
				errs = append(errs, err)
			} else {
				session.mu.Lock()
				if session.trees[tree.id] == tree {
					delete(session.trees, tree.id)
				}
				session.mu.Unlock()
			}
		}
		session.mu.Lock()
		if err := c.closeOrphansLocked(ctx, session, export); err != nil {
			errs = append(errs, err)
		}
		session.mu.Unlock()
		c.finishSessionRetirement(session)
	}
	return errors.Join(errs...)
}

// Completion only records already-known cleanup. It never retries native work.
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
	complete := s.retired && !s.finalizing && s.auth == nil && watcherDone &&
		s.openingTrees == 0 && len(s.trees) == 0 && len(s.authorities) == 0
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
	// Publishing retired state and capturing every response frame share the same
	// admission locks. The signer cannot be destroyed between frame lookup and use.
	s.mu.Lock()
	c.mu.Lock()
	s.retired = true
	s.mu.Unlock()
	s.retirementMu.Lock()
	if s.retiringFrames == nil {
		s.retiringFrames = make(map[requestFrame]struct{})
	}
	for _, pending := range c.pending {
		if pending.sessionID != s.id {
			continue
		}
		frame := requestFrame{connection: c, id: pending.frame}
		s.retiringFrames[frame] = struct{}{}
		if frame != except && pending.command != wire.Logoff {
			pending.cancel()
		}
	}
	s.retirementMu.Unlock()
	c.mu.Unlock()
	s.stopAuthenticationWatcher()
}
