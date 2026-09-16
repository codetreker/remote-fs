package smb

import (
	"context"
	"errors"
	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"math"
	"sync"
	"syscall"
	"time"
)

type authoritySession struct {
	raw                       storage.FileSession
	export                    *Export
	principal                 Principal
	installMu                 sync.RWMutex
	mu                        sync.Mutex
	closeMu                   cleanupGate
	treeCloseMu               sync.Mutex
	refs                      int
	orphan                    bool
	stopping, closed          bool
	initErr                   error
	ready                     chan struct{}
	done                      chan struct{}
	cancel                    context.CancelFunc
	epoch                     string
	revision, actionEpoch     uint64
	deadline, historyDeadline time.Time
}

func (a *authoritySession) isClosed() bool {
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	return a.closed
}
func (a *authoritySession) isStopping() bool {
	a.installMu.RLock()
	defer a.installMu.RUnlock()
	return a.stopping
}
func (a *authoritySession) acceptStatus(st storage.FileSessionStatus) error {
	if st.Epoch == "" || st.Revision == 0 || st.Remaining <= 0 || st.Fenced || st.Retired {
		return syscall.EIO
	}
	a.installMu.Lock()
	defer a.installMu.Unlock()
	if a.stopping || a.closed || a.epoch != "" && (a.epoch != st.Epoch || !time.Now().Before(a.deadline)) {
		return syscall.EIO
	}
	if st.Revision <= a.revision {
		return nil
	}
	now := time.Now()
	a.epoch = st.Epoch
	a.revision = st.Revision
	a.actionEpoch = st.ActionEpoch
	a.deadline = now.Add(st.Remaining)
	a.historyDeadline = now.Add(st.HistoryRemaining)
	return nil
}
func (a *authoritySession) close(ctx context.Context) error {
	return a.closeMu.run(ctx, func() error {
		a.installMu.Lock()
		a.stopping = true
		already := a.closed
		if a.cancel != nil {
			a.cancel()
		}
		a.installMu.Unlock()
		if already {
			return nil
		}
		if a.raw != nil {
			if err := a.raw.Close(WithPrincipal(ctx, a.principal)); err != nil {
				return err
			}
		}
		a.installMu.Lock()
		a.closed = true
		a.installMu.Unlock()
		return nil
	})
}
func (a *authoritySession) wait(ctx context.Context) error {
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (a *authoritySession) renew(ctx context.Context, limits Limits) {
	defer close(a.done)
	for {
		a.installMu.RLock()
		remaining := time.Until(a.deadline)
		stopping := a.stopping
		a.installMu.RUnlock()
		if stopping {
			return
		}
		interval := min(limits.FileSession.Lease/3, remaining/2)
		if interval <= 0 {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), limits.CleanupTimeout)
			err := a.close(cleanup)
			cancel()
			a.export.server.cleanupFailure(err)
			return
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		a.installMu.RLock()
		remaining = time.Until(a.deadline)
		a.installMu.RUnlock()
		if remaining <= 0 {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), limits.CleanupTimeout)
			err := a.close(cleanup)
			cancel()
			a.export.server.cleanupFailure(err)
			return
		}
		call, cancel := context.WithTimeout(WithPrincipal(ctx, a.principal), min(interval, remaining))
		err := a.export.server.config.Authorize.Authorize(call, authz.AccessRequest{Volume: a.export.share.Volume, Operation: storage.OpFileRenew})
		if err == nil {
			var st storage.FileSessionStatus
			st, err = a.raw.Renew(call)
			if err == nil {
				err = a.acceptStatus(st)
			}
		}
		cancel()
		if err != nil {
			cleanup, done := context.WithTimeout(context.WithoutCancel(ctx), limits.CleanupTimeout)
			closeErr := a.close(cleanup)
			done()
			a.export.server.cleanupFailure(errors.Join(err, closeErr))
			return
		}
	}
}
func (c *connection) connectVolume(ctx context.Context, s *session, key string, h *wire.Header) (body []byte, status uint32) {
	server := c.server
	server.mu.Lock()
	e := server.exports[key]
	if e == nil || !e.published || e.stopping {
		server.mu.Unlock()
		return nil, statusBadNetworkName
	}
	e.refs++
	e.active++
	server.mu.Unlock()
	keep := false
	defer func() {
		server.mu.Lock()
		e.active--
		if !keep {
			e.refs--
		}
		server.mu.Unlock()
	}()
	if err := server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: e.share.Volume, Operation: storage.OpFileSessionOpen}); err != nil {
		return nil, statusError(err)
	}
	s.mu.Lock()
	if s.retired {
		s.mu.Unlock()
		return nil, statusSessionDeleted
	}
	if len(s.trees)+s.openingTrees >= server.config.Limits.MaxTrees {
		s.mu.Unlock()
		return nil, statusResources
	}
	if s.authorities == nil {
		s.authorities = make(map[*Export]*authoritySession)
	}
	a := s.authorities[e]
	creator := a == nil
	if creator && len(s.authorities) >= server.config.Limits.MaxTrees {
		s.mu.Unlock()
		return nil, statusResources
	}
	s.openingTrees++
	if creator {
		principal, _ := PrincipalFromContext(ctx)
		a = &authoritySession{export: e, principal: principal, orphan: true, ready: make(chan struct{}), done: make(chan struct{})}
		s.authorities[e] = a
		server.mu.Lock()
		e.refs++
		server.mu.Unlock()
	}
	s.mu.Unlock()
	if creator {
		defer func() {
			if keep {
				return
			}
			s.mu.Lock()
			owned := s.authorities[e] == a
			s.mu.Unlock()
			if !owned {
				return
			}
			a.treeCloseMu.Lock()
			defer a.treeCloseMu.Unlock()
			a.installMu.Lock()
			a.mu.Lock()
			empty := a.refs == 0
			if empty {
				a.stopping = true
			}
			a.mu.Unlock()
			a.installMu.Unlock()
			if !empty {
				return
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), server.config.Limits.CleanupTimeout)
			defer cancel()
			err := a.close(cleanup)
			if err == nil {
				err = a.wait(cleanup)
			}
			if err != nil {
				status = statusIO
				server.cleanupFailure(err)
				return
			}
			a.releaseExport()
			s.mu.Lock()
			if s.authorities[e] == a {
				delete(s.authorities, e)
			}
			s.mu.Unlock()
		}()
	}
	defer func() { s.mu.Lock(); s.openingTrees--; s.mu.Unlock() }()
	if creator {
		raw, err := e.share.Backend.NewFileSession(ctx, server.config.Limits.FileSession)
		a.raw = raw
		if err == nil && raw == nil {
			err = syscall.EIO
		}
		if err == nil {
			err = server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: e.share.Volume, Operation: storage.OpFileStatus})
		}
		if err == nil {
			var st storage.FileSessionStatus
			st, err = raw.Status(ctx)
			if err == nil {
				err = a.acceptStatus(st)
			}
		}
		a.initErr = err
		if err == nil {
			renewCtx, cancel := context.WithCancel(WithPrincipal(c.ctx, a.principal))
			a.installMu.Lock()
			a.cancel = cancel
			a.installMu.Unlock()
			go a.renew(renewCtx, server.config.Limits)
		} else {
			close(a.done)
		}
		close(a.ready)
		if err != nil {
			return nil, statusError(err)
		}
	} else {
		select {
		case <-a.ready:
		case <-ctx.Done():
			return nil, statusError(ctx.Err())
		}
		if a.initErr != nil {
			return nil, statusIO
		}
	}
	a.installMu.RLock()
	s.mu.Lock()
	if s.retired || a.stopping || a.closed || time.Until(a.deadline) <= 0 || ctx.Err() != nil {
		s.mu.Unlock()
		a.installMu.RUnlock()
		return nil, statusSessionDeleted
	}
	c.mu.Lock()
	if c.nextTree == math.MaxUint32 {
		c.mu.Unlock()
		s.mu.Unlock()
		a.installMu.RUnlock()
		return nil, statusResources
	}
	c.nextTree++
	id := c.nextTree
	c.mu.Unlock()
	t := &tree{kind: volumeTree, id: id, sessionID: s.id, export: e, authority: a, done: make(chan struct{})}
	t.files = newHandleRegistry(t, server.config.Limits)
	a.mu.Lock()
	a.refs++
	a.mu.Unlock()
	if s.trees == nil {
		s.trees = make(map[uint32]*tree)
	}
	s.trees[id] = t
	s.mu.Unlock()
	a.installMu.RUnlock()
	h.TreeID = id
	keep = true
	return wire.TreeConnectResponseBody(1, 0x30, 0, 0x001f01ff), 0
}

func (a *authoritySession) releaseExport() {
	a.mu.Lock()
	held := a.orphan
	a.orphan = false
	a.mu.Unlock()
	if held {
		a.export.server.mu.Lock()
		a.export.refs--
		a.export.server.mu.Unlock()
	}
}
