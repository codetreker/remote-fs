package smb

import (
	"context"
	"errors"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

// One authenticated SMB session shares one FileSession per export. Trees are
// protocol aliases over that authority lifetime, not independent remote owners.
type authoritySession struct {
	raw       storage.FileSession
	export    *Export
	principal Principal

	installMu       sync.RWMutex
	mu              sync.Mutex
	closeMu         cleanupGate
	treeCloseMu     contextLock
	refs            int
	orphan          bool
	stopping        bool
	closed          bool
	initErr         error
	ready           chan struct{}
	done            chan struct{}
	cancel          context.CancelFunc
	epoch           string
	revision        uint64
	actionEpoch     uint64
	deadline        time.Time
	deleteMu        sync.Mutex
	deleteIntents   map[storage.DeleteIntentID]*deleteIntentCleanup
	deleteProduced  bool
	recoverySession storage.FileSession
	recoveryPending bool
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

// Remaining is measured from the request start by remote implementations. We
// consume it at receipt time and never let an older response extend authority.
func (a *authoritySession) acceptStatus(status storage.FileSessionStatus) error {
	if status.Epoch == "" || status.Revision == 0 || status.ActionEpoch == 0 ||
		status.Remaining <= 0 || status.Fenced || status.Retired {
		return syscall.EIO
	}
	a.installMu.Lock()
	defer a.installMu.Unlock()
	if a.stopping || a.closed || a.epoch != "" && (a.epoch != status.Epoch || !time.Now().Before(a.deadline)) {
		return syscall.EIO
	}
	if status.Revision <= a.revision {
		return nil
	}
	a.epoch = status.Epoch
	a.revision = status.Revision
	a.actionEpoch = status.ActionEpoch
	a.deadline = time.Now().Add(status.Remaining)
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
			return a.recoverDeleteIntents(ctx)
		}
		a.deleteMu.Lock()
		if a.deleteProduced {
			a.recoveryPending = true
		}
		a.deleteMu.Unlock()
		var closeErr error
		if a.raw != nil {
			closeErr = a.raw.Close(WithPrincipal(ctx, a.principal))
		}
		if closeErr == nil {
			a.installMu.Lock()
			a.closed = true
			a.installMu.Unlock()
		}
		return errors.Join(closeErr, a.recoverDeleteIntents(ctx))
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
		remaining, stopping := time.Until(a.deadline), a.stopping
		a.installMu.RUnlock()
		if stopping {
			return
		}
		interval := min(limits.FileSession.Lease/3, remaining/2)
		if interval <= 0 {
			a.retireAfterRenewalFailure(ctx, syscall.ESTALE, limits.CleanupTimeout)
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
			a.retireAfterRenewalFailure(ctx, syscall.ESTALE, limits.CleanupTimeout)
			return
		}
		call, cancel := context.WithTimeout(WithPrincipal(ctx, a.principal), min(interval, remaining))
		err := a.export.server.config.Authorize.Authorize(call, authz.AccessRequest{
			Volume: a.export.share.Volume, Operation: storage.OpFileRenew,
		})
		if err == nil {
			var status storage.FileSessionStatus
			status, err = a.raw.Renew(call)
			if err == nil {
				err = a.acceptStatus(status)
			}
			if err == nil {
				a.export.server.cleanupFailure(a.scanDeleteIntentsWith(call, a.raw.(storage.FileActions), status.ActionEpoch))
			}
		}
		cancel()
		if err != nil {
			a.retireAfterRenewalFailure(ctx, err, limits.CleanupTimeout)
			return
		}
	}
}

func (a *authoritySession) retireAfterRenewalFailure(parent context.Context, cause error, timeout time.Duration) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	closeErr := a.close(cleanup)
	cancel()
	a.export.server.cleanupFailure(errors.Join(cause, closeErr))
}

func (c *connection) connectVolume(ctx context.Context, s *session, key string, header *wire.Header) (body []byte, status uint32) {
	server := c.server
	server.mu.Lock()
	export := server.exports[key]
	if export == nil || !export.published || export.stopping {
		server.mu.Unlock()
		return nil, statusBadNetworkName
	}
	export.refs++
	export.active++
	server.mu.Unlock()
	keep, opening := false, false
	defer func() {
		server.mu.Lock()
		export.active--
		if !keep {
			export.refs--
		}
		server.mu.Unlock()
		if opening {
			s.mu.Lock()
			s.openingTrees--
			s.mu.Unlock()
			c.finishSessionRetirement(s)
		}
	}()

	if err := server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: export.share.Volume, Operation: storage.OpFileSessionOpen}); err != nil {
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
	authority := s.authorities[export]
	creator := authority == nil
	if creator && len(s.authorities) >= server.config.Limits.MaxTrees {
		s.mu.Unlock()
		return nil, statusResources
	}
	s.openingTrees++
	opening = true
	if creator {
		principal, _ := PrincipalFromContext(ctx)
		authority = &authoritySession{
			export: export, principal: principal, orphan: true,
			ready: make(chan struct{}), done: make(chan struct{}),
		}
		s.authorities[export] = authority
		server.mu.Lock()
		export.refs++
		server.mu.Unlock()
	}
	s.mu.Unlock()

	if creator {
		defer func() {
			if keep {
				return
			}
			s.mu.Lock()
			owned := s.authorities[export] == authority
			s.mu.Unlock()
			if !owned {
				return
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), server.config.Limits.CleanupTimeout)
			defer cancel()
			if err := authority.treeCloseMu.lock(cleanup); err != nil {
				status = statusIO
				server.cleanupFailure(err)
				return
			}
			defer authority.treeCloseMu.unlock()
			authority.installMu.Lock()
			authority.mu.Lock()
			empty := authority.refs == 0
			if empty {
				authority.stopping = true
			}
			authority.mu.Unlock()
			authority.installMu.Unlock()
			if !empty {
				return
			}
			err := authority.close(cleanup)
			if err == nil {
				err = authority.wait(cleanup)
			}
			if err != nil {
				status = statusIO
				server.cleanupFailure(err)
				return
			}
			authority.releaseExport()
			s.mu.Lock()
			if s.authorities[export] == authority {
				delete(s.authorities, export)
			}
			s.mu.Unlock()
		}()
	}

	if creator {
		raw, err := export.share.Backend.NewFileSession(ctx, server.config.Limits.FileSession)
		var current storage.FileSessionStatus
		authority.raw = raw
		if err == nil && raw == nil {
			err = syscall.EIO
		}
		if err == nil {
			err = checkSMBFileSession(raw)
		}
		if err == nil {
			err = server.config.Authorize.Authorize(ctx, authz.AccessRequest{Volume: export.share.Volume, Operation: storage.OpFileStatus})
		}
		if err == nil {
			current, err = raw.Status(ctx)
			if err == nil {
				err = authority.acceptStatus(current)
			}
		}
		if err == nil {
			err = authority.scanDeleteIntentsWith(ctx, raw.(storage.FileActions), current.ActionEpoch)
		}
		authority.initErr = err
		if err == nil {
			renewCtx, cancel := context.WithCancel(WithPrincipal(c.ctx, authority.principal))
			authority.installMu.Lock()
			authority.cancel = cancel
			authority.installMu.Unlock()
			go authority.renew(renewCtx, server.config.Limits)
		} else {
			close(authority.done)
		}
		close(authority.ready)
		if err != nil {
			return nil, statusError(err)
		}
	} else {
		select {
		case <-authority.ready:
		case <-ctx.Done():
			return nil, statusError(ctx.Err())
		}
		if authority.initErr != nil {
			return nil, statusIO
		}
	}

	authority.installMu.RLock()
	s.mu.Lock()
	if s.retired || authority.stopping || authority.closed || !time.Now().Before(authority.deadline) || ctx.Err() != nil {
		s.mu.Unlock()
		authority.installMu.RUnlock()
		return nil, statusSessionDeleted
	}
	c.mu.Lock()
	if c.nextTree == math.MaxUint32 {
		c.mu.Unlock()
		s.mu.Unlock()
		authority.installMu.RUnlock()
		return nil, statusResources
	}
	c.nextTree++
	id := c.nextTree
	c.mu.Unlock()
	tree := &tree{kind: volumeTree, id: id, sessionID: s.id, session: s, export: export, authority: authority, done: make(chan struct{})}
	tree.files = newHandleRegistry(tree, server.config.Limits)
	authority.mu.Lock()
	authority.refs++
	authority.mu.Unlock()
	server.mu.Lock()
	export.trees++
	server.mu.Unlock()
	s.trees[id] = tree
	s.mu.Unlock()
	authority.installMu.RUnlock()
	header.TreeID = id
	keep = true
	return wire.TreeConnectResponseBody(1, 0x30, 0, 0x001f01ff), statusOK
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
