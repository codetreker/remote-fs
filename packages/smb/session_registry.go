package smb

import (
	"context"
	"errors"
	"math"
	"sync"
	"syscall"
)

type sessionOwner struct {
	connection *connection
	session    *session
}

// The registry lock never encloses a connection or session lock. A global
// session charge remains held until native resources and response frames have
// both completed retirement.
type sessionRegistry struct {
	mu     sync.Mutex
	nextID uint64
	owners map[uint64]sessionOwner
}

func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{owners: make(map[uint64]sessionOwner)}
}

func (r *sessionRegistry) add(c *connection, s *session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.owners) >= c.server.config.Limits.MaxSessions || r.nextID >= math.MaxUint64-1 {
		return syscall.EAGAIN
	}
	r.nextID++
	s.id = r.nextID
	r.owners[s.id] = sessionOwner{connection: c, session: s}
	return nil
}

func (r *sessionRegistry) get(id uint64) sessionOwner {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.owners[id]
}

func (r *sessionRegistry) remove(s *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner := r.owners[s.id]; owner.session == s {
		delete(r.owners, s.id)
	}
}

func (c *connection) removeSessionLocked(s *session) {
	if c.sessions[s.id] == s {
		delete(c.sessions, s.id)
		c.server.sessions.remove(s)
	}
}

func samePrincipalIdentity(left, right Principal) bool {
	return left.SameIdentity(right)
}

// Previous-session retirement is authorized only by a newly authenticated
// principal from the same Windows user and logon session. Display names never
// participate in identity.
func (c *connection) retirePreviousSession(ctx context.Context, current *session, previous uint64, principal Principal) error {
	if previous == 0 || previous == current.id {
		return nil
	}
	old := c.server.sessions.get(previous)
	if old.session == nil {
		return nil
	}
	old.session.identityMu.RLock()
	sameIdentity := samePrincipalIdentity(old.session.principal, principal)
	old.session.identityMu.RUnlock()
	if !sameIdentity {
		return nil
	}
	if err := old.connection.cleanupMu.lock(ctx); err != nil {
		return err
	}
	defer old.connection.cleanupMu.unlock()
	if err := old.connection.logoff(WithPrincipal(ctx, principal), old.session); err != nil {
		old.session.mu.Lock()
		cleaned := old.session.cleaned
		old.session.mu.Unlock()
		if !errors.Is(err, ErrStopped) || !cleaned {
			return err
		}
	}
	return nil
}
