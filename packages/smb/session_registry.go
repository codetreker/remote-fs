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

// The registry lock never encloses a connection or session lock. Routing maps
// retain their own admission limits; this index gives PreviousSessionId one
// meaning across every connection owned by the server instance.
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
	if r.nextID >= math.MaxUint64-1 {
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

// MS-SMB2 3.3.5.5.3, step 13, authorizes old-session retirement only after
// successful authentication identifies the same user. An absent old identifier
// after server initialization and a different user's identifier are ignored.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/5ed93f06-a1d2-4837-8954-fa8b833c2654
func (c *connection) retirePreviousSession(ctx context.Context, current *session, previous uint64, principal Principal) error {
	if previous == 0 || previous == current.id {
		return nil
	}
	old := c.server.sessions.get(previous)
	if old.session == nil {
		return nil
	}
	old.session.identityMu.RLock()
	sameUser := old.session.principal.SID != "" && old.session.principal.SID == principal.SID
	old.session.identityMu.RUnlock()
	if !sameUser {
		return nil
	}
	old.connection.cleanupMu.Lock()
	defer old.connection.cleanupMu.Unlock()
	if err := old.connection.logoff(ctx, old.session); err != nil {
		old.session.mu.Lock()
		cleaned := old.session.cleaned
		old.session.mu.Unlock()
		if !errors.Is(err, ErrStopped) || !cleaned {
			return err
		}
	}
	return nil
}
