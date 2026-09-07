package locking

import (
	"context"
	"strconv"
	"time"
)

func (a *Authority) BeginEnrollment(ctx context.Context) (EnrollmentTicket, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.healthyLocked(ctx, false); err != nil {
		return "", err
	}
	expiry := a.tick(a.clock.Now().Add(a.options.TicketTTL))
	return EnrollmentTicket(a.token("t", "", strconv.FormatInt(expiry, 10))), nil
}

func (a *Authority) OpenSession(ctx context.Context, ticket EnrollmentTicket) (Session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.healthyLocked(ctx, false); err != nil {
		return Session{}, err
	}
	extra, err := a.verify(string(ticket), "t", "")
	if err != nil {
		return Session{}, err
	}
	expiry, err := a.ticketExpiry(extra)
	if err != nil {
		return Session{}, err
	}
	now := a.clock.Now()
	a.expireSessionsLocked(now)
	if !now.Before(expiry) {
		return Session{}, fail(Retired, "enrollment ticket expired")
	}
	if previous, ok := a.tickets[ticket]; ok {
		if s := a.sessions[previous.session]; s != nil {
			a.touchSessionLocked(s)
			return a.sessionResultLocked(s), nil
		}
		return Session{ID: previous.session, Authority: a.incarnation, Retired: true, NowMillis: a.tick(now)}, nil
	}
	if len(a.tickets) >= a.options.MaxTickets || len(a.sessions) >= a.options.MaxSessions {
		return Session{}, fail(Capacity, "session enrollment capacity exhausted")
	}
	id := SessionID(a.token("s", "", ""))
	s := &sessionRecord{id: id, expires: now.Add(a.options.SessionIdle), owners: make(map[OwnerID]*ownerRecord), creates: make(map[RequestID]OwnerID)}
	a.sessions[id] = s
	a.tickets[ticket] = ticketRecord{session: id, expires: expiry}
	a.wakeMaintenance()
	return a.sessionResultLocked(s), nil
}

func (a *Authority) sessionResultLocked(s *sessionRecord) Session {
	return Session{ID: s.id, Authority: a.incarnation, HistoryExpiresMillis: a.tick(s.expires), NowMillis: a.tick(a.clock.Now())}
}

func (a *Authority) CreateOwner(ctx context.Context, id SessionID, request RequestID) (Owner, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.healthyLocked(ctx, false); err != nil {
		return Owner{}, err
	}
	a.expireSessionsLocked(a.clock.Now())
	s, err := a.sessionLocked(id)
	if err != nil {
		return Owner{}, err
	}
	if !requestValid(request, a.options.MaxRequestBytes) {
		return Owner{}, fail(Invalid, "invalid owner creation request ID")
	}
	if previous, ok := s.creates[request]; ok {
		a.touchSessionLocked(s)
		if o := s.owners[previous]; o != nil {
			return a.ownerResultLocked(o), nil
		}
		return Owner{Ref: OwnerRef{Session: id, Owner: previous}, Retired: true, ActionCapacity: a.options.ActionsPerOwner,
			HistoryExpiresMillis: a.tick(s.expires), NowMillis: a.tick(a.clock.Now())}, nil
	}
	if len(a.owners) >= a.options.MaxOwners || len(s.owners) >= a.options.OwnersPerSession || len(s.creates) >= a.options.OwnerActionsPerSession {
		return Owner{}, fail(Capacity, "owner enrollment capacity exhausted")
	}
	ownerID := OwnerID(a.token("o", string(id), ""))
	o := &ownerRecord{ref: OwnerRef{Session: id, Owner: ownerID}, session: s, actions: make(map[RequestID]*actionRecord), grants: make(map[GrantID]*grantRecord)}
	a.owners[ownerID] = o
	s.owners[ownerID] = o
	s.creates[request] = ownerID
	a.touchLocked(o)
	return a.ownerResultLocked(o), nil
}

func (a *Authority) ownerResultLocked(o *ownerRecord) Owner {
	return Owner{Ref: o.ref, ActionCapacity: a.options.ActionsPerOwner, HistoryExpiresMillis: a.tick(o.session.expires), NowMillis: a.tick(a.clock.Now())}
}

func (a *Authority) RetireOwner(ctx context.Context, ref OwnerRef) error {
	for {
		a.mu.Lock()
		if err := a.healthyLocked(ctx, false); err != nil {
			a.mu.Unlock()
			return err
		}
		o, err := a.ownerLocked(ref)
		if err != nil {
			a.mu.Unlock()
			if CodeOf(err) == Retired {
				return nil
			}
			return err
		}
		if busy := a.ownerBusyLocked(o); busy != nil {
			a.mu.Unlock()
			select {
			case <-busy:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		a.retireOwnerLocked(o)
		a.mu.Unlock()
		a.wakeMaintenance()
		return nil
	}
}

func (a *Authority) CloseSession(ctx context.Context, id SessionID) error {
	for {
		a.mu.Lock()
		if err := a.healthyLocked(ctx, false); err != nil {
			a.mu.Unlock()
			return err
		}
		s, err := a.sessionLocked(id)
		if err != nil {
			a.mu.Unlock()
			if CodeOf(err) == Retired {
				return nil
			}
			return err
		}
		var busy <-chan struct{}
		for _, o := range s.owners {
			if busy = a.ownerBusyLocked(o); busy != nil {
				break
			}
		}
		if busy != nil {
			a.mu.Unlock()
			select {
			case <-busy:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		a.retireSessionLocked(s)
		a.mu.Unlock()
		a.wakeMaintenance()
		return nil
	}
}

func (a *Authority) ownerBusyLocked(o *ownerRecord) <-chan struct{} {
	for _, g := range o.grants {
		if g.resource.busy {
			return g.resource.busyDone
		}
	}
	return nil
}

func (a *Authority) retireOwnerLocked(o *ownerRecord) {
	for _, action := range o.actions {
		if action.receipt.Outcome == Pending {
			a.finishLocked(action, Cancelled, "")
		}
	}
	for _, g := range o.grants {
		if g.state == Active {
			a.endGrantLocked(g, OwnerRetired)
		}
	}
	a.actions -= len(o.actions)
	delete(a.owners, o.ref.Owner)
	delete(o.session.owners, o.ref.Owner)
}
func (a *Authority) retireSessionLocked(s *sessionRecord) {
	for _, o := range s.owners {
		a.retireOwnerLocked(o)
	}
	delete(a.sessions, s.id)
}

func (a *Authority) expireSessionsLocked(now time.Time) {
	for ticket, r := range a.tickets {
		if !now.Before(r.expires) {
			delete(a.tickets, ticket)
		}
	}
	for _, s := range a.sessions {
		if now.Before(s.expires) {
			continue
		}
		busy := false
		for _, o := range s.owners {
			if a.ownerBusyLocked(o) != nil {
				busy = true
				break
			}
		}
		if !busy {
			a.retireSessionLocked(s)
		}
	}
}
