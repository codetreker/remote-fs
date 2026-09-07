package locking

import (
	"context"
	"time"
)

func (a *Authority) Resolve(ctx context.Context, ref OwnerRef, path string) (ResourceRef, error) {
	ctx, finish, admissionErr := a.beginNative(ctx)
	if admissionErr != nil {
		return ResourceRef{}, admissionErr
	}
	defer finish()

	a.mu.Lock()
	err := a.healthyLocked(ctx, false)
	if err == nil {
		a.expireSessionsLocked(a.clock.Now())
		_, err = a.ownerLocked(ref)
	}
	a.mu.Unlock()
	if err != nil {
		return ResourceRef{}, err
	}
	var result ResourceRef
	err = a.native.Discover(ctx, path, func(key BackendKey) (bool, error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if err := a.healthyLocked(ctx, false); err != nil {
			return false, err
		}
		a.expireSessionsLocked(a.clock.Now())
		o, err := a.ownerLocked(ref)
		if err != nil {
			return false, err
		}
		if key == "" {
			return false, fail(Invalid, "backend returned an empty resource key")
		}
		r := a.keys[key]
		adopt := r == nil
		if adopt {
			if len(a.resources) >= a.options.MaxResources {
				return false, fail(Capacity, "resource capacity exhausted")
			}
			r = &resourceRecord{id: ResourceID(a.token("r", "", "")), key: key, grants: make(map[GrantID]*grantRecord), wake: make(chan struct{}, 1)}
			a.resources[r.id] = r
			a.keys[key] = r
		} else if r.retired || r.forgetting {
			return false, fail(StaleResource, "resource is retired")
		}
		now := a.clock.Now()
		r.referenceUntil = now.Add(a.options.ResourceTTL)
		a.touchLocked(o)
		result = ResourceRef{ID: r.id, ExpiresMillis: a.tick(r.referenceUntil), NowMillis: a.tick(now), HistoryExpiresMillis: a.tick(o.session.expires)}
		a.wakeMaintenance()
		return adopt, nil
	})
	return result, err
}

func (a *Authority) resourceLocked(ref ResourceRef) (*resourceRecord, error) {
	if _, err := a.verify(string(ref.ID), "r", ""); err != nil {
		return nil, err
	}
	r := a.resources[ref.ID]
	if r == nil || r.retired || r.forgetting {
		return nil, fail(StaleResource, "resource reference is retired")
	}
	now := a.clock.Now()
	if !now.Before(r.referenceUntil) || a.tick(now) >= ref.ExpiresMillis || ref.ExpiresMillis > a.tick(r.referenceUntil) {
		return nil, fail(StaleResource, "resource reference expired")
	}
	return r, nil
}

func (a *Authority) endGrantLocked(g *grantRecord, state GrantState) {
	if g.state != Active {
		return
	}
	g.state = state
	a.grants--
	g.owner.liveGrants--
	delete(g.resource.grants, g.ref.ID)
	a.signalLocked(g.resource)
	a.wakeMaintenance()
}

func (a *Authority) expireGrantsLocked(r *resourceRecord, now time.Time) {
	if r.busy {
		return
	}
	for _, g := range r.grants {
		if !now.Before(g.deadline) {
			a.endGrantLocked(g, Expired)
		}
	}
}

func (a *Authority) finishLocked(action *actionRecord, outcome ActionOutcome, code Code) {
	if action.receipt.Outcome != Pending {
		return
	}
	if action.waitUntil.IsZero() == false {
		a.queued--
		action.owner.queued--
		queue := action.resource.queue
		for i, v := range queue {
			if v == action {
				action.resource.queue = append(queue[:i], queue[i+1:]...)
				break
			}
		}
	}
	action.receipt.Outcome = outcome
	action.receipt.Code = code
	if action.resource != nil {
		a.signalLocked(action.resource)
	}
}

func (a *Authority) startWorkerLocked(r *resourceRecord) {
	if r.worker || len(r.queue) == 0 || a.closed {
		return
	}
	r.worker = true
	a.wg.Add(1)
	go a.runResource(r)
}

func (a *Authority) runResource(r *resourceRecord) {
	defer a.wg.Done()
	defer func() { a.mu.Lock(); r.worker = false; a.mu.Unlock(); a.wakeMaintenance() }()
	for {
		a.mu.Lock()
		if a.closed || a.fenced != nil || len(r.queue) == 0 {
			a.mu.Unlock()
			return
		}
		now := a.clock.Now()
		a.expireGrantsLocked(r, now)
		for _, action := range append([]*actionRecord(nil), r.queue...) {
			if !now.Before(action.waitUntil) {
				a.finishLocked(action, TimedOut, "")
			}
		}
		if len(r.queue) == 0 {
			a.mu.Unlock()
			return
		}
		current := r.queue[0]
		next := current.waitUntil
		for _, g := range r.grants {
			if g.deadline.Before(next) {
				next = g.deadline
			}
		}
		compatible := !r.busy && a.compatibleLocked(r, current.owner, current.receipt.Acquire.Mode)
		if compatible && !r.busy {
			current.processing = true
			r.holds++
		}
		a.mu.Unlock()
		if compatible {
			err := a.raise(a.workerContext, current.receipt.Acquire.TTL)
			if err == nil {
				err = a.native.Guard(a.workerContext, r.key, func() error {
					a.mu.Lock()
					defer a.mu.Unlock()
					if current.receipt.Outcome != Pending {
						return nil
					}
					if e := a.healthyLocked(a.workerContext, true); e != nil {
						return e
					}
					a.expireSessionsLocked(a.clock.Now())
					if a.owners[current.owner.ref.Owner] != current.owner {
						return fail(Retired, "owner history is retired")
					}
					if r.retired {
						return fail(StaleResource, "resource is retired")
					}
					now := a.clock.Now()
					a.expireGrantsLocked(r, now)
					if !now.Before(current.waitUntil) {
						a.finishLocked(current, TimedOut, "")
						return nil
					}
					if len(r.queue) > 0 && r.queue[0] == current && !r.busy && a.compatibleLocked(r, current.owner, current.receipt.Acquire.Mode) {
						return a.grantLocked(current)
					}
					return nil
				})
			}
			a.mu.Lock()
			current.processing = false
			r.holds--
			if err != nil && current.receipt.Outcome == Pending {
				current.failure = err
				a.finishLocked(current, Rejected, CodeOf(err))
			}
			a.mu.Unlock()
			continue
		}
		delay := next.Sub(a.clock.Now())
		if delay <= 0 {
			delay = time.Millisecond
		}
		select {
		case <-a.stop:
			return
		case <-r.wake:
		case <-a.clock.After(delay):
		}
	}
}

func (a *Authority) maintain() {
	defer a.wg.Done()
	for {
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return
		}
		now := a.clock.Now()
		a.expireSessionsLocked(now)
		next := now.Add(a.options.SessionIdle)
		for _, s := range a.sessions {
			if s.expires.After(now) && s.expires.Before(next) {
				next = s.expires
			}
		}
		for _, t := range a.tickets {
			if t.expires.Before(next) {
				next = t.expires
			}
		}
		var forgotten []*resourceRecord
		for _, r := range a.resources {
			a.expireGrantsLocked(r, now)
			a.startWorkerLocked(r)
			if r.referenceUntil.After(now) && r.referenceUntil.Before(next) {
				next = r.referenceUntil
			}
			for _, g := range r.grants {
				if g.deadline.After(now) && g.deadline.Before(next) {
					next = g.deadline
				}
			}
			if !r.forgetting && !now.Before(r.referenceUntil) && len(r.grants) == 0 && len(r.queue) == 0 && !r.busy && !r.worker && r.holds == 0 {
				r.forgetting = true
				forgotten = append(forgotten, r)
			}
		}
		a.mu.Unlock()
		for _, r := range forgotten {
			err := a.native.Forget(context.Background(), r.key)
			a.mu.Lock()
			if err != nil {
				a.fenced = err
			} else {
				delete(a.keys, r.key)
				delete(a.resources, r.id)
			}
			a.mu.Unlock()
		}
		delay := next.Sub(a.clock.Now())
		if delay <= 0 {
			delay = time.Millisecond
		}
		select {
		case <-a.stop:
			return
		case <-a.wake:
		case <-a.clock.After(delay):
		}
	}
}
