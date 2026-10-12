package locking

import (
	"context"
	"strconv"
	"time"
)

func (a *Authority) Acquire(ctx context.Context, request AcquireRequest) (ActionResult, error) {
	ctx, finish, admissionErr := a.beginNative(ctx)
	if admissionErr != nil {
		return ActionResult{}, admissionErr
	}
	defer finish()

	a.mu.Lock()
	if err := a.healthyLocked(ctx, true); err != nil {
		a.mu.Unlock()
		return ActionResult{}, err
	}
	a.expireSessionsLocked(a.clock.Now())
	o, err := a.ownerLocked(request.Owner)
	if err != nil {
		a.mu.Unlock()
		return ActionResult{}, err
	}
	if !requestValid(request.Request, a.options.MaxRequestBytes) || (request.Mode != Shared && request.Mode != Exclusive) || request.TTL < time.Millisecond || request.TTL > a.options.MaxLease || request.Wait < 0 || request.Wait > a.options.MaxWait || request.TTL%time.Millisecond != 0 || request.Wait%time.Millisecond != 0 {
		a.mu.Unlock()
		return ActionResult{}, fail(Invalid, "invalid acquisition intent")
	}
	if previous := o.actions[request.Request]; previous != nil {
		if previous.tombstone {
			a.mu.Unlock()
			return a.QueryAction(ctx, request.Owner, request.Request)
		}
		if previous.receipt.Kind != AcquireAction || previous.receipt.Acquire == nil || *previous.receipt.Acquire != request {
			a.mu.Unlock()
			return ActionResult{}, fail(RequestMismatch, "request ID has a different intent")
		}
		a.mu.Unlock()
		return a.QueryAction(ctx, request.Owner, request.Request)
	}
	if len(o.actions) >= a.options.ActionsPerOwner || a.actions >= a.options.MaxActions {
		a.mu.Unlock()
		return ActionResult{}, fail(Capacity, "action history capacity exhausted")
	}
	action := &actionRecord{owner: o, receipt: ActionReceipt{Kind: AcquireAction, Request: request.Request, Acquire: &request, Outcome: Pending}}
	o.actions[request.Request] = action
	a.actions++
	a.touchLocked(o)
	r, err := a.resourceLocked(request.Resource)
	if err != nil {
		a.finishLocked(action, Rejected, CodeOf(err))
		result, err := a.actionResultLocked(action)
		a.mu.Unlock()
		return result, err
	}
	action.resource = r
	action.processing = true
	r.holds++
	started := a.clock.Now()
	a.mu.Unlock()
	err = a.raise(ctx, request.TTL)
	if err == nil {
		err = a.native.Guard(ctx, r.key, func() error {
			a.mu.Lock()
			defer a.mu.Unlock()
			if action.receipt.Outcome != Pending {
				return nil
			}
			if e := a.healthyLocked(ctx, true); e != nil {
				return e
			}
			a.expireSessionsLocked(a.clock.Now())
			if a.owners[o.ref.Owner] != o {
				return fail(Retired, "owner history is retired")
			}
			if r.retired {
				return fail(StaleResource, "resource is retired")
			}
			a.expireGrantsLocked(r, a.clock.Now())
			for _, g := range r.grants {
				if g.owner == o {
					return fail(AlreadyHeld, "owner already holds this resource")
				}
			}
			if !r.busy && len(r.queue) == 0 && a.compatibleLocked(r, o, request.Mode) {
				return a.grantLocked(action)
			}
			if request.Wait == 0 {
				return fail(Conflict, "resource has conflicting holders or earlier waiters")
			}
			if a.queued >= a.options.MaxQueued || o.queued >= a.options.QueuedPerOwner || len(r.queue) >= a.options.QueuedPerResource {
				return fail(Capacity, "acquisition queue capacity exhausted")
			}
			if !a.clock.Now().Before(started.Add(request.Wait)) {
				a.finishLocked(action, TimedOut, "")
				return nil
			}
			action.waitUntil = started.Add(request.Wait)
			r.queue = append(r.queue, action)
			a.queued++
			o.queued++
			a.startWorkerLocked(r)
			a.signalLocked(r)
			return nil
		})
	}
	a.mu.Lock()
	action.processing = false
	r.holds--
	if err != nil && action.receipt.Outcome == Pending {
		action.failure = err
		a.finishLocked(action, Rejected, CodeOf(err))
	}
	a.mu.Unlock()
	a.wakeMaintenance()
	return a.QueryAction(ctx, request.Owner, request.Request)
}

func (a *Authority) compatibleLocked(r *resourceRecord, o *ownerRecord, mode Mode) bool {
	for _, g := range r.grants {
		if g.owner == o || mode == Exclusive || g.mode == Exclusive {
			return false
		}
	}
	return true
}

func (a *Authority) grantLocked(action *actionRecord) error {
	o, r := action.owner, action.resource
	if a.grants >= a.options.MaxGrants || o.liveGrants >= a.options.GrantsPerOwner {
		return fail(Capacity, "live grant capacity exhausted")
	}
	if a.lastGeneration == ^uint64(0) {
		return fail(Capacity, "grant generation exhausted")
	}
	a.lastGeneration++
	generation := a.lastGeneration
	parent := string(o.ref.Owner) + "/" + string(r.id) + "/" + strconv.FormatUint(generation, 10)
	ref := GrantRef{ID: GrantID(a.token("g", parent, "")), Resource: r.id, Generation: generation}
	g := &grantRecord{owner: o, resource: r, ref: ref, mode: action.receipt.Acquire.Mode, state: Active, deadline: a.clock.Now().Add(action.receipt.Acquire.TTL), revision: 1}
	r.grants[ref.ID] = g
	o.grants[ref.ID] = g
	a.grants++
	o.liveGrants++
	action.grant = g
	action.receipt.Grant = &g.ref
	action.receipt.DeadlineMillis = a.tick(g.deadline)
	action.receipt.Revision = 1
	a.finishLocked(action, Granted, "")
	a.touchLocked(o)
	a.wakeMaintenance()
	return nil
}

func (a *Authority) grantLockedForOwner(o *ownerRecord, ref GrantRef) (*grantRecord, error) {
	parent := string(o.ref.Owner) + "/" + string(ref.Resource) + "/" + strconv.FormatUint(ref.Generation, 10)
	if _, err := a.verify(string(ref.ID), "g", parent); err != nil {
		return nil, Wrap(StaleGrant, "grant capability is invalid", err)
	}
	g := o.grants[ref.ID]
	if g == nil || g.ref != ref {
		return nil, fail(StaleGrant, "grant history is unavailable")
	}
	a.expireGrantsLocked(g.resource, a.clock.Now())
	return g, nil
}

func (a *Authority) Renew(ctx context.Context, request RenewRequest) (ActionResult, error) {
	ctx, finish, admissionErr := a.beginNative(ctx)
	if admissionErr != nil {
		return ActionResult{}, admissionErr
	}
	defer finish()

	a.mu.Lock()
	if err := a.healthyLocked(ctx, true); err != nil {
		a.mu.Unlock()
		return ActionResult{}, err
	}
	a.expireSessionsLocked(a.clock.Now())
	o, err := a.ownerLocked(request.Owner)
	if err != nil {
		a.mu.Unlock()
		return ActionResult{}, err
	}
	if !requestValid(request.Request, a.options.MaxRequestBytes) || request.TTL < time.Millisecond || request.TTL > a.options.MaxLease || request.TTL%time.Millisecond != 0 {
		a.mu.Unlock()
		return ActionResult{}, fail(Invalid, "invalid renewal intent")
	}
	if previous := o.actions[request.Request]; previous != nil {
		if previous.receipt.Kind != RenewAction || previous.receipt.Renew == nil || *previous.receipt.Renew != request {
			a.mu.Unlock()
			return ActionResult{}, fail(RequestMismatch, "request ID has a different intent")
		}
		a.mu.Unlock()
		return a.QueryAction(ctx, request.Owner, request.Request)
	}
	if len(o.actions) >= a.options.ActionsPerOwner || a.actions >= a.options.MaxActions {
		a.mu.Unlock()
		return ActionResult{}, fail(Capacity, "action history capacity exhausted")
	}
	action := &actionRecord{owner: o, receipt: ActionReceipt{Kind: RenewAction, Request: request.Request, Renew: &request, Outcome: Pending}}
	o.actions[request.Request] = action
	a.actions++
	a.touchLocked(o)
	g, err := a.grantLockedForOwner(o, request.Grant)
	if err == nil && g.state != Active {
		err = fail(StaleGrant, "grant is not active")
	}
	if err != nil {
		a.finishLocked(action, Rejected, CodeOf(err))
		result, err := a.actionResultLocked(action)
		a.mu.Unlock()
		return result, err
	}
	action.grant = g
	action.resource = g.resource
	action.processing = true
	g.resource.holds++
	a.mu.Unlock()
	err = a.raise(ctx, request.TTL)
	if err == nil {
		err = a.native.Guard(ctx, g.resource.key, func() error {
			a.mu.Lock()
			defer a.mu.Unlock()
			if action.receipt.Outcome != Pending {
				return nil
			}
			if e := a.healthyLocked(ctx, true); e != nil {
				return e
			}
			a.expireSessionsLocked(a.clock.Now())
			if a.owners[o.ref.Owner] != o {
				return fail(Retired, "owner history is retired")
			}
			a.expireGrantsLocked(g.resource, a.clock.Now())
			if g.resource.retired || g.state != Active || !a.clock.Now().Before(g.deadline) {
				return fail(StaleGrant, "grant expired or was retired during renewal")
			}
			if g.resource.busy {
				return fail(Conflict, "resource publication has not completed")
			}
			deadline := a.clock.Now().Add(request.TTL)
			if deadline.After(g.deadline) {
				g.deadline = deadline
			}
			g.revision++
			action.receipt.Grant = &g.ref
			action.receipt.DeadlineMillis = a.tick(g.deadline)
			action.receipt.Revision = g.revision
			a.finishLocked(action, Renewed, "")
			a.touchLocked(o)
			a.signalLocked(g.resource)
			return nil
		})
	}
	a.mu.Lock()
	action.processing = false
	g.resource.holds--
	if err != nil && action.receipt.Outcome == Pending {
		action.failure = err
		a.finishLocked(action, Rejected, CodeOf(err))
	}
	a.mu.Unlock()
	a.wakeMaintenance()
	return a.QueryAction(ctx, request.Owner, request.Request)
}
