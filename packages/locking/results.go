package locking

import "context"

func (a *Authority) grantStatusLocked(g *grantRecord) GrantStatus {
	now := a.clock.Now()
	a.expireGrantsLocked(g.resource, now)
	remaining := g.deadline.Sub(now)
	if remaining < 0 || g.state != Active {
		remaining = 0
	}
	return GrantStatus{Ref: g.ref, Mode: g.mode, State: g.state, Revision: g.revision, DeadlineMillis: a.tick(g.deadline),
		RemainingMillis: remaining.Milliseconds(), NowMillis: a.tick(now), HistoryExpiresMillis: a.tick(g.owner.session.expires)}
}

func (a *Authority) actionResultLocked(action *actionRecord) (ActionResult, error) {
	receipt := action.receipt
	if receipt.Acquire != nil {
		copy := *receipt.Acquire
		receipt.Acquire = &copy
	}
	if receipt.Renew != nil {
		copy := *receipt.Renew
		receipt.Renew = &copy
	}
	if receipt.Grant != nil {
		copy := *receipt.Grant
		receipt.Grant = &copy
	}
	result := ActionResult{Recorded: true, Receipt: receipt, NowMillis: a.tick(a.clock.Now()), HistoryExpiresMillis: a.tick(action.owner.session.expires)}
	if action.grant != nil {
		status := a.grantStatusLocked(action.grant)
		result.Grant = &status
	}
	if receipt.Outcome == Rejected {
		return result, &Error{Code: receipt.Code, Recorded: true, Message: "management action was rejected", Cause: action.failure}
	}
	return result, nil
}

func (a *Authority) QueryAction(ctx context.Context, ref OwnerRef, request RequestID) (ActionResult, error) {
	for {
		a.mu.Lock()
		if err := a.healthyLocked(ctx, false); err != nil {
			a.mu.Unlock()
			return ActionResult{}, err
		}
		a.expireSessionsLocked(a.clock.Now())
		o, err := a.ownerLocked(ref)
		if err != nil {
			a.mu.Unlock()
			return ActionResult{}, err
		}
		if !requestValid(request, a.options.MaxRequestBytes) {
			a.mu.Unlock()
			return ActionResult{}, fail(Invalid, "invalid action request ID")
		}
		action := o.actions[request]
		if action == nil {
			a.mu.Unlock()
			return ActionResult{}, fail(OutcomeUnknown, "action was not observed; a delayed request may still be admitted")
		}
		if action.grant != nil && action.grant.resource.busy {
			done := action.grant.resource.busyDone
			a.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ActionResult{}, ctx.Err()
			}
		}
		if action.receipt.Outcome == Pending && !action.waitUntil.IsZero() && !a.clock.Now().Before(action.waitUntil) {
			a.finishLocked(action, TimedOut, "")
		}
		a.touchLocked(o)
		result, err := a.actionResultLocked(action)
		a.mu.Unlock()
		return result, err
	}
}

func (a *Authority) QueryGrant(ctx context.Context, ref OwnerRef, grant GrantRef) (GrantStatus, error) {
	for {
		a.mu.Lock()
		if err := a.healthyLocked(ctx, false); err != nil {
			a.mu.Unlock()
			return GrantStatus{}, err
		}
		a.expireSessionsLocked(a.clock.Now())
		o, err := a.ownerLocked(ref)
		if err != nil {
			a.mu.Unlock()
			return GrantStatus{}, err
		}
		g, err := a.grantLockedForOwner(o, grant)
		if err != nil {
			a.mu.Unlock()
			return GrantStatus{}, err
		}
		if g.resource.busy {
			done := g.resource.busyDone
			a.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return GrantStatus{}, ctx.Err()
			}
		}
		a.touchLocked(o)
		result := a.grantStatusLocked(g)
		a.mu.Unlock()
		return result, nil
	}
}

func (a *Authority) Release(ctx context.Context, ref OwnerRef, grant GrantRef) (ReleaseResult, error) {
	for {
		a.mu.Lock()
		if err := a.healthyLocked(ctx, false); err != nil {
			a.mu.Unlock()
			return ReleaseResult{}, err
		}
		a.expireSessionsLocked(a.clock.Now())
		o, err := a.ownerLocked(ref)
		if err != nil {
			a.mu.Unlock()
			return ReleaseResult{}, err
		}
		g, err := a.grantLockedForOwner(o, grant)
		if err != nil {
			a.mu.Unlock()
			return ReleaseResult{}, err
		}
		if g.resource.busy {
			done := g.resource.busyDone
			a.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ReleaseResult{}, ctx.Err()
			}
		}
		if g.release == nil {
			if g.state == Active {
				a.endGrantLocked(g, Released)
			}
			g.release = &ReleaseResult{State: g.state}
		}
		a.touchLocked(o)
		result := *g.release
		result.Grant = a.grantStatusLocked(g)
		result.NowMillis = a.tick(a.clock.Now())
		result.HistoryExpiresMillis = a.tick(o.session.expires)
		a.mu.Unlock()
		return result, nil
	}
}

func (a *Authority) Cancel(ctx context.Context, ref OwnerRef, request RequestID) (CancelResult, error) {
	for {
		a.mu.Lock()
		if err := a.healthyLocked(ctx, false); err != nil {
			a.mu.Unlock()
			return CancelResult{}, err
		}
		a.expireSessionsLocked(a.clock.Now())
		o, err := a.ownerLocked(ref)
		if err != nil {
			a.mu.Unlock()
			return CancelResult{}, err
		}
		if !requestValid(request, a.options.MaxRequestBytes) {
			a.mu.Unlock()
			return CancelResult{}, fail(Invalid, "invalid cancellation request ID")
		}
		action := o.actions[request]
		if action == nil {
			if len(o.actions) >= a.options.ActionsPerOwner || a.actions >= a.options.MaxActions {
				a.mu.Unlock()
				return CancelResult{}, fail(OutcomeUnknown, "cancellation history capacity exhausted")
			}
			action = &actionRecord{owner: o, receipt: ActionReceipt{Kind: AcquireAction, Request: request, Outcome: Cancelled}, tombstone: true}
			o.actions[request] = action
			a.actions++
		}
		if action.receipt.Kind != AcquireAction {
			a.mu.Unlock()
			return CancelResult{}, fail(RequestMismatch, "request ID identifies a renewal")
		}
		if action.grant != nil && action.grant.resource.busy {
			done := action.grant.resource.busyDone
			a.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return CancelResult{}, ctx.Err()
			}
		}
		if action.cancel == nil {
			if action.receipt.Outcome == Pending {
				a.finishLocked(action, Cancelled, "")
			}
			released := false
			if action.grant != nil {
				a.expireGrantsLocked(action.grant.resource, a.clock.Now())
				if action.grant.state == Active {
					a.endGrantLocked(action.grant, Released)
					released = true
				}
			}
			action.cancel = &CancelResult{Outcome: action.receipt.Outcome, Released: released}
		}
		a.touchLocked(o)
		result := *action.cancel
		result.NowMillis = a.tick(a.clock.Now())
		result.HistoryExpiresMillis = a.tick(o.session.expires)
		a.mu.Unlock()
		return result, nil
	}
}
