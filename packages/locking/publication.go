package locking

import (
	"context"
	"errors"
)

// Fence preserves uncertain backend outcomes until the volume is reopened.
func (a *Authority) Fence(err error) {
	if err == nil {
		return
	}
	a.mu.Lock()
	if a.fenced == nil {
		a.fenced = err
	}
	for _, r := range a.resources {
		a.signalLocked(r)
	}
	a.mu.Unlock()
}

// Check permits reads during recovery but refuses an uncertain or closed authority.
func (a *Authority) Check(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.healthyLocked(ctx, false)
}

func (a *Authority) Publish(ctx context.Context, publication Publication, transition func() PublicationOutcome) error {
	if transition == nil {
		return fail(Invalid, "publication transition is required")
	}
	if len(publication.Targets) > 2 {
		return fail(Invalid, "publication target limit exceeded")
	}
	switch publication.Kind {
	case WriteMutation, SetAttrMutation, RemoveMutation, RenameMutation, CreateMutation:
	default:
		return fail(Invalid, "unknown mutation kind")
	}
	if err := ValidateScope(publication.Scope, a.options.MaxProofs); err != nil {
		return err
	}
	a.mu.Lock()
	if err := a.healthyLocked(ctx, true); err != nil {
		a.mu.Unlock()
		return err
	}
	a.expireSessionsLocked(a.clock.Now())
	var owner *ownerRecord
	var err error
	if publication.Scope.Owner.Owner != "" {
		owner, err = a.ownerLocked(publication.Scope.Owner)
		if err != nil {
			a.mu.Unlock()
			return err
		}
	}
	keys := make(map[BackendKey]bool, len(publication.Targets))
	resources := make([]*resourceRecord, 0, len(publication.Targets))
	for _, key := range publication.Targets {
		if key == "" {
			a.mu.Unlock()
			return fail(Invalid, "publication contains an empty target")
		}
		if keys[key] {
			continue
		}
		keys[key] = true
		if r := a.keys[key]; r != nil {
			if r.busy {
				a.mu.Unlock()
				return fail(Unavailable, "backend publication ordering was violated")
			}
			a.expireGrantsLocked(r, a.clock.Now())
			resources = append(resources, r)
		}
	}
	proofs := make(map[ResourceID]*grantRecord, len(publication.Scope.Grants))
	for _, proof := range publication.Scope.Grants {
		g, e := a.grantLockedForOwner(owner, proof)
		if e != nil {
			a.mu.Unlock()
			return e
		}
		if g.state != Active || g.resource.retired || !a.clock.Now().Before(g.deadline) {
			a.mu.Unlock()
			return fail(StaleGrant, "mutation grant is not active")
		}
		if !keys[g.resource.key] {
			a.mu.Unlock()
			return fail(UnrelatedProof, "mutation grant does not protect an affected resource")
		}
		proofs[proof.Resource] = g
	}
	for _, r := range resources {
		if len(r.grants) == 0 {
			continue
		}
		g := proofs[r.id]
		if g == nil || g.owner != owner || g.mode != Exclusive {
			a.mu.Unlock()
			return fail(Conflict, "mutation lacks an exclusive grant for a protected resource")
		}
	}
	admission := a.clock.Now()
	for _, g := range proofs {
		if !admission.Before(g.deadline) {
			a.mu.Unlock()
			return fail(StaleGrant, "mutation grant expired before publication admission")
		}
	}
	for _, r := range resources {
		r.busy = true
		r.busyDone = make(chan struct{})
	}
	if a.publications == 0 {
		a.publicationDone = make(chan struct{})
	}
	a.publications++
	a.mu.Unlock()

	// Publication owns only these resources while native I/O runs. Unrelated
	// grant and publication transitions continue using the authority mutex.
	finished := false
	defer func() {
		if !finished {
			a.completePublication(resources, keys, PublicationOutcome{Err: fail(Unavailable, "publication did not report an outcome")})
		}
	}()
	outcome := transition()
	err = a.completePublication(resources, keys, outcome)
	finished = true
	return err
}

func (a *Authority) completePublication(resources []*resourceRecord, targets map[BackendKey]bool, outcome PublicationOutcome) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, key := range outcome.Retired {
		if !targets[key] {
			outcome.Known = false
			outcome.Err = errors.Join(outcome.Err, fail(Unavailable, "publication retired an undeclared target"))
			break
		}
	}
	if !outcome.Known {
		if outcome.Err == nil {
			outcome.Err = fail(Unavailable, "publication outcome is unknown")
		}
		if a.fenced == nil {
			a.fenced = outcome.Err
		}
	} else {
		for _, key := range outcome.Retired {
			if r := a.keys[key]; r != nil {
				r.retired = true
				r.referenceUntil = a.clock.Now()
				for _, g := range r.grants {
					a.endGrantLocked(g, TargetGone)
				}
				for _, action := range append([]*actionRecord(nil), r.queue...) {
					a.finishLocked(action, Rejected, StaleResource)
				}
			}
		}
	}
	for _, r := range resources {
		r.busy = false
		close(r.busyDone)
		r.busyDone = nil
		a.signalLocked(r)
	}
	a.publications--
	if a.publications == 0 {
		close(a.publicationDone)
	}
	a.wakeMaintenance()
	return outcome.Err
}
