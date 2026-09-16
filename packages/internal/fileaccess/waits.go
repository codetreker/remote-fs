package fileaccess

import (
	"context"
	"errors"
)

// Wait reports a changed revision or an ended registration. It never grants a
// range; clients must capture and commit their next owned set independently.
type Wait struct {
	coordinator  *Coordinator
	id           uint64
	owner        Owner
	scope        Scope
	proposed     []Acquisition
	dependencies []Owner
	done         chan struct{}
	revision     uint64
	err          error
	pending      bool
}

func (c *Coordinator) RegisterWait(id uint64, owner Owner, scope Scope, revision uint64, proposed []Acquisition, detectDeadlock bool, guard Guard) (*Wait, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOwnerScopeLocked(owner, scope); err != nil {
		return nil, err
	}
	if id == 0 || revision == 0 {
		return nil, ErrInvalid
	}
	if revision != c.revision {
		return nil, ErrRevision
	}
	if c.waits[id] != nil {
		return nil, ErrInvalid
	}
	if len(c.waits) == c.limits.MaxWaits || len(proposed) > c.limits.MaxWaitRanges-c.waitRanges {
		return nil, ErrCapacity
	}
	next, err := c.copySet(proposed)
	if err != nil {
		return nil, err
	}
	var dependencies []Owner
	if detectDeadlock {
		dependencies, err = c.blockersLocked(owner, scope, next, true)
		if err != nil {
			return nil, err
		}
		if len(dependencies) > c.limits.MaxDependencies-c.dependencies {
			return nil, ErrCapacity
		}
		if err := c.checkCycleLocked(owner, dependencies); err != nil {
			return nil, err
		}
	}
	if err := guard.check(); err != nil {
		return nil, err
	}
	wait := &Wait{coordinator: c, id: id, owner: owner, scope: scope, proposed: next, dependencies: dependencies,
		done: make(chan struct{}), revision: c.revision, pending: true}
	c.waits[id] = wait
	c.waitRanges += len(next)
	c.dependencies += len(dependencies)
	return wait, nil
}

func (c *Coordinator) checkCycleLocked(owner Owner, dependencies []Owner) error {
	graph := make(map[Owner][]Owner)
	work := 0
	for _, wait := range c.waits {
		for _, dependency := range wait.dependencies {
			if work == c.limits.MaxWork {
				return ErrCapacity
			}
			work++
			graph[wait.owner] = append(graph[wait.owner], dependency)
		}
	}
	seen := make(map[Owner]bool)
	stack := append([]Owner(nil), dependencies...)
	for len(stack) > 0 {
		if work == c.limits.MaxWork {
			return ErrCapacity
		}
		work++
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == owner {
			return ErrDeadlock
		}
		if seen[current] {
			continue
		}
		seen[current] = true
		for _, next := range graph[current] {
			if work == c.limits.MaxWork {
				return ErrCapacity
			}
			work++
			stack = append(stack, next)
		}
	}
	return nil
}

func (w *Wait) Await(ctx context.Context) (uint64, error) {
	select {
	case <-w.done:
	case <-ctx.Done():
		w.cancel(errors.Join(ctx.Err(), context.Cause(ctx)))
	}
	c := w.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	return w.revision, w.err
}

func (w *Wait) Cancel() { w.cancel(ErrCanceled) }

func (w *Wait) cancel(cause error) {
	c := w.coordinator
	c.mu.Lock()
	defer c.mu.Unlock()
	if w.pending {
		c.finishWaitLocked(w, cause)
	}
}

func (c *Coordinator) CancelOwnerWaits(owner Owner, scope Scope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOwnerScopeLocked(owner, scope); err != nil {
		return err
	}
	for _, wait := range c.waits {
		if wait.owner == owner && wait.scope == scope {
			c.finishWaitLocked(wait, ErrCanceled)
		}
	}
	return nil
}

func (c *Coordinator) finishWaitLocked(wait *Wait, cause error) {
	delete(c.waits, wait.id)
	c.waitRanges -= len(wait.proposed)
	c.dependencies -= len(wait.dependencies)
	wait.proposed, wait.dependencies = nil, nil
	wait.revision, wait.err, wait.pending = c.revision, cause, false
	close(wait.done)
}

// A state change invalidates every dependency derived from its previous guard.
// Removing those edges together with notification prevents stale graph cycles.
func (c *Coordinator) finishWaitsLocked(cause error, retired func(Owner) bool) {
	for _, wait := range c.waits {
		failure := cause
		if retired != nil && retired(wait.owner) {
			failure = ErrRetired
		}
		c.finishWaitLocked(wait, failure)
	}
}
