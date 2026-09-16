package fileaccess

import "slices"

func (c *Coordinator) Snapshot(owner Owner, scope Scope) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOwnerScopeLocked(owner, scope); err != nil {
		return Snapshot{}, err
	}
	record := c.owners[owner]
	var own []Acquisition
	ownedRanges := 0
	if record != nil {
		own = record.sets[scope]
		ownedRanges = record.ranges
	}
	total := 0
	for _, ranges := range c.scopes[scope] {
		if len(ranges) > c.limits.MaxSnapshotRanges-total {
			return Snapshot{}, ErrCapacity
		}
		total += len(ranges)
	}
	snapshot := Snapshot{Revision: c.revision, Own: slices.Clone(own), Other: make([]Held, 0, total-len(own)),
		Available:      min(c.limits.MaxSetRanges, c.limits.MaxRanges-c.ranges+len(own)),
		OwnerAvailable: min(c.limits.MaxSetRanges, c.limits.MaxOwnerRanges-ownedRanges+len(own))}
	if record == nil && len(c.owners) == c.limits.MaxOwners {
		snapshot.OwnerAvailable = 0
	}
	for other, ranges := range c.scopes[scope] {
		if other != owner {
			for _, held := range ranges {
				snapshot.Other = append(snapshot.Other, Held{Owner: other, Acquisition: held})
			}
		}
	}
	slices.SortFunc(snapshot.Other, compareHeld)
	return snapshot, nil
}

// ReplaceOwned validates even unchanged sets against the guard revision. The
// coordinator preserves acquisition identities and does not merge intervals.
func (c *Coordinator) ReplaceOwned(owner Owner, scope Scope, revision uint64, proposed []Acquisition, guard Guard) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOwnerScopeLocked(owner, scope); err != nil {
		return c.revision, err
	}
	if revision == 0 {
		return c.revision, ErrInvalid
	}
	if revision != c.revision {
		return c.revision, ErrRevision
	}
	next, err := c.copySet(proposed)
	if err != nil {
		return c.revision, err
	}
	if _, err := c.blockersLocked(owner, scope, next, false); err != nil {
		return c.revision, err
	}
	record := c.owners[owner]
	var old []Acquisition
	ownedRanges := 0
	if record != nil {
		old = record.sets[scope]
		ownedRanges = record.ranges
	}
	delta := len(next) - len(old)
	if delta > c.limits.MaxRanges-c.ranges || delta > c.limits.MaxOwnerRanges-ownedRanges {
		return c.revision, ErrCapacity
	}
	unchanged := slices.Equal(old, next)
	if !unchanged && record == nil && len(c.owners) == c.limits.MaxOwners {
		return c.revision, ErrCapacity
	}
	if err := guard.check(); err != nil {
		return c.revision, err
	}
	if unchanged {
		return c.revision, nil
	}
	if err := c.changeLocked(nil); err != nil {
		return c.revision, err
	}
	if record == nil {
		record = &ownerRecord{sets: make(map[Scope][]Acquisition)}
		c.owners[owner] = record
	}
	c.ranges += delta
	record.ranges += delta
	if len(next) == 0 {
		delete(record.sets, scope)
		delete(c.scopes[scope], owner)
		if len(c.scopes[scope]) == 0 {
			delete(c.scopes, scope)
		}
	} else {
		record.sets[scope] = next
		if c.scopes[scope] == nil {
			c.scopes[scope] = make(map[Owner][]Acquisition)
		}
		c.scopes[scope][owner] = next
	}
	if record.ranges == 0 {
		delete(c.owners, owner)
	}
	return c.revision, nil
}

func (c *Coordinator) checkOwnerScopeLocked(owner Owner, scope Scope) error {
	if c.failure != nil {
		return c.failure
	}
	if !owner.valid() || !scope.valid() {
		return ErrInvalid
	}
	return nil
}

func (c *Coordinator) copySet(proposed []Acquisition) ([]Acquisition, error) {
	if len(proposed) > c.limits.MaxSetRanges {
		return nil, ErrCapacity
	}
	next := slices.Clone(proposed)
	slices.SortFunc(next, func(a, b Acquisition) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for i, entry := range next {
		if entry.ID == 0 || !(Span{Start: entry.Start, End: entry.End, Boundary: entry.Boundary}).valid() || i != 0 && next[i-1].ID == entry.ID {
			return nil, ErrInvalid
		}
	}
	return next, nil
}

func (c *Coordinator) blockersLocked(owner Owner, scope Scope, proposed []Acquisition, all bool) ([]Owner, error) {
	var blockers []Owner
	work := 0
	for other, held := range c.scopes[scope] {
		if other == owner {
			continue
		}
		blocked := false
		for _, existing := range held {
			for _, next := range proposed {
				if work == c.limits.MaxWork {
					return nil, ErrCapacity
				}
				work++
				if overlap(existing, next) && (existing.Exclusive || next.Exclusive) {
					if !all {
						return nil, &Conflict{Held: Held{Owner: other, Acquisition: existing}}
					}
					blocked = true
					break
				}
			}
			if blocked {
				break
			}
		}
		if blocked {
			if len(blockers) == c.limits.MaxDependencies {
				return nil, ErrCapacity
			}
			blockers = append(blockers, other)
		}
	}
	return blockers, nil
}

// CheckIO ignores advisory domains. An absent actor never borrows the rights
// of owner zero; an exclusive holder may access its own interval, while every
// shared acquisition still excludes writes, including its holder's writes.
func (c *Coordinator) CheckIO(resource uint64, actor *Owner, region Span, write bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if resource == 0 || !region.valid() {
		return ErrInvalid
	}
	if actor != nil {
		if !actor.valid() {
			return ErrInvalid
		}
	}
	intent := Acquisition{Start: region.Start, End: region.End, Boundary: region.Boundary}
	work := 0
	for owner, ranges := range c.scopes[Scope{Resource: resource, Enforced: true}] {
		for _, held := range ranges {
			if work == c.limits.MaxWork {
				return ErrCapacity
			}
			work++
			if !overlap(held, intent) {
				continue
			}
			if held.Exclusive && actor != nil && owner == *actor {
				continue
			}
			if held.Exclusive || write {
				return &Conflict{Held: Held{Owner: owner, Acquisition: held}}
			}
		}
	}
	return nil
}

func compareHeld(a, b Held) int {
	for _, pair := range [][2]uint64{{a.Acquisition.Start, b.Acquisition.Start}, {a.Owner.Session, b.Owner.Session}, {a.Owner.ID, b.Owner.ID}, {a.Acquisition.ID, b.Acquisition.ID}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	return 0
}
