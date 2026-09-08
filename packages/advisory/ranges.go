package advisory

import (
	"sort"

	"github.com/codetreker/remote-fs/packages/storage"
)

func overlaps(a, b storage.FileLock) bool {
	return a.Start <= b.End && b.Start <= a.End
}

// Replacing a subrange can add at most two fragments. The result is built before
// touching live state so a fragment-budget rejection preserves the original lock.
func replaceRanges(old []storage.FileLock, lock storage.FileLock) []storage.FileLock {
	next := make([]storage.FileLock, 0, len(old)+2)
	for _, existing := range old {
		if !overlaps(existing, lock) {
			next = append(next, existing)
			continue
		}
		if existing.Start < lock.Start {
			left := existing
			left.End = lock.Start - 1
			next = append(next, left)
		}
		if existing.End > lock.End {
			right := existing
			right.Start = lock.End + 1
			next = append(next, right)
		}
	}
	if lock.Type != storage.Unlock {
		lock.Wait = false
		next = append(next, lock)
	}
	sort.Slice(next, func(i, j int) bool { return next[i].Start < next[j].Start })
	result := next[:0]
	for _, current := range next {
		if len(result) > 0 {
			previous := &result[len(result)-1]
			if previous.End+1 == current.Start && previous.Type == current.Type {
				previous.End = current.End
				continue
			}
		}
		result = append(result, current)
	}
	return result
}

func (c *Coordinator) conflictLocked(key ownerKey, lock storage.FileLock) storage.LockConflict {
	var conflict storage.LockConflict
	var selected ownerKey
	for other, state := range c.owners {
		if other == key || other.node != key.node || other.family != key.family {
			continue
		}
		for _, held := range state.ranges {
			if !overlaps(held, lock) || held.Type == storage.Shared && lock.Type == storage.Shared {
				continue
			}
			if conflict.Found && (held.Start > conflict.Lock.Start ||
				held.Start == conflict.Lock.Start && !ownerBefore(other, selected)) {
				continue
			}
			if other.session != key.session {
				held.PID = 0
			}
			conflict = storage.LockConflict{Found: true, Owner: other.owner, Lock: held}
			selected = other
		}
	}
	return conflict
}

func ownerBefore(a, b ownerKey) bool {
	return a.session < b.session || a.session == b.session && a.owner < b.owner
}

// POSIX deadlocks are cycles between process owners across files. Flock waits
// are deliberately excluded: Linux does not promise flock deadlock detection.
func (c *Coordinator) deadlockLocked(candidate *request) (bool, bool) {
	edges := make(map[actor][]actor)
	count := 0
	add := func(r *request) bool {
		if r.key.family != storage.POSIX {
			return true
		}
		for key, held := range c.owners {
			if key == r.key || key.node != r.key.node || key.family != storage.POSIX {
				continue
			}
			for _, lock := range held.ranges {
				if overlaps(lock, r.lock) && (lock.Type == storage.Exclusive || r.lock.Type == storage.Exclusive) {
					if count == c.config.MaxDeadlockEdges {
						return false
					}
					edges[r.key.actor] = append(edges[r.key.actor], key.actor)
					count++
					break
				}
			}
		}
		return true
	}
	for _, pending := range c.waiting {
		if !add(pending) {
			return false, false
		}
	}
	if !add(candidate) {
		return false, false
	}
	seen := make(map[actor]bool)
	stack := append([]actor(nil), edges[candidate.key.actor]...)
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == candidate.key.actor {
			return true, true
		}
		if seen[current] {
			continue
		}
		seen[current] = true
		stack = append(stack, edges[current]...)
	}
	return false, true
}
