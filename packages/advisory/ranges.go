package advisory

import (
	"math"
	"sort"

	"github.com/codetreker/remote-fs/packages/storage"
)

func overlaps(a, b storage.Range) bool {
	if a.Kind == storage.Boundary {
		return b.Kind == storage.Bytes && boundaryOverlaps(b.Start, b.Length, a.CutAt)
	}
	if b.Kind == storage.Boundary {
		return boundaryOverlaps(a.Start, a.Length, b.CutAt)
	}
	return bytesOverlap(a.Start, a.Length, b.Start, b.Length)
}

// A replacement is built before live state changes. Fragment-budget rejection
// therefore preserves the old ranges, including on a partial subtraction.
func replaceRanges(old []rangeClaim, command storage.RangeCommand) []rangeClaim {
	next := make([]rangeClaim, 0, len(old)+2)
	start := command.Range.Start
	last, _ := byteLast(start, command.Range.Length)
	for _, existing := range old {
		if !overlaps(existing.command.Range, command.Range) {
			next = append(next, existing)
			continue
		}
		oldRange := existing.command.Range
		oldLast, _ := byteLast(oldRange.Start, oldRange.Length)
		if oldRange.Start < start {
			left := existing
			left.command.Range.Length = start - oldRange.Start
			next = append(next, left)
		}
		if oldLast > last {
			right := existing
			right.command.Range.Start = last + 1
			right.command.Range.Length = oldLast - last
			next = append(next, right)
		}
	}
	if command.Edit != storage.Subtract {
		command.Wait = false
		command.Conversion = storage.PreserveBeforeAcquire
		next = append(next, rangeClaim{command: command})
	}
	sort.Slice(next, func(i, j int) bool { return next[i].command.Range.Start < next[j].command.Range.Start })
	result := next[:0]
	for _, current := range next {
		if len(result) > 0 {
			previous := &result[len(result)-1]
			previousLast, _ := byteLast(previous.command.Range.Start, previous.command.Range.Length)
			if previousLast != math.MaxUint64 && previousLast+1 == current.command.Range.Start &&
				previous.command.Mode == current.command.Mode && previous.command.Policy == current.command.Policy &&
				current.command.Range.Length <= math.MaxUint64-previous.command.Range.Length {
				previous.command.Range.Length += current.command.Range.Length
				continue
			}
		}
		result = append(result, current)
	}
	return result
}

func (c *Coordinator) conflictLocked(key ownerKey, command storage.RangeCommand) storage.RangeConflict {
	var conflict storage.RangeConflict
	var selected ownerKey
	for other, state := range c.owners {
		if other == key || other.node != key.node || other.domain != key.domain {
			continue
		}
		for _, held := range state.ranges {
			if !overlaps(held.command.Range, command.Range) ||
				held.command.Mode == storage.RangeShared && command.Mode == storage.RangeShared {
				continue
			}
			if conflict.Found && (rangeStart(held.command.Range) > rangeStart(conflict.Range) ||
				rangeStart(held.command.Range) == rangeStart(conflict.Range) && !ownerBefore(other, selected)) {
				continue
			}
			owner := storage.OwnerDiagnostic(0)
			if other.session == key.session {
				owner = storage.OwnerDiagnostic(other.owner)
			}
			conflict = storage.RangeConflict{Found: true, Owner: owner, Range: held.command.Range, Mode: held.command.Mode}
			selected = other
		}
	}
	return conflict
}

func rangeStart(r storage.Range) uint64 {
	if r.Kind == storage.Boundary {
		return r.CutAt
	}
	return r.Start
}

func ownerBefore(a, b ownerKey) bool {
	return a.session < b.session || a.session == b.session && a.owner < b.owner
}

type deadlockParticipant struct {
	session  uint64
	identity uint64
	grouped  bool
}

func (c *Coordinator) participant(key ownerKey) deadlockParticipant {
	binding := c.sessions[key.session].bindings[key.owner]
	if binding.options.Group != 0 {
		return deadlockParticipant{session: key.session, identity: binding.options.Group, grouped: true}
	}
	return deadlockParticipant{session: key.session, identity: uint64(key.owner)}
}

// Record-domain owners share a wait graph across files. Other domains have no
// deadlock-detection promise and do not contribute edges to this graph.
func (c *Coordinator) deadlockLocked(candidate *request) (bool, bool) {
	edges := make(map[deadlockParticipant][]deadlockParticipant)
	count := 0
	add := func(r *request) bool {
		if r.key.domain != storage.DomainRecord {
			return true
		}
		for key, held := range c.owners {
			if key == r.key || key.node != r.key.node || key.domain != storage.DomainRecord {
				continue
			}
			conflicts := false
			for _, command := range r.commands {
				for _, lock := range held.ranges {
					if overlaps(lock.command.Range, command.Range) &&
						(lock.command.Mode == storage.RangeExclusive || command.Mode == storage.RangeExclusive) {
						conflicts = true
						break
					}
				}
				if conflicts {
					break
				}
			}
			if conflicts {
				if count == c.config.MaxDeadlockEdges {
					return false
				}
				from := c.participant(r.key)
				edges[from] = append(edges[from], c.participant(key))
				count++
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
	participant := c.participant(candidate.key)
	seen := make(map[deadlockParticipant]bool)
	stack := append([]deadlockParticipant(nil), edges[participant]...)
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == participant {
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
