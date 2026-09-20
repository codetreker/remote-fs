package advisory

import (
	"slices"
	"strings"

	"github.com/codetreker/remote-fs/packages/storage"
)

type editResult struct {
	ranges         []rangeClaim
	effects        []storage.RangeEffect
	claims         []storage.ClaimID
	releaseRanges  []rangeClaim
	releaseEffects []storage.RangeEffect
	releaseIndexes []int
	acquired       bool
	conflict       storage.RangeConflict
	rejected       storage.RejectionCode
}

func (c *Coordinator) draftLocked(action *request) editResult {
	live := c.owners[action.key].ranges
	result := editResult{ranges: slices.Clone(live), releaseRanges: slices.Clone(live)}
	for index, command := range action.commands {
		if action.released[index] {
			continue
		}
		switch command.Edit {
		case storage.Replace, storage.AddExact:
			if conflict := c.conflictLocked(action.key, command); conflict.Found {
				result.conflict, result.rejected = conflict, storage.RangeBlocked
				return result
			}
		}
		effect := storage.RangeEffect{Command: command}
		switch command.Edit {
		case storage.Replace, storage.Subtract:
			result.ranges = replaceRanges(result.ranges, command)
			effect.Released = command.Edit == storage.Subtract
			result.acquired = result.acquired || command.Edit == storage.Replace
			if command.Edit == storage.Subtract {
				next := replaceRanges(result.releaseRanges, command)
				if !c.replacementFitsLocked(action.key, len(next)) {
					result.rejected = storage.RangeExhausted
					return result
				}
				result.releaseRanges = next
				result.releaseEffects = append(result.releaseEffects, effect)
				result.releaseIndexes = append(result.releaseIndexes, index)
			}
		case storage.AddExact:
			id, _ := storage.NewClaimID(action.id, index)
			held := command
			held.Wait = false
			held.Conversion = storage.PreserveBeforeAcquire
			result.ranges = append(result.ranges, rangeClaim{id: id, command: held})
			result.claims = append(result.claims, id)
			effect.Claim = id
			result.acquired = true
		case storage.RemoveExact:
			found := slices.IndexFunc(result.ranges, func(held rangeClaim) bool { return held.id == command.Claim })
			if found < 0 {
				result.rejected = storage.RangeNotHeld
				return result
			}
			held := result.ranges[found].command
			if held.Range != command.Range || held.Mode != command.Mode || held.Policy != command.Policy {
				result.rejected = storage.RangeInvalid
				return result
			}
			result.ranges = slices.Delete(result.ranges, found, found+1)
			effect.Claim, effect.Released = command.Claim, true
			if release := slices.IndexFunc(result.releaseRanges, func(held rangeClaim) bool { return held.id == command.Claim }); release >= 0 {
				result.releaseRanges = slices.Delete(result.releaseRanges, release, release+1)
			}
			result.releaseEffects = append(result.releaseEffects, effect)
			result.releaseIndexes = append(result.releaseIndexes, index)
		}
		result.effects = append(result.effects, effect)
	}
	return result
}

func (c *Coordinator) commitReleasePrefixLocked(action *request, edit editResult) {
	if len(edit.releaseEffects) == 0 {
		return
	}
	// Each retained release was checked against the unchanged live state while
	// drafting. Acquisitions remain only in edit.ranges and are discarded here.
	c.installRangesLocked(action.key, edit.releaseRanges)
	action.result.Effects = append(action.result.Effects, edit.releaseEffects...)
	for _, index := range edit.releaseIndexes {
		action.released[index] = true
	}
	c.reconcileRemovedClaimsLocked(action.key, edit.releaseEffects)
}

func (c *Coordinator) commitEditLocked(action *request, edit editResult) {
	if err := c.replaceLocked(action.key, edit.ranges); err != nil {
		c.commitReleasePrefixLocked(action, edit)
		c.completeLocked(action, storage.Rejected, storage.RangeExhausted)
		return
	}
	action.result.Effects = append(action.result.Effects, edit.effects...)
	action.result.Claims = edit.claims
	state := storage.Released
	if edit.acquired {
		state = storage.Granted
	}
	c.completeLocked(action, state, "")
	c.reconcileRemovedClaimsLocked(action.key, edit.effects)
}

func (c *Coordinator) reconcileRemovedClaimsLocked(key ownerKey, effects []storage.RangeEffect) {
	s := c.sessions[key.session]
	removed := make(map[storage.LockRequestID]bool)
	for _, effect := range effects {
		if !effect.Released || effect.Claim == "" {
			continue
		}
		index := strings.LastIndexByte(string(effect.Claim), ':')
		removed[storage.LockRequestID(effect.Claim[:index])] = true
	}
	if len(removed) == 0 {
		return
	}
	live := make(map[storage.ClaimID]bool)
	if owner := c.owners[key]; owner != nil {
		for _, held := range owner.ranges {
			live[held.id] = true
		}
	}
	for id := range removed {
		action := s.actions[id]
		if action == nil || action.key != key || action.result.State != storage.Granted {
			continue
		}
		if !slices.ContainsFunc(action.result.Claims, func(claim storage.ClaimID) bool { return live[claim] }) {
			c.completeLocked(action, storage.Released, "")
		}
	}
}

func (c *Coordinator) startLocked(action *request) {
	state := c.ownerLocked(action.key)
	defer c.pruneOwnerLocked(action.key)
	first := action.commands[0]
	dropping := !action.prepared && first.Conversion == storage.DropBeforeAcquire && len(state.ranges) > 0 && state.ranges[0].command.Mode != first.Mode
	projectedEffects := len(action.result.Effects)
	for index := range action.commands {
		if !action.released[index] {
			projectedEffects++
		}
	}
	if dropping {
		projectedEffects += len(state.ranges)
	}
	projectedClaims := 0
	for _, command := range action.commands {
		if command.Edit == storage.AddExact {
			projectedClaims++
		}
	}
	if projectedEffects > storage.MaxRangeEffects || projectedClaims > storage.MaxRangeClaims {
		c.completeLocked(action, storage.Rejected, storage.RangeTooLarge)
		return
	}
	if dropping {
		for _, held := range state.ranges {
			action.result.Effects = append(action.result.Effects, storage.RangeEffect{Claim: held.id, Command: held.command, Released: true})
		}
		_ = c.replaceLocked(action.key, nil)
		action.prepared, action.converting = true, true
		return
	}
	edit := c.draftLocked(action)
	if edit.rejected == "" {
		c.commitEditLocked(action, edit)
		return
	}
	c.commitReleasePrefixLocked(action, edit)
	action.result.Conflict = edit.conflict
	if edit.rejected != storage.RangeBlocked || !first.Wait {
		c.completeLocked(action, storage.Rejected, edit.rejected)
		return
	}
	s := c.sessions[action.key.session]
	if len(c.waiting) >= c.config.MaxWaiters || s.pending >= s.options.MaxPendingLocks || s.pending >= s.options.MaxWaiters {
		c.completeLocked(action, storage.Rejected, storage.RangeExhausted)
		return
	}
	if action.key.domain == storage.DomainRecord {
		deadlock, bounded := c.deadlockLocked(action)
		if deadlock || !bounded {
			code := storage.RangeExhausted
			if deadlock {
				code = storage.RangeDeadlock
			}
			c.completeLocked(action, storage.Rejected, code)
			return
		}
	}
	action.pending = true
	state.pending++
	s.pending++
	c.waiting = append(c.waiting, action)
}
