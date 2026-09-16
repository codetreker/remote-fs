package fuse

import (
	"context"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

const advisoryRetries = 8

func advisoryScope(family lockFamily) storage.RangeScope {
	if family == flockFamily {
		return storage.RangeScope{Domain: 0x464c4f434b}
	}
	return storage.RangeScope{Domain: 0x504f534958}
}

func (v *volume) localOwners() *advisoryOwners {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.owners == nil {
		v.owners = &advisoryOwners{limit: v.sessionOptions.MaxRangeOwners}
	}
	return v.owners
}

func checkRangeSnapshot(snapshot storage.RangeSnapshot, family lockFamily) error {
	if snapshot.Revision == 0 || snapshot.Available < 0 || snapshot.OwnerAvailable < 0 {
		return syscall.EIO
	}
	ids := make(map[storage.RangeAcquisitionID]bool, len(snapshot.Own))
	for _, held := range snapshot.Own {
		if held.Check() != nil || held.Boundary || held.End > math.MaxInt64 || ids[held.ID] {
			return syscall.EIO
		}
		ids[held.ID] = true
		if family == flockFamily && (held.Start != 0 || held.End != math.MaxInt64 || len(snapshot.Own) > 1) {
			return syscall.EIO
		}
	}
	for _, held := range snapshot.Other {
		if held.Owner.Session == "" || held.Range.Check() != nil || held.Range.End > math.MaxInt64 {
			return syscall.EIO
		}
	}
	return nil
}

func conflictingRange(snapshot storage.RangeSnapshot, lock fileLock) (storage.HeldRange, bool) {
	var selected storage.HeldRange
	found := false
	for _, other := range snapshot.Other {
		overlaps := other.Range.Start <= lock.End && lock.Start <= other.Range.End
		if other.Range.Boundary {
			overlaps = lock.Start < other.Range.Start && other.Range.Start <= lock.End
		}
		if !overlaps || !other.Range.Exclusive && lock.Type == sharedType {
			continue
		}
		if !found || other.Range.Start < selected.Range.Start || other.Range.Start == selected.Range.Start &&
			(other.Owner.Session < selected.Owner.Session || other.Owner.Session == selected.Owner.Session && other.Owner.ID < selected.Owner.ID) {
			selected, found = other, true
		}
	}
	return selected, found
}

func ownLocks(snapshot storage.RangeSnapshot, family lockFamily) []fileLock {
	out := make([]fileLock, len(snapshot.Own))
	for i, held := range snapshot.Own {
		kind := sharedType
		if held.Exclusive {
			kind = exclusiveType
		}
		out[i] = fileLock{Family: family, Type: kind, Start: held.Start, End: held.End}
	}
	return out
}

func (v *volume) rangeAcquisitions(locks []fileLock) ([]storage.RangeAcquisition, error) {
	if len(locks) > min(v.sessionOptions.MaxRanges, storage.MaxRangeAcquisitions) {
		return nil, syscall.ENOLCK
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if uint64(len(locks)) > math.MaxUint64-v.nextRange {
		return nil, syscall.ENOLCK
	}
	out := make([]storage.RangeAcquisition, len(locks))
	for i, lock := range locks {
		v.nextRange++
		out[i] = storage.RangeAcquisition{ID: storage.RangeAcquisitionID(v.nextRange), Start: lock.Start, End: lock.End, Exclusive: lock.Type == exclusiveType}
	}
	return out, nil
}

func (h *handle) replaceAdvisory(ctx context.Context, owner lockOwner, lock fileLock) syscall.Errno {
	v := h.node.volume
	key := advisoryOwnerKey{node: h.node.id.node, owner: owner, family: lock.Family}
	owners := v.localOwners()
	state, release, err := owners.pin(key)
	if err != nil {
		return errnoOf(err)
	}
	defer release()
	state.mu.Lock()
	generation := state.generation
	state.mu.Unlock()
	active, stop := context.WithCancel(ctx)
	defer stop()
	stopGeneration := context.AfterFunc(generation, stop)
	defer stopGeneration()
	for attempts := 0; ; attempts++ {
		state.mu.Lock()
		if generation.Err() != nil {
			state.mu.Unlock()
			return syscall.EINTR
		}
		if active.Err() != nil {
			state.mu.Unlock()
			return errnoOf(active.Err())
		}
		if err := h.check(); err != nil {
			state.mu.Unlock()
			return errnoOf(err)
		}
		call, cancel := context.WithTimeout(active, v.flushTimeout)
		snapshot, err := h.file.RangeSnapshot(call, storage.RangeOwnerID(owner), advisoryScope(lock.Family))
		cancel()
		if err == nil {
			err = checkRangeSnapshot(snapshot, lock.Family)
		}
		if err != nil {
			state.mu.Unlock()
			return errnoOf(err)
		}
		old := ownLocks(snapshot, lock.Family)
		dropConversion := lock.Family == flockFamily && len(old) != 0 && old[0].Type != lock.Type
		var planned []fileLock
		if !dropConversion {
			planned = replacePOSIXRanges(old, lock)
		}
		capacityDenied := len(planned) > min(snapshot.Available, snapshot.OwnerAvailable)
		ranges := snapshot.Own
		if !capacityDenied {
			ranges, err = v.rangeAcquisitions(planned)
			if err != nil {
				state.mu.Unlock()
				return errnoOf(err)
			}
		}
		request := storage.RangeReplaceRequest{Owner: storage.RangeOwnerID(owner), Scope: advisoryScope(lock.Family), ExpectedRevision: snapshot.Revision, Ranges: ranges}
		call, cancel = context.WithTimeout(active, v.flushTimeout)
		receipt, err := v.fileActionWithin(active, call, storage.OpFileReplaceRanges, func(call context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return h.file.ReplaceRanges(call, request, id)
		})
		cancel()
		if err == nil {
			if receipt.RangeRevision == 0 {
				state.mu.Unlock()
				return h.unknownLock(fmt.Errorf("range action lacks its committed revision: %w", syscall.EIO))
			}
			if capacityDenied {
				state.mu.Unlock()
				return syscall.ENOLCK
			}
			owners.remember(key, len(ranges) != 0)
			if !dropConversion {
				state.pid.Store(lock.PID)
			}
			state.mu.Unlock()
			if dropConversion && lock.Type != unlockType {
				continue
			}
			return errnoOf(h.check())
		}
		state.mu.Unlock()
		if receipt.State != storage.FileActionNotApplied || receipt.Conflict == nil {
			return errnoOf(err)
		}
		switch receipt.Conflict.Kind {
		case storage.ConflictRevision:
			if attempts < advisoryRetries {
				continue
			}
			if !lock.Wait {
				return syscall.EAGAIN
			}
		case storage.ConflictRange:
			if !lock.Wait {
				return syscall.EAGAIN
			}
		case storage.ConflictCapacity:
			return syscall.ENOLCK
		default:
			return errnoOf(err)
		}
		if errno := h.waitAdvisory(active, request, lock.Family == posixFamily); errno != 0 {
			return errno
		}
		attempts = 0
	}
}

func (h *handle) waitAdvisory(ctx context.Context, replace storage.RangeReplaceRequest, detectDeadlock bool) syscall.Errno {
	v := h.node.volume
	request := storage.RangeWaitRequest{Owner: replace.Owner, Scope: replace.Scope, ExpectedRevision: replace.ExpectedRevision, Ranges: replace.Ranges, DetectDeadlock: detectDeadlock}
	call, cancel := context.WithTimeout(ctx, v.flushTimeout)
	receipt, err := v.fileActionWithin(ctx, call, storage.OpFileWaitRanges, func(call context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return h.file.WaitRanges(call, request, id)
	})
	expired := call.Err() != nil
	cancel()
	if receipt.State == storage.FileActionNotApplied && ctx.Err() != nil && (receipt.Errno == syscall.EINTR || receipt.Errno == 0) {
		return errnoOf(ctx.Err())
	}
	if receipt.State == storage.FileActionNotApplied && receipt.Errno == syscall.EAGAIN && receipt.Conflict != nil && receipt.Conflict.Kind == storage.ConflictRevision {
		if ctx.Err() != nil {
			return errnoOf(ctx.Err())
		}
		return errnoOf(h.check())
	}
	if receipt.State == storage.FileActionNotApplied && expired && ctx.Err() == nil &&
		(receipt.Errno == syscall.EINTR || receipt.Errno == 0) && h.check() == nil {
		return 0
	}
	if err == nil && receipt.RangeRevision == 0 {
		return h.unknownLock(fmt.Errorf("range wait lacks its observed revision: %w", syscall.EIO))
	}
	return errnoOf(err)
}

func (h *handle) dropAdvisory(ctx context.Context, owner lockOwner, family lockFamily) error {
	v := h.node.volume
	key := advisoryOwnerKey{node: h.node.id.node, owner: owner, family: family}
	owners := v.localOwners()
	state, release, err := owners.pinExisting(key)
	if err != nil || state == nil {
		return err
	}
	defer release()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.retireGeneration()
	_, err = v.fileAction(ctx, storage.OpFileRetireRanges, func(call context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return h.file.RetireRangeOwner(call, storage.RangeOwnerID(owner), advisoryScope(family), id)
	})
	if err == nil {
		owners.remember(key, false)
	}
	return err
}
