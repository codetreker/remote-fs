package smb

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
)

type rangePendingKey struct{}

type clientRangePlan struct {
	ranges  []storage.RangeAcquisition
	wait    []storage.RangeAcquisition
	applied int
	err     error
}

func rangeRegion(r windowsLockRange) storage.RangeAcquisition {
	v := storage.RangeAcquisition{Start: r.Offset, End: r.Offset, Boundary: r.Length == 0, Exclusive: r.Type == lockExclusive}
	if r.Length != 0 {
		v.End += r.Length - 1
	}
	return v
}

func rangesOverlap(a, b storage.RangeAcquisition) bool {
	if a.Boundary {
		return !b.Boundary && b.Start < a.Start && a.Start <= b.End
	}
	if b.Boundary {
		return a.Start < b.Start && b.Start <= a.End
	}
	return a.Start <= b.End && b.Start <= a.End
}

func planWindowsRanges(snapshot storage.RangeSnapshot, batch windowsLockBatch, next *storage.RangeAcquisitionID) clientRangePlan {
	p := clientRangePlan{ranges: slices.Clone(snapshot.Own)}
	if err := batch.Check(); err != nil {
		p.err = err
		return p
	}
	unlock := batch.Ranges[0].Type == lockUnlock
	for _, item := range batch.Ranges {
		if err := item.Check(); err != nil {
			p.err = err
			return p
		}
		if (item.Type == lockUnlock) != unlock || (!unlock && len(batch.Ranges) > 1 && !item.FailImmediately) {
			p.err = syscall.EINVAL
			return p
		}
		region := rangeRegion(item)
		if unlock {
			index := -1
			for i, held := range p.ranges {
				if held.Start == region.Start && held.End == region.End && held.Boundary == region.Boundary {
					if index < 0 || held.Exclusive {
						index = i
					}
					if held.Exclusive {
						break
					}
				}
			}
			if index < 0 {
				p.err = &windowsError{Failure: windowsRangeNotLocked, Err: syscall.ENOLCK}
				return p
			}
			p.ranges = slices.Delete(p.ranges, index, index+1)
			p.applied++
			continue
		}
		conflict := false
		for _, held := range snapshot.Other {
			if (region.Exclusive || held.Range.Exclusive) && rangesOverlap(region, held.Range) {
				conflict = true
				break
			}
		}
		for _, held := range p.ranges {
			if region.Exclusive && rangesOverlap(region, held) {
				conflict = true
				break
			}
		}
		if conflict && item.FailImmediately {
			p.ranges = slices.Clone(snapshot.Own)
			p.applied = 0
			p.err = &windowsError{Failure: windowsLockConflict, Err: syscall.EAGAIN}
			return p
		}
		if len(p.ranges) >= snapshot.Available || len(p.ranges) >= snapshot.OwnerAvailable || *next == ^storage.RangeAcquisitionID(0) {
			p.err = syscall.ENOLCK
			return p
		}
		*next++
		region.ID = *next
		p.ranges = append(p.ranges, region)
		if conflict {
			p.wait = p.ranges
			p.ranges = slices.Clone(snapshot.Own)
			return p
		}
		p.applied++
	}
	return p
}

type clientRangeRetention struct {
	current  storage.FileActionID
	terminal bool
	expires  time.Time
}

func (a *clientRangeAction) retention() clientRangeRetention {
	if value := a.history.Load(); value != nil {
		return *value
	}
	return clientRangeRetention{}
}

type clientRangeAction struct {
	charge         int64
	submitted      bool
	cancelled      bool
	history        atomic.Pointer[clientRangeRetention]
	expires        time.Time
	applied        int
	finalErr       error
	mu             sync.Mutex
	file           *clientFile
	batch          windowsLockBatch
	current        storage.FileActionID
	wait           *storage.RangeWaitRequest
	plan           clientRangePlan
	guard          uint64
	contentionWait bool
	pendingSent    bool
	finished       bool
	result         windowsActionResult
	err            error
}

func (f *clientFile) LockBatch(ctx context.Context, batch windowsLockBatch, id windowsActionID) (windowsActionResult, error) {
	if err := batch.Check(); err != nil {
		return windowsActionResult{}, err
	}
	if f.access&(windowsReadData|windowsWriteData) == 0 {
		return windowsActionResult{}, syscall.EACCES
	}
	if err := f.authorize(ctx, storage.OpFileRangeSnapshot, 0); err != nil {
		return windowsActionResult{}, err
	}
	if err := f.authorize(ctx, storage.OpFileReplaceRanges, storage.EffectRangesChanged); err != nil {
		return windowsActionResult{}, err
	}
	a := &clientRangeAction{file: f, current: id}
	if err := f.session.resizeRangePlan(a, int64(len(batch.Ranges))*int64(unsafe.Sizeof(windowsLockRange{}))); err != nil {
		return windowsActionResult{}, err
	}
	a.batch = windowsLockBatch{Ranges: slices.Clone(batch.Ranges)}
	a.history.Store(&clientRangeRetention{current: id})
	if err := f.session.remember(id, clientAction{rangeAction: a}); err != nil {
		a.batch.Ranges = nil
		_ = f.session.resizeRangePlan(a, 0)
		return windowsActionResult{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	result, err := a.run(ctx, id, false)
	if err != nil && !a.submitted {
		a.abandon(f.session, id)
	}
	return result, err
}

func (a *clientRangeAction) abandon(s *clientSession, id windowsActionID) {
	a.finished = true
	a.batch.Ranges = nil
	a.plan = clientRangePlan{}
	a.wait = nil
	a.file = nil
	_ = s.resizeRangePlan(a, 0)
	s.forgetRangeAction(id, a)
}

func (s *clientSession) rangeQuery(ctx context.Context, id windowsActionID, a *clientRangeAction) (windowsActionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return s.rangeFinished(ctx, id, a)
	}
	result, err := a.run(ctx, id, true)
	if err != nil && !a.submitted {
		a.abandon(s, id)
	}
	return result, err
}

func (s *clientSession) rangeCancel(ctx context.Context, id windowsActionID, a *clientRangeAction) (windowsActionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finished {
		return s.rangeFinished(ctx, id, a)
	}
	r, err := s.raw.CancelAction(ctx, a.current)
	if r.State == storage.FileActionUnknown || r.State == storage.FileActionPending || r.Action != a.current {
		r, err = s.raw.QueryAction(ctx, a.current)
	}
	if a.wait != nil && rangeTerminal(r, a.current) {
		return a.finish(ctx, id, r, syscall.EINTR, true)
	}
	return a.finish(ctx, id, r, err, false)
}

func (s *clientSession) rangeFinished(ctx context.Context, id windowsActionID, a *clientRangeAction) (windowsActionResult, error) {
	remaining := time.Until(a.expires)
	if remaining > 0 {
		result := a.result
		result.HistoryRemaining = remaining
		result.Receipt.HistoryRemaining = remaining
		return result, a.err
	}
	r, err := s.raw.QueryAction(ctx, a.current)
	if r.Action != a.current {
		return windowsActionResult{}, errors.Join(syscall.EIO, err)
	}
	sidecar := &clientAction{}
	if r.State == storage.FileActionCompleted {
		sidecar.applied = a.applied
		sidecar.finalErr = a.finalErr
	}
	result, err := s.project(ctx, r, err, sidecar)
	result.Action = id
	if a.cancelled && rangeTerminal(r, a.current) && r.State != storage.FileActionRetired {
		result.State = windowsActionCancelled
		result.Errno = syscall.EINTR
		err = syscall.EINTR
	}
	if r.State == storage.FileActionRetired {
		err = errors.Join(err, syscall.ESTALE)
		result.State = windowsActionRejected
		result.Errno = syscall.ESTALE
	}
	return result, err
}

func rangeTerminal(r storage.FileActionReceipt, id storage.FileActionID) bool {
	return r.Action == id && (r.State == storage.FileActionCompleted || r.State == storage.FileActionNotApplied || r.State == storage.FileActionRetired)
}

func rangeRevisionConflict(r storage.FileActionReceipt) bool {
	return r.State == storage.FileActionNotApplied && r.Conflict != nil && r.Conflict.Kind == storage.ConflictRevision
}

func (a *clientRangeAction) finish(ctx context.Context, id windowsActionID, r storage.FileActionReceipt, err error, cancelled bool) (windowsActionResult, error) {
	sidecar := &clientAction{}
	if a.wait == nil && r.State == storage.FileActionCompleted {
		sidecar.applied = a.plan.applied
		sidecar.finalErr = a.plan.err
	}
	result, err := a.file.session.project(ctx, r, err, sidecar)
	result.Action = id
	if r.State == storage.FileActionRetired {
		err = errors.Join(err, syscall.ESTALE)
		result.State = windowsActionRejected
		result.Errno = syscall.ESTALE
	}
	if cancelled {
		result.State = windowsActionCancelled
		result.Errno = syscall.EINTR
	}
	if rangeTerminal(r, a.current) {
		a.cancelled = cancelled
		a.expires = time.Now().Add(r.HistoryRemaining)
		a.applied, a.finalErr = sidecar.applied, sidecar.finalErr
		cached := result
		cached.Attr = windowsAttr{}
		cached.File = nil
		cached.Symlink = nil
		cached.Receipt.Observation = storage.FileObservation{}
		a.finished, a.result, a.err = true, cached, err
		a.batch.Ranges = nil
		a.plan = clientRangePlan{}
		a.wait = nil
		_ = a.file.session.resizeRangePlan(a, 0)
		a.file = nil
		a.history.Store(&clientRangeRetention{current: a.current, terminal: true, expires: a.expires})
	}
	return result, err
}

func (a *clientRangeAction) reconcile(ctx context.Context) (storage.FileActionReceipt, error) {
	s := a.file.session
	r, err := s.raw.QueryAction(ctx, a.current)
	if r.Action == a.current && r.State == storage.FileActionPending && a.wait != nil {
		// A revision wait has no range effects. Cancelling its original action
		// establishes a terminal boundary before a new snapshot is permitted.
		r, err = s.raw.CancelAction(ctx, a.current)
		if !rangeTerminal(r, a.current) {
			r, err = s.raw.QueryAction(ctx, a.current)
		}
	}
	return r, err
}

func (a *clientRangeAction) run(ctx context.Context, original windowsActionID, reconcile bool) (windowsActionResult, error) {
	f := a.file
	const fastAttempts = 8
	attempts := 0
	for {
		var receipt storage.FileActionReceipt
		var err error
		freshInvocation := !reconcile
		if reconcile {
			receipt, err = a.reconcile(ctx)
			reconcile = false
		} else if a.contentionWait {
			a.contentionWait = false
			if authErr := f.authorize(ctx, storage.OpFileWaitRanges, 0); authErr != nil {
				return windowsActionResult{}, authErr
			}
			if !a.pendingSent {
				if pending, ok := ctx.Value(rangePendingKey{}).(func() error); ok {
					if pendingErr := pending(); pendingErr != nil {
						return windowsActionResult{}, pendingErr
					}
				}
				a.pendingSent = true
			}
			a.submitted = true
			receipt, err = f.raw.WaitRanges(ctx, *a.wait, a.current)
		} else {
			if authErr := f.authorize(ctx, storage.OpFileRangeSnapshot, 0); authErr != nil {
				return windowsActionResult{}, authErr
			}
			snapshot, snapshotErr := f.raw.RangeSnapshot(ctx, f.owner, storage.RangeScope{Enforced: true})
			if snapshotErr != nil {
				return windowsActionResult{}, snapshotErr
			}
			if snapshot.Revision == 0 || snapshot.Available < 0 || snapshot.OwnerAvailable < 0 {
				return windowsActionResult{}, syscall.EIO
			}
			charge := int64(len(a.batch.Ranges)) * int64(unsafe.Sizeof(windowsLockRange{}))
			charge += int64(len(snapshot.Own)+3*(len(snapshot.Own)+len(a.batch.Ranges))+cap(a.plan.ranges)+cap(a.plan.wait)) * int64(unsafe.Sizeof(storage.RangeAcquisition{}))
			charge += int64(len(snapshot.Other)) * int64(unsafe.Sizeof(storage.HeldRange{}))
			for _, held := range snapshot.Other {
				charge += int64(len(held.Owner.Session))
			}
			if budgetErr := f.session.resizeRangePlan(a, charge); budgetErr != nil {
				return windowsActionResult{}, budgetErr
			}
			a.guard = snapshot.Revision
			f.rangeMu.Lock()
			for _, held := range snapshot.Own {
				if held.ID > f.nextRangeID {
					f.nextRangeID = held.ID
				}
			}
			a.plan = planWindowsRanges(snapshot, a.batch, &f.nextRangeID)
			f.rangeMu.Unlock()
			a.wait = nil
			if a.plan.wait != nil {
				if authErr := f.authorize(ctx, storage.OpFileWaitRanges, 0); authErr != nil {
					return windowsActionResult{}, authErr
				}
				a.wait = &storage.RangeWaitRequest{Owner: f.owner, Scope: storage.RangeScope{Enforced: true}, ExpectedRevision: snapshot.Revision, Ranges: a.plan.wait}
				if !a.pendingSent {
					if pending, ok := ctx.Value(rangePendingKey{}).(func() error); ok {
						if pendingErr := pending(); pendingErr != nil {
							return windowsActionResult{}, pendingErr
						}
					}
					a.pendingSent = true
				}
				a.submitted = true
				receipt, err = f.raw.WaitRanges(ctx, *a.wait, a.current)
			} else {
				if authErr := f.authorize(ctx, storage.OpFileReplaceRanges, storage.EffectRangesChanged); authErr != nil {
					return windowsActionResult{}, authErr
				}
				a.submitted = true
				receipt, err = f.raw.ReplaceRanges(ctx, storage.RangeReplaceRequest{Owner: f.owner, Scope: storage.RangeScope{Enforced: true}, ExpectedRevision: snapshot.Revision, Ranges: a.plan.ranges}, a.current)
			}
		}
		if freshInvocation && storage.IsFileCallNotAdmitted(err) {
			a.submitted = false
			return rejectedInvocation(original, err)
		}
		if !rangeTerminal(receipt, a.current) {
			check, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.session.backend.limits.CleanupTimeout)
			receipt, err = a.reconcile(check)
			cancel()
			if !rangeTerminal(receipt, a.current) {
				return a.finish(ctx, original, receipt, errors.Join(syscall.EIO, err), false)
			}
		}
		if a.wait == nil && !rangeRevisionConflict(receipt) {
			return a.finish(ctx, original, receipt, err, false)
		}
		if ctx.Err() != nil {
			return a.finish(ctx, original, receipt, syscall.EINTR, true)
		}
		if a.wait != nil && receipt.State != storage.FileActionCompleted && !rangeRevisionConflict(receipt) && receipt.Errno != syscall.EINTR {
			return a.finish(ctx, original, receipt, err, false)
		}
		attempts++
		blocking := len(a.batch.Ranges) == 1 && a.batch.Ranges[0].Type != lockUnlock && !a.batch.Ranges[0].FailImmediately
		if !blocking && attempts >= fastAttempts {
			return a.finish(ctx, original, receipt, syscall.EAGAIN, false)
		}
		if blocking && a.wait == nil && attempts%fastAttempts == 0 {
			a.wait = &storage.RangeWaitRequest{Owner: f.owner, Scope: storage.RangeScope{Enforced: true}, ExpectedRevision: a.guard, Ranges: a.plan.ranges}
			a.contentionWait = true
		}
		next, nextErr := f.session.next(ctx)
		if nextErr != nil {
			return windowsActionResult{}, nextErr
		}
		a.current = next
		a.submitted = false
		a.history.Store(&clientRangeRetention{current: next})
	}
}
